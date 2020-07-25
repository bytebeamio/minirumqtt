pub mod network;

use argh::FromArgs;
use std::time::{Instant, Duration};
use std::{io, thread};
use tokio::select;
use tokio::task;
use tokio::stream;
use tokio::stream::StreamExt;
use tokio::net::{TcpStream, TcpListener};
use crate::network::{Network, Request, Incoming};
use mqtt4bytes::{Publish, QoS, PubAck, Connect, ConnAck, ConnectReturnCode};
use std::fs::File;
use pprof::ProfilerGuard;
use prost::Message;
use std::io::Write;

#[derive(FromArgs, Clone)]
/// Reach new heights.
struct Config {
    /// size of payload
    #[argh(option, short = 'p', default = "16")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1_000_000")]
    count: usize,
    /// number of messages
    #[argh(option, short = 'f', default = "100")]
    flow_control_size: usize,
    /// mode 1 = only server, mode 2 = only client, mode 3 = both
    #[argh(option, short = 'm', default = "1")]
    mode: usize,
}

async fn server() -> Result<(), io::Error> {
    let mut listener = TcpListener::bind("127.0.0.1:1883").await?;
    let (stream, _) = listener.accept().await?;
    let mut network = Network::new(stream);
    network.read_connect().await?;
    network.connack(ConnAck::new(ConnectReturnCode::Accepted, false)).await?;
    loop {
        let packets = network.readb().await?;
        for packet in packets {
            match packet {
                Incoming::Publish(publish) => {
                    if publish.pkid > 0 {
                        let request = Request::PubAck(PubAck::new(publish.pkid));
                        network.fill2(request)?;
                    }
                }
                Incoming::PingReq => {
                    let request = Request::PingResp;
                    network.fill2(request)?;
                }
                _ => todo!()
            };
        }

        network.flush().await?;
    }

}

async fn client(config: Config) -> Result<(), io::Error> {
    tokio::time::delay_for(Duration::from_millis(1)).await;
    let socket = TcpStream::connect("127.0.0.1:1883").await.unwrap();
    let mut network =  Network::new(socket);
    network.connect(Connect::new("minirumqtt")).await?;
    network.read_connack().await?;
    let mut rx = stream::iter(packets(config.payload_size, config.count));
    // let (tx, mut rx) = async_channel::bounded(100);
    // let config2 = config.clone();
    // thread::spawn(move || {
    //     packetstream(config2.payload_size, config2.count, tx) ;
    // });

    let mut acked = 0;
    let mut sent = 0;
    let start = Instant::now();
    'main: loop {
        select! {
            // sent - acked guard prevents bounded queue deadlock ( assuming 100 packets doesn't
            // cause framed.send() to block )
            Some(packet) = rx.next(), if sent - acked < config.flow_control_size => {
                network.fill2(packet).unwrap();
                network.flush().await.unwrap();
                sent += 1;
            }
            o = network.readb() => {
                let packets = o.unwrap();
                for packet in packets {
                    match packet {
                        Incoming::Publish(_publish) => (),
                        Incoming::PubAck(_ack) => {
                            acked += 1;
                            if acked >= config.count {
                                break 'main;
                            }
                        },
                        _ => todo!()
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (acked as usize * 1000) / elapsed.as_millis() as usize;
    println!("Id = tokio, Total = {}, Payload size (bytes) = {}, Flow control window len = {}, Throughput (messages/sec) = {}", acked, config.payload_size, config.flow_control_size, throughput);
    Ok(())
}

#[tokio::main(core_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = argh::from_env();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    match config.mode {
        1 => {
            server().await?;
        }
        2 => {
            client(config).await?;
        }
        3 => {
            task::spawn(server());
            client(config).await?;
        }
        mode => panic!("Invalid mode = {}", mode)
    }

    profile("/home/tekjar/Downloads/profile.pb", guard);
    Ok(())
}


pub fn packets(size: usize, count: usize) -> Vec<Request> {
    let mut out = Vec::new();
    for i in 0..count {
        let pkid = (i % 65000) as u16 + 1;
        let payload = vec![i as u8; size];
        let mut publish = Publish::new("hello/world", QoS::AtLeastOnce, payload);
        publish.set_pkid(pkid);
        out.push(Request::Publish(publish))
    }

    out
}

pub fn packetstream(size: usize, count: usize, tx: async_channel::Sender<Request>) {
    for i in 0..count {
        let pkid = (i % 65000) as u16 + 1;
        let payload = vec![i as u8; size];
        let mut publish = Publish::new("hello/world", QoS::AtLeastOnce, payload);
        publish.set_pkid(pkid);
        blocking::block_on(tx.send(Request::Publish(publish))).unwrap();
    }
}

fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}