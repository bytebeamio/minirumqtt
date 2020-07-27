pub mod network;

use argh::FromArgs;
use std::time::{Instant, Duration};
use std::{io, thread};
use tokio::select;
use smol::{self, Async, Task};
use crate::network::{Network, Request, Incoming};
use mqtt4bytes::{Publish, QoS, PubAck, Connect, ConnAck, ConnectReturnCode};
use futures_util::stream::StreamExt;
use std::fs::File;
use pprof::ProfilerGuard;
use prost::Message;
use std::io::Write;
use std::net::{TcpStream, TcpListener, SocketAddrV4, Ipv4Addr};
use bytes::BytesMut;

#[derive(FromArgs, Clone)]
/// Reach new heights.
struct Config {
    /// size of payload
    #[argh(option, short = 'p', default = "100")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1_000_000")]
    count: usize,
    /// number of messages
    #[argh(option, short = 'f', default = "100")]
    flow_control_size: usize,
    /// mode 1 = only server, mode 2 = only client, mode 3 = both
    #[argh(option, short = 'm', default = "3")]
    mode: usize,
}

async fn server() -> Result<(), io::Error> {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1883);
    let listener = Async::<TcpListener>::bind(addr).unwrap();
    let (stream, _) = listener.accept().await?;
    let mut network = Network::new(stream);
    network.read_connect().await?;
    network.connack(ConnAck::new(ConnectReturnCode::Accepted, false)).await?;
    loop {
        let packet = network.readb().await?;
        // dbg!();
        for packet in packet {
            match packet.into() {
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
    smol::Timer::new(Duration::from_secs(1)).await;
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1883);
    let socket = Async::<TcpStream>::connect(addr).await?;

    let mut network =  Network::new(socket);
    network.connect(Connect::new("minirumqtt")).await?;
    network.read_connack().await?;
    // let mut rx = stream::iter(packets(config.payload_size, config.count));
    let (tx, mut rx) = async_channel::bounded(10);
    let config2 = config.clone();
    thread::spawn(move || {
        packetstream(config2.payload_size, config2.count, tx) ;
    });

    let mut acked = 0;
    let mut sent = 0;
    let start = Instant::now();
    let mut blocked_count = 0;
    'main: loop {
        if sent - acked >= config.flow_control_size {
            blocked_count += 1;
        }

        select! {
            // sent - acked guard prevents bounded queue deadlock ( assuming 100 packets doesn't
            // cause framed.send() to block )
            Some(packet) = rx.next(), if sent - acked < config.flow_control_size => {
                network.fill3(packet).unwrap();
                sent += 1;

                for _i in 0..10 {
                    match rx.try_recv() {
                        Ok(packet) => {
                            network.fill3(packet).unwrap();
                            sent += 1;
                        }
                        Err(_) => break
                    }
                }

                network.flush().await.unwrap();
            }
            o = network.read() => {
                let packet = o.unwrap();
                // for packet in packets {
                    match packet.into() {
                        Incoming::Publish(_publish) => (),
                        Incoming::PubAck(_ack) => {
                            acked += 1;
                            if acked >= config.count {
                                break 'main;
                            }
                        },
                        _ => todo!()
                    }
                // }
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (acked as usize * 1000) / elapsed.as_millis() as usize;
    println!("Id =smol, Total = {}, Payload size (bytes) = {}, Flow control window len = {}, Throughput (messages/sec) = {}", acked, config.payload_size, config.flow_control_size, throughput);
    dbg!(blocked_count);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = argh::from_env();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    smol::run(async {
        match config.mode {
            1 => {
                server().await.unwrap();
            }
            2 => {
                client(config).await.unwrap();
            }
            3 => {
                Task::spawn(server()).detach();
                client(config).await.unwrap();
            }
            mode => panic!("Invalid mode = {}", mode)
        }
    });


    profile("/home/tekjar/Downloads/smol.pb", guard);
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

pub fn packetstream(size: usize, count: usize, tx: async_channel::Sender<BytesMut>) {
    for i in 0..count {
        let pkid = (i % 65000) as u16 + 1;
        let payload = vec![i as u8; size];
        let mut out = BytesMut::with_capacity(5 + 11 + payload.len());
        let mut publish = Publish::new("hello/world", QoS::AtLeastOnce, payload);
        publish.set_pkid(pkid);
        publish.write(&mut out).unwrap();
        blocking::block_on(tx.send(out)).unwrap();
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