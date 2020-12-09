#[macro_use]
extern crate log;

mod network;
mod state;

use std::time::{Instant, Duration};
use std::fs::File;
use std::io::Write;

use argh::FromArgs;
use tokio::{select, task};
use tokio::net::{TcpStream, TcpListener};
use pprof::ProfilerGuard;
use prost::Message;

use network::*;
use state::*;
use mqtt4bytes::*;

#[derive(FromArgs, Clone)]
/// Reach new heights.
struct Config {
    /// size of payload
    #[argh(option, short = 'p', default = "100")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1000000")]
    count: usize,
    /// number of messages
    #[argh(option, short = 'f', default = "100")]
    flow_control_size: u16,
    /// qos
    #[argh(option, short = 'q', default = "1")]
    qos: u8,
    /// mode 1 = only server, mode 2 = only client, mode 3 = both
    #[argh(option, short = 'm', default = "1")]
    mode: usize,
}

async fn server() {
    let listener = TcpListener::bind("127.0.0.1:1883").await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mut network = Network::new(stream, 100 * 1024);
    let mut state = MqttState::new(100);

    match network.read().await.unwrap() {
        Incoming::Connect(_) => (),
        v => unimplemented!("{:?}", v)
    }
    network.connack(ConnAck::new(ConnectReturnCode::Accepted, false)).await.unwrap();

    loop {
        network.readb(&mut state).await.unwrap();
        network.flush(&mut state.write).await.unwrap();
    }
}

async fn client(config: Config) {
    tokio::time::sleep(Duration::from_millis(1)).await;
    let socket = TcpStream::connect("127.0.0.1:1883").await.unwrap();
    let mut network =  Network::new(socket, 100 * 1024);
    let mut state = MqttState::new(config.flow_control_size);
    let mut requests = Requests::new(config.count, config.payload_size, config.qos);
    let mut acked = 0;
    let mut expected = config.count;

    if config.qos == 0 {
        expected = 1;
    }

    let start = Instant::now();
    network.connect(Connect::new("minirumqtt")).await.unwrap();
    match network.read().await.unwrap() {
        Incoming::ConnAck(_) => (),
        v => unimplemented!("{:?}", v)
    }

    'main: loop {
        let inflight_full = state.inflight >= config.flow_control_size;
        let collision = state.collision.is_some();

        // Read buffered events from previous polls before calling a new poll
        while let Some(Event::Incoming(packet)) = state.events.pop_front() {
            match packet {
                Incoming::PubAck(_ack) => {
                    acked += 1;
                    if acked >= expected {
                        break 'main;
                    }
                },
                _ => todo!()
            }
        }

        // this loop is necessary since self.incoming.pop_front() might return None. In that case,
        // instead of returning a None event, we try again.
        select! {
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(&mut state) => {
                o.unwrap();
                // flush all the acks and return first incoming packet
                network.flush(&mut state.write).await.unwrap();
            },
            Some(publish) = requests.next(), if !inflight_full && !collision => {
                state.handle_outgoing_packet(publish).unwrap();
                network.flush(&mut state.write).await.unwrap();
            },
        }
    }

    // Assume all the messages have reached broker
    if config.qos == 0 {
        acked = config.count;
    }
    let elapsed = start.elapsed();
    let throughput = (acked as usize * 1000) / elapsed.as_millis() as usize;

    println!("Id = tokio, Total = {}, Payload size (bytes) = {}, Flow control window len = {}, Throughput (messages/sec) = {}", acked, config.payload_size, config.flow_control_size, throughput);
}

#[tokio::main(worker_threads = 2)]
async fn main() {
    pretty_env_logger::init();

    let config: Config = argh::from_env();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    match config.mode {
        1 => {
            server().await;
        }
        2 => {
            client(config).await;
        }
        3 => {
            task::spawn(server());
            client(config).await;
        }
        mode => panic!("Invalid mode = {}", mode)
    }

    profile("base.pb", guard);
}

struct Requests {
    current_count: usize,
    max_count: usize,
    qos: u8,
    size: usize
}

impl Requests {
    pub fn new(max_count: usize, size: usize, qos: u8) -> Requests {
        Requests {
            current_count: 0,
            max_count,
            qos,
            size
        }
    }

    pub async fn next(&mut self) -> Option<Packet> {
        if self.current_count < self.max_count {
            let payload = vec![1 as u8; self.size];
            let publish = Publish::new("hello/world", qos(self.qos).unwrap(), payload);
            let publish = Packet::Publish(publish);
            self.current_count += 1;
            return Some(publish)
        }

        // Send one more packet as sync marker (assumes broker is ordered)
        if self.current_count == self.max_count && self.qos == 0 {
            let payload = vec![1 as u8; self.size];
            let publish = Publish::new("hello/world", QoS::AtLeastOnce, payload);
            let publish = Packet::Publish(publish);
            self.current_count += 1;
            return Some(publish)
        }

        None
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