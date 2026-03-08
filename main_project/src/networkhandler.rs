use network_rust::udpnet;
use std::net;
use crossbeam_channel as cbc;
use tokio::sync::broadcast;
use crate::config::MSG_PORT;
use crate::types::*;
use std::collections::HashMap;


impl Heartbeat {

    pub async fn new() -> Self {
        let local_ip = net::TcpStream::connect("8.8.8.8:53")
            .unwrap()
            .local_addr()
            .unwrap()
            .ip();
        println!("local ip {}", local_ip);

        let (tx_broadcast, tx_udp) = Self::start_channels().await;
        let heartbeatmsg = HeartbeatMSG {
            id: local_ip.to_string(),
            external_orders: Vec::new(),
            internal_orders: Vec::new(),
            floor: 0,
            direction: 0,
            status: Behaviour::Idle,
            counter: 0,
            role: Roles::Slave,
            assignments: HashMap::new(),
            cleared_order: None,
        };

        Self {
            msg: heartbeatmsg,
            tx_broadcast,
            tx_udp,
        }
    }


    pub async fn start_channels() -> (broadcast::Sender<HeartbeatMSG>, cbc::Sender<HeartbeatMSG>) {
        let (crossbeam_tx, crossbeam_tx_rx) = cbc::unbounded::<HeartbeatMSG>();
        let (crossbeam_rx_tx, crossbeam_rx) = cbc::unbounded::<HeartbeatMSG>();

        std::thread::spawn(move || {
            println!("[UDP] Starting broadcast RX on port {}", MSG_PORT);
            match udpnet::bcast::rx(MSG_PORT, crossbeam_rx_tx) {
                Ok(_) => println!("[UDP] RX completed"),
                Err(e) => eprintln!("[UDP] RX failed: {:?}", e),
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        std::thread::spawn(move || {
            println!("[UDP] Starting broadcast TX on port {}", MSG_PORT);
            match udpnet::bcast::tx(MSG_PORT, crossbeam_tx_rx) {
                Ok(_) => println!("[UDP] TX completed"),
                Err(e) => eprintln!("[UDP] TX failed: {:?}", e),
            }
        });

        let (bcast_tx, _) = broadcast::channel::<HeartbeatMSG>(512);

        let bcast_tx_relay = bcast_tx.clone();
        std::thread::spawn(move || {
            loop {
                match crossbeam_rx.recv() {
                    Ok(msg) => { let _ = bcast_tx_relay.send(msg); }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        println!("Network channels initialized");

        (bcast_tx, crossbeam_tx)
    }


    pub async fn network_controller(&self) {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        self.tx_udp.send(self.msg.clone()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }


    pub async fn collect_gossip_heartbeats(&self) -> Vec<HeartbeatMSG> {
        let mut heartbeats: std::collections::HashMap<String, HeartbeatMSG> = std::collections::HashMap::new();
        let mut rx = self.tx_broadcast.subscribe();
        
        let timeout_duration = std::time::Duration::from_millis(50);
        let start = std::time::Instant::now();
        
        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(
                std::time::Duration::from_millis(10),
                rx.recv()
            ).await {
                Ok(Ok(msg)) => {
                    if msg.id == self.msg.id {
                        continue;
                    }
                    
                    
                    if let Some(existing) = heartbeats.get(&msg.id) {
                        if msg.counter > existing.counter {
                            heartbeats.insert(msg.id.clone(), msg);
                        }
                    } else {
                        heartbeats.insert(msg.id.clone(), msg);
                    }
                }
                _ => {
                    continue;
                }
            }
        }
        
        heartbeats.into_values().collect()
    }

    pub fn id(&self) -> &str {
        &self.msg.id
    }

    pub fn order_completed(&mut self, order: Order) {
        self.msg.external_orders.retain(|o| o != &order);
        self.msg.internal_orders.retain(|o| o != &order);
        self.msg.cleared_order = Some(order);
        self.msg.counter += 1;
    }
}