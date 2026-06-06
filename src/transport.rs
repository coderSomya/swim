use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum SwimMessage {
    Ping { from: SocketAddr, seq: u64 },
    Ack { to: SocketAddr, seq: u64 },
    PingReq { from: SocketAddr, target: SocketAddr, seq: u64 },
    PingReqAck { from: SocketAddr, original_target: SocketAddr, seq: u64 },
}

pub struct NetworkLayer {
    socket: Arc<UdpSocket>,
    pending_acks: Arc<Mutex<HashMap<(SocketAddr, u64), tokio::sync::oneshot::Sender<()>>>>,
}

impl NetworkLayer {
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let pending_acks = Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn receiver task
        let layer = Self {
            socket: socket.clone(),
            pending_acks: pending_acks.clone(),
        };
        
        tokio::spawn(Self::receiver_task(socket, pending_acks));
        
        Ok(layer)
    }
    
    async fn receiver_task(
        socket: Arc<UdpSocket>,
        pending_acks: Arc<Mutex<HashMap<(SocketAddr, u64), tokio::sync::oneshot::Sender<()>>>>,
    ) {
        let mut buf = [0u8; 1024];
        
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, _src)) => {
                    if let Ok(message) = bincode::deserialize(&buf[..size]) {
                        match message {
                            SwimMessage::Ack { to, seq } => {
                                let key = (to, seq);
                                let mut pending = pending_acks.lock().await;
                                if let Some(sender) = pending.remove(&key) {
                                    let _ = sender.send(());
                                }
                            }
                            SwimMessage::Ping { from, seq } => {
                                // Respond with ack
                                let ack = SwimMessage::Ack { to: from, seq };
                                if let Ok(data) = bincode::serialize(&ack) {
                                    let _ = socket.send_to(&data, from).await;
                                }
                            }
                            // Handle other message types...
                            _ => {}
                        }
                    }
                }
                Err(e) => eprintln!("Failed to receive: {}", e),
            }
        }
    }
    
    pub async fn send_ping(&self, target: SocketAddr) -> std::io::Result<()> {
        let seq = rand::random::<u64>();
        let msg = SwimMessage::Ping { from: self.socket.local_addr()?, seq };
        let data = bincode::serialize(&msg).unwrap();
        self.socket.send_to(&data, target).await?;
        Ok(())
    }
    
    pub async fn wait_for_ack(&self, from: SocketAddr) -> Result<(), ()> {
        let seq = 0; // TODO need to track sequence numbers properly
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert((from, seq), tx);
        }
        
        rx.await.map_err(|_| ())
    }
    
    pub async fn send_ping_req(&self, helper: SocketAddr, target: SocketAddr, timeout_dur: Duration) -> Result<bool, ()> {
        let seq = rand::random::<u64>();
        let msg = SwimMessage::PingReq {
            from: self.socket.local_addr().map_err(|_| ())?,
            target,
            seq,
        };
        
        let data = bincode::serialize(&msg).unwrap();
        self.socket.send_to(&data, helper).await.map_err(|_| ())?;
        
        // Wait for ping-req-ack
        match timeout(timeout_dur, self.wait_for_ack(target)).await {
            Ok(Ok(())) => Ok(true),
            _ => Ok(false),
        }
    }
}
