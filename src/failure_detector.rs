use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use crate::membership::MembershipList;
use crate::transport::NetworkLayer;
use crate::disseminator::DisseminationComponent;

pub struct FailureDetector {
    membership: Arc<Mutex<MembershipList>>,
    network: Arc<NetworkLayer>,
    dissemination: Arc<DisseminationComponent>,
    protocol_period: Duration,
    timeout: Duration,
    k: usize,  // Indirect probe count
}

impl FailureDetector {
    pub fn new(
        membership: Arc<Mutex<MembershipList>>,
        network: Arc<NetworkLayer>,
        dissemination: Arc<DisseminationComponent>,
        protocol_period: Duration,
        timeout: Duration,
        k: usize,
    ) -> Self {
        Self {
            membership,
            network,
            dissemination,
            protocol_period,
            timeout,
            k,
        }
    }

    pub async fn start(&self) {
        let mut interval = time::interval(self.protocol_period);
        
        loop {
            interval.tick().await;
            self.run_protocol_period().await;
        }
    }

    async fn run_protocol_period(&self) {
        // Select random target
        let target = {
            let membership = self.membership.lock().await;
            membership.get_random_alive()
        };

        if let Some(target_addr) = target {
            self.probe_member(target_addr).await;
        }
    }

    async fn probe_member(&self, target: SocketAddr) {
        // Send direct ping
        let _start = std::time::Instant::now();
        
        match self.network.send_ping(target).await {
            Ok(()) => {
                // Wait for ack with timeout
                match tokio::time::timeout(self.timeout, self.network.wait_for_ack(target)).await {
                    Ok(Ok(())) => {
                        // Success! Mark as alive
                        let mut membership = self.membership.lock().await;
                        membership.mark_alive(target, 0);
                    }
                    _ => {
                        // No ack, use indirect probing
                        self.indirect_probe(target).await;
                    }
                }
            }
            Err(_) => {
                self.indirect_probe(target).await;
            }
        }
    }

    async fn indirect_probe(&self, target: SocketAddr) {
        let ping_req_nodes = self.get_random_members(self.k).await;
        
        let mut responses = Vec::new();
        for node in ping_req_nodes {
            if let Ok(ack_received) = self.network.send_ping_req(node, target, self.timeout).await {
                if ack_received {
                    responses.push(node);
                }
            }
        }
        
        if responses.is_empty() {
            // No responses - mark as suspected
            let mut membership = self.membership.lock().await;
            if membership.mark_suspected(target, 1) {
                let _ = self.dissemination.disseminate_suspect(target, 1).await;
            }
        } else {
            // Got indirect ack, mark alive
            let mut membership = self.membership.lock().await;
            membership.mark_alive(target, 0);
        }
    }

    async fn get_random_members(&self, count: usize) -> Vec<SocketAddr> {
        let membership = self.membership.lock().await;
        let alive_members: Vec<_> = membership.get_alive_members();
        
        let mut result = Vec::new();
        let n = alive_members.len();
        
        while result.len() < count.min(n) {
            let idx = (rand::random::<u64>() as usize) % n;
            let candidate = alive_members[idx];
            if !result.contains(&candidate) {
                result.push(candidate);
            }
        }
        result
    }
}
