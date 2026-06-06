use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::membership::MembershipList;
use crate::transport::NetworkLayer;

#[derive(Debug, Clone)]
pub enum MembershipUpdate {
    Suspect { addr: SocketAddr, incarnation: u64 },
    Alive { addr: SocketAddr, incarnation: u64 },
    Confirm { addr: SocketAddr, incarnation: u64 },
    Join { addr: SocketAddr },
    Leave { addr: SocketAddr },
}

pub struct DisseminationComponent {
    membership: Arc<Mutex<MembershipList>>,
    network: Arc<NetworkLayer>,
    pending_updates: Arc<Mutex<VecDeque<(MembershipUpdate, usize)>>>,
    gossip_limit: usize,  // Number of times to gossip each update (lambda * log n)
}

impl DisseminationComponent {
    pub fn new(
        membership: Arc<Mutex<MembershipList>>,
        network: Arc<NetworkLayer>,
        gossip_limit: usize,
    ) -> Self {
        Self {
            membership,
            network,
            pending_updates: Arc::new(Mutex::new(VecDeque::new())),
            gossip_limit,
        }
    }

    pub async fn disseminate_suspect(&self, addr: SocketAddr, incarnation: u64) {
        let update = MembershipUpdate::Suspect { addr, incarnation };
        self.add_pending_update(update).await;
    }

    pub async fn disseminate_alive(&self, addr: SocketAddr, incarnation: u64) {
        let update = MembershipUpdate::Alive { addr, incarnation };
        self.add_pending_update(update).await;
    }

    pub async fn disseminate_confirm(&self, addr: SocketAddr, incarnation: u64) {
        let update = MembershipUpdate::Confirm { addr, incarnation };
        self.add_pending_update(update).await;
    }

    async fn add_pending_update(&self, update: MembershipUpdate) {
        let mut pending = self.pending_updates.lock().await;
        pending.push_back((update, 0));
    }

    pub async fn get_updates_to_piggyback(&self) -> Vec<MembershipUpdate> {
        let mut pending = self.pending_updates.lock().await;
        let mut updates = Vec::new();
        
        // Take up to 6 updates (max packet size ~135B)
        while let Some((update, _count)) = pending.pop_front() {
            updates.push(update);
            if updates.len() >= 6 {
                break;
            }
        }
        
        updates
    }

    pub async fn handle_received_update(&self, update: MembershipUpdate, from: SocketAddr) {
        let mut membership = self.membership.lock().await;
        
        match update {
            MembershipUpdate::Suspect { addr, incarnation } => {
                membership.mark_suspected(addr, incarnation);
                // Re-gossip if we haven't exceeded limit
                self.regossip_update(update).await;
            }
            MembershipUpdate::Alive { addr, incarnation } => {
                membership.mark_alive(addr, incarnation);
                self.regossip_update(update).await;
            }
            MembershipUpdate::Confirm { addr, incarnation } => {
                membership.mark_confirmed(addr, incarnation);
                membership.remove_member(&addr);
                self.regossip_update(update).await;
            }
            MembershipUpdate::Join { addr } => {
                membership.add_member(addr, 0);
                self.regossip_update(update).await;
            }
            MembershipUpdate::Leave { addr } => {
                membership.remove_member(&addr);
                self.regossip_update(update).await;
            }
        }
    }

    async fn regossip_update(&self, update: MembershipUpdate) {
        let mut pending = self.pending_updates.lock().await;
        let count = pending.iter()
            .find(|(u, _)| self.updates_equal(u, &update))
            .map(|(_, c)| *c)
            .unwrap_or(0);
        
        if count < self.gossip_limit {
            pending.push_back((update, count + 1));
        }
    }

    fn updates_equal(&self, u1: &MembershipUpdate, u2: &MembershipUpdate) -> bool {
        match (u1, u2) {
            (MembershipUpdate::Suspect { addr: a1, incarnation: i1 },
             MembershipUpdate::Suspect { addr: a2, incarnation: i2 }) => a1 == a2 && i1 == i2,
            (MembershipUpdate::Alive { addr: a1, incarnation: i1 },
             MembershipUpdate::Alive { addr: a2, incarnation: i2 }) => a1 == a2 && i1 == i2,
            _ => false,
        }
    }
}
