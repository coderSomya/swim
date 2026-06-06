use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum MemberState {
    Alive,
    Suspected { incarnation: u64, since: Instant },
    Confirmed { incarnation: u64 },
}

#[derive(Debug, Clone)]
pub struct Member {
    pub addr: SocketAddr,
    pub incarnation: u64,
    pub state: MemberState,
    pub last_heard: Instant,
}

pub struct MembershipList {
    members: HashMap<SocketAddr, Member>,
    suspect_timeout: Duration,
}

impl MembershipList {
    pub fn new(suspect_timeout: Duration) -> Self {
        Self {
            members: HashMap::new(),
            suspect_timeout,
        }
    }

    pub fn add_member(&mut self, addr: SocketAddr, incarnation: u64) {
        self.members.insert(addr, Member {
            addr,
            incarnation,
            state: MemberState::Alive,
            last_heard: Instant::now(),
        });
    }

    pub fn get_random_alive(&self) -> Option<SocketAddr> {
        let alive: Vec<_> = self.members.iter()
            .filter(|(_, m)| matches!(m.state, MemberState::Alive))
            .map(|(addr, _)| *addr)
            .collect();
        
        if alive.is_empty() {
            None
        } else {
            let idx = (rand::random::<u64>() as usize) % alive.len();
            Some(alive[idx])
        }
    }

    pub fn get_alive_members(&self) -> Vec<SocketAddr> {
        self.members.iter()
            .filter(|(_, m)| matches!(m.state, MemberState::Alive))
            .map(|(addr, _)| *addr)
            .collect()
    }

    pub fn mark_suspected(&mut self, addr: SocketAddr, incarnation: u64) -> bool {
        if let Some(member) = self.members.get_mut(&addr) {
            if incarnation > member.incarnation {
                member.state = MemberState::Suspected {
                    incarnation,
                    since: Instant::now(),
                };
                member.incarnation = incarnation;
                return true;
            }
        }
        false
    }

    pub fn mark_alive(&mut self, addr: SocketAddr, incarnation: u64) -> bool {
        if let Some(member) = self.members.get_mut(&addr) {
            if incarnation >= member.incarnation {
                member.state = MemberState::Alive;
                member.incarnation = incarnation;
                member.last_heard = Instant::now();
                return true;
            }
        }
        false
    }

    pub fn mark_confirmed(&mut self, addr: SocketAddr, incarnation: u64) -> bool {
        if let Some(member) = self.members.get_mut(&addr) {
            if incarnation >= member.incarnation {
                member.state = MemberState::Confirmed { incarnation };
                member.incarnation = incarnation;
                return true;
            }
        }
        false
    }

    pub fn remove_member(&mut self, addr: &SocketAddr) {
        self.members.remove(addr);
    }

    pub fn cleanup_suspected(&mut self) -> Vec<SocketAddr> {
        let mut to_confirm = Vec::new();
        let now = Instant::now();
        
        for (addr, member) in self.members.iter_mut() {
            if let MemberState::Suspected { since, incarnation } = member.state {
                if now.duration_since(since) > self.suspect_timeout {
                    member.state = MemberState::Confirmed { incarnation };
                    to_confirm.push(*addr);
                }
            }
        }
        to_confirm
    }
}
