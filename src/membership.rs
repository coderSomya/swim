#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Alive,
    Suspect,
    Dead,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub node: Node,
    pub incarnation: u64,
    pub status: Status,
}

pub struct Membership {
    members: HashMap<NodeId, Member>,
}

impl Membership {
    pub fn alive(&mut self, node: NodeId, inc: u64);

    pub fn suspect(&mut self, node: NodeId, inc: u64);

    pub fn dead(&mut self, node: NodeId, inc: u64);

    pub fn random_member(&self) -> Option<Node>;

    pub fn random_k_members(
        &self,
        k: usize,
        exclude: NodeId,
    ) -> Vec<Node>;
}
