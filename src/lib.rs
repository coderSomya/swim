pub type NodeId = u64;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
}

impl Swim {
    pub async fn join(
        &self,
        seed: SocketAddr,
    );

    pub async fn leave(
        &self,
    );

    pub async fn members(
        &self,
    ) -> Vec<Member>;
}
