#[async_trait]
pub trait Transport {
    async fn send(
        &self,
        addr: SocketAddr,
        msg: Message,
    );

    async fn recv(
        &self,
    ) -> (SocketAddr, Message);
}

pub struct UdpTransport
// will probably use tokio::net::UdpSocket
