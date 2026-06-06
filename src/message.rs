#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Ping {
        from: NodeId,
    },

    Ack {
        from: NodeId,
    },

    PingReq {
        target: NodeId,
        requester: NodeId,
    },

    Alive {
        node: NodeId,
        incarnation: u64,
    },

    Suspect {
        node: NodeId,
        incarnation: u64,
    },

    Dead {
        node: NodeId,
        incarnation: u64,
    },
}

