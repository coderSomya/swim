pub struct FailureDetector {
    ping_timeout: Duration,
    pingreq_timeout: Duration,
    indirect_count: usize,
}

// pseudo for now

pub async fn protocol_period() {
    let target = membership.random_member();

    send_ping(target);

    if ack_received(target) {
        return;
    }

    let helpers =
        membership.random_k_members(k, target);

    for helper in helpers {
        send_pingreq(helper, target);
    }

    if ack_received(target) {
        return;
    }

    suspect(target);
}
