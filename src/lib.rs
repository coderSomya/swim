mod disseminator;
mod failure_detector;
mod membership;
mod transport;

mod tests {
    use crate::membership;
    use crate::disseminator;
    use crate::failure_detector;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[test]
    async fn basics() {


        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Initialize components
        let membership = Arc::new(Mutex::new(membership::MembershipList::new(
            Duration::from_secs(6), // 3*log(n) seconds
        )));

        let network = Arc::new(transport::NetworkLayer::bind(addr).await?);
        let dissemination = Arc::new(disseminator::DisseminationComponent::new(
            membership.clone(),
            network.clone(),
            3, // gossip_limit = lambda * log(n), lambda=3
        ));

        let failure_detector = failure_detector::FailureDetector::new(
            membership.clone(),
            network.clone(),
            dissemination.clone(),
            Duration::from_secs(2),     // Protocol period
            Duration::from_millis(500), // Timeout
            1,                          // k = 1 as per paper's experiments
        );

        // Start the failure detector
        tokio::spawn(async move {
            failure_detector.start().await;
        });

        // Main loop - handle updates from dissemination component
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            // Check for and process any pending updates
            let updates = dissemination.get_updates_to_piggyback().await;
            if !updates.is_empty() {
                println!("Piggybacking {} updates", updates.len());
            }
        }
    }
}
