use super::sample::{
    build_sample_query, decode_sample_response, random_transaction_id, response_matches_transaction,
    SampleInfohashesResponse,
};
use anyhow::Result;
use mainline::Id;
use rand::Rng;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::time::timeout;

pub async fn sample_infohashes_timeout(
    socket: &tokio::net::UdpSocket,
    node: SocketAddr,
    target: Id,
    sample_timeout: Duration,
) -> Result<SampleInfohashesResponse> {
    let node_id = random_node_id();
    let transaction_id = random_transaction_id();
    let payload = build_sample_query(node_id, target, transaction_id)?;
    socket.send_to(&payload, node).await?;

    let mut buf = vec![0u8; 65536];
    let started = Instant::now();
    while started.elapsed() < sample_timeout {
        let remaining = sample_timeout.saturating_sub(started.elapsed());
        let (len, _) = match timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok(packet)) => packet,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => break,
        };
        if !response_matches_transaction(&buf[..len], &transaction_id) {
            continue;
        }
        if let Ok(response) = decode_sample_response(&buf[..len]) {
            return Ok(response);
        }
    }
    anyhow::bail!("sample_infohashes timeout waiting for response from {node}")
}

fn random_node_id() -> [u8; 20] {
    let mut bytes = [0u8; 20];
    rand::thread_rng().fill(&mut bytes);
    bytes
}
