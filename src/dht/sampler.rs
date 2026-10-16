use super::krpc::{sample_infohashes_timeout, SampleInfohashesResponse};
use super::sample_store::SampleStore;
use crate::infra::UdpPool;
use crate::{DiscoveredHash, DiscoverySource, InfoHash};
use anyhow::Result;
use std::collections::HashSet;
use mainline::async_dht::AsyncDht;
use mainline::Id;
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub async fn run_bep51_sampler(
    dht: Arc<AsyncDht>,
    discovery_tx: mpsc::Sender<DiscoveredHash>,
    sample_store: Arc<Mutex<SampleStore>>,
    udp: UdpPool,
    interval_secs: u64,
    batch: usize,
    concurrency: usize,
    sample_timeout_secs: u64,
    cancel: CancellationToken,
) {
    let mut target = Id::random();
    let mut node_cooldown: HashMap<SocketAddrV4, Instant> = HashMap::new();
    let concurrency = concurrency.max(1);
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let nodes = dht.find_node(target).await;
        let now = Instant::now();
        let mut eligible = Vec::new();
        for node in nodes.iter().take(batch) {
            let addr = SocketAddrV4::from(node.address());
            if let Some(until) = node_cooldown.get(&addr) {
                if *until > now {
                    continue;
                }
            }
            eligible.push(addr);
        }

        let mut set = tokio::task::JoinSet::new();
        for addr in eligible {
            while set.len() >= concurrency {
                if let Some(join_result) = set.join_next().await {
                    if let Ok(sample_result) = join_result {
                        apply_sample_result(
                            sample_result,
                            &discovery_tx,
                            &sample_store,
                            &mut target,
                            &mut node_cooldown,
                            interval_secs,
                        )
                        .await;
                    }
                }
            }
            let udp = udp.clone();
            let sample_timeout = Duration::from_secs(sample_timeout_secs.max(1));
            set.spawn(async move {
                sample_from_node(
                    SocketAddr::V4(addr),
                    target,
                    sample_timeout,
                    &udp,
                )
                .await
                .map(|response| (addr, response))
            });
        }
        while let Some(join_result) = set.join_next().await {
            if let Ok(sample_result) = join_result {
                apply_sample_result(
                    sample_result,
                    &discovery_tx,
                    &sample_store,
                    &mut target,
                    &mut node_cooldown,
                    interval_secs,
                )
                .await;
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
    }
}

async fn sample_from_node(
    addr: SocketAddr,
    target: Id,
    sample_timeout: Duration,
    udp: &UdpPool,
) -> Result<SampleInfohashesResponse, anyhow::Error> {
    let lease = udp.acquire().await?;
    sample_infohashes_timeout(&lease.socket, addr, target, sample_timeout).await
}

async fn apply_sample_result(
    result: Result<(SocketAddrV4, SampleInfohashesResponse), anyhow::Error>,
    discovery_tx: &mpsc::Sender<DiscoveredHash>,
    sample_store: &Arc<Mutex<SampleStore>>,
    target: &mut Id,
    node_cooldown: &mut HashMap<SocketAddrV4, Instant>,
    interval_secs: u64,
) {
    match result {
        Ok((addr, response)) => {
            let sample_count = response.samples.len();
            for hash in response.samples {
                if let Ok(mut store) = sample_store.lock() {
                    store.record(hash);
                }
                if discovery_tx
                    .try_send(DiscoveredHash {
                        info_hash: hash,
                        peer: None,
                        source: DiscoverySource::Bep51,
                    })
                    .is_err()
                {
                    metrics::counter!("discovery_dropped_total").increment(1);
                    return;
                }
            }
            *target = advance_target(*target);
            let cooldown = interval_secs.max(response.interval);
            node_cooldown.insert(addr, Instant::now() + Duration::from_secs(cooldown));
            metrics::counter!("bep51_samples_total").increment(sample_count as u64);
            if sample_count > 0 {
                debug!(samples = sample_count, node = %addr, num = response.num, "bep51 sample");
            }
        }
        Err(error) => {
            debug!(%error, "bep51 sample failed");
        }
    }
}

const LIVE_SAMPLE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
const LIVE_SAMPLE_NODES_PER_TARGET: usize = 32;
const LIVE_SAMPLE_CONCURRENCY: usize = 8;
const LIVE_SAMPLE_WARMUP_ROUNDS: usize = 20;

pub async fn discover_sample_hashes(
    dht: &AsyncDht,
    overall_timeout: Duration,
    min_count: usize,
) -> Result<Vec<InfoHash>> {
    use std::collections::HashSet;

    let deadline = Instant::now() + overall_timeout;
    let mut target = Id::random();
    let mut collected = Vec::new();
    let mut seen = HashSet::new();

    for _ in 0..LIVE_SAMPLE_WARMUP_ROUNDS {
        if Instant::now() >= deadline {
            break;
        }
        let _ = dht.find_node(Id::random()).await;
    }

    while Instant::now() < deadline {
        let nodes = dht.find_node(target).await;
        let mut set = tokio::task::JoinSet::new();
        for node in nodes.iter().take(LIVE_SAMPLE_NODES_PER_TARGET) {
            if Instant::now() >= deadline {
                break;
            }
            let addr = SocketAddr::V4(node.address());
            let remaining = deadline.saturating_duration_since(Instant::now());
            let attempt_timeout = remaining.min(LIVE_SAMPLE_ATTEMPT_TIMEOUT);
            if attempt_timeout.is_zero() {
                break;
            }
            while set.len() >= LIVE_SAMPLE_CONCURRENCY {
                if let Some(join_result) = set.join_next().await {
                    if let Ok(sample_result) = join_result {
                        merge_samples(sample_result, &mut collected, &mut seen, min_count)?;
                    }
                }
            }
            let sample_target = target;
            set.spawn(async move {
                let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
                sample_infohashes_timeout(&socket, addr, sample_target, attempt_timeout).await
            });
        }
        while let Some(join_result) = set.join_next().await {
            if let Ok(sample_result) = join_result {
                merge_samples(sample_result, &mut collected, &mut seen, min_count)?;
            }
        }
        if collected.len() >= min_count {
            return Ok(collected);
        }
        target = advance_target(target);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!(
        "no bep51 samples within {:?} (collected {})",
        overall_timeout,
        collected.len()
    )
}

fn merge_samples(
    sample_result: Result<SampleInfohashesResponse, anyhow::Error>,
    collected: &mut Vec<InfoHash>,
    seen: &mut HashSet<InfoHash>,
    min_count: usize,
) -> Result<()> {
    match sample_result {
        Ok(response) if !response.samples.is_empty() => {
            for hash in response.samples {
                if seen.insert(hash) {
                    collected.push(hash);
                }
            }
            if collected.len() >= min_count {
                return Ok(());
            }
        }
        Ok(_) | Err(_) => {}
    }
    Ok(())
}

fn advance_target(id: Id) -> Id {
    let mut bytes = *id.as_bytes();
    for index in (0..bytes.len()).rev() {
        bytes[index] = bytes[index].wrapping_add(1);
        if bytes[index] != 0 {
            break;
        }
    }
    Id::from_bytes(bytes).expect("id length")
}
