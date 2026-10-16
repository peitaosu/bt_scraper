use crate::infra::TrackerGateway;
use crate::peer_policy::{is_storable_peer, peer_rank_score};
use crate::ports::DhtPeerLookup;
use crate::store::Store;
use crate::{InfoHash, Peer};
use std::collections::HashSet;
use std::time::Duration;

pub struct PeerCollectConfig {
    pub skip_dht_if_cached: bool,
    pub tracker_urls: Vec<String>,
    pub tracker_urls_per_lookup: usize,
    pub peer_timeout: Duration,
    pub cached_peer_limit: usize,
}

pub async fn collect_peers(
    store: &Store,
    dht: &dyn DhtPeerLookup,
    trackers: &TrackerGateway,
    info_hash: &InfoHash,
    config: &PeerCollectConfig,
) -> Vec<Peer> {
    let hash = *info_hash;
    let cached = store
        .get_peers(&hash, config.cached_peer_limit)
        .await
        .unwrap_or_default();
    let has_cached = cached.iter().any(|peer| is_storable_peer(peer));

    let dht_peers = if config.skip_dht_if_cached && has_cached {
        Vec::new()
    } else {
        dht.lookup_peers(info_hash).await
    };

    let tracker_urls: Vec<_> = config
        .tracker_urls
        .iter()
        .take(config.tracker_urls_per_lookup.max(1))
        .cloned()
        .collect();
    let tracker_peers = fetch_tracker_peers(
        trackers,
        info_hash,
        &tracker_urls,
        config.peer_timeout,
    )
    .await;

    let mut set = HashSet::new();
    let mut peers = Vec::new();
    let mut pending_upsert = Vec::new();

    for peer in cached {
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }
    for peer in dht_peers {
        pending_upsert.push(peer);
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }
    for peer in tracker_peers {
        pending_upsert.push(peer);
        if set.insert(peer.addr) {
            peers.push(peer);
        }
    }

    if !pending_upsert.is_empty() {
        let _ = store.upsert_peers(info_hash, &pending_upsert).await;
    }

    peers.sort_by_key(|peer| peer_rank_score(peer, 0));
    peers
}

async fn fetch_tracker_peers(
    trackers: &TrackerGateway,
    info_hash: &InfoHash,
    tracker_urls: &[String],
    timeout: Duration,
) -> Vec<Peer> {
    let mut set = tokio::task::JoinSet::new();
    for url in tracker_urls {
        let url = url.clone();
        let hash = *info_hash;
        let trackers = trackers.clone();
        set.spawn(async move {
            trackers.fetch_peers(&url, &hash, timeout).await
        });
    }
    let mut peers = Vec::new();
    while let Some(result) = set.join_next().await {
        if let Ok(Ok(mut tracker_peers)) = result {
            peers.append(&mut tracker_peers);
        }
    }
    peers
}
