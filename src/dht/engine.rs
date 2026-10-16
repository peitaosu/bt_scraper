use anyhow::{Context, Result, bail};
use mainline::async_dht::AsyncDht;
use mainline::{Dht, Id, ServerSettings};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::info;

use crate::{DiscoveredHash, InfoHash, Peer, PeerSource};
use super::discovery_filter::DiscoveryFilter;
use super::sample_store::SampleStore;

pub struct DhtEngine {
    dht: Arc<AsyncDht>,
    sample_store: Arc<Mutex<SampleStore>>,
}

fn parse_bind_v4(bind: &str) -> Result<(Ipv4Addr, u16)> {
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("parse dht bind address {bind}"))?;
    match addr {
        SocketAddr::V4(v4) => Ok((*v4.ip(), v4.port())),
        SocketAddr::V6(_) => bail!("ipv6 dht bind is not supported yet"),
    }
}

pub fn build_dht_node(
    bind: &str,
    bootstrap: &[String],
    persisted_bootstrap: &[String],
    discovery_tx: mpsc::Sender<DiscoveredHash>,
    sample_store: Arc<Mutex<SampleStore>>,
) -> Result<Arc<AsyncDht>> {
    let (bind_ip, port) = parse_bind_v4(bind)?;
    let mut settings = ServerSettings::default();
    settings.filter = Box::new(DiscoveryFilter::new(discovery_tx, sample_store));

    let mut builder = Dht::builder();
    builder.server_mode();
    builder.bind_address(bind_ip);
    builder.port(port);
    builder.server_settings(settings);

    let mut nodes: Vec<SocketAddr> = persisted_bootstrap
        .iter()
        .chain(bootstrap.iter())
        .filter_map(|s| s.parse().ok())
        .collect();
    nodes.sort();
    nodes.dedup();
    if !nodes.is_empty() {
        builder.extra_bootstrap(&nodes);
    }

    let dht = builder.build().context("bind mainline dht node")?;
    Ok(Arc::new(dht.as_async()))
}

pub fn build_ephemeral_dht_node(
    bootstrap: &[String],
    persisted_bootstrap: &[String],
) -> Result<Arc<AsyncDht>> {
    let (tx, _rx) = mpsc::channel(1);
    let store = Arc::new(Mutex::new(SampleStore::default_cap()));
    build_dht_node("0.0.0.0:0", bootstrap, persisted_bootstrap, tx, store)
}

impl DhtEngine {
    pub async fn start(
        bind: &str,
        bootstrap: &[String],
        persisted_bootstrap: &[String],
        discovery_tx: mpsc::Sender<DiscoveredHash>,
    ) -> Result<Arc<Self>> {
        let sample_store = Arc::new(Mutex::new(SampleStore::default_cap()));
        let dht = build_dht_node(
            bind,
            bootstrap,
            persisted_bootstrap,
            discovery_tx,
            sample_store.clone(),
        )?;
        info!(bind = %bind, "dht node listening");
        Ok(Arc::new(Self { dht, sample_store }))
    }

    pub fn async_dht(&self) -> Arc<AsyncDht> {
        self.dht.clone()
    }

    pub fn sample_store(&self) -> Arc<Mutex<SampleStore>> {
        self.sample_store.clone()
    }

    pub async fn wait_bootstrapped(&self) {
        let started = Instant::now();
        let mut last_log = Instant::now();
        loop {
            if self.dht.bootstrapped().await {
                info!(elapsed_secs = started.elapsed().as_secs(), "dht bootstrapped");
                return;
            }
            if last_log.elapsed() >= Duration::from_secs(5) {
                info!(
                    elapsed_secs = started.elapsed().as_secs(),
                    "waiting for dht bootstrap"
                );
                last_log = Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    pub async fn lookup_peers(&self, info_hash: &InfoHash) -> Vec<Peer> {
        use futures_lite::StreamExt;
        let id = match Id::from_bytes(info_hash.0) {
            Ok(id) => id,
            Err(_) => return Vec::new(),
        };
        let mut stream = self.dht.get_peers(id);
        let mut peers = Vec::new();
        let lookup = async {
            while let Some(addrs) = stream.next().await {
                for addr in addrs {
                    peers.push(Peer::new(SocketAddr::from(addr), PeerSource::Dht));
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(5), lookup).await;
        peers
    }

    pub async fn export_bootstrap(&self) -> Vec<String> {
        self.dht.to_bootstrap().await
    }
}

pub async fn run_find_node_burst(
    dht: Arc<AsyncDht>,
    interval_secs: u64,
    burst_size: usize,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        for _ in 0..burst_size {
            let target = Id::random();
            let dht = dht.clone();
            tokio::spawn(async move {
                let _ = dht.find_node(target).await;
            });
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
    }
}

pub async fn run_routing_table_persistence(
    dht: Arc<AsyncDht>,
    path: String,
    interval_secs: u64,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(interval_secs)) => {}
        }
        let nodes = dht.to_bootstrap().await;
        if let Err(error) = crate::routing::save_routing_table(&path, &nodes) {
            tracing::warn!(%error, "routing table save failed");
        }
    }
}
