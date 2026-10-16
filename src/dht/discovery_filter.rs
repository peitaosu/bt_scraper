use super::discovery::discovery_from_request;
use super::sample_store::SampleStore;
use crate::DiscoveredHash;
use mainline::{RequestFilter, RequestSpecific};
use std::fmt;
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Clone)]
pub struct DiscoveryFilter {
    discovery_tx: Arc<mpsc::Sender<DiscoveredHash>>,
    sample_store: Arc<Mutex<SampleStore>>,
}

impl DiscoveryFilter {
    pub fn new(
        discovery_tx: mpsc::Sender<DiscoveredHash>,
        sample_store: Arc<Mutex<SampleStore>>,
    ) -> Self {
        Self {
            discovery_tx: Arc::new(discovery_tx),
            sample_store,
        }
    }

    fn emit(&self, event: DiscoveredHash) {
        if let Ok(mut store) = self.sample_store.lock() {
            store.record(event.info_hash);
        }
        if self.discovery_tx.try_send(event).is_err() {
            metrics::counter!("discovery_dropped_total").increment(1);
            debug!("discovery channel full, event dropped");
        }
    }
}

impl fmt::Debug for DiscoveryFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiscoveryFilter").finish_non_exhaustive()
    }
}

impl RequestFilter for DiscoveryFilter {
    fn allow_request(&self, request: &RequestSpecific, from: SocketAddrV4) -> bool {
        if let Some(event) = discovery_from_request(request, from) {
            self.emit(event);
        }
        true
    }
}
