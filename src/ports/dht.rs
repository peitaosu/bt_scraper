use crate::{InfoHash, Peer};
use std::future::Future;
use std::pin::Pin;

pub trait DhtPeerLookup: Send + Sync {
    fn lookup_peers<'a>(
        &'a self,
        info_hash: &'a InfoHash,
    ) -> Pin<Box<dyn Future<Output = Vec<Peer>> + Send + 'a>>;
}

impl DhtPeerLookup for crate::dht::DhtEngine {
    fn lookup_peers<'a>(
        &'a self,
        info_hash: &'a InfoHash,
    ) -> Pin<Box<dyn Future<Output = Vec<Peer>> + Send + 'a>> {
        Box::pin(crate::dht::DhtEngine::lookup_peers(self, info_hash))
    }
}
