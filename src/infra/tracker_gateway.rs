use super::{DnsCache, SharedHttp, UdpPool};
use crate::{InfoHash, Peer};
use std::time::Duration;

#[derive(Clone)]
pub struct TrackerGateway {
    http: SharedHttp,
    udp: UdpPool,
    dns: DnsCache,
}

impl TrackerGateway {
    pub fn new(http: SharedHttp, udp: UdpPool, dns: DnsCache) -> Self {
        Self { http, udp, dns }
    }

    pub async fn fetch_peers(
        &self,
        tracker_url: &str,
        info_hash: &InfoHash,
        timeout: Duration,
    ) -> anyhow::Result<Vec<Peer>> {
        crate::tracker::fetch_peers_with(
            tracker_url,
            info_hash,
            timeout,
            &self.http,
            &self.udp,
            &self.dns,
        )
        .await
    }

    pub async fn scrape_counts(
        &self,
        tracker_url: &str,
        info_hashes: &[InfoHash],
        timeout: Duration,
    ) -> anyhow::Result<Vec<(InfoHash, u32, u32)>> {
        crate::tracker::scrape_counts_with(
            tracker_url,
            info_hashes,
            timeout,
            &self.http,
            &self.udp,
            &self.dns,
        )
        .await
    }
}
