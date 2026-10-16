mod dns;
mod http;
mod tracker_gateway;
mod udp_pool;

pub use dns::DnsCache;
pub use http::SharedHttp;
pub use tracker_gateway::TrackerGateway;
pub use udp_pool::UdpPool;

use std::sync::Arc;

#[derive(Clone)]
pub struct RuntimeServices {
    pub http: SharedHttp,
    pub udp: UdpPool,
    pub dns: DnsCache,
    pub trackers: Arc<TrackerGateway>,
}

impl RuntimeServices {
    pub fn new(http_timeout_secs: u64, udp_pool_size: usize) -> Self {
        let http = SharedHttp::new(http_timeout_secs);
        let udp = UdpPool::new(udp_pool_size);
        let dns = DnsCache::new();
        let trackers = Arc::new(TrackerGateway::new(http.clone(), udp.clone(), dns.clone()));
        Self {
            http,
            udp,
            dns,
            trackers,
        }
    }
}
