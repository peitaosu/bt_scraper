use anyhow::{Context, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TTL: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct DnsCache {
    inner: Arc<Mutex<HashMap<String, CachedAddrs>>>,
}

struct CachedAddrs {
    addrs: Vec<SocketAddr>,
    expires: Instant,
}

impl DnsCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn resolve_tracker_v4(&self, host: &str, port: u16) -> Result<SocketAddr> {
        let key = format!("{host}:{port}");
        if let Some(addr) = self.cached_v4(&key) {
            return Ok(addr);
        }
        let addrs = resolve_host(host, port)?;
        if let Ok(mut map) = self.inner.lock() {
            map.insert(
                key,
                CachedAddrs {
                    addrs: addrs.clone(),
                    expires: Instant::now() + TTL,
                },
            );
        }
        pick_ipv4(&addrs).context("no ipv4 tracker address")
    }

    fn cached_v4(&self, key: &str) -> Option<SocketAddr> {
        let map = self.inner.lock().ok()?;
        let entry = map.get(key)?;
        if entry.expires <= Instant::now() {
            return None;
        }
        pick_ipv4(&entry.addrs).ok()
    }
}

fn resolve_host(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;
    let mut addrs: Vec<SocketAddr> = (host, port)
        .to_socket_addrs()
        .context("resolve tracker")?
        .collect();
    addrs.sort_by_key(|addr| match addr {
        SocketAddr::V4(_) => 0,
        SocketAddr::V6(_) => 1,
    });
    Ok(addrs)
}

fn pick_ipv4(addrs: &[SocketAddr]) -> Result<SocketAddr> {
    use std::net::IpAddr;
    addrs
        .iter()
        .copied()
        .find(|addr| matches!(addr.ip(), IpAddr::V4(ip) if !ip.is_unspecified()))
        .context("no ipv4 tracker address")
}
