use anyhow::{Context, Result, bail};
use crate::infra::{DnsCache, SharedHttp, UdpPool};
use crate::{InfoHash, Peer, PeerSource};
use rand::Rng;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use url::Url;

const UDP_SESSION_TTL: Duration = Duration::from_secs(50);

fn udp_tracker_sessions() -> &'static Mutex<HashMap<SocketAddr, (u64, Instant)>> {
    static SESSIONS: OnceLock<Mutex<HashMap<SocketAddr, (u64, Instant)>>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

const CONNECT: u32 = 0;
const ANNOUNCE: u32 = 1;
const SCRAPE: u32 = 2;
const ERROR: u32 = 3;
const ANNOUNCE_RESPONSE_HEADER_LEN: usize = 20;
const CONNECT_RESPONSE_LEN: usize = 16;
const CONNECT_MAGIC: u64 = 0x41727101980;

pub async fn fetch_peers_with(
    tracker_url: &str,
    info_hash: &InfoHash,
    timeout_duration: Duration,
    http: &SharedHttp,
    udp: &UdpPool,
    dns: &DnsCache,
) -> Result<Vec<Peer>> {
    let url = Url::parse(tracker_url).context("parse tracker url")?;
    match url.scheme() {
        "http" | "https" => fetch_peers_http(&url, info_hash, timeout_duration, http).await,
        _ => fetch_peers_udp(tracker_url, info_hash, timeout_duration, udp, dns).await,
    }
}

async fn fetch_peers_udp(
    tracker_url: &str,
    info_hash: &InfoHash,
    timeout_duration: Duration,
    udp: &UdpPool,
    dns: &DnsCache,
) -> Result<Vec<Peer>> {
    let (host, port) = parse_tracker(tracker_url)?;
    let addr = dns.resolve_tracker_v4(&host, port)?;
    let lease = udp.acquire().await?;
    let socket = &lease.socket;
    let connection_id = udp_connection_id(socket, addr, timeout_duration).await?;
    let announce_tx = random_transaction_id();
    let announce = build_announce(connection_id, info_hash, announce_tx);
    socket.send_to(&announce, addr).await?;
    let response = udp_recv_action(
        &socket,
        timeout_duration,
        announce_tx,
        ANNOUNCE,
        ANNOUNCE_RESPONSE_HEADER_LEN,
    )
    .await?;
    parse_announce_peers(&response)
}

async fn fetch_peers_http(
    url: &Url,
    info_hash: &InfoHash,
    timeout_duration: Duration,
    http: &SharedHttp,
) -> Result<Vec<Peer>> {
    let peer_id = tracker_peer_id();
    let announce_url = build_http_announce_url(url, info_hash, &peer_id)?;
    let response = timeout(
        timeout_duration,
        http.client().get(announce_url).send(),
    )
    .await??;
    let body = response.bytes().await?;
    parse_http_announce_peers(&body)
}

fn build_http_announce_url(
    url: &Url,
    info_hash: &InfoHash,
    peer_id: &[u8; 20],
) -> Result<String> {
    let mut announce = format!(
        "{}://{}{}/announce?peer_id=",
        url.scheme(),
        url.host_str().context("tracker host missing")?,
        if let Some(port) = url.port() {
            format!(":{}", port)
        } else {
            String::new()
        }
    );
    for byte in peer_id {
        announce.push_str(&format!("%{:02x}", byte));
    }
    announce.push_str("&info_hash=");
    for byte in info_hash.0 {
        announce.push_str(&format!("%{:02x}", byte));
    }
    announce.push_str("&port=6881&uploaded=0&downloaded=0&left=1&compact=1&numwant=50&event=started");
    Ok(announce)
}

fn parse_http_announce_peers(body: &[u8]) -> Result<Vec<Peer>> {
    use serde_bencode::value::Value;
    let root = serde_bencode::from_bytes::<Value>(body).context("decode http announce")?;
    let Value::Dict(root_map) = root else {
        bail!("http announce root not dict");
    };
    let mut peers = Vec::new();
    if let Some(Value::Bytes(compact)) = root_map.get(b"peers".as_slice()) {
        for chunk in compact.chunks_exact(6) {
            let ip = IpAddr::from([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            if port > 0 {
                peers.push(Peer::new(SocketAddr::new(ip, port), PeerSource::Tracker));
            }
        }
    }
    Ok(peers)
}

pub async fn scrape_counts_with(
    tracker_url: &str,
    info_hashes: &[InfoHash],
    timeout_duration: Duration,
    http: &SharedHttp,
    udp: &UdpPool,
    dns: &DnsCache,
) -> Result<Vec<(InfoHash, u32, u32)>> {
    if info_hashes.is_empty() {
        return Ok(Vec::new());
    }
    let url = Url::parse(tracker_url).context("parse tracker url")?;
    match url.scheme() {
        "http" | "https" => {
            scrape_counts_http(&url, info_hashes, timeout_duration, http).await
        }
        _ => scrape_counts_udp(tracker_url, info_hashes, timeout_duration, udp, dns).await,
    }
}

async fn scrape_counts_udp(
    tracker_url: &str,
    info_hashes: &[InfoHash],
    timeout_duration: Duration,
    udp: &UdpPool,
    dns: &DnsCache,
) -> Result<Vec<(InfoHash, u32, u32)>> {
    let (host, port) = parse_tracker(tracker_url)?;
    let addr = dns.resolve_tracker_v4(&host, port)?;
    let lease = udp.acquire().await?;
    let socket = &lease.socket;
    let connection_id = udp_connection_id(socket, addr, timeout_duration).await?;
    let scrape_tx = random_transaction_id();
    let scrape = build_scrape(connection_id, info_hashes, scrape_tx);
    socket.send_to(&scrape, addr).await?;
    let response = udp_recv_action(
        &socket,
        timeout_duration,
        scrape_tx,
        SCRAPE,
        8 + info_hashes.len() * 12,
    )
    .await?;
    parse_scrape_response(&response, info_hashes)
}

async fn scrape_counts_http(
    url: &Url,
    info_hashes: &[InfoHash],
    timeout_duration: Duration,
    http: &SharedHttp,
) -> Result<Vec<(InfoHash, u32, u32)>> {
    let mut scrape_url = format!(
        "{}://{}{}/scrape",
        url.scheme(),
        url.host_str().context("tracker host missing")?,
        if let Some(port) = url.port() {
            format!(":{}", port)
        } else {
            String::new()
        }
    );
    scrape_url.push('?');
    for hash in info_hashes {
        scrape_url.push_str("info_hash=");
        for byte in hash.0 {
            scrape_url.push_str(&format!("%{:02x}", byte));
        }
        scrape_url.push('&');
    }
    let response = timeout(
        timeout_duration,
        http.client().get(scrape_url).send(),
    )
    .await??;
    let body = response.bytes().await?;
    parse_http_scrape_response(&body, info_hashes)
}

fn parse_http_scrape_response(
    body: &[u8],
    info_hashes: &[InfoHash],
) -> Result<Vec<(InfoHash, u32, u32)>> {
    use serde_bencode::value::Value;
    let root = serde_bencode::from_bytes::<Value>(body).context("decode http scrape")?;
    let Value::Dict(root_map) = root else {
        bail!("http scrape root not dict");
    };
    let files = root_map
        .get(b"files".as_slice())
        .context("missing files in scrape response")?;
    let Value::Dict(files_map) = files else {
        bail!("files not dict");
    };
    let mut results = Vec::new();
    for hash in info_hashes {
        let key = hash.0.to_vec();
        if let Some(Value::Dict(stats)) = files_map.get(&key) {
            let seeders = dict_int_value(stats, b"complete").unwrap_or(0) as u32;
            let leechers = dict_int_value(stats, b"incomplete").unwrap_or(0) as u32;
            results.push((*hash, seeders, leechers));
        }
    }
    Ok(results)
}

fn dict_int_value(map: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>, key: &[u8]) -> Option<i64> {
    match map.get(key) {
        Some(serde_bencode::value::Value::Int(value)) => Some(*value),
        _ => None,
    }
}

fn parse_tracker(url_str: &str) -> Result<(String, u16)> {
    let url = Url::parse(url_str).context("parse tracker url")?;
    let host = url
        .host_str()
        .context("tracker host missing")?
        .to_string();
    let port = url.port().unwrap_or(if url.scheme() == "udp" { 6969 } else { 80 });
    Ok((host, port))
}

async fn udp_connection_id(
    socket: &UdpSocket,
    addr: SocketAddr,
    timeout_duration: Duration,
) -> Result<u64> {
    if let Ok(map) = udp_tracker_sessions().lock() {
        if let Some((connection_id, until)) = map.get(&addr) {
            if *until > Instant::now() {
                return Ok(*connection_id);
            }
        }
    }
    let connection_id = udp_connect(socket, addr, timeout_duration).await?;
    if let Ok(mut map) = udp_tracker_sessions().lock() {
        map.insert(addr, (connection_id, Instant::now() + UDP_SESSION_TTL));
    }
    Ok(connection_id)
}

fn tracker_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];
    peer_id[..8].copy_from_slice(b"-BT0001-");
    rand::thread_rng().fill(&mut peer_id[8..]);
    peer_id
}

fn random_transaction_id() -> [u8; 4] {
    let mut tx = [0u8; 4];
    rand::thread_rng().fill(&mut tx);
    tx
}

async fn udp_connect(
    socket: &UdpSocket,
    addr: SocketAddr,
    timeout_duration: Duration,
) -> Result<u64> {
    let connect_tx = random_transaction_id();
    let connect = build_connect(CONNECT_MAGIC, connect_tx);
    socket.send_to(&connect, addr).await?;
    let response = udp_recv_action(
        socket,
        timeout_duration,
        connect_tx,
        CONNECT,
        CONNECT_RESPONSE_LEN,
    )
    .await?;
    Ok(u64::from_be_bytes(
        response[8..16]
            .try_into()
            .context("connect response connection id")?,
    ))
}

fn response_action_and_transaction(data: &[u8]) -> Option<(u32, [u8; 4])> {
    if data.len() < 8 {
        return None;
    }
    let action = u32::from_be_bytes(data[0..4].try_into().ok()?);
    let transaction_id = data[4..8].try_into().ok()?;
    Some((action, transaction_id))
}

async fn udp_recv_action(
    socket: &UdpSocket,
    timeout_duration: Duration,
    transaction_id: [u8; 4],
    expected_action: u32,
    min_len: usize,
) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout_duration;
    let mut buf = [0u8; 1500];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("tracker response timeout");
        }
        let (len, _) = timeout(remaining, socket.recv_from(&mut buf)).await??;
        let data = &buf[..len];
        if let Some(message) = parse_tracker_error(data, transaction_id)? {
            bail!("tracker error: {message}");
        }
        let Some((action, response_tx)) = response_action_and_transaction(data) else {
            continue;
        };
        if response_tx != transaction_id {
            continue;
        }
        if action == expected_action && data.len() >= min_len {
            return Ok(data.to_vec());
        }
    }
}

fn parse_tracker_error(data: &[u8], transaction_id: [u8; 4]) -> Result<Option<String>> {
    if data.len() < 8 {
        return Ok(None);
    }
    let action = u32::from_be_bytes(data[0..4].try_into()?);
    if action != ERROR {
        return Ok(None);
    }
    if data[4..8] != transaction_id {
        return Ok(None);
    }
    let message = String::from_utf8_lossy(&data[8..]).trim_end_matches('\0').to_string();
    Ok(Some(message))
}

fn build_connect(connection_id: u64, transaction_id: [u8; 4]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    buf.extend_from_slice(&CONNECT.to_be_bytes());
    buf.extend_from_slice(&transaction_id);
    buf
}

fn build_announce(connection_id: u64, info_hash: &InfoHash, transaction_id: [u8; 4]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(98);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    buf.extend_from_slice(&ANNOUNCE.to_be_bytes());
    buf.extend_from_slice(&transaction_id);
    buf.extend_from_slice(&info_hash.0);
    buf.extend_from_slice(&tracker_peer_id());
    buf.extend_from_slice(&0i64.to_be_bytes());
    buf.extend_from_slice(&1i64.to_be_bytes());
    buf.extend_from_slice(&0i64.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    let key: i32 = rand::thread_rng().gen();
    buf.extend_from_slice(&key.to_be_bytes());
    buf.extend_from_slice(&(-1i32).to_be_bytes());
    buf.extend_from_slice(&6881u16.to_be_bytes());
    buf
}

fn build_scrape(connection_id: u64, info_hashes: &[InfoHash], transaction_id: [u8; 4]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + info_hashes.len() * 20);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    buf.extend_from_slice(&SCRAPE.to_be_bytes());
    buf.extend_from_slice(&transaction_id);
    for hash in info_hashes {
        buf.extend_from_slice(&hash.0);
    }
    buf
}

fn parse_announce_peers(data: &[u8]) -> Result<Vec<Peer>> {
    if data.len() < ANNOUNCE_RESPONSE_HEADER_LEN {
        bail!("short announce response");
    }
    let action = u32::from_be_bytes(data[0..4].try_into()?);
    if action != ANNOUNCE {
        bail!("unexpected action");
    }
    let mut peers = Vec::new();
    let mut offset = ANNOUNCE_RESPONSE_HEADER_LEN;
    let remaining = data.len().saturating_sub(offset);
    if remaining == 0 {
        return Ok(peers);
    }
    let stride = if remaining % 18 == 0 { 18 } else { 6 };
    while offset + stride <= data.len() {
        let peer = if stride == 18 {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&data[offset..offset + 16]);
            let ip = IpAddr::from(octets);
            let port = u16::from_be_bytes([data[offset + 16], data[offset + 17]]);
            SocketAddr::new(ip, port)
        } else {
            let ip = IpAddr::from([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            let port = u16::from_be_bytes([data[offset + 4], data[offset + 5]]);
            SocketAddr::new(ip, port)
        };
        if peer.port() > 0 {
            peers.push(Peer::new(peer, PeerSource::Tracker));
        }
        offset += stride;
    }
    Ok(peers)
}

fn parse_scrape_response(data: &[u8], info_hashes: &[InfoHash]) -> Result<Vec<(InfoHash, u32, u32)>> {
    if data.len() < 8 {
        bail!("short scrape response");
    }
    let action = u32::from_be_bytes(data[0..4].try_into()?);
    if action != SCRAPE {
        bail!("unexpected action");
    }
    let mut results = Vec::new();
    let mut offset = 8;
    for hash in info_hashes {
        if offset + 12 > data.len() {
            break;
        }
        let seeders = u32::from_be_bytes(data[offset..offset + 4].try_into()?);
        let _completed = u32::from_be_bytes(data[offset + 4..offset + 8].try_into()?);
        let leechers = u32::from_be_bytes(data[offset + 8..offset + 12].try_into()?);
        results.push((*hash, seeders, leechers));
        offset += 12;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn parse_tracker_extracts_host_and_port() {
        let (host, port) = parse_tracker("udp://tracker.opentrackr.org:1337/announce").unwrap();
        assert_eq!(host, "tracker.opentrackr.org");
        assert_eq!(port, 1337);
    }

    #[test]
    fn parse_announce_peers_reads_compact_list() {
        let mut data = Vec::new();
        data.extend_from_slice(&ANNOUNCE.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&60u32.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);
        let peers = parse_announce_peers(&data).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(peers[0].addr.port(), 6881);
        assert_eq!(peers[0].source, PeerSource::Tracker);
    }

    #[test]
    fn announce_packet_length_matches_bep15() {
        let hash = InfoHash([0xAB; 20]);
        let packet = build_announce(0x1234567890ABCDEF, &hash, [0xAA, 0xBB, 0xCC, 0xDD]);
        assert_eq!(packet.len(), 98);
        assert_eq!(u32::from_be_bytes(packet[8..12].try_into().unwrap()), ANNOUNCE);
    }

    #[test]
    fn connect_packet_length_matches_bep15() {
        let packet = build_connect(CONNECT_MAGIC, [0xAA, 0xBB, 0xCC, 0xDD]);
        assert_eq!(packet.len(), 16);
        assert_eq!(u32::from_be_bytes(packet[8..12].try_into().unwrap()), CONNECT);
        assert_eq!(u64::from_be_bytes(packet[0..8].try_into().unwrap()), CONNECT_MAGIC);
    }

    #[test]
    fn parse_scrape_response_reads_counts() {
        let hashes = vec![InfoHash([0xAB; 20])];
        let mut data = Vec::new();
        data.extend_from_slice(&SCRAPE.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&10u32.to_be_bytes());
        data.extend_from_slice(&5u32.to_be_bytes());
        data.extend_from_slice(&3u32.to_be_bytes());
        let results = parse_scrape_response(&data, &hashes).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 10);
        assert_eq!(results[0].2, 3);
    }
}
