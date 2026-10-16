use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(hex_str: &str) -> Option<Self> {
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() != 20 {
            return None;
        }
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHash({})", self.to_hex())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerSource {
    Announce,
    Dht,
    Tracker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Peer {
    pub addr: SocketAddr,
    pub source: PeerSource,
}

impl Peer {
    pub fn new(addr: SocketAddr, source: PeerSource) -> Self {
        Self { addr, source }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoverySource {
    Bep51,
    GetPeers,
    AnnouncePeer,
}

#[derive(Clone, Debug)]
pub struct DiscoveredHash {
    pub info_hash: InfoHash,
    pub peer: Option<Peer>,
    pub source: DiscoverySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnrichmentStatus {
    Discovered,
    Enriching,
    Enriched,
    Failed,
    Blocked,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentFile {
    pub path: Vec<String>,
    pub length: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentMetadata {
    pub name: String,
    pub files: Vec<TorrentFile>,
    pub total_length: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentRecord {
    pub info_hash: InfoHash,
    pub status: EnrichmentStatus,
    pub metadata: Option<TorrentMetadata>,
    pub seeders: u32,
    pub leechers: u32,
    pub attempt_count: u32,
    pub discovered_at: u64,
    pub enriched_at: Option<u64>,
    pub next_retry_at: Option<u64>,
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn info_hash_hex_roundtrip() {
        let bytes = [0xab; 20];
        let hash = InfoHash(bytes);
        let hex = hash.to_hex();
        let parsed = InfoHash::from_hex(&hex).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn info_hash_from_hex_rejects_invalid_length() {
        assert!(InfoHash::from_hex("abcd").is_none());
    }

    #[test]
    fn peer_holds_address_and_source() {
        let addr = SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 6881));
        let peer = Peer::new(addr, PeerSource::Dht);
        assert_eq!(peer.addr, addr);
        assert_eq!(peer.source, PeerSource::Dht);
    }
}
