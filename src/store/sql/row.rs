use crate::{EnrichmentStatus, InfoHash, PeerSource};
use std::net::IpAddr;

pub(crate) fn parse_hash_rows(
    rows: impl Iterator<Item = rusqlite::Result<Vec<u8>>>,
) -> anyhow::Result<Vec<InfoHash>> {
    let mut hashes = Vec::new();
    for row in rows {
        let bytes = row?;
        if bytes.len() == 20 {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&bytes);
            hashes.push(InfoHash(arr));
        }
    }
    Ok(hashes)
}

pub(crate) fn ip_to_blob(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

pub(crate) fn blob_to_ip(bytes: &[u8]) -> Option<IpAddr> {
    match bytes.len() {
        4 => Some(IpAddr::from([bytes[0], bytes[1], bytes[2], bytes[3]])),
        16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(IpAddr::from(arr))
        }
        _ => None,
    }
}

pub(crate) fn peer_source_str(source: PeerSource) -> &'static str {
    match source {
        PeerSource::Announce => "announce",
        PeerSource::Dht => "dht",
        PeerSource::Tracker => "tracker",
    }
}

pub(crate) fn parse_peer_source(s: &str) -> PeerSource {
    match s {
        "announce" => PeerSource::Announce,
        "tracker" => PeerSource::Tracker,
        _ => PeerSource::Dht,
    }
}

pub(crate) fn parse_status(s: &str) -> EnrichmentStatus {
    match s {
        "enriching" => EnrichmentStatus::Enriching,
        "enriched" => EnrichmentStatus::Enriched,
        "failed" => EnrichmentStatus::Failed,
        "blocked" => EnrichmentStatus::Blocked,
        _ => EnrichmentStatus::Discovered,
    }
}
