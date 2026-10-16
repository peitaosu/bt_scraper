use super::peers::upsert_peer;
use super::torrent::{insert_discovered, is_blocked, is_enriched};
use crate::{InfoHash, Peer, PeerSource};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

pub struct DiscoveryBatchResult {
    pub to_enrich: Vec<InfoHash>,
    pub newly_discovered: usize,
}

pub fn process_discovery_batch(
    conn: &Connection,
    items: &[(InfoHash, Option<Peer>)],
) -> Result<DiscoveryBatchResult> {
    let tx = conn.unchecked_transaction()?;
    let mut to_enrich = Vec::new();
    let mut enrich_set = HashSet::new();
    let mut newly_discovered = 0usize;
    for (info_hash, peer) in items {
        if is_enriched(&tx, info_hash)? || is_blocked(&tx, info_hash)? {
            continue;
        }
        if insert_discovered(&tx, info_hash)? {
            newly_discovered += 1;
        }
        if let Some(peer) = peer {
            upsert_peer(&tx, info_hash, peer)?;
        }
        let has_announce = peer.is_some_and(|p| p.source == PeerSource::Announce);
        if has_announce && enrich_set.insert(*info_hash) {
            to_enrich.push(*info_hash);
        }
    }
    tx.commit()?;
    Ok(DiscoveryBatchResult {
        to_enrich,
        newly_discovered,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TorrentMetadata;
    use std::net::{Ipv4Addr, SocketAddr};

    fn temp_conn() -> (tempfile::NamedTempFile, Connection) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        (file, conn)
    }

    #[test]
    fn discovery_batch_inserts_multiple_hashes() {
        let (_file, conn) = temp_conn();
        let items = vec![
            (InfoHash([1u8; 20]), None),
            (InfoHash([2u8; 20]), None),
            (InfoHash([3u8; 20]), None),
        ];
        let result = process_discovery_batch(&conn, &items).unwrap();
        assert_eq!(result.newly_discovered, 3);
        assert!(result.to_enrich.is_empty());
        let (total, _, _) = super::super::stats::count_by_status(&conn).unwrap();
        assert_eq!(total, 3);
    }

    #[test]
    fn discovery_batch_dedupes_repeated_hash() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([9u8; 20]);
        let result = process_discovery_batch(
            &conn,
            &[(hash, None), (hash, None), (hash, None)],
        )
        .unwrap();
        assert_eq!(result.newly_discovered, 1);
        assert!(result.to_enrich.is_empty());
        let (total, _, _) = super::super::stats::count_by_status(&conn).unwrap();
        assert_eq!(total, 1);
    }

    #[test]
    fn discovery_batch_enqueues_announce_peer() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([4u8; 20]);
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 6882)),
            PeerSource::Announce,
        );
        let result = process_discovery_batch(&conn, &[(hash, Some(peer))]).unwrap();
        assert_eq!(result.newly_discovered, 1);
        assert_eq!(result.to_enrich.len(), 1);
    }

    #[test]
    fn discovery_batch_skips_enriched() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([5u8; 20]);
        insert_discovered(&conn, &hash).unwrap();
        super::super::torrent::save_enriched(
            &conn,
            &hash,
            &TorrentMetadata {
                name: "done".into(),
                files: vec![],
                total_length: 1,
            },
        )
        .unwrap();

        let result = process_discovery_batch(&conn, &[(hash, None)]).unwrap();
        assert_eq!(result.newly_discovered, 0);
        assert!(result.to_enrich.is_empty());
    }

    #[test]
    fn discovery_batch_upserts_peers() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([6u8; 20]);
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 6881)),
            PeerSource::Announce,
        );
        process_discovery_batch(&conn, &[(hash, Some(peer))]).unwrap();
        let peers = super::super::peers::get_peers(&conn, &hash, 10).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].source, PeerSource::Announce);
    }

    #[test]
    fn discovery_batch_skips_blocked() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([12u8; 20]);
        insert_discovered(&conn, &hash).unwrap();
        super::super::torrent::mark_blocked(&conn, &hash).unwrap();

        let result = process_discovery_batch(&conn, &[(hash, None)]).unwrap();
        assert_eq!(result.newly_discovered, 0);
        assert!(result.to_enrich.is_empty());
        let record = super::super::torrent::get_record(&conn, &hash).unwrap().unwrap();
        assert_eq!(record.status, crate::EnrichmentStatus::Blocked);
        assert!(record.metadata.is_none());
    }

    #[test]
    fn discovery_batch_skips_failed_in_backoff() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([14u8; 20]);
        insert_discovered(&conn, &hash).unwrap();
        let future = crate::unix_now() + 3600;
        super::super::torrent::mark_failed(&conn, &hash, future).unwrap();

        let result = process_discovery_batch(&conn, &[(hash, None)]).unwrap();
        assert_eq!(result.newly_discovered, 0);
        assert!(result.to_enrich.is_empty());

        let past = crate::unix_now().saturating_sub(1);
        super::super::torrent::mark_failed(&conn, &hash, past).unwrap();
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 3), 6883)),
            PeerSource::Announce,
        );
        let result = process_discovery_batch(&conn, &[(hash, Some(peer))]).unwrap();
        assert_eq!(result.to_enrich.len(), 1);
    }
}
