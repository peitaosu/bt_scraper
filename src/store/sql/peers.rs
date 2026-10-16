use super::row::{blob_to_ip, ip_to_blob, parse_peer_source, peer_source_str};
use crate::peer_policy::{is_storable_peer, peer_rank_score};
use crate::{InfoHash, Peer, unix_now};
use anyhow::Result;
use rusqlite::{params, Connection};
use std::net::SocketAddr;

pub fn upsert_peer(conn: &Connection, info_hash: &InfoHash, peer: &Peer) -> Result<()> {
    if !is_storable_peer(peer) {
        return Ok(());
    }
    let ip = ip_to_blob(peer.addr.ip());
    let now = unix_now() as i64;
    let source = peer_source_str(peer.source);
    conn.execute(
        "INSERT INTO peers (info_hash, ip, port, source, seen_at, fail_count, last_fail_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL)
         ON CONFLICT(info_hash, ip, port) DO UPDATE SET
            source = excluded.source,
            seen_at = excluded.seen_at,
            fail_count = 0,
            last_fail_at = NULL",
        params![
            info_hash.0.as_slice(),
            ip,
            peer.addr.port() as i64,
            source,
            now
        ],
    )?;
    refresh_quality_peer_count(conn, info_hash)?;
    Ok(())
}

pub fn upsert_peers(conn: &Connection, info_hash: &InfoHash, peers: &[Peer]) -> Result<()> {
    if peers.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for peer in peers {
        if !is_storable_peer(peer) {
            continue;
        }
        let ip = ip_to_blob(peer.addr.ip());
        let now = unix_now() as i64;
        let source = peer_source_str(peer.source);
        tx.execute(
            "INSERT INTO peers (info_hash, ip, port, source, seen_at, fail_count, last_fail_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL)
             ON CONFLICT(info_hash, ip, port) DO UPDATE SET
                source = excluded.source,
                seen_at = excluded.seen_at,
                fail_count = 0,
                last_fail_at = NULL",
            params![
                info_hash.0.as_slice(),
                ip,
                peer.addr.port() as i64,
                source,
                now
            ],
        )?;
    }
    tx.commit()?;
    refresh_quality_peer_count(conn, info_hash)?;
    Ok(())
}

pub fn record_peer_failures(
    conn: &Connection,
    info_hash: &InfoHash,
    addrs: &[SocketAddr],
) -> Result<()> {
    if addrs.is_empty() {
        return Ok(());
    }
    let now = unix_now() as i64;
    let tx = conn.unchecked_transaction()?;
    for addr in addrs {
        let ip = ip_to_blob(addr.ip());
        tx.execute(
            "UPDATE peers SET
                fail_count = fail_count + 1,
                last_fail_at = ?4
             WHERE info_hash = ?1 AND ip = ?2 AND port = ?3",
            params![
                info_hash.0.as_slice(),
                ip,
                addr.port() as i64,
                now
            ],
        )?;
    }
    tx.commit()?;
    refresh_quality_peer_count(conn, info_hash)?;
    Ok(())
}

pub fn refresh_quality_peer_count(conn: &Connection, info_hash: &InfoHash) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET quality_peer_count = (
            SELECT COUNT(*) FROM peers p
            WHERE p.info_hash = ?1
              AND p.source IN ('announce', 'tracker', 'dht')
              AND p.fail_count < 3
         )
         WHERE info_hash = ?1",
        params![info_hash.0.as_slice()],
    )?;
    Ok(())
}

pub fn has_quality_peer(conn: &Connection, info_hash: &InfoHash, fail_threshold: u32) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM peers
         WHERE info_hash = ?1
           AND source IN ('announce', 'tracker', 'dht')
           AND fail_count < ?2",
        params![info_hash.0.as_slice(), fail_threshold as i64],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub struct StoredPeer {
    pub peer: Peer,
    pub fail_count: u32,
}

pub fn get_peers(conn: &Connection, info_hash: &InfoHash, limit: usize, fail_threshold: u32) -> Result<Vec<StoredPeer>> {
    let mut stmt = conn.prepare(
        "SELECT ip, port, source, fail_count FROM peers
         WHERE info_hash = ?1
           AND fail_count < ?2
         ORDER BY CASE source WHEN 'announce' THEN 0 WHEN 'dht' THEN 1 ELSE 2 END,
                  fail_count ASC,
                  seen_at DESC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(
        params![info_hash.0.as_slice(), fail_threshold as i64, limit as i64],
        |row| {
            let ip_bytes: Vec<u8> = row.get(0)?;
            let port: i64 = row.get(1)?;
            let source: String = row.get(2)?;
            let fail_count: i64 = row.get(3)?;
            Ok((ip_bytes, port, source, fail_count))
        },
    )?;
    let mut peers = Vec::new();
    for row in rows {
        let (ip_bytes, port, source, fail_count) = row?;
        if let Some(ip) = blob_to_ip(&ip_bytes) {
            let peer = Peer::new(
                SocketAddr::new(ip, port as u16),
                parse_peer_source(&source),
            );
            if is_storable_peer(&peer) {
                peers.push(StoredPeer {
                    peer,
                    fail_count: fail_count as u32,
                });
            }
        }
    }
    peers.sort_by_key(|stored| peer_rank_score(&stored.peer, stored.fail_count));
    Ok(peers)
}

pub fn prune_stale_peers(conn: &Connection, older_than: u64) -> Result<u64> {
    let tx = conn.unchecked_transaction()?;
    let changed = tx.execute(
        "DELETE FROM peers WHERE seen_at < ?1",
        params![older_than as i64],
    )?;
    tx.execute(
        "UPDATE torrents SET quality_peer_count = (
            SELECT COUNT(*) FROM peers p
            WHERE p.info_hash = torrents.info_hash
              AND p.source IN ('announce', 'tracker', 'dht')
              AND p.fail_count < 3
         )",
        [],
    )?;
    tx.commit()?;
    Ok(changed as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InfoHash, Peer, PeerSource};
    use std::net::{Ipv4Addr, SocketAddr};

    fn temp_conn() -> (tempfile::NamedTempFile, Connection) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        super::super::schema::initialize_store(&conn).unwrap();
        (file, conn)
    }

    fn dht_peer(octet: u8, port: u16) -> Peer {
        Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, octet), port)),
            PeerSource::Dht,
        )
    }

    #[test]
    fn quality_peer_count_tracks_storable_sources() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([1u8; 20]);
        super::super::torrent::insert_discovered(&conn, &hash).unwrap();
        upsert_peers(&conn, &hash, &[dht_peer(1, 6881), dht_peer(2, 6882)]).unwrap();
        assert!(has_quality_peer(&conn, &hash, 3).unwrap());
        let count: i64 = conn
            .query_row(
                "SELECT quality_peer_count FROM torrents WHERE info_hash = ?1",
                params![hash.0.as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn record_peer_failures_drops_quality_when_threshold_reached() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([2u8; 20]);
        let addr = SocketAddr::from((Ipv4Addr::new(1, 2, 3, 9), 6999));
        super::super::torrent::insert_discovered(&conn, &hash).unwrap();
        upsert_peer(&conn, &hash, &dht_peer(9, 6999)).unwrap();
        assert!(has_quality_peer(&conn, &hash, 3).unwrap());
        for _ in 0..3 {
            record_peer_failures(&conn, &hash, &[addr]).unwrap();
        }
        assert!(!has_quality_peer(&conn, &hash, 3).unwrap());
        let stored = get_peers(&conn, &hash, 10, 3).unwrap();
        assert!(stored.is_empty());
    }

    #[test]
    fn get_peers_prefers_announce_over_tracker() {
        let (_file, conn) = temp_conn();
        let hash = InfoHash([3u8; 20]);
        super::super::torrent::insert_discovered(&conn, &hash).unwrap();
        let tracker = Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 3), 6883)),
            PeerSource::Tracker,
        );
        let announce = Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 6884)),
            PeerSource::Announce,
        );
        upsert_peers(&conn, &hash, &[tracker, announce]).unwrap();
        let peers = get_peers(&conn, &hash, 10, 3).unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer.source, PeerSource::Announce);
    }
}
