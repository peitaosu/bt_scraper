use super::row::{blob_to_ip, ip_to_blob, parse_peer_source, peer_source_str};
use crate::peer_policy::is_storable_peer;
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
        "INSERT INTO peers (info_hash, ip, port, source, seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(info_hash, ip, port) DO UPDATE SET
            source = excluded.source,
            seen_at = excluded.seen_at",
        params![
            info_hash.0.as_slice(),
            ip,
            peer.addr.port() as i64,
            source,
            now
        ],
    )?;
    Ok(())
}

pub fn upsert_peers(conn: &Connection, info_hash: &InfoHash, peers: &[Peer]) -> Result<()> {
    for peer in peers {
        upsert_peer(conn, info_hash, peer)?;
    }
    Ok(())
}

pub fn get_peers(conn: &Connection, info_hash: &InfoHash, limit: usize) -> Result<Vec<Peer>> {
    let mut stmt = conn.prepare(
        "SELECT ip, port, source FROM peers
         WHERE info_hash = ?1
         ORDER BY CASE source WHEN 'announce' THEN 0 WHEN 'dht' THEN 1 ELSE 2 END,
                  seen_at DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![info_hash.0.as_slice(), limit as i64], |row| {
        let ip_bytes: Vec<u8> = row.get(0)?;
        let port: i64 = row.get(1)?;
        let source: String = row.get(2)?;
        Ok((ip_bytes, port, source))
    })?;
    let mut peers = Vec::new();
    for row in rows {
        let (ip_bytes, port, source) = row?;
        if let Some(ip) = blob_to_ip(&ip_bytes) {
            peers.push(Peer::new(
                SocketAddr::new(ip, port as u16),
                parse_peer_source(&source),
            ));
        }
    }
    Ok(peers)
}

pub fn prune_stale_peers(conn: &Connection, older_than: u64) -> Result<u64> {
    let changed = conn.execute(
        "DELETE FROM peers WHERE seen_at < ?1",
        params![older_than as i64],
    )?;
    Ok(changed as u64)
}
