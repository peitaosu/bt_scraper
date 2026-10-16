use super::row::parse_hash_rows;
use super::row::parse_status;
use crate::{InfoHash, TorrentMetadata, TorrentRecord, unix_now};
use anyhow::Result;
use rusqlite::{params, Connection};

pub fn reset_failed_without_quality_peers(conn: &Connection) -> Result<u64> {
    let changed = conn.execute(
        "UPDATE torrents SET status = 'discovered', next_retry_at = NULL
         WHERE status = 'failed'
           AND NOT EXISTS (
             SELECT 1 FROM peers p
             WHERE p.info_hash = torrents.info_hash
               AND p.source IN ('announce', 'tracker', 'dht')
           )",
        [],
    )?;
    Ok(changed as u64)
}

pub fn is_enriched(conn: &Connection, info_hash: &InfoHash) -> Result<bool> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM torrents WHERE info_hash = ?1",
            params![info_hash.0.as_slice()],
            |row| row.get(0),
        )
        .ok();
    Ok(status.as_deref() == Some("enriched"))
}

pub fn is_blocked(conn: &Connection, info_hash: &InfoHash) -> Result<bool> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM torrents WHERE info_hash = ?1",
            params![info_hash.0.as_slice()],
            |row| row.get(0),
        )
        .ok();
    Ok(status.as_deref() == Some("blocked"))
}

pub fn insert_discovered(conn: &Connection, info_hash: &InfoHash) -> Result<bool> {
    let now = unix_now() as i64;
    let changed = conn.execute(
        "INSERT OR IGNORE INTO torrents (info_hash, status, discovered_at)
         VALUES (?1, 'discovered', ?2)",
        params![info_hash.0.as_slice(), now],
    )?;
    Ok(changed > 0)
}

pub fn mark_enriching(conn: &Connection, info_hash: &InfoHash) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET status = 'enriching',
         attempt_count = attempt_count + 1
         WHERE info_hash = ?1",
        params![info_hash.0.as_slice()],
    )?;
    Ok(())
}

pub fn save_enriched(conn: &Connection, info_hash: &InfoHash, metadata: &TorrentMetadata) -> Result<()> {
    let now = unix_now() as i64;
    let files_json = serde_json::to_string(&metadata.files)?;
    conn.execute(
        "UPDATE torrents SET
            status = 'enriched',
            name = ?2,
            files_json = ?3,
            total_length = ?4,
            enriched_at = ?5,
            next_retry_at = NULL
         WHERE info_hash = ?1",
        params![
            info_hash.0.as_slice(),
            metadata.name,
            files_json,
            metadata.total_length as i64,
            now
        ],
    )?;
    Ok(())
}

pub fn mark_failed(conn: &Connection, info_hash: &InfoHash, next_retry_at: u64) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET status = 'failed', next_retry_at = ?2
         WHERE info_hash = ?1",
        params![info_hash.0.as_slice(), next_retry_at as i64],
    )?;
    Ok(())
}

pub fn mark_blocked(conn: &Connection, info_hash: &InfoHash) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET status = 'blocked', next_retry_at = NULL
         WHERE info_hash = ?1",
        params![info_hash.0.as_slice()],
    )?;
    Ok(())
}

pub fn get_retry_candidates(conn: &Connection, limit: usize, now: u64) -> Result<Vec<InfoHash>> {
    let mut stmt = conn.prepare(
        "SELECT info_hash FROM torrents
         WHERE status IN ('discovered', 'failed')
           AND (next_retry_at IS NULL OR next_retry_at <= ?1)
         ORDER BY
           (SELECT COUNT(*) FROM peers p
            WHERE p.info_hash = torrents.info_hash
              AND p.source IN ('announce', 'tracker', 'dht')) DESC,
           COALESCE(peer_lookup_at, 0) ASC,
           discovered_at ASC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![now as i64, limit as i64], |row| {
        let bytes: Vec<u8> = row.get(0)?;
        Ok(bytes)
    })?;
    parse_hash_rows(rows)
}

pub fn touch_peer_lookup(conn: &Connection, info_hash: &InfoHash) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET peer_lookup_at = ?2 WHERE info_hash = ?1",
        params![info_hash.0.as_slice(), unix_now() as i64],
    )?;
    Ok(())
}

pub fn get_record(conn: &Connection, info_hash: &InfoHash) -> Result<Option<TorrentRecord>> {
    let result = conn.query_row(
        "SELECT status, name, files_json, total_length, seeders, leechers,
                attempt_count, discovered_at, enriched_at, next_retry_at
         FROM torrents WHERE info_hash = ?1",
        params![info_hash.0.as_slice()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
            ))
        },
    );

    match result {
        Ok((
            status,
            name,
            files_json,
            total_length,
            seeders,
            leechers,
            attempt_count,
            discovered_at,
            enriched_at,
            next_retry_at,
        )) => {
            let metadata = if status == "enriched" {
                let files = files_json
                    .and_then(|j| serde_json::from_str(&j).ok())
                    .unwrap_or_default();
                Some(TorrentMetadata {
                    name: name.unwrap_or_default(),
                    files,
                    total_length: total_length.unwrap_or(0) as u64,
                })
            } else {
                None
            };
            Ok(Some(TorrentRecord {
                info_hash: *info_hash,
                status: parse_status(&status),
                metadata,
                seeders: seeders as u32,
                leechers: leechers as u32,
                attempt_count: attempt_count as u32,
                discovered_at: discovered_at as u64,
                enriched_at: enriched_at.map(|v| v as u64),
                next_retry_at: next_retry_at.map(|v| v as u64),
            }))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn requeue_failed(
    conn: &Connection,
    now: u64,
    max_attempts: u32,
    retry_after_secs: u64,
) -> Result<u64> {
    let cutoff = now.saturating_sub(retry_after_secs) as i64;
    let changed = conn.execute(
        "UPDATE torrents SET status = 'discovered', attempt_count = 0, next_retry_at = NULL
         WHERE status = 'failed'
           AND attempt_count >= ?1
           AND discovered_at <= ?2",
        params![max_attempts as i64, cutoff],
    )?;
    Ok(changed as u64)
}

pub fn count_retry_queue(conn: &Connection, now: u64) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM torrents
         WHERE status IN ('discovered', 'failed')
           AND (next_retry_at IS NULL OR next_retry_at <= ?1)",
        params![now as i64],
        |row| row.get(0),
    )?;
    Ok(count as u64)
}

pub fn export_enriched_hashes(conn: &Connection) -> Result<Vec<InfoHash>> {
    let mut stmt = conn.prepare(
        "SELECT info_hash FROM torrents WHERE status = 'enriched' ORDER BY enriched_at ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let bytes: Vec<u8> = row.get(0)?;
        Ok(bytes)
    })?;
    parse_hash_rows(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnrichmentStatus, Peer, PeerSource};
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn mark_blocked_leaves_no_metadata() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        let hash = InfoHash([13u8; 20]);
        insert_discovered(&conn, &hash).unwrap();
        mark_enriching(&conn, &hash).unwrap();
        mark_blocked(&conn, &hash).unwrap();
        let record = get_record(&conn, &hash).unwrap().unwrap();
        assert_eq!(record.status, EnrichmentStatus::Blocked);
        assert!(record.metadata.is_none());
        let (_, enriched, pending) = super::super::stats::count_by_status(&conn).unwrap();
        assert_eq!(enriched, 0);
        assert_eq!(pending, 0);
    }

    #[test]
    fn announce_peer_enqueues_during_backoff() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        let hash = InfoHash([15u8; 20]);
        insert_discovered(&conn, &hash).unwrap();
        mark_failed(&conn, &hash, unix_now() + 3600).unwrap();
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 4), 6884)),
            PeerSource::Announce,
        );
        let result =
            super::super::discovery::process_discovery_batch(&conn, &[(hash, Some(peer))])
                .unwrap();
        assert_eq!(result.to_enrich.len(), 1);
    }
}
