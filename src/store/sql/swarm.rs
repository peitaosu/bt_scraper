use super::row::parse_hash_rows;
use crate::InfoHash;
use anyhow::Result;
use rusqlite::{params, Connection};

pub fn get_scrape_candidates(conn: &Connection, limit: usize, stale_before: u64) -> Result<Vec<InfoHash>> {
    let mut stmt = conn.prepare(
        "SELECT info_hash FROM torrents
         WHERE status = 'enriched'
           AND (tracker_at IS NULL OR tracker_at <= ?1)
         ORDER BY COALESCE(tracker_at, 0) ASC, enriched_at ASC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![stale_before as i64, limit as i64], |row| {
        let bytes: Vec<u8> = row.get(0)?;
        Ok(bytes)
    })?;
    parse_hash_rows(rows)
}

pub fn update_tracker_stats(
    conn: &Connection,
    info_hash: &InfoHash,
    seeders: u32,
    leechers: u32,
    tracker_at: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET
            seeders = ?2,
            leechers = ?3,
            tracker_at = ?4,
            indexed_at = NULL
         WHERE info_hash = ?1",
        params![
            info_hash.0.as_slice(),
            seeders as i64,
            leechers as i64,
            tracker_at as i64
        ],
    )?;
    Ok(())
}

pub fn update_tracker_stats_batch(
    conn: &Connection,
    updates: &[(InfoHash, u32, u32, u64)],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    for (hash, seeders, leechers, tracker_at) in updates {
        update_tracker_stats(&tx, hash, *seeders, *leechers, *tracker_at)?;
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TorrentMetadata, unix_now};

    #[test]
    fn tracker_stats_batch_updates_rows() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let conn = super::super::schema::open_write(file.path()).unwrap();
        let hash = InfoHash([8u8; 20]);
        super::super::torrent::insert_discovered(&conn, &hash).unwrap();
        super::super::torrent::save_enriched(
            &conn,
            &hash,
            &TorrentMetadata {
                name: "tracked".into(),
                files: vec![],
                total_length: 1,
            },
        )
        .unwrap();
        let now = unix_now();
        update_tracker_stats_batch(&conn, &[(hash, 12, 3, now)]).unwrap();
        let record = super::super::torrent::get_record(&conn, &hash).unwrap().unwrap();
        assert_eq!(record.seeders, 12);
        assert_eq!(record.leechers, 3);
    }
}
