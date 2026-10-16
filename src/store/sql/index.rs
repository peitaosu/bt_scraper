use crate::{InfoHash, TorrentMetadata, unix_now};
use anyhow::Result;
use rusqlite::{params, Connection};

use super::super::IndexCandidate;

pub fn count_indexed(conn: &Connection) -> Result<u64> {
    let indexed: i64 = conn.query_row(
        "SELECT COUNT(*) FROM torrents WHERE indexed_at IS NOT NULL",
        [],
        |row| row.get(0),
    )?;
    Ok(indexed as u64)
}

pub fn get_index_candidates(conn: &Connection, limit: usize) -> Result<Vec<IndexCandidate>> {
    let mut stmt = conn.prepare(
        "SELECT info_hash, name, files_json, total_length, seeders, leechers,
                discovered_at, enriched_at
         FROM torrents
         WHERE status = 'enriched'
           AND name IS NOT NULL
           AND (
             indexed_at IS NULL
             OR (tracker_at IS NOT NULL AND tracker_at > indexed_at)
           )
         ORDER BY enriched_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit as i64], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    let mut candidates = Vec::new();
    for row in rows {
        let (
            bytes,
            name,
            files_json,
            total_length,
            seeders,
            leechers,
            discovered_at,
            enriched_at,
        ) = row?;
        if bytes.len() != 20 {
            continue;
        }
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        let files = files_json
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default();
        candidates.push(IndexCandidate {
            info_hash: InfoHash(arr),
            metadata: TorrentMetadata {
                name,
                files,
                total_length: total_length as u64,
            },
            seeders: seeders as u32,
            leechers: leechers as u32,
            discovered_at: discovered_at as u64,
            enriched_at: enriched_at as u64,
        });
    }
    Ok(candidates)
}

pub fn mark_indexed(
    conn: &Connection,
    info_hash: &InfoHash,
    content_type: &str,
    tags: &[String],
) -> Result<()> {
    let now = unix_now() as i64;
    let tags_json = serde_json::to_string(tags)?;
    conn.execute(
        "UPDATE torrents SET
            content_type = ?2,
            tags_json = ?3,
            indexed_at = ?4
         WHERE info_hash = ?1",
        params![info_hash.0.as_slice(), content_type, tags_json, now],
    )?;
    Ok(())
}

pub fn clear_all_indexed_flags(conn: &Connection) -> Result<u64> {
    let changed =
        conn.execute("UPDATE torrents SET indexed_at = NULL WHERE indexed_at IS NOT NULL", [])?;
    Ok(changed as u64)
}
