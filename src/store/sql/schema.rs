use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

pub struct SqlitePragmas {
    pub cache_size_kb: i32,
    pub mmap_size_bytes: i64,
}

impl Default for SqlitePragmas {
    fn default() -> Self {
        Self {
            cache_size_kb: -64000,
            mmap_size_bytes: 256 * 1024 * 1024,
        }
    }
}

pub fn open_write(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path).context("open database")?;
    init_schema(&conn)?;
    Ok(conn)
}

pub fn open_write_with_pragmas(path: &Path, pragmas: &SqlitePragmas) -> Result<Connection> {
    let conn = open_write(path)?;
    apply_pragmas(&conn, pragmas)?;
    Ok(conn)
}

pub fn open_read(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("open read database")?;
    Ok(conn)
}

pub fn apply_pragmas(conn: &Connection, pragmas: &SqlitePragmas) -> Result<()> {
    conn.execute_batch(&format!(
        "PRAGMA cache_size = {};
         PRAGMA mmap_size = {};",
        pragmas.cache_size_kb, pragmas.mmap_size_bytes
    ))?;
    Ok(())
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;

        CREATE TABLE IF NOT EXISTS torrents (
            info_hash      BLOB PRIMARY KEY,
            status         TEXT NOT NULL,
            name           TEXT,
            files_json     TEXT,
            total_length   INTEGER,
            content_type   TEXT,
            tags_json      TEXT,
            seeders        INTEGER NOT NULL DEFAULT 0,
            leechers       INTEGER NOT NULL DEFAULT 0,
            attempt_count  INTEGER NOT NULL DEFAULT 0,
            discovered_at  INTEGER NOT NULL,
            enriched_at    INTEGER,
            next_retry_at  INTEGER,
            tracker_at     INTEGER,
            indexed_at     INTEGER,
            peer_lookup_at INTEGER,
            quality_peer_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
        CREATE INDEX IF NOT EXISTS idx_torrents_retry ON torrents(next_retry_at)
            WHERE status IN ('discovered', 'failed');
        CREATE INDEX IF NOT EXISTS idx_torrents_index_queue ON torrents(status, enriched_at)
            WHERE status = 'enriched';
        CREATE INDEX IF NOT EXISTS idx_torrents_scrape ON torrents(status, tracker_at)
            WHERE status = 'enriched';

        CREATE TABLE IF NOT EXISTS peers (
            info_hash   BLOB NOT NULL,
            ip          BLOB NOT NULL,
            port        INTEGER NOT NULL,
            source      TEXT NOT NULL,
            seen_at     INTEGER NOT NULL,
            fail_count  INTEGER NOT NULL DEFAULT 0,
            last_fail_at INTEGER,
            PRIMARY KEY (info_hash, ip, port)
        );

        CREATE INDEX IF NOT EXISTS idx_peers_hash ON peers(info_hash);
        CREATE INDEX IF NOT EXISTS idx_peers_seen ON peers(seen_at);
        ",
    )?;
    migrate_columns(conn)?;
    Ok(())
}

fn migrate_columns(conn: &Connection) -> Result<()> {
    let _ = conn.execute(
        "ALTER TABLE torrents ADD COLUMN peer_lookup_at INTEGER",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE torrents ADD COLUMN quality_peer_count INTEGER NOT NULL DEFAULT 0",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE peers ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE peers ADD COLUMN last_fail_at INTEGER",
        [],
    );
    conn.execute(
        "UPDATE torrents SET quality_peer_count = (
            SELECT COUNT(*) FROM peers p
            WHERE p.info_hash = torrents.info_hash
              AND p.source IN ('announce', 'tracker', 'dht')
              AND p.fail_count < 3
         )
         WHERE quality_peer_count = 0",
        [],
    )?;
    Ok(())
}

pub fn reset_stuck_enriching(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE torrents SET status = 'discovered'
         WHERE status = 'enriching'",
        [],
    )?;
    Ok(())
}

pub fn initialize_store(conn: &Connection) -> Result<()> {
    reset_stuck_enriching(conn)?;
    if let Ok(count) = super::torrent::reset_failed_without_quality_peers(conn) {
        if count > 0 {
            tracing::info!(count, "reset failed torrents without quality peers");
        }
    }
    Ok(())
}
