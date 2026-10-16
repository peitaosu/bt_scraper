use anyhow::{Context, Result};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OpenFlags;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct ReadPool {
    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl ReadPool {
    pub fn open(path: &Path, pool_size: u32) -> Result<Arc<Self>> {
        let manager = SqliteConnectionManager::file(path).with_flags(
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        );
        let pool = r2d2::Pool::builder()
            .max_size(pool_size.max(1))
            .build(manager)
            .context("build sqlite read pool")?;
        Ok(Arc::new(Self { pool }))
    }

    pub fn path_hint(&self) -> PathBuf {
        PathBuf::new()
    }

    pub fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<T>,
    {
        let conn = self.pool.get().context("acquire read connection")?;
        f(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InfoHash, store::sql};

    #[test]
    fn pool_serves_parallel_reads() {
        let file = tempfile::NamedTempFile::new().unwrap();
        {
            let conn = sql::open_write(file.path()).unwrap();
            sql::initialize_store(&conn).unwrap();
            sql::process_discovery_batch(&conn, &[(InfoHash([50u8; 20]), None)]).unwrap();
        }
        let pool = ReadPool::open(file.path(), 4).unwrap();
        for _ in 0..12 {
            let total = pool
                .with_conn(|conn| sql::count_by_status(conn).map(|counts| counts.0))
                .unwrap();
            assert_eq!(total, 1);
        }
    }
}
