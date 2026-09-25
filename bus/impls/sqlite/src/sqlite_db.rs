/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result as AnyhowResult;
use rusqlite::Connection;
use rusqlite::Result;
use rusqlite::Row;

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// A cloneable handle to a shared SQLite connection.
///
/// Runtime operations run on Tokio's blocking pool and serialize access to the
/// connection.
#[derive(Clone)]
pub struct SqliteDb {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDb {
    /// Open (or create) a SQLite database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn connection(&self) -> AnyhowResult<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite connection lock is poisoned"))
    }

    /// Execute a statement synchronously during initialization.
    pub fn execute_blocking<P>(&self, sql: &str, params: P) -> AnyhowResult<usize>
    where
        P: rusqlite::Params,
    {
        self.connection()?.execute(sql, params).map_err(Into::into)
    }

    async fn run<T, F>(&self, operation: F) -> AnyhowResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("SQLite connection lock is poisoned"))?;
            let result = catch_unwind(AssertUnwindSafe(|| operation(&conn)));
            // Release the guard normally before propagating a panic so one failed
            // operation does not poison the shared connection for future callers.
            drop(conn);
            match result {
                Ok(result) => result.map_err(Into::into),
                Err(payload) => resume_unwind(payload),
            }
        })
        .await
        .context("SQLite blocking task failed")?
    }

    /// Execute a statement on the blocking pool.
    pub async fn execute<P>(&self, sql: &'static str, params: P) -> AnyhowResult<usize>
    where
        P: rusqlite::Params + Send + 'static,
    {
        self.run(move |conn| conn.execute(sql, params)).await
    }

    /// Query rows on the blocking pool and map each with the provided closure.
    pub async fn query_rows<T, P, F>(
        &self,
        sql: &'static str,
        params: P,
        mut map: F,
    ) -> AnyhowResult<Vec<T>>
    where
        T: Send + 'static,
        P: rusqlite::Params + Send + 'static,
        F: FnMut(&Row<'_>) -> Result<T> + Send + 'static,
    {
        self.run(move |conn| {
            let mut statement = conn.prepare(sql)?;
            let rows = statement.query_map(params, &mut map)?;
            rows.collect()
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = SqliteDb::open(&path).unwrap();
        drop(db);

        assert!(
            path.exists(),
            "Database file should be created after opening"
        );
    }

    #[test]
    fn durable_wal_configuration_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();

        let conn = db.connection().unwrap();
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        let busy_timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode, "wal", "file storage should use WAL mode");
        assert_eq!(
            synchronous, 2,
            "FULL synchronous mode should have SQLite value 2"
        );
        assert_eq!(
            busy_timeout,
            BUSY_TIMEOUT.as_millis() as i64,
            "connection should use the configured busy timeout"
        );
    }

    #[tokio::test]
    async fn operation_panic_does_not_poison_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();

        db.run::<(), _>(|_| panic!("test panic"))
            .await
            .expect_err("blocking task should report the panic");

        db.execute("CREATE TABLE after_panic (value INTEGER)", [])
            .await
            .expect("connection should remain usable after an operation panic");
    }
}
