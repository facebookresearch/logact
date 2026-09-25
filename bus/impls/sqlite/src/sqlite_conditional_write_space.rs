/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_api::ConditionalWriteError;
use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use anyhow::Error as AnyhowError;
use anyhow::anyhow;
use bytes::Bytes;
use rusqlite::ErrorCode;
use rusqlite::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::ValueRef;

use crate::sqlite_db::SqliteDb;

struct SqliteBlob(Bytes);

impl ToSql for SqliteBlob {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.0.as_ref())))
    }
}

/// A ConditionalWriteSpace backed by SQLite.
///
/// All clones share the same underlying connection via the `SqliteDb` handle.
#[derive(Clone)]
pub struct SqliteConditionalWriteSpace {
    db: SqliteDb,
}

impl SqliteConditionalWriteSpace {
    /// Create a `SqliteConditionalWriteSpace`, initializing the backing table if needed.
    pub fn new(db: &SqliteDb) -> anyhow::Result<Self> {
        db.execute_blocking(
            "CREATE TABLE IF NOT EXISTS conditional_write (
                space_id TEXT    NOT NULL,
                address  INTEGER NOT NULL,
                version  INTEGER NOT NULL,
                val      BLOB    NOT NULL,
                PRIMARY KEY (space_id, address)
            ) WITHOUT ROWID",
            [],
        )?;

        Ok(Self { db: db.clone() })
    }
}

/// SQLite's INTEGER type is a signed 64-bit integer, so addresses >= 2^63
/// cannot be stored. Reject them early with a clear error instead of silently
/// wrapping to a negative value.
fn to_sqlite_address(address: u64) -> ConditionalWriteResult<i64> {
    i64::try_from(address).map_err(|_| {
        ConditionalWriteError::InternalError(anyhow!(
            "address {address} exceeds SQLite signed 64-bit integer range"
        ))
    })
}

fn version_from_u64(n: u64) -> Version {
    Version(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn try_version_to_u64(v: &Version) -> Option<u64> {
    let bytes: [u8; 8] = v.0[..].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn write_error(error: AnyhowError) -> ConditionalWriteError {
    let is_conflict = match error.downcast_ref::<rusqlite::Error>() {
        Some(rusqlite::Error::SqliteFailure(failure, _)) => failure.code == ErrorCode::DatabaseBusy,
        _ => false,
    };
    if is_conflict {
        ConditionalWriteError::TransactionConflict(error)
    } else {
        ConditionalWriteError::BackendUnavailable(error)
    }
}

impl TailableSpace for SqliteConditionalWriteSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let addrs: Vec<u64> = self
            .db
            .query_rows(
                "SELECT address FROM conditional_write WHERE space_id = ?1
                 ORDER BY address DESC LIMIT ?2",
                (space_id.to_owned(), window_size as i64),
                |row| row.get::<_, i64>(0).map(|address| address as u64),
            )
            .await
            .map_err(|e| TailError::BackendUnavailable(e.to_string()))?;

        let Some(&max_addr) = addrs.first() else {
            return Ok(0);
        };
        let end = max_addr + 1;

        let written: std::collections::HashSet<u64> = addrs.into_iter().collect();
        let window_start = end.saturating_sub(window_size);
        Ok((window_start..end)
            .find(|addr| !written.contains(addr))
            .unwrap_or(end))
    }
}

impl ConditionalWriteSpace for SqliteConditionalWriteSpace {
    async fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> ConditionalWriteResult<bool> {
        let addr = to_sqlite_address(address)?;
        let rows_changed = match expected_version {
            None => {
                // Write to empty address — INSERT only if row does not exist.
                self.db
                    .execute(
                        "INSERT OR IGNORE INTO conditional_write (space_id, address, version, val)
                         VALUES (?1, ?2, 0, ?3)",
                        (space_id.to_owned(), addr, SqliteBlob(value)),
                    )
                    .await
                    .map_err(write_error)?
            }
            Some(v) => {
                let current = match try_version_to_u64(&v) {
                    Some(n) => n,
                    None => {
                        return Err(ConditionalWriteError::InternalError(anyhow!(
                            "invalid version: expected 8 bytes, got {}",
                            v.0.len()
                        )));
                    }
                };
                let new_version = current + 1;
                // CAS update — only update if the current version matches.
                self.db
                    .execute(
                        "UPDATE conditional_write SET version = ?1, val = ?2
                         WHERE space_id = ?3 AND address = ?4 AND version = ?5",
                        (
                            new_version as i64,
                            SqliteBlob(value),
                            space_id.to_owned(),
                            addr,
                            current as i64,
                        ),
                    )
                    .await
                    .map_err(write_error)?
            }
        };

        Ok(rows_changed > 0)
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        let addr = match to_sqlite_address(address) {
            Ok(a) => a,
            Err(_) => return Ok(None),
        };

        let rows: Vec<VersionedValue> = self
            .db
            .query_rows(
                "SELECT version, val FROM conditional_write
                 WHERE space_id = ?1 AND address = ?2",
                (space_id.to_owned(), addr),
                |row| {
                    let version: i64 = row.get(0)?;
                    let val: Vec<u8> = row.get(1)?;
                    Ok(VersionedValue {
                        version: version_from_u64(version as u64),
                        value: Bytes::from(val),
                    })
                },
            )
            .await
            .map_err(ConditionalWriteError::BackendUnavailable)?;

        Ok(rows.into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    use bytes::Bytes;
    use rusqlite::Connection;

    use super::*;
    use crate::sqlite_db::SqliteDb;

    #[tokio::test]
    async fn write_none_succeeds_on_empty_address() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();

        let wrote = space
            .write("s", 0, None, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert!(wrote, "First write to empty address should succeed");

        let val = space.read("s", 0).await.unwrap().unwrap();
        assert_eq!(val.version, version_from_u64(0));
        assert_eq!(val.value, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn write_none_fails_on_occupied_address() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();

        space
            .write("s", 0, None, Bytes::from_static(b"first"))
            .await
            .unwrap();

        let wrote = space
            .write("s", 0, None, Bytes::from_static(b"second"))
            .await
            .unwrap();
        assert!(
            !wrote,
            "Second write with None should fail on occupied address"
        );
    }

    #[tokio::test]
    async fn cas_update_succeeds_with_correct_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();

        space
            .write("s", 0, None, Bytes::from_static(b"v1"))
            .await
            .unwrap();

        let v0 = space.read("s", 0).await.unwrap().unwrap().version;
        let wrote = space
            .write("s", 0, Some(v0), Bytes::from_static(b"v2"))
            .await
            .unwrap();
        assert!(wrote, "CAS with matching version should succeed");

        let val = space.read("s", 0).await.unwrap().unwrap();
        assert_eq!(
            val.version,
            version_from_u64(1),
            "Version should increment to 1 after second write"
        );
        assert_eq!(val.value, Bytes::from_static(b"v2"));
    }

    #[tokio::test]
    async fn cas_update_fails_with_wrong_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();

        space
            .write("s", 0, None, Bytes::from_static(b"v1"))
            .await
            .unwrap();

        let wrote = space
            .write(
                "s",
                0,
                Some(version_from_u64(99)),
                Bytes::from_static(b"wrong"),
            )
            .await
            .unwrap();
        assert!(!wrote, "CAS with wrong version should fail");
    }

    #[tokio::test]
    async fn cas_update_returns_error_with_bogus_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("test.db")).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();

        space
            .write("s", 0, None, Bytes::from_static(b"v1"))
            .await
            .unwrap();

        let result = space
            .write(
                "s",
                0,
                Some(Version(Bytes::from("bogus"))),
                Bytes::from_static(b"wrong"),
            )
            .await;
        assert!(
            matches!(result, Err(ConditionalWriteError::InternalError(_))),
            "CAS with non-u64 version should return InternalError, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn data_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let db = SqliteDb::open(&path).unwrap();
            let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();
            space
                .write("s", 0, None, Bytes::from_static(b"persisted"))
                .await
                .unwrap();
        }

        {
            let db = SqliteDb::open(&path).unwrap();
            let space = SqliteConditionalWriteSpace::new(&db).unwrap();
            let val = space.read("s", 0).await.unwrap().unwrap();
            assert_eq!(
                val.value,
                Bytes::from_static(b"persisted"),
                "Data should persist across database reopens"
            );
        }
    }

    #[test]
    fn independent_connections_coordinate_compare_and_swap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = SqliteDb::open(&path).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime
            .block_on(space.write("s", 0, None, Bytes::from_static(b"initial")))
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let run_write = |value: &'static [u8]| {
            let path = path.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let db = SqliteDb::open(&path).unwrap();
                let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                barrier.wait();
                runtime
                    .block_on(space.write(
                        "s",
                        0,
                        Some(version_from_u64(0)),
                        Bytes::from_static(value),
                    ))
                    .unwrap()
            })
        };

        let first = run_write(b"first");
        let second = run_write(b"second");
        let first_won = first.join().unwrap();
        let second_won = second.join().unwrap();
        assert_ne!(first_won, second_won, "exactly one CAS should succeed");

        let expected = if first_won {
            Bytes::from_static(b"first")
        } else {
            Bytes::from_static(b"second")
        };
        let stored = runtime.block_on(space.read("s", 0)).unwrap().unwrap();
        assert_eq!(stored.version, version_from_u64(1));
        assert_eq!(stored.value, expected);
    }

    #[tokio::test]
    async fn writer_lock_is_transaction_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = SqliteDb::open(&path).unwrap();
        let mut space = SqliteConditionalWriteSpace::new(&db).unwrap();
        db.query_rows("PRAGMA busy_timeout = 0", [], |_| Ok(()))
            .await
            .unwrap();

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let error = space
            .write("s", 0, None, Bytes::from_static(b"blocked"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConditionalWriteError::TransactionConflict(_)),
            "a competing writer should produce a transaction conflict"
        );

        blocker.execute_batch("ROLLBACK").unwrap();
        assert!(
            space
                .write("s", 0, None, Bytes::from_static(b"written"))
                .await
                .unwrap()
        );
    }

    #[test]
    fn locked_errors_are_backend_unavailable() {
        let error = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_LOCKED),
            None,
        );
        assert!(
            matches!(
                write_error(AnyhowError::new(error)),
                ConditionalWriteError::BackendUnavailable(_)
            ),
            "a connection-local lock should not be treated as ordinary writer contention"
        );
    }

    #[test]
    fn to_sqlite_address_accepts_valid_values() {
        assert_eq!(to_sqlite_address(0).unwrap(), 0);
        assert_eq!(to_sqlite_address(1).unwrap(), 1);
        assert_eq!(to_sqlite_address(i64::MAX as u64).unwrap(), i64::MAX);
    }

    #[test]
    fn to_sqlite_address_rejects_overflow() {
        assert!(
            to_sqlite_address(i64::MAX as u64 + 1).is_err(),
            "First value beyond i64::MAX should be rejected"
        );
        assert!(
            to_sqlite_address(u64::MAX).is_err(),
            "u64::MAX should be rejected"
        );
    }
}
