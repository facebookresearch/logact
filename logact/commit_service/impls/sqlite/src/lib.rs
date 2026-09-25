/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! SQLite-backed CommitService storage.

use std::path::Path;

use agentbus_sqlite::SqliteDb;
use anyhow::Error as AnyhowError;
use bytes::Bytes;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::StorageResult;
use rusqlite::ErrorCode;

/// CommitService storage backed by SQLite.
pub struct SqliteStorage {
    db: SqliteDb,
}

impl SqliteStorage {
    /// Open or create a file-backed database.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = SqliteDb::open(path.as_ref())?;
        db.execute_blocking(
            "CREATE TABLE IF NOT EXISTS logact_commit_service_storage (
                storage_key TEXT PRIMARY KEY NOT NULL,
                position    INTEGER NOT NULL,
                value       BLOB NOT NULL
            )",
            [],
        )?;
        Ok(Self { db })
    }
}

fn storage_error(error: AnyhowError, operation: &'static str) -> StorageError {
    let is_conflict = match error.downcast_ref::<rusqlite::Error>() {
        Some(rusqlite::Error::SqliteFailure(failure, _)) => failure.code == ErrorCode::DatabaseBusy,
        _ => false,
    };
    let source = error.context(operation);
    if is_conflict {
        StorageError::TransactionConflict(source)
    } else {
        StorageError::BackendUnavailable(source)
    }
}

#[async_trait::async_trait(?Send)]
impl Storage for SqliteStorage {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        let rows = self
            .db
            .query_rows(
                "SELECT value, position FROM logact_commit_service_storage
                 WHERE storage_key = ?1",
                (key.to_owned(),),
                |row| {
                    let value: Vec<u8> = row.get(0)?;
                    let position = row.get(1)?;
                    Ok((Bytes::from(value), position))
                },
            )
            .await
            .map_err(|error| storage_error(error, "SQLite get failed"))?;

        Ok(rows.into_iter().next())
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        let rows_changed = match expected {
            None => {
                self.db
                    .execute(
                        "INSERT INTO logact_commit_service_storage (storage_key, position, value)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(storage_key) DO NOTHING",
                        (key.to_owned(), new_position, value.to_vec()),
                    )
                    .await
            }
            Some(expected) if new_position <= expected => return Ok(false),
            Some(expected) => {
                self.db
                    .execute(
                        "UPDATE logact_commit_service_storage SET position = ?1, value = ?2
                         WHERE storage_key = ?3 AND position = ?4",
                        (new_position, value.to_vec(), key.to_owned(), expected),
                    )
                    .await
            }
        }
        .map_err(|error| storage_error(error, "SQLite put failed"))?;

        Ok(rows_changed == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn data_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit-service.db");

        {
            let storage = SqliteStorage::open(&path).unwrap();
            assert!(
                storage
                    .put("key", Bytes::from_static(b"persisted"), None, 7)
                    .await
                    .unwrap()
            );
        }

        {
            let storage = SqliteStorage::open(&path).unwrap();
            assert_eq!(
                storage.get("key").await.unwrap(),
                Some((Bytes::from_static(b"persisted"), 7))
            );
        }
    }

    #[tokio::test]
    async fn independent_connections_coordinate_compare_and_swap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit-service.db");
        let first = SqliteStorage::open(&path).unwrap();
        let second = SqliteStorage::open(&path).unwrap();

        assert!(
            first
                .put("key", Bytes::from_static(b"initial"), None, 0)
                .await
                .unwrap()
        );

        let (first_result, second_result) = tokio::join!(
            first.put("key", Bytes::from_static(b"first"), Some(0), 1),
            second.put("key", Bytes::from_static(b"second"), Some(0), 2),
        );
        let first_won = first_result.unwrap();
        let second_won = second_result.unwrap();
        assert_ne!(first_won, second_won, "exactly one CAS should succeed");

        let expected = if first_won {
            (Bytes::from_static(b"first"), 1)
        } else {
            (Bytes::from_static(b"second"), 2)
        };
        assert_eq!(first.get("key").await.unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn writer_lock_is_transaction_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commit-service.db");
        let storage = SqliteStorage::open(&path).unwrap();
        storage
            .db
            .query_rows("PRAGMA busy_timeout = 0", [], |_| Ok(()))
            .await
            .unwrap();

        let blocker = rusqlite::Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let error = storage
            .put("key", Bytes::from_static(b"blocked"), None, 0)
            .await
            .unwrap_err();
        assert!(
            matches!(error, StorageError::TransactionConflict(_)),
            "a competing writer should produce a transaction conflict"
        );

        blocker.execute_batch("ROLLBACK").unwrap();
        assert!(
            storage
                .put("key", Bytes::from_static(b"written"), None, 0)
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
                storage_error(AnyhowError::new(error), "SQLite operation failed"),
                StorageError::BackendUnavailable(_)
            ),
            "a connection-local lock should not be treated as ordinary writer contention"
        );
    }
}
