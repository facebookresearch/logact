/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Error as AnyhowError;
use bytes::Bytes;
use thiserror::Error;

/// Failure categories for storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("logical position must be nonnegative: {0}")]
    InvalidPosition(i64),
    #[error("transaction conflict")]
    TransactionConflict(#[source] AnyhowError),
    #[error("timeout")]
    Timeout(#[source] AnyhowError),
    #[error("backend unavailable")]
    BackendUnavailable(#[source] AnyhowError),
    #[error("internal error")]
    InternalError(#[source] AnyhowError),
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Key-value store with compare-and-swap and highest-position writes.
///
/// Every value is stored alongside a position (i64). `put` is a CAS operation:
/// it succeeds only if the current stored position matches `expected`, which
/// maps directly to conditional-write storage APIs.
#[async_trait::async_trait(?Send)]
pub trait Storage {
    /// Returns `Ok(Some((value, position)))` if the key exists, `Ok(None)` if
    /// absent, or `Err` on backend failure.
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>>;

    /// Compare-and-swap write. Succeeds only if both conditions hold:
    /// 1. The current position matches `expected`:
    ///    - `expected: None` — key must not exist
    ///    - `expected: Some(pos)` — current position must equal `pos`
    /// 2. `new_position` is strictly greater than the current position
    ///    (or the key is absent).
    ///
    /// On success, stores `value` at `new_position`. Returns `Ok(true)` on
    /// success, `Ok(false)` if either condition failed, or `Err` on backend failure.
    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool>;

    /// Atomically stores `value` at `new_position` if the key is absent or its
    /// stored position is strictly lower. Equal or higher positions leave the
    /// existing value unchanged. Returns `Ok(true)` if written, `Ok(false)` if
    /// unchanged, or `Err` on backend failure.
    ///
    /// Returns `InvalidPosition` for a negative `new_position`. Keys used with
    /// this method must contain only nonnegative positions, including writes
    /// through `put`. A timeout can leave the outcome unknown; retrying the same
    /// write may return `Ok(false)` after it landed.
    async fn put_if_newer(&self, key: &str, value: Bytes, new_position: i64)
    -> StorageResult<bool>;
}

/// `Rc<S>` is itself a `Storage`, delegating to the shared inner store. This lets
/// owners (e.g. the engine and its applicators) ref-count a single storage
/// instance instead of requiring `Storage` to be `Clone`.
#[async_trait::async_trait(?Send)]
impl<S: Storage + ?Sized> Storage for Rc<S> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        (**self).get(key).await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        (**self).put(key, value, expected, new_position).await
    }

    async fn put_if_newer(
        &self,
        key: &str,
        value: Bytes,
        new_position: i64,
    ) -> StorageResult<bool> {
        (**self).put_if_newer(key, value, new_position).await
    }
}

/// In-memory `Storage` backed by `RefCell<HashMap>`. Not `Clone`: share it by
/// wrapping in `Rc` (see the `Storage for Rc<S>` impl above).
#[derive(Default)]
pub struct InMemoryStorage {
    data: RefCell<HashMap<String, (Bytes, i64)>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait(?Send)]
impl Storage for InMemoryStorage {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        Ok(self.data.borrow().get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        let mut data = self.data.borrow_mut();
        let current = data.get(key).map(|(_, pos)| *pos);
        if current != expected {
            return Ok(false);
        }
        if let Some(pos) = current {
            if new_position <= pos {
                return Ok(false);
            }
        }
        data.insert(key.to_string(), (value, new_position));
        Ok(true)
    }

    async fn put_if_newer(
        &self,
        key: &str,
        value: Bytes,
        new_position: i64,
    ) -> StorageResult<bool> {
        if new_position < 0 {
            return Err(StorageError::InvalidPosition(new_position));
        }
        let mut data = self.data.borrow_mut();
        if data
            .get(key)
            .is_some_and(|(_, position)| *position >= new_position)
        {
            return Ok(false);
        }
        data.insert(key.to_owned(), (value, new_position));
        Ok(true)
    }
}

#[cfg(test)]
pub(crate) enum FaultyStorage {
    GetTimeout,
    PutUnavailable,
    PutConflict,
    RejectPut,
}

#[cfg(test)]
#[async_trait::async_trait(?Send)]
impl Storage for FaultyStorage {
    async fn get(&self, _key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        match self {
            Self::GetTimeout => Err(StorageError::Timeout(anyhow::anyhow!("get timed out"))),
            Self::PutUnavailable | Self::PutConflict | Self::RejectPut => Ok(None),
        }
    }

    async fn put(
        &self,
        _key: &str,
        _value: Bytes,
        _expected: Option<i64>,
        _new_position: i64,
    ) -> StorageResult<bool> {
        match self {
            Self::GetTimeout => Ok(true),
            Self::PutUnavailable => Err(StorageError::BackendUnavailable(anyhow::anyhow!(
                "put unavailable"
            ))),
            Self::PutConflict => Err(StorageError::TransactionConflict(anyhow::anyhow!(
                "put conflicted"
            ))),
            Self::RejectPut => Ok(false),
        }
    }

    async fn put_if_newer(
        &self,
        key: &str,
        value: Bytes,
        new_position: i64,
    ) -> StorageResult<bool> {
        if new_position < 0 {
            return Err(StorageError::InvalidPosition(new_position));
        }
        self.put(key, value, None, new_position).await
    }
}
