/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RetryingStorage: retries transient failures of a Storage.

use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use bytes::Bytes;

use crate::Storage;
use crate::StorageError;
use crate::StorageResult;

/// Retries `Timeout` and `BackendUnavailable` failures of the wrapped storage.
/// `TransactionConflict` is left to the caller's concurrency handling.
///
/// A failed `put` attempt may still have landed, so a retry can find its own
/// write and return `Ok(false)`. Callers already reload after any rejected
/// `put`, which reconciles with whichever write won.
pub struct RetryingStorage<S, E> {
    inner: S,
    environment: Rc<E>,
    config: RetryConfig,
}

impl<S, E> RetryingStorage<S, E> {
    pub fn new(inner: S, environment: Rc<E>, config: RetryConfig) -> Self {
        Self {
            inner,
            environment,
            config,
        }
    }
}

fn decide(error: &StorageError) -> RetryDecision {
    match error {
        StorageError::Timeout(_) | StorageError::BackendUnavailable(_) => {
            RetryDecision::RetryWithBackoff
        }
        StorageError::TransactionConflict(_) | StorageError::InternalError(_) => {
            RetryDecision::Stop
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage, E: Environment> Storage for RetryingStorage<S, E> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.get(key),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.put(key, value.clone(), expected, new_position),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }
}
