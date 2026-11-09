/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RetryingWriteOnceSpace: retries transient failures of a WriteOnceSpace.

use std::cell::Cell;
use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use agentbus_api::retry;
use bytes::Bytes;

/// Retries transient failures (`Timeout`, `BackendUnavailable`) of the wrapped space.
///
/// A failed write attempt may still have landed, so a retry that then finds the
/// address taken returns `AmbiguousWrite` rather than `AddressAlreadyExists`.
pub struct RetryingWriteOnceSpace<W, E> {
    inner: W,
    environment: Rc<E>,
    config: RetryConfig,
}

impl<W: Clone, E> Clone for RetryingWriteOnceSpace<W, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            environment: self.environment.clone(),
            config: self.config,
        }
    }
}

impl<W, E> RetryingWriteOnceSpace<W, E> {
    pub fn new(inner: W, environment: Rc<E>, config: RetryConfig) -> Self {
        Self {
            inner,
            environment,
            config,
        }
    }
}

fn is_transient(error: &WriteOnceError) -> bool {
    match error {
        WriteOnceError::Timeout(_) | WriteOnceError::BackendUnavailable(_) => true,
        WriteOnceError::AddressAlreadyExists(_)
        | WriteOnceError::AmbiguousWrite(_)
        | WriteOnceError::InternalError(_) => false,
    }
}

fn decide(error: &WriteOnceError) -> RetryDecision {
    if is_transient(error) {
        RetryDecision::RetryWithBackoff
    } else {
        RetryDecision::Stop
    }
}

impl<W: TailableSpace, E: Environment> TailableSpace for RetryingWriteOnceSpace<W, E> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.tail(space_id, window_size),
            |error| match error {
                TailError::BackendUnavailable(_) => RetryDecision::RetryWithBackoff,
                TailError::ZeroWindowSize => RetryDecision::Stop,
            },
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }
}

impl<W: WriteOnceSpace + Clone, E: Environment> WriteOnceSpace for RetryingWriteOnceSpace<W, E> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let may_have_landed = Cell::new(false);
        retry(
            &*self.environment,
            self.config,
            || {
                let mut inner = self.inner.clone();
                let (value, may_have_landed) = (value.clone(), &may_have_landed);
                async move {
                    match inner.write(space_id, address, value).await {
                        Err(WriteOnceError::AddressAlreadyExists(_)) if may_have_landed.get() => {
                            Err(WriteOnceError::AmbiguousWrite(address))
                        }
                        Err(error) => {
                            may_have_landed.set(may_have_landed.get() || is_transient(&error));
                            Err(error)
                        }
                        Ok(()) => Ok(()),
                    }
                }
            },
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        retry(
            &*self.environment,
            self.config,
            || self.inner.read(space_id, address),
            decide,
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }
}
