/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::rc::Rc;

use agentbus_simulator::Simulator;
use anyhow::anyhow;
use bytes::Bytes;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::StorageResult;

/// Injects the simulator's latency into every request and response through
/// the wrapped `Storage`, and times out callers whose response is late.
pub struct LatencyStorage<S> {
    inner: Rc<S>,
    env: Rc<Simulator>,
}

impl<S> LatencyStorage<S> {
    pub fn new(inner: S, env: Rc<Simulator>) -> Self {
        Self {
            inner: Rc::new(inner),
            env,
        }
    }
}

impl<S: 'static> LatencyStorage<S> {
    /// Issues `op` against a handle to the wrapped storage.
    async fn call<R: 'static, F>(&self, op: impl FnOnce(Rc<S>) -> F) -> StorageResult<R>
    where
        F: Future<Output = StorageResult<R>> + 'static,
    {
        self.env
            .inject(op(self.inner.clone()))
            .await
            .unwrap_or_else(|| {
                Err(StorageError::Timeout(anyhow!(
                    "injected latency exceeded the client timeout"
                )))
            })
    }
}

#[async_trait::async_trait(?Send)]
impl<S: Storage + 'static> Storage for LatencyStorage<S> {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        let key = key.to_owned();
        self.call(async move |storage| storage.get(&key).await)
            .await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        let key = key.to_owned();
        self.call(async move |storage| storage.put(&key, value, expected, new_position).await)
            .await
    }
}
