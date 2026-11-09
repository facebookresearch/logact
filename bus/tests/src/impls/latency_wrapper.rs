/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! LatencyWrapper: injects the simulator's latency into the bottom-most
//! AgentBus, WriteOnceSpace, and ConditionalWriteSpace implementations.

use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::ConditionalWriteError;
use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use agentbus_api::traits::*;
use anyhow::anyhow;
use bytes::Bytes;

use crate::simulator::Simulator;

const TIMEOUT_MESSAGE: &str = "injected latency exceeded the client timeout";

/// Delays every request and response through the wrapped implementation, and
/// times out callers whose response is late.
pub struct LatencyWrapper<T> {
    inner: Rc<T>,
    env: Rc<Simulator>,
}

impl<T> LatencyWrapper<T> {
    pub fn new(inner: T, env: Rc<Simulator>) -> Self {
        Self {
            inner: Rc::new(inner),
            env,
        }
    }
}

impl<T: Clone> Clone for LatencyWrapper<T> {
    fn clone(&self) -> Self {
        Self::new((*self.inner).clone(), self.env.clone())
    }
}

fn bus_timeout<R>() -> BusResult<R> {
    Err(AgentBusError::Timeout(anyhow!(TIMEOUT_MESSAGE)))
}

fn write_once_timeout<R>() -> WriteOnceResult<R> {
    Err(WriteOnceError::Timeout(anyhow!(TIMEOUT_MESSAGE)))
}

fn conditional_write_timeout<R>() -> ConditionalWriteResult<R> {
    Err(ConditionalWriteError::Timeout(anyhow!(TIMEOUT_MESSAGE)))
}

impl<T: 'static> LatencyWrapper<T> {
    /// Issues `op` against a handle to the wrapped implementation.
    async fn call<R: 'static, F>(&self, op: impl FnOnce(Rc<T>) -> F) -> Option<R>
    where
        F: Future<Output = R> + 'static,
    {
        self.env.inject(op(self.inner.clone())).await
    }
}

impl<T: Clone + 'static> LatencyWrapper<T> {
    async fn call_mut<R: 'static, F>(&mut self, op: impl FnOnce(T) -> F) -> Option<R>
    where
        F: Future<Output = R> + 'static,
    {
        self.env.inject(op((*self.inner).clone())).await
    }
}

impl<T: AgentBus + 'static> AgentBus for LatencyWrapper<T> {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.call(async move |bus| bus.append(request).await)
            .await
            .unwrap_or_else(bus_timeout)
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        self.call(async move |bus| bus.poll(request).await)
            .await
            .unwrap_or_else(bus_timeout)
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        self.call(async move |bus| bus.read_next(request).await)
            .await
            .unwrap_or_else(bus_timeout)
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        self.call(async move |bus| bus.check_tail(request).await)
            .await
            .unwrap_or_else(bus_timeout)
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        let wait = Duration::from_millis(request.timeout_ms.max(0) as u64);
        let bus = self.inner.clone();
        self.env
            .inject_waiting(wait, async move { bus.blocking_poll(request).await })
            .await
            .unwrap_or_else(bus_timeout)
    }
}

impl<T: TailableSpace + 'static> TailableSpace for LatencyWrapper<T> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let space_id = space_id.to_owned();
        self.call(async move |space| space.tail(&space_id, window_size).await)
            .await
            .unwrap_or_else(|| Err(TailError::BackendUnavailable(TIMEOUT_MESSAGE.to_string())))
    }
}

impl<T: WriteOnceSpace + Clone + 'static> WriteOnceSpace for LatencyWrapper<T> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let space_id = space_id.to_owned();
        self.call_mut(async move |mut space| space.write(&space_id, address, value).await)
            .await
            .unwrap_or_else(write_once_timeout)
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        let space_id = space_id.to_owned();
        self.call(async move |space| space.read(&space_id, address).await)
            .await
            .unwrap_or_else(write_once_timeout)
    }
}

impl<T: ConditionalWriteSpace + Clone + 'static> ConditionalWriteSpace for LatencyWrapper<T> {
    async fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> ConditionalWriteResult<Option<Version>> {
        let space_id = space_id.to_owned();
        self.call_mut(async move |mut space| {
            space
                .write(&space_id, address, expected_version, value)
                .await
        })
        .await
        .unwrap_or_else(conditional_write_timeout)
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        let space_id = space_id.to_owned();
        self.call(async move |space| space.read(&space_id, address).await)
            .await
            .unwrap_or_else(conditional_write_timeout)
    }
}
