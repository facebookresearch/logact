/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! FaultInjectingWriteOnceSpace: A wrapper that injects faults into any WriteOnceSpace implementation

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use agentbus_api::environment::Environment;
use anyhow::anyhow;
use bytes::Bytes;
use rand::distr::Distribution;
use rand::distr::Uniform;

use crate::common::fault_config::Fate;
use crate::common::fault_config::FaultConfig;
use crate::simulator::Simulator;

/// Injects one transaction conflict, optionally occupying the attempted slot first.
#[derive(Clone)]
pub struct ConflictOnceWriteOnceSpace<W> {
    inner: W,
    conflict_next_write: Rc<Cell<bool>>,
    occupy_conflicting_slot: bool,
}

impl<W> ConflictOnceWriteOnceSpace<W> {
    pub fn new(inner: W, occupy_conflicting_slot: bool) -> Self {
        Self {
            inner,
            conflict_next_write: Rc::new(Cell::new(true)),
            occupy_conflicting_slot,
        }
    }
}

impl<W: WriteOnceSpace> TailableSpace for ConflictOnceWriteOnceSpace<W> {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        self.inner.tail(space_id, window_size).await
    }
}

impl<W: WriteOnceSpace> WriteOnceSpace for ConflictOnceWriteOnceSpace<W> {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        if self.conflict_next_write.replace(false) {
            if self.occupy_conflicting_slot {
                self.inner.write(space_id, address, value.clone()).await?;
            }
            return Err(WriteOnceError::TransactionConflict(anyhow!(
                "injected transaction conflict"
            )));
        }

        self.inner.write(space_id, address, value).await
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        self.inner.read(space_id, address).await
    }
}

/// FaultInjectingWriteOnceSpace - wraps any WriteOnceSpace with fault injection and optional latency
pub struct FaultInjectingWriteOnceSpace<
    W: WriteOnceSpace + Clone,
    LatencyMs: Distribution<u64> = Uniform<u64>,
> {
    inner: W,
    fault_config: FaultConfig,
    write_latency_ms: LatencyMs,
    read_latency_ms: LatencyMs,
    env: Rc<Simulator>,
}

impl<W: WriteOnceSpace + Clone, LatencyMs: Distribution<u64> + Clone> Clone
    for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault_config: self.fault_config.clone(),
            write_latency_ms: self.write_latency_ms.clone(),
            read_latency_ms: self.read_latency_ms.clone(),
            env: self.env.clone(),
        }
    }
}

impl<W: WriteOnceSpace + Clone, LatencyMs: Distribution<u64>>
    FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    pub fn new(
        inner: W,
        fault_config: FaultConfig,
        write_latency_ms: LatencyMs,
        read_latency_ms: LatencyMs,
        env: Rc<Simulator>,
    ) -> Result<Self, String> {
        fault_config.validate()?;
        Ok(Self {
            inner,
            fault_config,
            write_latency_ms,
            read_latency_ms,
            env,
        })
    }

    fn determine_fate(&self) -> Fate {
        self.env
            .with_rng(|rng| self.fault_config.determine_fate(rng))
    }
}

impl<W: WriteOnceSpace + Clone + 'static, LatencyMs: Distribution<u64> + Clone + 'static>
    TailableSpace for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let ms = self.env.with_rng(|rng| self.read_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.tail(space_id, window_size).await
    }
}

impl<W: WriteOnceSpace + Clone + 'static, LatencyMs: Distribution<u64> + Clone + 'static>
    WriteOnceSpace for FaultInjectingWriteOnceSpace<W, LatencyMs>
{
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let ms = self.env.with_rng(|rng| self.write_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        match self.determine_fate() {
            Fate::Success => self.inner.write(space_id, address, value).await,
            Fate::Lost => Err(WriteOnceError::BackendUnavailable(anyhow!("Lost"))),
            Fate::CommitThenError => {
                let _ = self.inner.write(space_id, address, value).await;
                Err(WriteOnceError::BackendUnavailable(anyhow!(
                    "CommitThenError"
                )))
            }
            Fate::ErrorThenCommit => {
                let mut inner_clone = self.inner.clone();
                let space_id = space_id.to_string();
                self.env.spawn(async move {
                    let _ = inner_clone.write(&space_id, address, value).await;
                });
                Err(WriteOnceError::BackendUnavailable(anyhow!(
                    "ErrorThenCommit"
                )))
            }
        }
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        let ms = self.env.with_rng(|rng| self.read_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.read(space_id, address).await
    }
}
