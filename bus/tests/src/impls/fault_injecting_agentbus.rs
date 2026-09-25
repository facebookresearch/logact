/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! FaultInjectingAgentBus: A wrapper that injects faults into any AgentBus implementation
//!
//! This wrapper takes any AgentBus and adds fault injection capabilities based on FaultConfig.

use std::rc::Rc;
use std::time::Duration;

use agentbus_api::environment::Environment;
use agentbus_api::traits::*;
use rand::distr::Distribution;
use rand::distr::Uniform;

use crate::common::fault_config::Fate;
use crate::common::fault_config::FaultConfig;
use crate::simulator::Simulator;

/// FaultInjectingAgentBus - wraps any AgentBus with fault injection and optional latency
pub struct FaultInjectingAgentBus<A: AgentBus, LatencyMs: Distribution<u64> = Uniform<u64>> {
    inner: Rc<A>,
    fault_config: FaultConfig,
    append_latency_ms: LatencyMs,
    poll_latency_ms: LatencyMs,
    env: Rc<Simulator>,
}

impl<A: AgentBus, LatencyMs: Distribution<u64> + Clone> Clone
    for FaultInjectingAgentBus<A, LatencyMs>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault_config: self.fault_config.clone(),
            append_latency_ms: self.append_latency_ms.clone(),
            poll_latency_ms: self.poll_latency_ms.clone(),
            env: self.env.clone(),
        }
    }
}

impl<A: AgentBus, LatencyMs: Distribution<u64>> FaultInjectingAgentBus<A, LatencyMs> {
    pub fn new(
        inner: A,
        fault_config: FaultConfig,
        append_latency_ms: LatencyMs,
        poll_latency_ms: LatencyMs,
        env: Rc<Simulator>,
    ) -> Result<Self, String> {
        fault_config.validate()?;
        Ok(Self {
            inner: Rc::new(inner),
            fault_config,
            append_latency_ms,
            poll_latency_ms,
            env,
        })
    }

    fn determine_fate(&self) -> Fate {
        self.env
            .with_rng(|rng| self.fault_config.determine_fate(rng))
    }
}

impl<A: AgentBus + 'static, LatencyMs: Distribution<u64> + Clone + 'static> AgentBus
    for FaultInjectingAgentBus<A, LatencyMs>
{
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let ms = self.env.with_rng(|rng| self.append_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        let fate = self.determine_fate();

        match fate {
            Fate::Success => self.inner.append(request).await,
            Fate::Lost => Err(AgentBusError::Unavailable(anyhow::anyhow!("Lost"))),
            Fate::CommitThenError => {
                let _ = self.inner.append(request).await;
                Err(AgentBusError::Unavailable(anyhow::anyhow!(
                    "CommitThenError"
                )))
            }
            Fate::ErrorThenCommit => {
                let inner_clone = self.inner.clone();
                self.env.spawn(async move {
                    let _ = inner_clone.append(request).await;
                });
                Err(AgentBusError::Unavailable(anyhow::anyhow!(
                    "ErrorThenCommit"
                )))
            }
        }
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let ms = self.env.with_rng(|rng| self.poll_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.poll(request).await
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        let ms = self.env.with_rng(|rng| self.poll_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.read_next(request).await
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let ms = self.env.with_rng(|rng| self.poll_latency_ms.sample(rng));
        self.env.sleep(Duration::from_millis(ms)).await;

        self.inner.check_tail(request).await
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        self.inner.blocking_poll(request).await
    }
}
