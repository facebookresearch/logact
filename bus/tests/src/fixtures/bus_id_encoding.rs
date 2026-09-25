/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test adapters that run the same AgentBus scenarios with legacy and typed IDs.

use std::marker::PhantomData;
use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::AppendRequest;
use agentbus_api::AppendResponse;
use agentbus_api::BlockingPollRequest;
use agentbus_api::BlockingPollResponse;
use agentbus_api::BusId;
use agentbus_api::BusResult;
use agentbus_api::CheckTailRequest;
use agentbus_api::CheckTailResponse;
use agentbus_api::PollRequest;
use agentbus_api::PollResponse;
use agentbus_api::ReadNextRequest;
use agentbus_api::ReadNextResponse;
use anyhow::Result;
use conformance::ConformanceFixture;
use fbinit::FacebookInit;

use super::IntegrationFixture;
use super::SimulatorFixture;
use crate::simulator::Simulator;

pub trait BusIdEncoding {
    fn rewrite(operation: &'static str, agent_bus_id: &mut String, bus_id: &mut Option<BusId>);
}

pub struct LegacyBusIdEncoding;

impl BusIdEncoding for LegacyBusIdEncoding {
    fn rewrite(_operation: &'static str, agent_bus_id: &mut String, bus_id: &mut Option<BusId>) {
        let typed = bus_id
            .take()
            .expect("AgentBus scenarios must populate the typed bus_id field");
        *agent_bus_id = typed.agent_bus_id;
    }
}

pub struct TypedBusIdEncoding;

impl BusIdEncoding for TypedBusIdEncoding {
    fn rewrite(operation: &'static str, agent_bus_id: &mut String, bus_id: &mut Option<BusId>) {
        assert!(
            bus_id.is_some(),
            "AgentBus scenarios must populate the typed bus_id field"
        );
        *agent_bus_id = format!("ignored-legacy-{operation}");
    }
}

pub struct BusIdEncodingAgentBus<B, M> {
    inner: B,
    mode: PhantomData<M>,
}

impl<B: Clone, M> Clone for BusIdEncodingAgentBus<B, M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            mode: PhantomData,
        }
    }
}

impl<B, M> BusIdEncodingAgentBus<B, M> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            mode: PhantomData,
        }
    }
}

impl<B: AgentBus, M: BusIdEncoding> AgentBus for BusIdEncodingAgentBus<B, M> {
    async fn append(&self, mut request: AppendRequest) -> BusResult<AppendResponse> {
        M::rewrite("append", &mut request.agent_bus_id, &mut request.bus_id);
        self.inner.append(request).await
    }

    async fn poll(&self, mut request: PollRequest) -> BusResult<PollResponse> {
        M::rewrite("poll", &mut request.agent_bus_id, &mut request.bus_id);
        self.inner.poll(request).await
    }

    async fn read_next(&self, mut request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        M::rewrite("read-next", &mut request.agent_bus_id, &mut request.bus_id);
        self.inner.read_next(request).await
    }

    async fn check_tail(&self, mut request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        M::rewrite("check-tail", &mut request.agent_bus_id, &mut request.bus_id);
        self.inner.check_tail(request).await
    }

    async fn blocking_poll(
        &self,
        mut request: BlockingPollRequest,
    ) -> BusResult<BlockingPollResponse> {
        M::rewrite(
            "blocking-poll",
            &mut request.agent_bus_id,
            &mut request.bus_id,
        );
        self.inner.blocking_poll(request).await
    }
}

pub struct BusIdEncodingFixture<F, M> {
    inner: F,
    mode: PhantomData<M>,
}

impl<F, M> ConformanceFixture for BusIdEncodingFixture<F, M>
where
    F: ConformanceFixture,
    F::Impl: AgentBus,
    M: BusIdEncoding,
{
    type Env = F::Env;
    type Impl = BusIdEncodingAgentBus<F::Impl, M>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        BusIdEncodingAgentBus::new(self.inner.create_impl())
    }
}

impl<F, M> SimulatorFixture for BusIdEncodingFixture<F, M>
where
    F: SimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
            mode: PhantomData,
        }
    }
}

impl<F, M> IntegrationFixture for BusIdEncodingFixture<F, M>
where
    F: IntegrationFixture,
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
            mode: PhantomData,
        })
    }
}
