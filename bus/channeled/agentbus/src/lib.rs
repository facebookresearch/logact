/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Cloneable channel-backed `AgentBus` frontend built on [`channeled_svc`].

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::AppendResponse;
use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollResponse;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::CheckTailResponse;
use agent_bus_proto_rust::agent_bus::PollRequest;
use agent_bus_proto_rust::agent_bus::PollResponse;
use agent_bus_proto_rust::agent_bus::ReadNextRequest;
use agent_bus_proto_rust::agent_bus::ReadNextResponse;
use agentbus_api::AgentBus;
use agentbus_api::BusResult;
use agentbus_api::Environment;
use channeled_svc::Call;
use channeled_svc::ChanneledHandle;
use channeled_svc::ChanneledOp;

enum Op {
    Append(Call<AppendRequest, BusResult<AppendResponse>>),
    Poll(Call<PollRequest, BusResult<PollResponse>>),
    ReadNext(Call<ReadNextRequest, BusResult<ReadNextResponse>>),
    CheckTail(Call<CheckTailRequest, BusResult<CheckTailResponse>>),
    BlockingPoll(Call<BlockingPollRequest, BusResult<BlockingPollResponse>>),
}

impl<T: AgentBus + 'static> ChanneledOp<T> for Op {
    async fn dispatch(self, bus: Rc<T>) {
        match self {
            Self::Append(call) => call.serve(|request| bus.append(request)).await,
            Self::Poll(call) => call.serve(|request| bus.poll(request)).await,
            Self::ReadNext(call) => call.serve(|request| bus.read_next(request)).await,
            Self::CheckTail(call) => call.serve(|request| bus.check_tail(request)).await,
            Self::BlockingPoll(call) => call.serve(|request| bus.blocking_poll(request)).await,
        }
    }
}

/// Cloneable channel-backed `AgentBus`.
#[derive(Clone)]
pub struct ChanneledAgentBus(ChanneledHandle<Op>);

impl ChanneledAgentBus {
    /// Run `bus` on `env`'s local executor behind a channel.
    pub fn spawn_on<T, E>(env: Rc<E>, bus: T) -> Self
    where
        T: AgentBus + 'static,
        E: Environment + 'static,
    {
        Self(ChanneledHandle::spawn_on(env, bus))
    }
}

impl AgentBus for ChanneledAgentBus {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        self.0.call("append", request, Op::Append).await?
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        self.0.call("poll", request, Op::Poll).await?
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        self.0.call("read_next", request, Op::ReadNext).await?
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        self.0.call("check_tail", request, Op::CheckTail).await?
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        self.0
            .call("blocking_poll", request, Op::BlockingPoll)
            .await?
    }
}
