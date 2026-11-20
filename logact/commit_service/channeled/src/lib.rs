/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Cloneable channel-backed commit service built on [`channeled_svc`].
//!
//! Transport handlers must be safe to call from their server runtimes, while
//! commit service implementations may own `Rc` state and start per-bus loops
//! with `spawn_local()`. The service runs on a worker behind its own channel,
//! and `agent_bus()` returns the bus it was spawned with, such as a
//! `ChanneledAgentBus` with its own worker and channel.

use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::Environment;
use channeled_svc::Call;
use channeled_svc::ChanneledHandle;
use channeled_svc::ChanneledOp;
use channeled_svc::WorkerTerminated;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;

enum Op {
    CreateBus(Call<CreateBusCommand, CommitResult<()>>),
    CommitIntention(Call<CommitIntentionCommand, CommitResult<CommitIntentionOutcome>>),
}

impl<S: CommitSvc + 'static> ChanneledOp<S> for Op {
    async fn dispatch(self, service: Rc<S>) {
        match self {
            Self::CreateBus(call) => call.serve(|request| service.create_bus(request)).await,
            Self::CommitIntention(call) => {
                call.serve(|request| service.commit_intention(request))
                    .await
            }
        }
    }
}

fn unavailable(error: WorkerTerminated) -> CommitError {
    CommitError::Unavailable(error.into())
}

/// Cloneable transport handle for commit-service operations.
#[derive(Clone)]
pub struct ChanneledCommitServiceHandle(ChanneledHandle<Op>);

impl ChanneledCommitServiceHandle {
    pub async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.0
            .call("create_bus", request, Op::CreateBus)
            .await
            .map_err(unavailable)?
    }

    pub async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.0
            .call("commit_intention", request, Op::CommitIntention)
            .await
            .map_err(unavailable)?
    }
}

/// Cloneable channel-backed frontend for a commit service and its AgentBus.
#[derive(Clone)]
pub struct ChanneledCommitService<B> {
    commit: ChanneledCommitServiceHandle,
    bus: B,
}

impl<B> ChanneledCommitService<B> {
    /// Spawn `service` on `env`, with `bus` as `agent_bus()`. Pass the bus the
    /// service was built on, without decorators such as `ObservableAgentBus`
    /// that external callers should bypass.
    pub fn spawn_on<S, E>(env: Rc<E>, bus: B, service: S) -> Self
    where
        S: CommitSvc + 'static,
        E: Environment + 'static,
    {
        Self {
            commit: ChanneledCommitServiceHandle(ChanneledHandle::spawn_on(env, service)),
            bus,
        }
    }

    /// Consume the composed service and return its transport handle.
    pub fn into_commit_handle(self) -> ChanneledCommitServiceHandle {
        self.commit
    }
}

impl<B: AgentBus> CommitSvc for ChanneledCommitService<B> {
    type Bus = B;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.commit.create_bus(request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.commit.commit_intention(request).await
    }
}
