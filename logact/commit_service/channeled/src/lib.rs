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
//! `ChanneledAgentBus` with its own worker and channel. The worker admits
//! every commit-service operation through a [`RequestGate`] before the service
//! runs it. A gate denial is reported as `InvalidArgument` and an unavailable
//! gate as `Unavailable`.

use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::AuthorizationError;
use agentbus_api::Environment;
use agentbus_api::RequestContext;
use agentbus_api::RequestGate;
use channeled_svc::Call;
use channeled_svc::CallError;
use channeled_svc::ChanneledHandle;
use channeled_svc::ChanneledOp;
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

fn commit_error(error: CallError) -> CommitError {
    match error {
        CallError::Rejected(error @ AuthorizationError::Denied(_)) => {
            CommitError::InvalidArgument(error.into())
        }
        CallError::Rejected(error @ AuthorizationError::Unavailable(_)) => {
            CommitError::Unavailable(error.into())
        }
        CallError::Terminated(error) => CommitError::Unavailable(error.into()),
    }
}

/// Cloneable transport handle for commit-service operations.
#[derive(Clone)]
pub struct ChanneledCommitServiceHandle(ChanneledHandle<Op>);

impl ChanneledCommitServiceHandle {
    /// The same service, making its requests as `ctx`.
    pub fn for_caller(&self, ctx: RequestContext) -> Self {
        Self(self.0.for_caller(ctx))
    }

    pub async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.0
            .call("create_bus", request.bus_id.clone(), request, Op::CreateBus)
            .await
            .map_err(commit_error)?
    }

    pub async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.0
            .call(
                "commit_intention",
                request.bus_id.agent_bus_id.clone(),
                request,
                Op::CommitIntention,
            )
            .await
            .map_err(commit_error)?
    }
}

/// Cloneable channel-backed frontend for a commit service and its AgentBus.
#[derive(Clone)]
pub struct ChanneledCommitService<B> {
    commit: ChanneledCommitServiceHandle,
    bus: B,
}

impl<B> ChanneledCommitService<B> {
    /// Spawn `service` on `env`, with `bus` as `agent_bus()`, admitting each
    /// commit-service operation through `gate`. Pass the bus the service was
    /// built on, without decorators such as `ObservableAgentBus` that external
    /// callers should bypass. The returned service has no caller identities.
    pub fn spawn_on<S, G, E>(env: Rc<E>, bus: B, service: S, gate: G) -> Self
    where
        S: CommitSvc + 'static,
        G: RequestGate + 'static,
        E: Environment + 'static,
    {
        Self {
            commit: ChanneledCommitServiceHandle(ChanneledHandle::spawn_on(env, service, gate)),
            bus,
        }
    }

    /// The same service, making its commit-service requests as `ctx`.
    pub fn for_caller(&self, ctx: RequestContext) -> Self
    where
        B: Clone,
    {
        Self {
            commit: self.commit.for_caller(ctx),
            bus: self.bus.clone(),
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

#[cfg(test)]
mod tests {
    use channeled_svc::WorkerTerminated;

    use super::*;

    #[test]
    fn call_errors_map_to_commit_errors() {
        let rejected = |rejection: fn(anyhow::Error) -> AuthorizationError| {
            commit_error(CallError::Rejected(rejection(anyhow::anyhow!("rejected"))))
        };
        assert!(matches!(
            rejected(AuthorizationError::Denied),
            CommitError::InvalidArgument(_)
        ));
        assert!(matches!(
            rejected(AuthorizationError::Unavailable),
            CommitError::Unavailable(_)
        ));
        assert!(matches!(
            commit_error(
                WorkerTerminated {
                    method: "create_bus"
                }
                .into()
            ),
            CommitError::Unavailable(_)
        ));
    }
}
