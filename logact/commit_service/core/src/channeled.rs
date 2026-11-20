/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Cloneable channel-backed frontend for the LogAct commit service.
//!
//! Transport handlers must be safe to call from their server runtimes, while
//! commit service implementations may own `Rc` state and start per-bus loops
//! with `spawn_local()`. This module keeps the state behind a cloneable channel
//! handle, following the channeled AgentBus pattern. The handle is backed by
//! the worker channel `new_on_environment()` creates, or by a gated
//! `channeled_svc` channel from `new_gated_on_environment()`.

use std::rc::Rc;
use std::sync::Arc;

use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_api::RequestContext;
use agentbus_api::RequestGate;
use async_trait::async_trait;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use logact_commit_service_channeled::ChanneledCommitServiceHandle as GatedHandle;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

enum Op {
    CreateBus {
        request: CreateBusCommand,
        reply: oneshot::Sender<CommitResult<()>>,
    },
    CommitIntention {
        request: CommitIntentionCommand,
        reply: oneshot::Sender<CommitResult<CommitIntentionOutcome>>,
    },
}

/// Cloneable channel-backed frontend for a commit service.
#[derive(Clone)]
pub struct ChanneledCommitService<B> {
    commit: ChanneledCommitServiceHandle,
    bus: B,
}

/// Cloneable transport handle for commit-service operations.
#[derive(Clone)]
pub struct ChanneledCommitServiceHandle(Arc<dyn Channel>);

/// The channel behind a [`ChanneledCommitServiceHandle`].
#[async_trait]
trait Channel: Send + Sync {
    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()>;
    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome>;

    /// This channel making its requests as `caller()`, or `None` if its
    /// requests carry no caller.
    fn for_caller(&self, caller: &dyn Fn() -> RequestContext) -> Option<Arc<dyn Channel>>;
}

/// The worker channel `new_on_environment()` creates.
struct WorkerChannel {
    tx: UnboundedSender<Op>,
}

impl<B> ChanneledCommitService<B> {
    /// Create a channel-backed handle that runs on the provided environment's
    /// local executor. The same logical bus is retained for `agent_bus()` and
    /// passed to `factory` for the worker-owned commit-service implementation.
    pub fn new_on_environment<S, E, Factory>(env: Rc<E>, bus: B, factory: Factory) -> Self
    where
        S: CommitSvc + 'static,
        E: Environment + 'static,
        B: AgentBus + Clone + 'static,
        Factory: FnOnce(B) -> S + 'static,
    {
        let (tx, rx) = unbounded_channel::<Op>();
        let worker_bus = bus.clone();
        let worker_env = env.clone();
        env.spawn_local(async move {
            Self::run_worker(factory(worker_bus), worker_env, rx).await;
        });

        Self {
            commit: ChanneledCommitServiceHandle(Arc::new(WorkerChannel { tx })),
            bus,
        }
    }

    /// Like `new_on_environment()`, but behind a gated `channeled_svc` channel
    /// that admits each commit-service operation through `gate`.
    pub fn new_gated_on_environment<S, E, G, Factory>(
        env: Rc<E>,
        bus: B,
        gate: G,
        factory: Factory,
    ) -> Self
    where
        S: CommitSvc + 'static,
        E: Environment + 'static,
        G: RequestGate + 'static,
        B: AgentBus + Clone + 'static,
        Factory: FnOnce(B) -> S + 'static,
    {
        let service = factory(bus.clone());
        let gated = logact_commit_service_channeled::ChanneledCommitService::spawn_on(
            env,
            bus.clone(),
            service,
            gate,
        );
        Self {
            commit: ChanneledCommitServiceHandle(Arc::new(gated.into_commit_handle())),
            bus,
        }
    }

    /// The same service, making its commit-service requests as `caller()`.
    pub fn for_caller(&self, caller: impl Fn() -> RequestContext) -> Self
    where
        B: Clone,
    {
        Self {
            commit: self.commit.for_caller(caller),
            bus: self.bus.clone(),
        }
    }

    /// Consume the composed service and return its transport handle.
    pub fn into_commit_handle(self) -> ChanneledCommitServiceHandle {
        self.commit
    }

    async fn run_worker<S, E>(state: S, env: Rc<E>, mut rx: UnboundedReceiver<Op>)
    where
        S: CommitSvc + 'static,
        E: Environment + 'static,
    {
        // Each in-flight commit gets its own `Rc<S>` so the spawned task can
        // hold the borrow needed by the commit future without tying it back to
        // the worker's stack frame.
        let state = Rc::new(state);
        while let Some(op) = rx.recv().await {
            match op {
                Op::CreateBus { request, reply } => {
                    let state = state.clone();
                    env.spawn_local(async move {
                        let _ = reply.send(state.create_bus(request).await);
                    });
                }
                Op::CommitIntention { request, reply } => {
                    let state = state.clone();
                    env.spawn_local(async move {
                        let _ = reply.send(state.commit_intention(request).await);
                    });
                }
            }
        }
    }
}

impl ChanneledCommitServiceHandle {
    pub async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        self.0.create_bus(request).await
    }

    pub async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.0.commit_intention(request).await
    }

    /// The same handle, making its requests as `caller()`. The worker channel
    /// carries no caller, so it never calls it.
    pub fn for_caller(&self, caller: impl Fn() -> RequestContext) -> Self {
        self.0
            .for_caller(&caller)
            .map_or_else(|| self.clone(), Self)
    }
}

#[async_trait]
impl Channel for WorkerChannel {
    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Op::CreateBus {
                request,
                reply: reply_tx,
            })
            .map_err(|_| worker_terminated())?;
        reply_rx.await.map_err(|_| worker_terminated())?
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Op::CommitIntention {
                request,
                reply: reply_tx,
            })
            .map_err(|_| worker_terminated())?;

        reply_rx.await.map_err(|_| worker_terminated())?
    }

    fn for_caller(&self, _caller: &dyn Fn() -> RequestContext) -> Option<Arc<dyn Channel>> {
        None
    }
}

#[async_trait]
impl Channel for GatedHandle {
    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        GatedHandle::create_bus(self, request).await
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        GatedHandle::commit_intention(self, request).await
    }

    fn for_caller(&self, caller: &dyn Fn() -> RequestContext) -> Option<Arc<dyn Channel>> {
        Some(Arc::new(GatedHandle::for_caller(self, caller())))
    }
}

fn worker_terminated() -> CommitError {
    CommitError::Unavailable(anyhow::anyhow!("worker terminated"))
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
