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
//! handle, following the channeled AgentBus pattern.

use std::io;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use agentbus_api::AgentBus;
use agentbus_api::Environment;
use agentbus_api::RealEnvironment;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

enum Op {
    CommitIntention {
        request: CommitIntentionCommand,
        reply: oneshot::Sender<CommitResult<CommitIntentionOutcome>>,
    },
}

/// Cloneable channel-backed frontend for any `CommitSvc` implementation.
#[derive(Clone)]
pub struct ChanneledCommitService<B> {
    commit: ChanneledCommitServiceHandle,
    bus: B,
}

/// Cloneable transport handle for the commit-only surface.
#[derive(Clone)]
pub struct ChanneledCommitServiceHandle {
    tx: UnboundedSender<Op>,
}

impl<B> ChanneledCommitService<B> {
    /// Try to create an in-process channel-backed service. The underlying
    /// service runs on a dedicated current-thread runtime so `Rc`-based
    /// implementations can be shared with `Send` callers such as a tonic
    /// server.
    pub fn try_new_in_process<S, Factory>(bus: B, factory: Factory) -> io::Result<Self>
    where
        S: CommitSvc + 'static,
        B: AgentBus + Clone + Send + 'static,
        Factory: FnOnce(B) -> S + Send + 'static,
    {
        let (tx, rx) = unbounded_channel::<Op>();
        let worker_bus = bus.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        std::thread::Builder::new()
            .name("commit-svc".to_string())
            .spawn(move || {
                // The receiver is owned by the worker. Unwinding drops it and
                // all in-flight replies before the panic is logged, so callers
                // observe `WorkerTerminated`.
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    let local = tokio::task::LocalSet::new();
                    local.block_on(&runtime, async move {
                        let env = Rc::new(RealEnvironment::new());
                        Self::run_worker(factory(worker_bus), env, rx).await;
                    });
                }));
                if let Err(payload) = result {
                    let message = payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("non-string panic payload");
                    tracing::error!(panic = message, "CommitService worker thread panicked");
                }
            })?;

        Ok(Self {
            commit: ChanneledCommitServiceHandle { tx },
            bus,
        })
    }

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
            commit: ChanneledCommitServiceHandle { tx },
            bus,
        }
    }

    /// Consume the composed service and return its commit-only transport handle.
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
    pub async fn commit_intention(
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
}

fn worker_terminated() -> CommitError {
    CommitError::Unavailable(anyhow::anyhow!("worker terminated"))
}

impl<B: AgentBus> CommitSvc for ChanneledCommitService<B> {
    type Bus = B;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.commit.commit_intention(request).await
    }
}
