/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Cloneable channel-backed frontend for any single-threaded service.
//!
//! Service implementations may own `Rc` state and start work with
//! `spawn_local()`. A [`ChanneledHandle`] forwards operations over a channel
//! to a worker that owns the service and runs each operation as its own local
//! task on the same environment, so concurrent requests overlap instead of
//! serializing.
//!
//! Every operation carries its caller's [`RequestContext`], and the worker runs
//! it only after its [`RequestGate`] admits that caller. Bind a caller with
//! [`ChanneledHandle::for_caller`].
//!
//! A service plugs in by defining an operation enum with one [`Call`] per
//! method and implementing [`ChanneledOp`] for it.

use std::future::Future;
use std::rc::Rc;

use agentbus_api::AgentBusError;
use agentbus_api::AuthorizationError;
use agentbus_api::Environment;
use agentbus_api::RequestContext;
use agentbus_api::RequestGate;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

/// A request paired with the channel its reply is sent on.
pub struct Call<Req, Resp> {
    request: Req,
    reply: oneshot::Sender<Resp>,
}

impl<Req, Resp> Call<Req, Resp> {
    /// Run `handler` on the request and send its result to the caller, if the
    /// caller is still waiting.
    pub async fn serve<F, Fut>(self, handler: F)
    where
        F: FnOnce(Req) -> Fut,
        Fut: Future<Output = Resp>,
    {
        let _ = self.reply.send(handler(self.request).await);
    }
}

/// An operation that a worker runs against a service of type `S`.
pub trait ChanneledOp<S> {
    /// Run the operation against `service` and reply to its caller.
    fn dispatch(self, service: Rc<S>) -> impl Future<Output = ()> + 'static;
}

/// The worker stopped before replying.
#[derive(Debug, thiserror::Error)]
#[error("{method}: worker terminated")]
pub struct WorkerTerminated {
    pub method: &'static str,
}

/// Why a call returned no reply.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The worker's gate rejected the caller.
    #[error(transparent)]
    Rejected(AuthorizationError),
    #[error(transparent)]
    Terminated(#[from] WorkerTerminated),
}

/// A denial is `InvalidArgument`, an unavailable gate is `Unavailable`, and a
/// terminated worker is `Internal`.
impl From<CallError> for AgentBusError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Rejected(error @ AuthorizationError::Denied(_)) => {
                Self::InvalidArgument(error.into())
            }
            CallError::Rejected(error @ AuthorizationError::Unavailable(_)) => {
                Self::Unavailable(error.into())
            }
            CallError::Terminated(error) => Self::Internal(error.into()),
        }
    }
}

/// An operation with the caller and target its gate admits it by.
struct Envelope<Op> {
    ctx: RequestContext,
    method: &'static str,
    agent_bus_id: String,
    rejected: oneshot::Sender<AuthorizationError>,
    op: Op,
}

/// Cloneable sender for operations of type `Op`, made as one caller.
pub struct ChanneledHandle<Op> {
    tx: UnboundedSender<Envelope<Op>>,
    ctx: RequestContext,
}

impl<Op> Clone for ChanneledHandle<Op> {
    fn clone(&self) -> Self {
        self.for_caller(self.ctx.clone())
    }
}

impl<Op: 'static> ChanneledHandle<Op> {
    /// Run `service` on `env`'s local executor, admitting each operation
    /// through `gate`, and return a handle with no caller identities.
    pub fn spawn_on<S, G, E>(env: Rc<E>, service: S, gate: G) -> Self
    where
        Op: ChanneledOp<S>,
        S: 'static,
        G: RequestGate + 'static,
        E: Environment + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        env.spawn_local(run_worker(service, Rc::new(gate), env.clone(), rx));
        Self {
            tx,
            ctx: RequestContext::default(),
        }
    }
}

impl<Op> ChanneledHandle<Op> {
    /// A handle to the same worker whose operations carry `ctx`.
    pub fn for_caller(&self, ctx: RequestContext) -> Self {
        Self {
            tx: self.tx.clone(),
            ctx,
        }
    }

    /// Send `request` on `agent_bus_id` to the worker as the operation built by
    /// `wrap`, and wait for the reply.
    pub async fn call<Req, Resp>(
        &self,
        method: &'static str,
        agent_bus_id: String,
        request: Req,
        wrap: impl FnOnce(Call<Req, Resp>) -> Op,
    ) -> Result<Resp, CallError> {
        let (reply, reply_rx) = oneshot::channel();
        let (rejected, rejected_rx) = oneshot::channel();
        self.tx
            .send(Envelope {
                ctx: self.ctx.clone(),
                method,
                agent_bus_id,
                rejected,
                op: wrap(Call { request, reply }),
            })
            .map_err(|_| WorkerTerminated { method })?;
        let Ok(response) = reply_rx.await else {
            return Err(rejected_rx
                .await
                .map_or(WorkerTerminated { method }.into(), CallError::Rejected));
        };
        Ok(response)
    }
}

async fn run_worker<S, Op, E>(
    service: S,
    gate: Rc<dyn RequestGate>,
    env: Rc<E>,
    mut rx: UnboundedReceiver<Envelope<Op>>,
) where
    S: 'static,
    Op: ChanneledOp<S> + 'static,
    E: Environment,
{
    let service = Rc::new(service);
    while let Some(envelope) = rx.recv().await {
        let (service, gate) = (service.clone(), gate.clone());
        env.spawn_local(async move {
            let Envelope {
                ctx,
                method,
                agent_bus_id,
                rejected,
                op,
            } = envelope;
            match gate.admit(&ctx, method, &agent_bus_id).await {
                Ok(()) => op.dispatch(service).await,
                Err(error) => {
                    let _ = rejected.send(error);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::time::Duration;

    use agentbus_api::ExtremelyUnsafeNoOpGate;
    use agentbus_api::environment::Clock;
    use agentbus_simulator::Simulator;
    use async_trait::async_trait;
    use futures::FutureExt;
    use futures::StreamExt;

    use super::*;

    const LATENCY: Duration = Duration::from_millis(50);
    const BUS: &str = "codex.alice.session-123";

    struct Sleeper {
        env: Rc<Simulator>,
        calls: Rc<Cell<usize>>,
    }

    enum SleeperOp {
        Sleep(Call<Duration, ()>),
    }

    impl ChanneledOp<Sleeper> for SleeperOp {
        async fn dispatch(self, sleeper: Rc<Sleeper>) {
            sleeper.calls.set(sleeper.calls.get() + 1);
            match self {
                Self::Sleep(call) => call.serve(|duration| sleeper.env.sleep(duration)).await,
            }
        }
    }

    /// Records each admission and rejects every caller with `rejection`, if set.
    #[derive(Default)]
    struct TestGate {
        rejection: Option<fn(anyhow::Error) -> AuthorizationError>,
        admits: RefCell<Vec<(RequestContext, &'static str, String)>>,
    }

    #[async_trait(?Send)]
    impl RequestGate for TestGate {
        async fn admit(
            &self,
            ctx: &RequestContext,
            method: &'static str,
            agent_bus_id: &str,
        ) -> Result<(), AuthorizationError> {
            self.admits
                .borrow_mut()
                .push((ctx.clone(), method, agent_bus_id.to_owned()));
            self.rejection
                .map_or(Ok(()), |reject| Err(reject(anyhow::anyhow!("rejected"))))
        }
    }

    /// Spawn a sleeper behind `gate`; the counter tracks the calls it serves.
    fn spawn_sleeper<G: RequestGate + 'static>(
        gate: G,
    ) -> (Rc<Simulator>, ChanneledHandle<SleeperOp>, Rc<Cell<usize>>) {
        let env = Rc::new(Simulator::new(agentbus_simulator::generate_seed()));
        let calls = Rc::new(Cell::new(0));
        let sleeper = Sleeper {
            env: env.clone(),
            calls: calls.clone(),
        };
        let handle = ChanneledHandle::spawn_on(env.clone(), sleeper, gate);
        (env, handle, calls)
    }

    fn sleep(handle: &ChanneledHandle<SleeperOp>) -> impl Future<Output = Result<(), CallError>> {
        handle.call("sleep", BUS.to_owned(), LATENCY, SleeperOp::Sleep)
    }

    #[test]
    fn concurrent_calls_overlap() {
        const CALLS: usize = 10;
        let (env, handle, _) = spawn_sleeper(ExtremelyUnsafeNoOpGate);

        let test_env = env.clone();
        let result = env.spawn(async move {
            let start = test_env.with_clock(|c| c.monotonic_time());
            let results: Vec<_> = futures::stream::iter(0..CALLS)
                .map(|_| sleep(&handle))
                .buffer_unordered(CALLS)
                .collect()
                .await;
            for result in results {
                result.expect("worker should reply");
            }
            test_env.with_clock(|c| c.monotonic_time()) - start
        });

        env.run();
        let elapsed = futures::executor::block_on(result).expect("test task should finish");
        assert!(
            elapsed < LATENCY * 2,
            "{CALLS} concurrent {LATENCY:?} calls took {elapsed:?}; they should overlap, not run back to back"
        );
    }

    #[test]
    fn worker_keeps_serving_after_a_caller_gives_up() {
        let (env, handle, _) = spawn_sleeper(ExtremelyUnsafeNoOpGate);

        assert!(
            sleep(&handle).now_or_never().is_none(),
            "the call should still be waiting when it is dropped"
        );

        let result = env.spawn(async move { sleep(&handle).await });
        env.run();
        futures::executor::block_on(result)
            .expect("test task should finish")
            .expect("a later call should still be served");
    }

    #[test]
    fn call_fails_when_worker_is_gone() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let handle = ChanneledHandle {
            tx,
            ctx: RequestContext::default(),
        };

        let error = futures::executor::block_on(sleep(&handle))
            .expect_err("call to a dropped worker should fail");
        assert!(
            matches!(
                &error,
                CallError::Terminated(WorkerTerminated { method: "sleep" })
            ),
            "{error:?}"
        );
        assert_eq!(error.to_string(), "sleep: worker terminated");
    }

    #[test]
    fn gate_admits_each_call_as_its_caller() {
        let gate = Rc::new(TestGate::default());
        let (env, handle, calls) = spawn_sleeper(gate.clone());
        let alice = RequestContext::default().with_user("alice");

        let ctx = alice.clone();
        let result = env.spawn(async move {
            let caller = handle.for_caller(ctx);
            sleep(&caller).await?;
            sleep(&caller.clone()).await
        });
        env.run();
        futures::executor::block_on(result)
            .expect("test task should finish")
            .expect("admitted calls should be served");

        let admitted = (alice, "sleep", BUS.to_owned());
        assert_eq!(*gate.admits.borrow(), [admitted.clone(), admitted]);
        assert_eq!(
            calls.get(),
            2,
            "both admitted calls should reach the service"
        );
    }

    #[test]
    fn rejected_call_does_not_reach_the_service() {
        let gate = TestGate {
            rejection: Some(AuthorizationError::Denied),
            ..Default::default()
        };
        let (env, handle, calls) = spawn_sleeper(gate);

        let result = env.spawn(async move { sleep(&handle).await });
        env.run();
        let error = futures::executor::block_on(result)
            .expect("test task should finish")
            .expect_err("a rejected call should fail");
        assert!(
            matches!(&error, CallError::Rejected(AuthorizationError::Denied(_))),
            "the gate's rejection should reach the caller: {error:?}"
        );
        assert_eq!(
            calls.get(),
            0,
            "a rejected call should not reach the service"
        );
    }

    /// Waits before deciding, so concurrent admissions overlap; admits only
    /// alice.
    struct YieldingGate {
        env: Rc<Simulator>,
        admits: RefCell<Vec<(RequestContext, &'static str, String)>>,
    }

    #[async_trait(?Send)]
    impl RequestGate for YieldingGate {
        async fn admit(
            &self,
            ctx: &RequestContext,
            method: &'static str,
            agent_bus_id: &str,
        ) -> Result<(), AuthorizationError> {
            self.env.sleep(LATENCY).await;
            self.admits
                .borrow_mut()
                .push((ctx.clone(), method, agent_bus_id.to_owned()));
            if ctx.users.iter().any(|user| user == "alice") {
                return Ok(());
            }
            Err(AuthorizationError::Denied(anyhow::anyhow!("not alice")))
        }
    }

    #[test]
    fn concurrent_admissions_keep_each_callers_context() {
        let env = Rc::new(Simulator::new(agentbus_simulator::generate_seed()));
        let gate = Rc::new(YieldingGate {
            env: env.clone(),
            admits: RefCell::default(),
        });
        let calls = Rc::new(Cell::new(0));
        let sleeper = Sleeper {
            env: env.clone(),
            calls: calls.clone(),
        };
        let handle = ChanneledHandle::spawn_on(env.clone(), sleeper, gate.clone());
        let alice = RequestContext::default().with_user("alice");
        let bob = RequestContext::default().with_user("bob");

        let (alice_ctx, bob_ctx) = (alice.clone(), bob.clone());
        let result = env.spawn(async move {
            let alice = handle.for_caller(alice_ctx);
            let bob = handle.for_caller(bob_ctx);
            futures::join!(
                alice.call(
                    "sleep",
                    "codex.alice.s1".to_owned(),
                    LATENCY,
                    SleeperOp::Sleep
                ),
                bob.call(
                    "sleep",
                    "codex.bob.s2".to_owned(),
                    LATENCY,
                    SleeperOp::Sleep
                ),
            )
        });
        env.run();
        let (alice_result, bob_result) =
            futures::executor::block_on(result).expect("test task should finish");

        alice_result.expect("alice should be admitted");
        assert!(
            matches!(
                bob_result,
                Err(CallError::Rejected(AuthorizationError::Denied(_)))
            ),
            "bob should be denied: {bob_result:?}"
        );
        assert_eq!(calls.get(), 1, "only alice's call should reach the service");
        let mut admits = gate.admits.take();
        admits.sort_by(|a, b| a.2.cmp(&b.2));
        assert_eq!(
            admits,
            [
                (alice, "sleep", "codex.alice.s1".to_owned()),
                (bob, "sleep", "codex.bob.s2".to_owned()),
            ],
            "each admission should see its own caller, method and bus"
        );
    }

    #[test]
    fn call_errors_map_to_agent_bus_errors() {
        let rejected = |rejection: fn(anyhow::Error) -> AuthorizationError| {
            AgentBusError::from(CallError::Rejected(rejection(anyhow::anyhow!("rejected"))))
        };
        assert!(matches!(
            rejected(AuthorizationError::Denied),
            AgentBusError::InvalidArgument(_)
        ));
        assert!(matches!(
            rejected(AuthorizationError::Unavailable),
            AgentBusError::Unavailable(_)
        ));
        assert!(matches!(
            AgentBusError::from(CallError::from(WorkerTerminated { method: "sleep" })),
            AgentBusError::Internal(_)
        ));
    }
}
