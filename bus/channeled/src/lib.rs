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
//! A service plugs in by defining an operation enum with one [`Call`] per
//! method and implementing [`ChanneledOp`] for it.

use std::future::Future;
use std::rc::Rc;

use agentbus_api::AgentBusError;
use agentbus_api::Environment;
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

impl From<WorkerTerminated> for AgentBusError {
    fn from(error: WorkerTerminated) -> Self {
        Self::Internal(error.into())
    }
}

/// Cloneable sender for operations of type `Op`.
pub struct ChanneledHandle<Op> {
    tx: UnboundedSender<Op>,
}

impl<Op> Clone for ChanneledHandle<Op> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<Op: 'static> ChanneledHandle<Op> {
    /// Run `service` on `env`'s local executor and return a handle to it.
    pub fn spawn_on<S, E>(env: Rc<E>, service: S) -> Self
    where
        Op: ChanneledOp<S>,
        S: 'static,
        E: Environment + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        env.spawn_local(run_worker(service, env.clone(), rx));
        Self { tx }
    }
}

impl<Op> ChanneledHandle<Op> {
    /// Send `request` to the worker as the operation built by `wrap`, and wait
    /// for the reply.
    pub async fn call<Req, Resp>(
        &self,
        method: &'static str,
        request: Req,
        wrap: impl FnOnce(Call<Req, Resp>) -> Op,
    ) -> Result<Resp, WorkerTerminated> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(wrap(Call { request, reply }))
            .map_err(|_| WorkerTerminated { method })?;
        reply_rx.await.map_err(|_| WorkerTerminated { method })
    }
}

async fn run_worker<S, Op, E>(service: S, env: Rc<E>, mut rx: UnboundedReceiver<Op>)
where
    Op: ChanneledOp<S>,
    E: Environment,
{
    let service = Rc::new(service);
    while let Some(op) = rx.recv().await {
        env.spawn_local(op.dispatch(service.clone()));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentbus_api::environment::Clock;
    use agentbus_simulator::Simulator;
    use futures::FutureExt;
    use futures::StreamExt;

    use super::*;

    const LATENCY: Duration = Duration::from_millis(50);

    struct Sleeper {
        env: Rc<Simulator>,
    }

    enum SleeperOp {
        Sleep(Call<Duration, ()>),
    }

    impl ChanneledOp<Sleeper> for SleeperOp {
        async fn dispatch(self, sleeper: Rc<Sleeper>) {
            match self {
                Self::Sleep(call) => call.serve(|duration| sleeper.env.sleep(duration)).await,
            }
        }
    }

    fn spawn_sleeper() -> (Rc<Simulator>, ChanneledHandle<SleeperOp>) {
        let env = Rc::new(Simulator::new(agentbus_simulator::generate_seed()));
        let handle = ChanneledHandle::spawn_on(env.clone(), Sleeper { env: env.clone() });
        (env, handle)
    }

    #[test]
    fn concurrent_calls_overlap() {
        const CALLS: usize = 10;
        let (env, handle) = spawn_sleeper();

        let test_env = env.clone();
        let result = env.spawn(async move {
            let start = test_env.with_clock(|c| c.monotonic_time());
            let results: Vec<_> = futures::stream::iter(0..CALLS)
                .map(|_| handle.call("sleep", LATENCY, SleeperOp::Sleep))
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
        let (env, handle) = spawn_sleeper();

        assert!(
            handle
                .call("sleep", LATENCY, SleeperOp::Sleep)
                .now_or_never()
                .is_none(),
            "the call should still be waiting when it is dropped"
        );

        let result =
            env.spawn(async move { handle.call("sleep", LATENCY, SleeperOp::Sleep).await });
        env.run();
        futures::executor::block_on(result)
            .expect("test task should finish")
            .expect("a later call should still be served");
    }

    #[test]
    fn call_fails_when_worker_is_gone() {
        let (tx, rx) = mpsc::unbounded_channel::<SleeperOp>();
        drop(rx);
        let handle = ChanneledHandle { tx };

        let error = futures::executor::block_on(handle.call("sleep", LATENCY, SleeperOp::Sleep))
            .expect_err("call to a dropped worker should fail");
        assert_eq!(error.method, "sleep");
        assert_eq!(error.to_string(), "sleep: worker terminated");
    }
}
