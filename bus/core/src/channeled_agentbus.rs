/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;

use crate::AgentBus;
use crate::AgentBusError;
use crate::AppendRequest;
use crate::AppendResponse;
use crate::BlockingPollRequest;
use crate::BlockingPollResponse;
use crate::BusId;
use crate::BusResult;
use crate::CheckTailRequest;
use crate::CheckTailResponse;
use crate::Environment;
use crate::PollRequest;
use crate::PollResponse;
use crate::ReadNextRequest;
use crate::ReadNextResponse;
use crate::RealEnvironment;

type AppendCall = (AppendRequest, oneshot::Sender<BusResult<AppendResponse>>);
type PollCall = (PollRequest, oneshot::Sender<BusResult<PollResponse>>);
type ReadNextCall = (
    ReadNextRequest,
    oneshot::Sender<BusResult<ReadNextResponse>>,
);
type TailCall = (
    CheckTailRequest,
    oneshot::Sender<BusResult<CheckTailResponse>>,
);
type BlockingPollCall = (
    BlockingPollRequest,
    oneshot::Sender<BusResult<BlockingPollResponse>>,
);

/// Cloneable channel-backed frontend for any single-threaded `AgentBus`.
///
/// `new_in_process()` runs the underlying bus on a dedicated current-thread
/// runtime so callers can share an `Rc`-based implementation directly without a
/// gRPC hop.
#[derive(Clone)]
pub struct ChanneledAgentBus {
    append_tx: UnboundedSender<AppendCall>,
    poll_tx: UnboundedSender<PollCall>,
    read_next_tx: UnboundedSender<ReadNextCall>,
    tail_tx: UnboundedSender<TailCall>,
    blocking_poll_tx: UnboundedSender<BlockingPollCall>,
}

impl ChanneledAgentBus {
    /// Create an in-process channel-backed handle for an `AgentBus`
    /// implementation. The underlying bus runs on a dedicated single-threaded
    /// runtime so `Rc`-based implementations can be used directly without
    /// requiring `Send`.
    pub fn new_in_process<F, T>(factory: F) -> Self
    where
        F: FnOnce() -> T + Send + 'static,
        T: AgentBus + 'static,
    {
        Self::new_with_worker(
            move |append_rx, poll_rx, read_next_rx, tail_rx, blocking_poll_rx| {
                // Use a dedicated thread rather than `tokio::spawn` so the underlying
                // bus does not need to implement `Send`.
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create tokio runtime for worker");

                    let local = tokio::task::LocalSet::new();
                    local.block_on(&rt, async move {
                        let agent_bus_impl = factory();
                        let env = Rc::new(RealEnvironment::new());
                        Self::run_worker(
                            agent_bus_impl,
                            env,
                            append_rx,
                            poll_rx,
                            read_next_rx,
                            tail_rx,
                            blocking_poll_rx,
                        )
                        .await;
                    });
                });
            },
        )
    }

    /// Create a channel-backed handle that runs on the provided environment's
    /// local executor. This is useful for simulator tests that need the bus to
    /// stay single-threaded and deterministic.
    pub fn new_on_environment<E, F, T>(env: Rc<E>, factory: F) -> Self
    where
        E: Environment + 'static,
        F: FnOnce() -> T + 'static,
        T: AgentBus + 'static,
    {
        Self::new_with_worker(
            move |append_rx, poll_rx, read_next_rx, tail_rx, blocking_poll_rx| {
                let agent_bus_impl = factory();
                let worker_env = env.clone();
                env.spawn_local(async move {
                    Self::run_worker(
                        agent_bus_impl,
                        worker_env,
                        append_rx,
                        poll_rx,
                        read_next_rx,
                        tail_rx,
                        blocking_poll_rx,
                    )
                    .await;
                });
            },
        )
    }

    fn new_with_worker<SpawnWorker>(spawn_worker: SpawnWorker) -> Self
    where
        SpawnWorker: FnOnce(
            UnboundedReceiver<AppendCall>,
            UnboundedReceiver<PollCall>,
            UnboundedReceiver<ReadNextCall>,
            UnboundedReceiver<TailCall>,
            UnboundedReceiver<BlockingPollCall>,
        ),
    {
        let (append_tx, append_rx) = mpsc::unbounded();
        let (poll_tx, poll_rx) = mpsc::unbounded();
        let (read_next_tx, read_next_rx) = mpsc::unbounded();
        let (tail_tx, tail_rx) = mpsc::unbounded();
        let (blocking_poll_tx, blocking_poll_rx) = mpsc::unbounded();

        spawn_worker(append_rx, poll_rx, read_next_rx, tail_rx, blocking_poll_rx);

        Self {
            append_tx,
            poll_tx,
            read_next_tx,
            tail_tx,
            blocking_poll_tx,
        }
    }

    /// Worker task that processes messages from all channels.
    ///
    /// Each operation is dispatched via `env.spawn_local()` so concurrent
    /// requests can overlap instead of serializing behind one another.
    pub async fn run_worker<T: AgentBus + 'static, E: Environment + 'static>(
        agent_bus_impl: T,
        env: Rc<E>,
        mut append_rx: UnboundedReceiver<AppendCall>,
        mut poll_rx: UnboundedReceiver<PollCall>,
        mut read_next_rx: UnboundedReceiver<ReadNextCall>,
        mut tail_rx: UnboundedReceiver<TailCall>,
        mut blocking_poll_rx: UnboundedReceiver<BlockingPollCall>,
    ) {
        let bus = Rc::new(agent_bus_impl);
        loop {
            futures::select_biased! {
                (request, response_tx) = append_rx.select_next_some() => {
                    let bus = bus.clone();
                    env.spawn_local(async move {
                        let result = bus.append(request).await;
                        let _ = response_tx.send(result);
                    });
                }
                (request, response_tx) = poll_rx.select_next_some() => {
                    let bus = bus.clone();
                    env.spawn_local(async move {
                        let result = bus.poll(request).await;
                        let _ = response_tx.send(result);
                    });
                }
                (request, response_tx) = read_next_rx.select_next_some() => {
                    let bus = bus.clone();
                    env.spawn_local(async move {
                        let result = bus.read_next(request).await;
                        let _ = response_tx.send(result);
                    });
                }
                (request, response_tx) = tail_rx.select_next_some() => {
                    let bus = bus.clone();
                    env.spawn_local(async move {
                        let result = bus.check_tail(request).await;
                        let _ = response_tx.send(result);
                    });
                }
                (request, response_tx) = blocking_poll_rx.select_next_some() => {
                    let bus = bus.clone();
                    env.spawn_local(async move {
                        let result = bus.blocking_poll(request).await;
                        let _ = response_tx.send(result);
                    });
                }
                complete => break,
            }
        }
    }

    async fn call<Req, Resp>(
        tx: &UnboundedSender<(Req, oneshot::Sender<BusResult<Resp>>)>,
        request: Req,
        method_name: &'static str,
    ) -> BusResult<Resp> {
        let (response_tx, response_rx) = oneshot::channel();

        tx.unbounded_send((request, response_tx)).map_err(|_| {
            AgentBusError::Internal(anyhow::anyhow!("{}: worker thread terminated", method_name))
        })?;

        response_rx.await.map_err(|_| {
            AgentBusError::Internal(anyhow::anyhow!("{}: worker thread terminated", method_name))
        })?
    }
}

impl AgentBus for ChanneledAgentBus {
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        Self::call(&self.append_tx, request, "append").await
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        Self::call(&self.poll_tx, request, "poll").await
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        Self::call(&self.read_next_tx, request, "read_next").await
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        Self::call(&self.tail_tx, request, "check_tail").await
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        Self::call(&self.blocking_poll_tx, request, "blocking_poll").await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentbus_api::environment::Clock;
    use agentbus_api::traits::AgentBus;
    use agentbus_api::*;
    use agentbus_simple::InMemoryAgentBus;
    use rand::distr::Uniform;

    use super::*;

    /// Thin wrapper that adds fixed latency to each operation.
    struct SlowAgentBus<T, E> {
        inner: T,
        latency: Duration,
        env: Rc<E>,
    }

    impl<T: AgentBus, E: Environment> AgentBus for SlowAgentBus<T, E> {
        async fn append(&self, req: AppendRequest) -> BusResult<AppendResponse> {
            self.env.sleep(self.latency).await;
            self.inner.append(req).await
        }

        async fn poll(&self, req: PollRequest) -> BusResult<PollResponse> {
            self.env.sleep(self.latency).await;
            self.inner.poll(req).await
        }

        async fn read_next(&self, req: ReadNextRequest) -> BusResult<ReadNextResponse> {
            self.inner.read_next(req).await
        }

        async fn check_tail(&self, req: CheckTailRequest) -> BusResult<CheckTailResponse> {
            self.inner.check_tail(req).await
        }

        async fn blocking_poll(&self, req: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
            self.inner.blocking_poll(req).await
        }
    }

    const LATENCY_MS: u64 = 5;
    const NUM_APPENDS: usize = 10;
    const NUM_POLLS: usize = 10;

    /// Verify that `run_worker` dispatches poll operations concurrently.
    ///
    /// Uses a zero-jitter simulator and fixed 5ms latency so timing assertions
    /// are exact: N concurrent polls complete in exactly 5ms (one sleep
    /// duration), not N x 5ms.
    ///
    /// Phase 1: Sequential appends (one at a time, 5ms each -> 50ms total).
    /// Phase 2: Concurrent polls (all at once, 5ms each -> 5ms total if
    /// concurrent).
    #[test]
    fn test_concurrent_dispatch() {
        let seed: u64 = rand::random();
        let simulator =
            agentbus_simulator::Simulator::with_jitter(seed, Uniform::new(0, 1).unwrap());
        let env = Rc::new(simulator);

        let test_env = env.clone();
        let handle = env.spawn(async move {
            let inner = InMemoryAgentBus::new(test_env.clone());
            let bus = SlowAgentBus {
                inner,
                latency: Duration::from_millis(LATENCY_MS),
                env: test_env.clone(),
            };

            let (append_tx, append_rx) = mpsc::unbounded();
            let (poll_tx, poll_rx) = mpsc::unbounded();
            let (_read_next_tx, read_next_rx) = mpsc::unbounded();
            let (_tail_tx, tail_rx) = mpsc::unbounded();
            let (_blocking_poll_tx, blocking_poll_rx) = mpsc::unbounded();

            let worker_env = test_env.clone();
            test_env.spawn_local(async move {
                ChanneledAgentBus::run_worker(
                    bus,
                    worker_env,
                    append_rx,
                    poll_rx,
                    read_next_rx,
                    tail_rx,
                    blocking_poll_rx,
                )
                .await;
            });

            let bus_id = "test-bus".to_string();

            for i in 0..NUM_APPENDS {
                let (reply_tx, reply_rx) = oneshot::channel();
                append_tx
                    .unbounded_send((
                        AppendRequest {
                            agent_bus_id: bus_id.clone(),
                            bus_id: Some(BusId {
                                agent_bus_id: bus_id.clone(),
                            }),
                            payload: Some(Payload {
                                payload: Some(payload::Payload::Intention(Intention {
                                    intention: Some(intention::Intention::StringIntention(
                                        format!("append-{}", i),
                                    )),
                                    ..Default::default()
                                })),
                            }),
                            ..Default::default()
                        },
                        reply_tx,
                    ))
                    .unwrap();
                let _ = reply_rx.await;
            }

            let t0 = test_env.with_clock(|c| c.monotonic_time());

            let mut poll_replies = Vec::new();
            for _ in 0..NUM_POLLS {
                let (reply_tx, reply_rx) = oneshot::channel();
                poll_tx
                    .unbounded_send((
                        PollRequest {
                            agent_bus_id: bus_id.clone(),
                            bus_id: Some(BusId {
                                agent_bus_id: bus_id.clone(),
                            }),
                            start_log_position: 0,
                            max_entries: 10,
                            filter: None,
                            ..Default::default()
                        },
                        reply_tx,
                    ))
                    .unwrap();
                poll_replies.push(reply_rx);
            }

            for rx in poll_replies {
                let _ = rx.await;
            }

            let poll_time = test_env.with_clock(|c| c.monotonic_time()) - t0;

            assert_eq!(
                poll_time,
                Duration::from_millis(LATENCY_MS),
                "{} concurrent polls took {:?}. Expected exactly {}ms with concurrent dispatch.",
                NUM_POLLS,
                poll_time,
                LATENCY_MS,
            );

            anyhow::Ok(())
        });

        env.run();
        futures::executor::block_on(handle)
            .expect("Test scenario should complete successfully")
            .expect("Test assertions should pass");
    }
}
