/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AppServer — plain container holding bus and sub-component state.
//! Domain logic lives in grpc.rs; shared helpers live here.

pub mod grpc;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::environment::Clock;
use agentbus_api::environment::Environment;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;

use crate::decider::Decider;
use crate::mailbox::MailLogEntry;
use crate::mailbox::Mailbox;
use crate::tracing_events::event_type;
use crate::vote_trackers::create_vote_tracker_for_decider_policy;
use crate::voter::Voter;
use crate::voter::VoterLoop;

/// Future watched by the mailbox/decider/voter loops spawned in
/// `spawn_bus_runtime`. Resolves when the corresponding `oneshot::Sender`
/// held by `BusRuntime` is dropped.
type StopSignal = Shared<oneshot::Receiver<()>>;

pub struct AppServerConfig {
    pub bus_id: String,
    pub gate_check_timeout: Duration,
    /// Internal polling interval for WriteOnce-backed blocking_poll. None uses default.
    pub blocking_poll_interval: Option<Duration>,
}

pub struct GateResult {
    pub approved: bool,
    pub reason: String,
    pub intention_id: i64,
}

/// Runtime state for one agent bus ID.
///
/// This owns the handles needed by request handlers after the decider, mailbox,
/// and voter loops have been spawned on a local executor.
///
/// The `_stop` sender pairs with `StopSignal` futures held by the spawned
/// loops. When `BusRuntime` is dropped, `_stop` is dropped, the receivers
/// resolve, and the loops exit at their next yield point. This keeps the
/// spawned tasks' lifetimes tied to the runtime's lifetime — important for
/// simulator tests where unbounded loops would prevent the executor from
/// going idle.
pub struct BusRuntime<T: AgentBus, E: Environment> {
    bus: T,
    bus_id: String,
    env: Rc<E>,
    mail_handle: Rc<RefCell<VecDeque<MailLogEntry>>>,
    _stop: oneshot::Sender<()>,
}

impl<T: AgentBus + Clone, E: Environment> BusRuntime<T, E> {
    pub fn new(bus: T, bus_id: String, env: Rc<E>) -> Self {
        // No spawned loops in this constructor, so the receiver side is
        // discarded; the sender exists only to give the struct a uniform shape.
        let (stop_tx, _) = oneshot::channel();
        Self {
            bus,
            bus_id,
            env,
            mail_handle: Rc::new(RefCell::new(VecDeque::new())),
            _stop: stop_tx,
        }
    }

    pub fn bus(&self) -> &T {
        &self.bus
    }

    pub fn bus_id(&self) -> &str {
        &self.bus_id
    }

    pub fn env(&self) -> &Rc<E> {
        &self.env
    }

    pub fn mail_handle(&self) -> &Rc<RefCell<VecDeque<MailLogEntry>>> {
        &self.mail_handle
    }
}

/// Plain container — holds bus and sub-component state.
pub struct AppServer<T: AgentBus, E: Environment> {
    runtime: BusRuntime<T, E>,
    gate_check_timeout: Duration,
}

impl<T: AgentBus + Clone, E: Environment> AppServer<T, E> {
    /// Create an AppServer without spawning background loops.
    /// For production use, prefer `build_and_spawn` which starts the decider,
    /// mailbox, and voter. This constructor is useful for tests.
    pub fn new(bus: T, bus_id: String, gate_check_timeout: Duration, env: Rc<E>) -> Self {
        Self {
            runtime: BusRuntime::new(bus, bus_id, env),
            gate_check_timeout,
        }
    }

    pub fn bus(&self) -> &T {
        self.runtime.bus()
    }
    pub fn bus_id(&self) -> &str {
        self.runtime.bus_id()
    }
    pub fn gate_check_timeout(&self) -> Duration {
        self.gate_check_timeout
    }
    pub fn env(&self) -> &Rc<E> {
        self.runtime.env()
    }
    pub fn mail_handle(&self) -> &Rc<RefCell<VecDeque<MailLogEntry>>> {
        self.runtime.mail_handle()
    }
}

/// Creates Mailbox, Decider, and zero or more voter loops for one bus ID.
///
/// The loops are spawned via `env.spawn_local()`, so callers must run this from
/// the local executor that should own the runtime.
///
/// `start_position` is the bus position the decider and voter loops start polling
/// from. Pass 0 to replay the full history; pass the current tail (or the
/// position of a freshly-appended bootstrap policy) to skip replay on restart.
///
/// The returned `BusRuntime` owns a stop signal that the spawned loops watch.
/// Dropping the returned `BusRuntime` cancels the loops at their next yield
/// point; production callers that hold the runtime forever get the original
/// "loop forever" behavior, while test callers that drop the runtime at end
/// of test get clean shutdown.
pub fn spawn_bus_runtime<T: AgentBus + Clone + 'static, E: Environment + 'static>(
    bus: T,
    bus_id: String,
    env: Rc<E>,
    start_position: i64,
    initial_voters: Vec<Box<dyn Voter>>,
) -> BusRuntime<T, E> {
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let stop_signal: StopSignal = stop_rx.shared();

    // Mailbox
    let mut mailbox = Mailbox::new(bus.clone(), bus_id.clone(), env.clone());
    let mail_handle = mailbox.mail_handle();
    let mailbox_stop = stop_signal.clone();
    env.spawn_local(async move {
        mailbox.run_until_cancelled(None, mailbox_stop).await;
    });

    // Decider — always starts with ON_BY_DEFAULT.  Callers that want a
    // different policy append a DeciderPolicy entry to the log.
    let decider_bus = bus.clone();
    let decider_bus_id = bus_id.clone();
    let decider_stop = stop_signal.clone();
    env.spawn_local(async move {
        let mut decider = Decider::new(
            decider_bus,
            decider_bus_id,
            start_position,
            create_vote_tracker_for_decider_policy,
        );
        let _ = decider.run_until_cancelled(None, decider_stop).await;
    });

    // Voters — each gets its own independent polling loop.
    for voter in initial_voters {
        let mut voter_loop = VoterLoop::new(
            bus.clone(),
            bus_id.clone(),
            start_position,
            env.clone(),
            voter,
        );
        let voter_stop = stop_signal.clone();
        env.spawn_local(async move {
            let _ = voter_loop.run_until_cancelled(None, voter_stop).await;
        });
    }

    BusRuntime {
        bus,
        bus_id,
        env,
        mail_handle,
        _stop: stop_tx,
    }
}

/// Creates Mailbox, Decider, and zero or more voter loops. Spawns independent
/// polling loops via `env.spawn_local()`. Returns the `AppServer` container.
pub fn build_and_spawn<T: AgentBus + Clone + 'static, E: Environment + 'static>(
    bus: T,
    config: AppServerConfig,
    env: Rc<E>,
    initial_voters: Vec<Box<dyn Voter>>,
) -> AppServer<T, E> {
    let gate_check_timeout = config.gate_check_timeout;
    let runtime = spawn_bus_runtime(bus, config.bus_id, env, 0, initial_voters);

    AppServer {
        runtime,
        gate_check_timeout,
    }
}

/// Gate logic: propose intention, poll for Commit/Abort within timeout.
/// Uses the environment clock instead of `std::time::Instant` for simulator compatibility.
pub async fn gate_check<T: AgentBus + Clone + 'static, E: Environment + 'static>(
    app: &Rc<AppServer<T, E>>,
    intention: &str,
) -> anyhow::Result<GateResult> {
    gate_check_for_bus(
        app.bus(),
        app.bus_id(),
        app.env(),
        app.gate_check_timeout(),
        intention,
    )
    .await
}

/// Gate logic for an explicit bus ID.
///
/// This is shared by the legacy appserver and services that route requests to
/// arbitrary agent bus IDs.
pub async fn gate_check_for_bus<T: AgentBus, E: Environment>(
    bus: &T,
    bus_id: &str,
    env: &Rc<E>,
    timeout: Duration,
    intention: &str,
) -> anyhow::Result<GateResult> {
    let append_request = AppendRequest {
        agent_bus_id: bus_id.to_string(),
        bus_id: Some(BusId {
            agent_bus_id: bus_id.to_string(),
        }),
        payload: Some(Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention(intention.to_string())),
                ..Default::default()
            })),
        }),
    };

    let append_response = bus.append(append_request).await?;
    let intention_id = append_response.log_position;

    tracing::info!(
        agent_bus_id = %bus_id,
        event_type = event_type::GATE_STARTED,
        "Gate check started for intention {}",
        intention_id,
    );

    let start = env.with_clock(|c| c.monotonic_time());
    let mut next_position = intention_id + 1;

    loop {
        let elapsed = env.with_clock(|c| c.monotonic_time()) - start;
        if elapsed > timeout {
            tracing::warn!(
                agent_bus_id = %bus_id,
                event_type = event_type::GATE_TIMEOUT,
                "Gate check timed out for intention {}",
                intention_id,
            );
            return Ok(GateResult {
                approved: false,
                reason: "Gate check timed out".to_string(),
                intention_id,
            });
        }

        let remaining_ms = (timeout - elapsed).as_millis() as i32;

        let response = bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: bus_id.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_string(),
                }),
                start_log_position: next_position,
                max_entries: 64,
                filter: Some(PayloadTypeFilter {
                    payload_types: vec![
                        SelectivePollType::Commit as i32,
                        SelectivePollType::Abort as i32,
                    ],
                }),
                timeout_ms: remaining_ms,
            })
            .await?;

        for entry in &response.entries {
            if let Some(ref p) = entry.payload {
                match &p.payload {
                    Some(payload::Payload::Commit(commit))
                        if commit.intention_id == intention_id =>
                    {
                        tracing::info!(
                            agent_bus_id = %bus_id,
                            event_type = event_type::GATE_COMPLETED,
                            "Intention {} approved: {}",
                            intention_id,
                            commit.reason,
                        );
                        return Ok(GateResult {
                            approved: true,
                            reason: commit.reason.clone(),
                            intention_id,
                        });
                    }
                    Some(payload::Payload::Abort(abort)) if abort.intention_id == intention_id => {
                        tracing::info!(
                            agent_bus_id = %bus_id,
                            event_type = event_type::GATE_COMPLETED,
                            "Intention {} rejected: {}",
                            intention_id,
                            abort.reason,
                        );
                        return Ok(GateResult {
                            approved: false,
                            reason: abort.reason.clone(),
                            intention_id,
                        });
                    }
                    _ => {}
                }
            }
        }
        next_position = response.next_start_position;
    }
}
