/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Voter engine for agentbus.
//!
//! Concrete voter implementations live in separate crates and implement the
//! `agentbus_api::voter::Voter` trait. This module owns the bus-facing loop.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::intention::Intention as IntentionEnum;
use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
pub use agentbus_api::voter::Voter;
use agentbus_api::voter::VoterContext;
use thiserror::Error;

use crate::tracing_events::event_type;

/// Error type for voter loop operations.
#[derive(Error, Debug)]
pub enum VoterError {
    #[error("AgentBus call failed: {0}")]
    FailedAgentBusCall(#[from] anyhow::Error),

    #[error("Unknown payload type encountered at log position {0}")]
    UnknownPayloadType(i64),
}

/// Engine: polls the bus for intentions and voter policies, dispatches to a
/// `Voter` implementation, and appends votes back to the bus.
pub struct VoterLoop<T: AgentBus, E: Environment> {
    agent_bus: T,
    agent_bus_id: String,
    next_log_position: i64,
    environment: Rc<E>,
    voter: Box<dyn Voter>,
}

impl<T: AgentBus, E: Environment> VoterLoop<T, E> {
    /// `initial_log_position` is the bus position the voter loop starts polling
    /// from; pass 0 to replay from the beginning.
    pub fn new(
        agent_bus: T,
        agent_bus_id: String,
        initial_log_position: i64,
        environment: Rc<E>,
        voter: Box<dyn Voter>,
    ) -> Self {
        Self {
            agent_bus,
            agent_bus_id,
            next_log_position: initial_log_position,
            environment,
            voter,
        }
    }

    /// Poll the AgentBus and process new entries.
    /// `timeout`: how long to wait for new entries. `None` returns immediately (non-blocking).
    pub async fn poll_and_vote(&mut self, timeout: Option<Duration>) -> Result<usize, VoterError> {
        let timeout_ms = timeout.map(|d| d.as_millis() as i32).unwrap_or(0);
        let payload_types = vec![
            SelectivePollType::Intention as i32,
            SelectivePollType::VoterPolicy as i32,
        ];

        let response = self
            .agent_bus
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position: self.next_log_position,
                max_entries: 1,
                filter: Some(PayloadTypeFilter {
                    payload_types: payload_types.clone(),
                }),
                timeout_ms,
            })
            .await
            .map_err(|e| {
                VoterError::FailedAgentBusCall(anyhow::anyhow!("blocking_poll failed: {:?}", e))
            })?;

        let entries_count = response.entries.len();
        for entry in response.entries {
            self.process_entry(entry).await?;
        }
        self.next_log_position = response.next_start_position;

        Ok(entries_count)
    }

    /// Run the voter in a loop, calling poll_and_vote repeatedly.
    /// `blocking_timeout` controls how long each call waits. Defaults to 10s if `None`.
    ///
    /// Runs forever. Prefer `run_until_cancelled` in tests so the loop exits
    /// when the owning `BusRuntime` is dropped.
    pub async fn run(&mut self, blocking_timeout: Option<Duration>) -> Result<(), VoterError> {
        self.run_until_cancelled(blocking_timeout, std::future::pending::<()>())
            .await
    }

    /// Like `run`, but exits when `stop` resolves. Used by `spawn_bus_runtime`
    /// so that dropping the owning `BusRuntime` causes this loop to terminate
    /// at the next yield point.
    pub async fn run_until_cancelled<S>(
        &mut self,
        blocking_timeout: Option<Duration>,
        stop: S,
    ) -> Result<(), VoterError>
    where
        S: std::future::Future + Unpin,
    {
        use futures::FutureExt;
        tracing::info!(
            agent_bus_id = %self.agent_bus_id,
            voter = %self.voter.describe(),
            "Starting VoterLoop"
        );

        const DEFAULT_BLOCKING_TIMEOUT: Duration = Duration::from_secs(10);
        let timeout = Some(blocking_timeout.unwrap_or(DEFAULT_BLOCKING_TIMEOUT));
        let mut stop = stop.fuse();

        loop {
            futures::select_biased! {
                _ = stop => return Ok(()),
                result = self.poll_and_vote(timeout).fuse() => match result {
                    Ok(_) => {}
                    Err(VoterError::FailedAgentBusCall(e)) => {
                        tracing::error!("AgentBus call failed: {}", e);
                        self.environment.sleep(Duration::from_millis(500)).await;
                    }
                    Err(VoterError::UnknownPayloadType(position)) => {
                        tracing::warn!("Unknown payload type at log position {}", position);
                        self.next_log_position = position + 1;
                    }
                }
            }
        }
    }

    async fn process_entry(&mut self, entry: BusEntry) -> Result<(), VoterError> {
        let log_position = entry.header.as_ref().map(|h| h.log_position).unwrap_or(0);

        if let Some(payload) = entry.payload {
            match payload.payload {
                Some(payload::Payload::Intention(intention)) => {
                    self.process_intention(log_position, intention).await?;
                }
                Some(payload::Payload::VoterPolicy(voter_policy)) => {
                    self.process_voter_policy(voter_policy);
                }
                Some(payload::Payload::Vote(_))
                | Some(payload::Payload::DeciderPolicy(_))
                | Some(payload::Payload::Commit(_))
                | Some(payload::Payload::Abort(_))
                | Some(payload::Payload::Control(_))
                | Some(payload::Payload::InferenceInput(_))
                | Some(payload::Payload::InferenceOutput(_))
                | Some(payload::Payload::ActionOutput(_))
                | Some(payload::Payload::AgentInput(_))
                | Some(payload::Payload::AgentOutput(_)) => {
                    return Err(VoterError::UnknownPayloadType(log_position));
                }
                Some(payload::Payload::Mail(_)) => {}
                None => return Err(VoterError::UnknownPayloadType(log_position)),
            }
        }

        self.next_log_position = log_position + 1;
        Ok(())
    }

    async fn process_intention(
        &mut self,
        log_position: i64,
        intention: Intention,
    ) -> Result<(), VoterError> {
        let code = match &intention.intention {
            Some(IntentionEnum::StringIntention(s)) => s.as_str(),
            None => "",
        };

        let (is_safe, reason) = self
            .voter
            .evaluate(VoterContext::new(&self.agent_bus_id, code))
            .await;
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(is_safe)),
            }),
            intention_id: log_position,
            voter_config: None,
            voter_id: String::new(),
            reason,
        };

        self.agent_bus
            .append(AppendRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Vote(vote)),
                }),
            })
            .await
            .map_err(|e| {
                VoterError::FailedAgentBusCall(anyhow::anyhow!("Append vote failed: {:?}", e))
            })?;

        tracing::info!(
            agent_bus_id = %self.agent_bus_id,
            event_type = event_type::VOTER_DECISION,
            "Voted on intention {}: approved={} voter={}",
            log_position,
            is_safe,
            self.voter.describe(),
        );

        Ok(())
    }

    fn process_voter_policy(&mut self, voter_policy: VoterPolicy) {
        match voter_policy.config {
            Some(any) => self.voter.apply_policy(&any),
            None => {
                tracing::warn!(
                    agent_bus_id = %self.agent_bus_id,
                    "VoterPolicy with no config — ignoring"
                );
            }
        }
    }
}
