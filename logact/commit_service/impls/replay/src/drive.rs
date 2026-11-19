/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::SpeculativeDecision;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use anyhow::Context;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::StateMachineSpec;
use logact_commit_service_engine::VoterFactory;
use logact_commit_service_engine::forced_abort;
use logact_commit_service_engine::outcome_for;
use prost::Message;

use crate::ReplayState;
use crate::decider::decided_intention;
use crate::player::LogPlayer;
use crate::player::append;
use crate::player::entry_position;
use crate::state::Step;

/// One drive: materializes the log forward from a checkpoint until an intention
/// is decided, appending each entry's decision and votes as it goes, unless an
/// equivalent output is already logged.
pub(crate) struct Drive<'a, T, F> {
    bus: &'a T,
    bus_id: &'a str,
    player: LogPlayer<'a, T>,
    voter_factory: &'a F,
    pub(crate) state: ReplayState,
    /// Intentions with a decision in the entries this drive has read or appended.
    logged_decisions: HashSet<i64>,
    /// Votes in those entries, by intention, voter ID, and encoded voter config.
    logged_votes: HashSet<(i64, String, Vec<u8>)>,
}

impl<'a, T: AgentBus, F: VoterFactory> Drive<'a, T, F> {
    /// Start from `state`, which is materialized through `start - 1`.
    pub(crate) fn new(
        bus: &'a T,
        bus_id: &'a str,
        start: i64,
        voter_factory: &'a F,
        state: ReplayState,
    ) -> Self {
        Self {
            bus,
            bus_id,
            player: LogPlayer::new(bus, bus_id, start),
            voter_factory,
            state,
            logged_decisions: HashSet::new(),
            logged_votes: HashSet::new(),
        }
    }

    /// Materialize until `proposed_position` has a decision, returning it and
    /// the position of the deciding entry, through which `state` is then
    /// materialized. Forces an abort if the log drains first.
    pub(crate) async fn until_decided(
        &mut self,
        proposed_position: i64,
    ) -> CommitResult<(CommitIntentionOutcome, i64)> {
        self.player.saw_append(proposed_position);
        loop {
            let batch = self.player.next_batch().await?;
            if batch.is_empty() {
                self.append(forced_abort(proposed_position)).await?;
                continue;
            }
            for payload in batch.iter().filter_map(|entry| entry.payload.as_ref()) {
                self.record(payload);
            }
            for entry in &batch {
                let position = entry_position(entry)?;
                if let Some(speculation) = self.materialize(entry).await? {
                    // Check if it's the intention this commit proposed.
                    if position == proposed_position {
                        let outcome = CommitIntentionOutcome {
                            approved: speculation.approved,
                            reason: speculation.reason,
                            log_position: position,
                        };
                        return Ok((outcome, position));
                    }
                    continue;
                }
                if let Some(outcome) = entry
                    .payload
                    .as_ref()
                    .and_then(|payload| outcome_for(payload, proposed_position))
                {
                    let outcome = CommitIntentionOutcome {
                        approved: outcome.approved,
                        reason: outcome.reason,
                        log_position: outcome.log_position,
                    };
                    return Ok((outcome, position));
                }
            }
        }
    }

    /// Materialize `entry`, appending its outputs. Returns the intention's
    /// speculative decision if it is correct, which calls for no outputs.
    async fn materialize(&mut self, entry: &BusEntry) -> CommitResult<Option<SpeculativeDecision>> {
        let position = entry_position(entry)?;
        if entry.payload.is_some() {
            let decided_earlier = self.logged_decisions.contains(&position);
            let step = self
                .state
                .materialize(entry, &|config| self.voter_factory.validate_config(config))
                .map_err(apply_error)?;
            let (decision, voters) = match step {
                // Short-circuit for correct speculative decision.
                Step::ShortCircuited(speculation) => return Ok(Some(speculation)),
                Step::Continue { decision, voters } => (decision, voters),
            };
            if let Some(decision) = decision
                && !decision
                    .payload
                    .as_ref()
                    .and_then(decided_intention)
                    .is_some_and(|intention_id| self.logged_decisions.contains(&intention_id))
            {
                self.append(decision).await?;
            }
            // Voters may be expensive (e.g. LLM calls), so an intention decided
            // before this drive reached it is not evaluated again.
            if !decided_earlier {
                for (voter_id, config) in voters {
                    let key = (position, voter_id.clone(), config.encode_to_vec());
                    if self.logged_votes.contains(&key) {
                        continue;
                    }
                    let voter = self
                        .voter_factory
                        .create_voter(&StateMachineSpec::new(voter_id.clone(), config))
                        .with_context(|| format!("building voter '{voter_id}'"))
                        .map_err(CommitError::Internal)?;
                    if let Some(vote) = voter
                        .apply(self.bus_id, entry, &mut ())
                        .await
                        .map_err(apply_error)?
                    {
                        self.append(vote).await?;
                    }
                }
            }
        }
        Ok(None)
    }

    async fn append(&mut self, payload: Payload) -> CommitResult<()> {
        self.record(&payload);
        let position = append(self.bus, self.bus_id, payload).await?;
        self.player.saw_append(position);
        Ok(())
    }

    fn record(&mut self, payload: &Payload) {
        match payload.payload.as_ref() {
            Some(payload::Payload::Vote(vote)) => {
                let config = vote.voter_config.as_ref().map(Message::encode_to_vec);
                self.logged_votes.insert((
                    vote.intention_id,
                    vote.voter_id.clone(),
                    config.unwrap_or_default(),
                ));
            }
            Some(inner) => {
                if let Some(intention_id) = decided_intention(inner) {
                    self.logged_decisions.insert(intention_id);
                }
            }
            None => {}
        }
    }
}

pub fn apply_error(error: ApplyError) -> CommitError {
    match error {
        ApplyError::Bus(error) => CommitError::from_bus_error(error),
        error => CommitError::Internal(anyhow::Error::new(error)),
    }
}
