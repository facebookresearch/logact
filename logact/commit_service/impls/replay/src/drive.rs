/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
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

use crate::ReplayState;
use crate::player::LogPlayer;
use crate::player::append;
use crate::player::entry_position;

/// One drive: materializes the log forward from a checkpoint until an intention
/// is decided, appending each entry's decision and votes as it goes.
pub(crate) struct Drive<'a, T, F> {
    bus: &'a T,
    bus_id: &'a str,
    player: LogPlayer<'a, T>,
    voter_factory: &'a F,
    pub(crate) state: ReplayState,
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
            for entry in &batch {
                let position = self.materialize(entry).await?;
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

    /// Materialize `entry`, appending its outputs, and return its position.
    async fn materialize(&mut self, entry: &BusEntry) -> CommitResult<i64> {
        let position = entry_position(entry)?;
        if entry.payload.is_some() {
            let step = self
                .state
                .materialize(entry, &|config| self.voter_factory.validate_config(config))
                .map_err(apply_error)?;
            if let Some(decision) = step.decision {
                self.append(decision).await?;
            }
            for (voter_id, config) in step.voters {
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
        Ok(position)
    }

    async fn append(&mut self, payload: Payload) -> CommitResult<()> {
        let position = append(self.bus, self.bus_id, payload).await?;
        self.player.saw_append(position);
        Ok(())
    }
}

pub fn apply_error(error: ApplyError) -> CommitError {
    match error {
        ApplyError::Bus(error) => CommitError::from_bus_error(error),
        error => CommitError::Internal(anyhow::Error::new(error)),
    }
}
