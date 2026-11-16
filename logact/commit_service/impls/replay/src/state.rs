/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::payload;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitResult;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::abort_payload;
use logact_commit_service_engine::apply_policy_entry;
use logact_commit_service_engine::load_decider_spec_from_state;
use logact_commit_service_engine::policy_constraint_failure;
use logact_commit_service_engine::state_voter_configs;

use crate::decider::DeciderMachine;

/// The state materialized from a bus's log: the installed policy and the
/// decider's state. It is a deterministic function of the log.
#[derive(Clone, Debug, Default)]
pub struct ReplayState {
    pub(crate) policy: PerBusEngineState,
    decider: DeciderMachine,
}

/// The outputs one materialized entry calls for.
#[derive(Default)]
pub struct Step {
    pub decision: Option<Payload>,
    /// The voters to ask about the entry, if it is an intention.
    pub voters: Vec<(String, VoterConfig)>,
}

impl ReplayState {
    /// Materialize `entry`, in V1 playback order: the policy-version constraint,
    /// the decider and voters, then any policy change. Returns the outputs it
    /// calls for.
    pub fn materialize(
        &mut self,
        entry: &BusEntry,
        validate_config: &dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>,
    ) -> Result<Step, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .ok_or(ApplyError::MissingHeader)?
            .log_position;
        let Some(payload) = entry.payload.as_ref() else {
            return Ok(Step::default());
        };
        let mut step = Step::default();
        if let Some(reason) = policy_constraint_failure(payload, self.policy.applied_policy_version)
        {
            step.decision = Some(abort_payload(position, &reason));
        } else if let Some(inner) = payload.payload.as_ref() {
            step.decision = self.decider.step(position, inner);
            if matches!(inner, payload::Payload::Intention(_)) {
                step.voters = state_voter_configs(&self.policy);
            }
        }

        let decider_before = self.policy.decider.clone();
        apply_policy_entry(&mut self.policy, payload, position, validate_config)?;
        if self.policy.decider != decider_before {
            // A newly installed decider starts from empty state, even with an
            // unchanged policy, as V1 keys decider state by the installing entry.
            self.decider = DeciderMachine::new(load_decider_spec_from_state(&self.policy)?.config);
        }
        Ok(step)
    }
}

impl ReplayState {
    /// This state as V1's engine state, with the decider's state in
    /// `replay_decider_state`.
    pub(crate) fn to_engine_state(&self) -> PerBusEngineState {
        PerBusEngineState {
            replay_decider_state: self.decider.to_stored(),
            ..self.policy.clone()
        }
    }

    /// The state an engine-state record holds, written by V1 or by this crate.
    pub(crate) fn from_engine_state(mut policy: PerBusEngineState) -> CommitResult<Self> {
        let stored = policy.replay_decider_state.take();
        let spec = load_decider_spec_from_state(&policy)
            .map_err(|error| CommitError::Internal(anyhow::Error::new(error)))?;
        let decider =
            DeciderMachine::from_stored(spec.config, stored).map_err(CommitError::Internal)?;
        Ok(Self { policy, decider })
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use prost::Message;

    use super::*;

    #[test]
    fn decider_state_round_trips_through_v1_engine_state() {
        let state = ReplayState {
            policy: PerBusEngineState {
                decider: Some(logact_commit_service_engine::VersionedDecider {
                    policy: DeciderPolicy::FirstBooleanWins as i32,
                    id: 2,
                }),
                applied_policy_version: Some(3),
                ..Default::default()
            },
            decider: DeciderMachine::FirstBooleanWins { pending: Some(4) },
        };

        let record = state.to_engine_state().encode_to_vec();
        let v1 = PerBusEngineState::decode(record.as_slice()).expect("V1 should decode the record");
        assert_eq!(v1.applied_policy_version, Some(3));
        let decoded = ReplayState::from_engine_state(v1).expect("the record should decode");
        assert_eq!(decoded.decider, state.decider);
    }
}
