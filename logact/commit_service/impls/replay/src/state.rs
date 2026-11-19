/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::SpeculativeDecision;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::payload;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::abort_payload;
use logact_commit_service_engine::apply_policy_entry;
use logact_commit_service_engine::load_decider_spec_from_state;
use logact_commit_service_engine::policy_constraint_failure;
use logact_commit_service_engine::state_voter_configs;

use crate::CheckpointError;
use crate::CheckpointResult;
use crate::decider::DeciderMachine;

/// The state materialized from a bus's log: the installed policy and the
/// decider's state. It is a deterministic function of the log.
#[derive(Clone, Debug, Default)]
pub struct ReplayState {
    pub(crate) policy: PerBusEngineState,
    decider: DeciderMachine,
}

/// The outputs one materialized entry calls for.
pub enum Step {
    /// The intention's speculative decision is correct: the intention is
    /// decided at its own position, without a logged `Commit` or `Abort`, and
    /// nothing else is called for.
    ShortCircuited(SpeculativeDecision),
    /// Continue the protocol: append `decision`, if any, and ask `voters`
    /// about the entry, which are only ever set for an intention.
    Continue {
        decision: Option<Payload>,
        voters: Vec<(String, VoterConfig)>,
    },
}

impl ReplayState {
    /// Materialize `entry`, in V1 playback order: the policy-version constraint,
    /// the decider and voters, then any policy change. An intention with a
    /// correct speculative decision short-circuits after the constraint.
    /// Returns the outputs it calls for.
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
            return Ok(Step::Continue {
                decision: None,
                voters: Vec::new(),
            });
        };
        let mut decision = None;
        let mut voters = Vec::new();
        if let Some(reason) = policy_constraint_failure(payload, self.policy.applied_policy_version)
        {
            decision = Some(abort_payload(position, &reason));
        } else if let Some(inner) = payload.payload.as_ref() {
            if let payload::Payload::Intention(intention) = inner
                && let Some(speculation) = intention.speculative_decision.as_ref()
                && self.policy.applied_policy_version == Some(speculation.policy_version)
            {
                // Short-circuit for correct speculative decision.
                return Ok(Step::ShortCircuited(speculation.clone()));
            }
            decision = self.decider.step(position, inner);
            if matches!(inner, payload::Payload::Intention(_)) {
                voters = state_voter_configs(&self.policy);
            }
        }

        let decider_before = self.policy.decider.clone();
        apply_policy_entry(&mut self.policy, payload, position, validate_config)?;
        if self.policy.decider != decider_before {
            // A newly installed decider starts from empty state, even with an
            // unchanged policy, as V1 keys decider state by the installing entry.
            self.decider = DeciderMachine::new(load_decider_spec_from_state(&self.policy)?.config);
        }
        Ok(Step::Continue { decision, voters })
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
    pub(crate) fn from_engine_state(mut policy: PerBusEngineState) -> CheckpointResult<Self> {
        let stored = policy.replay_decider_state.take();
        let spec = load_decider_spec_from_state(&policy)
            .map_err(|error| CheckpointError::Internal(anyhow::Error::new(error)))?;
        let decider =
            DeciderMachine::from_stored(spec.config, stored).map_err(CheckpointError::Internal)?;
        Ok(Self { policy, decider })
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::intention;
    use prost::Message;

    use super::*;

    fn intention(speculative_decision: Option<SpeculativeDecision>) -> Payload {
        Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("tool".to_string())),
                policy_version_constraint: None,
                speculative_decision,
            })),
        }
    }

    fn materialize(state: &mut ReplayState, position: i64, payload: &Payload) -> Step {
        let entry = BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(payload.clone()),
        };
        state
            .materialize(&entry, &|_| anyhow::bail!("no voters are configured"))
            .expect("the entry should materialize")
    }

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

    #[test]
    fn a_v1_intention_is_decided_by_the_decider() {
        let mut state = ReplayState::default();
        let Step::Continue { decision, .. } = materialize(&mut state, 0, &intention(None)) else {
            panic!("a V1 intention should continue the protocol");
        };
        assert!(matches!(
            decision.and_then(|decision| decision.payload),
            Some(payload::Payload::Commit(_))
        ));
    }

    #[test]
    fn a_speculation_is_correct_only_at_the_applied_version() {
        let mut state = ReplayState::default();
        state.policy.applied_policy_version = Some(3);
        let speculation = |policy_version| SpeculativeDecision {
            policy_version,
            approved: false,
            reason: "speculated".to_string(),
        };

        assert!(
            matches!(
                materialize(&mut state, 5, &intention(Some(speculation(3)))),
                Step::ShortCircuited(short_circuited) if short_circuited == speculation(3)
            ),
            "a correct speculation short-circuits"
        );

        let Step::Continue { decision, .. } =
            materialize(&mut state, 6, &intention(Some(speculation(2))))
        else {
            panic!("a stale speculation should continue the protocol");
        };
        assert!(
            decision.is_some(),
            "a stale speculation falls back to the decider"
        );
    }
}
