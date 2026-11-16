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
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::abort_payload;
use logact_commit_service_engine::apply_policy_entry;
use logact_commit_service_engine::load_decider_spec_from_state;
use logact_commit_service_engine::policy_constraint_failure;
use logact_commit_service_engine::state_voter_configs;

use crate::ApplicatorLite;
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

/// Materialization as an [`ApplicatorLite`]: applies an entry to `state`, in V1
/// playback order (the policy-version constraint, the decider and voters, then
/// any policy change), and returns the outputs it calls for.
pub struct Materializer<'a> {
    pub state: &'a mut ReplayState,
    pub validate_config: &'a dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>,
}

impl ApplicatorLite for Materializer<'_> {
    type Output = Step;

    fn apply(&mut self, entry: &BusEntry) -> Result<Step, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .ok_or(ApplyError::MissingHeader)?
            .log_position;
        let Some(payload) = entry.payload.as_ref() else {
            return Ok(Step::default());
        };
        let mut step = Step::default();
        if let Some(reason) =
            policy_constraint_failure(payload, self.state.policy.applied_policy_version)
        {
            step.decision = Some(abort_payload(position, &reason));
        } else if let Some(inner) = payload.payload.as_ref() {
            step.decision = self.state.decider.apply(entry)?;
            if matches!(inner, payload::Payload::Intention(_)) {
                step.voters = state_voter_configs(&self.state.policy);
            }
        }

        let decider_before = self.state.policy.decider.clone();
        PolicyPlayback {
            policy: &mut self.state.policy,
            validate_config: self.validate_config,
        }
        .apply(entry)?;
        if self.state.policy.decider != decider_before {
            // A newly installed decider starts from empty state, even with an
            // unchanged policy, as V1 keys decider state by the installing entry.
            self.state.decider =
                DeciderMachine::new(load_decider_spec_from_state(&self.state.policy)?.config);
        }
        Ok(step)
    }
}

/// Policy playback as an [`ApplicatorLite`]: applies the policy change, if any,
/// that an entry carries. It produces no output.
struct PolicyPlayback<'a> {
    policy: &'a mut PerBusEngineState,
    validate_config: &'a dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>,
}

impl ApplicatorLite for PolicyPlayback<'_> {
    type Output = Option<Payload>;

    fn apply(&mut self, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry
            .header
            .as_ref()
            .ok_or(ApplyError::MissingHeader)?
            .log_position;
        if let Some(payload) = entry.payload.as_ref() {
            apply_policy_entry(self.policy, payload, position, self.validate_config)?;
        }
        Ok(None)
    }
}
