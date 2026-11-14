/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::AddVoterOp;
use agent_bus_proto_rust::agent_bus::BaseEngineControl;
use agent_bus_proto_rust::agent_bus::Control;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::RemoveVoterOp;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::VoterOp;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::voter_op;
use anyhow::Context;
use logact_commit_service_api::Policy;
use logact_commit_service_api::VersionedPolicy;

use crate::ApplyError;
use crate::DeprecatedOperationKind;
use crate::MalformedPolicyBatchKind;
use crate::PerBusEngineState;
use crate::VersionedDecider;

/// Plan the `PolicyBatch` that moves a bus from `current`, applied at
/// `current_version`, to `desired`. `None` means no policy is installed yet.
/// Returns `None` if `desired` is not newer than `current_version`.
pub fn plan(
    current: Option<&Policy>,
    current_version: Option<i64>,
    desired: &VersionedPolicy,
) -> anyhow::Result<Option<PolicyBatch>> {
    if current_version.is_some_and(|version| desired.version <= version) {
        return Ok(None);
    }

    let mut batch = PolicyBatch {
        expected_current_version: current_version,
        new_version: desired.version,
        ..Default::default()
    };
    if current.is_none_or(|current| current.decider_policy != desired.policy.decider_policy) {
        batch.decider_policy = Some(desired.policy.decider_policy as i32);
    }
    let current_voters = current.map(|current| &current.voters);
    for (voter_id, desired_config) in &desired.policy.voters {
        match current_voters.and_then(|voters| voters.get(voter_id)) {
            Some(current_config) if current_config == desired_config => {}
            Some(_) => {
                anyhow::bail!("voter '{voter_id}' is already installed with a different config");
            }
            None => {
                batch.voter_ops.insert(
                    voter_id.clone(),
                    VoterOp {
                        op: Some(voter_op::Op::Add(AddVoterOp {
                            config: Some(desired_config.clone()),
                        })),
                    },
                );
            }
        }
    }
    for voter_id in current_voters.into_iter().flat_map(|voters| voters.keys()) {
        if !desired.policy.voters.contains_key(voter_id) {
            batch.voter_ops.insert(
                voter_id.clone(),
                VoterOp {
                    op: Some(voter_op::Op::Remove(RemoveVoterOp {})),
                },
            );
        }
    }
    Ok(Some(batch))
}

/// The policy installed in `state`. An unconfigured decider is `ON_BY_DEFAULT`.
pub fn installed_policy(state: &PerBusEngineState) -> anyhow::Result<Policy> {
    Ok(Policy {
        decider_policy: state
            .decider
            .as_ref()
            .map(|decider| DeciderPolicy::try_from(decider.policy))
            .transpose()
            .context("engine state has an invalid decider policy")?
            .unwrap_or(DeciderPolicy::OnByDefault),
        voters: state
            .voters
            .iter()
            .map(|(voter_id, config)| (voter_id.clone(), config.clone()))
            .collect(),
    })
}

/// Apply the policy change, if any, carried by the payload logged at
/// `entry_position`. Returns whether `state` changed. A malformed or deprecated
/// policy entry is an error and leaves `state` unchanged.
pub fn apply_policy_entry(
    state: &mut PerBusEngineState,
    payload: &Payload,
    entry_position: i64,
    validate_config: &dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>,
) -> Result<bool, ApplyError> {
    match base_engine_control_of(payload) {
        Some(base_engine_control::Control::PolicyBatch(batch)) => {
            if !should_apply_policy_batch(state, batch) {
                return Ok(false);
            }
            apply_policy_batch(state, batch, entry_position, validate_config)?;
            Ok(true)
        }
        None if matches!(
            payload.payload.as_ref(),
            Some(payload::Payload::Control(c))
                if matches!(c.control.as_ref(), Some(control::Control::BaseEngineControl(_)))
        ) =>
        {
            Err(malformed_policy(
                MalformedPolicyBatchKind::PolicyBatch,
                anyhow::anyhow!("BaseEngineControl policy batch is missing or unrecognized"),
            ))
        }
        None if matches!(
            payload.payload.as_ref(),
            Some(payload::Payload::DeciderPolicy(_))
        ) =>
        {
            Err(ApplyError::DeprecatedOperation {
                operation: DeprecatedOperationKind::SetDeciderPolicy,
            })
        }
        None => Ok(false),
    }
}

/// Apply `batch`, logged at `entry_position`, to `state`.
pub(crate) fn apply_policy_batch(
    state: &mut PerBusEngineState,
    batch: &PolicyBatch,
    entry_position: i64,
    validate_config: &dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>,
) -> Result<(), ApplyError> {
    let voter_ops = batch
        .voter_ops
        .iter()
        .map(|(voter_id, op)| {
            let op = match op.op.as_ref() {
                Some(voter_op::Op::Add(add)) => {
                    let config = validate_config(add.config.as_ref())
                        .with_context(|| {
                            format!("PolicyBatch voter '{voter_id}' has invalid config")
                        })
                        .map_err(|error| {
                            malformed_policy(MalformedPolicyBatchKind::AddVoter, error)
                        })?;
                    ValidatedVoterOp::Add(config)
                }
                Some(voter_op::Op::Remove(_)) => {
                    if !state.voters.contains_key(voter_id) {
                        return Err(malformed_policy(
                            MalformedPolicyBatchKind::RemoveVoter,
                            anyhow::anyhow!("PolicyBatch cannot remove unknown voter '{voter_id}'"),
                        ));
                    }
                    ValidatedVoterOp::Remove
                }
                None => {
                    return Err(malformed_policy(
                        MalformedPolicyBatchKind::PolicyBatch,
                        anyhow::anyhow!("PolicyBatch voter '{voter_id}' has no operation"),
                    ));
                }
            };
            Ok((voter_id, op))
        })
        .collect::<Result<Vec<_>, ApplyError>>()?;

    set_decider_policy_in_engine_state(state, batch.decider_policy, entry_position)
        .with_context(|| {
            format!(
                "PolicyBatch decider policy {:?} is invalid",
                batch.decider_policy
            )
        })
        .map_err(|error| malformed_policy(MalformedPolicyBatchKind::PolicyBatch, error))?;
    state.applied_policy_version = Some(batch.new_version);
    for (voter_id, op) in voter_ops {
        match op {
            ValidatedVoterOp::Add(config) => {
                state.voters.insert(voter_id.clone(), config);
            }
            ValidatedVoterOp::Remove => {
                state.voters.remove(voter_id);
            }
        }
    }
    Ok(())
}

/// Why `payload`, if it is an intention, violates its policy-version constraint
/// at `applied_version`.
pub fn policy_constraint_failure(
    payload: &Payload,
    applied_version: Option<i64>,
) -> Option<String> {
    let payload::Payload::Intention(intention) = payload.payload.as_ref()? else {
        return None;
    };
    match intention.policy_version_constraint.as_ref()? {
        intention::PolicyVersionConstraint::RequiredPolicyVersion(required)
            if applied_version != Some(*required) =>
        {
            Some(format!(
                "required policy version {required}, but applied version is {applied_version:?}"
            ))
        }
        intention::PolicyVersionConstraint::MinimumPolicyVersion(minimum)
            if applied_version.is_none_or(|applied| applied < *minimum) =>
        {
            Some(format!(
                "minimum policy version {minimum}, but applied version is {applied_version:?}"
            ))
        }
        _ => None,
    }
}

/// Wrap a `PolicyBatch` in the `Control` payload the engine appends to provision
/// policy as a single atomic log entry.
pub fn policy_batch_payload(batch: PolicyBatch) -> Payload {
    Payload {
        payload: Some(payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::PolicyBatch(batch)),
            })),
        })),
    }
}

fn should_apply_policy_batch(state: &PerBusEngineState, batch: &PolicyBatch) -> bool {
    state.applied_policy_version == batch.expected_current_version
        && batch
            .expected_current_version
            .is_none_or(|current| batch.new_version > current)
}

fn malformed_policy(kind: MalformedPolicyBatchKind, error: anyhow::Error) -> ApplyError {
    ApplyError::MalformedPolicy {
        kind,
        message: format!("{error:#}"),
    }
}

enum ValidatedVoterOp {
    Add(VoterConfig),
    Remove,
}

/// Record `policy` with this entry's log position as its ID.
fn set_decider_policy_in_engine_state(
    state: &mut PerBusEngineState,
    policy: Option<i32>,
    position: i64,
) -> anyhow::Result<()> {
    if let Some(policy) = policy {
        let policy = DeciderPolicy::try_from(policy).context("invalid decider policy")?;
        state.decider = Some(VersionedDecider {
            policy: policy as i32,
            id: position,
        });
    }
    Ok(())
}

/// The `BaseEngineControl` directive carried by `payload`, or `None` for other
/// payloads such as a standalone `DeciderPolicy`.
fn base_engine_control_of(payload: &Payload) -> Option<&base_engine_control::Control> {
    match payload.payload.as_ref()? {
        payload::Payload::Control(control) => {
            let control::Control::BaseEngineControl(base) = control.control.as_ref()?;
            base.control.as_ref()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::VoterConfig;
    use agent_bus_proto_rust::agent_bus::voter_config;

    use super::*;

    fn voter_config(name: &str) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::Custom(prost_types::Any {
                type_url: name.to_string(),
                value: Vec::new(),
            })),
        }
    }

    fn voter(name: &str) -> (String, VoterConfig) {
        (name.to_string(), voter_config(name))
    }

    fn policy(
        decider_policy: DeciderPolicy,
        voters: impl IntoIterator<Item = (String, VoterConfig)>,
    ) -> Policy {
        Policy {
            decider_policy,
            voters: voters.into_iter().collect(),
        }
    }

    fn desired(policy: Policy, version: i64) -> VersionedPolicy {
        VersionedPolicy { policy, version }
    }

    #[test]
    fn installed_policy_uses_typed_decider_and_preserves_voters() {
        let voters = [voter("a")].into_iter().collect();
        let mut state = PerBusEngineState {
            voters,
            ..Default::default()
        };
        let installed = installed_policy(&state).expect("default policy should load");
        assert_eq!(installed.decider_policy, DeciderPolicy::OnByDefault);
        assert_eq!(installed.voters, state.voters);

        state.decider = Some(VersionedDecider {
            policy: DeciderPolicy::FirstBooleanWins as i32,
            id: 17,
        });
        let installed = installed_policy(&state).expect("configured policy should load");
        assert_eq!(installed.decider_policy, DeciderPolicy::FirstBooleanWins);
        assert_eq!(installed.voters, state.voters);

        state.decider = Some(VersionedDecider { policy: 99, id: 17 });
        assert!(installed_policy(&state).is_err());
    }

    #[test]
    fn plan_initial_on_by_default_policy_sets_decider() {
        let desired = desired(policy(DeciderPolicy::OnByDefault, []), 1);
        let batch = plan(None, None, &desired)
            .expect("initial policy should be valid")
            .expect("an unconfigured bus needs an initial policy batch");
        assert_eq!(
            batch.decider_policy,
            Some(DeciderPolicy::OnByDefault as i32)
        );
        assert_eq!(batch.expected_current_version, None);
        assert_eq!(batch.new_version, 1);
    }

    #[test]
    fn plan_sets_policy_and_adds_missing_voters() {
        let desired = desired(
            policy(DeciderPolicy::OffByDefault, [voter("a"), voter("b")]),
            3,
        );

        let batch = plan(None, None, &desired)
            .expect("planning should succeed")
            .expect("a newer desired policy should produce a batch");
        assert_eq!(batch.expected_current_version, None);
        assert_eq!(batch.new_version, 3);
        assert_eq!(
            batch.decider_policy,
            Some(DeciderPolicy::OffByDefault as i32)
        );
        assert_eq!(batch.voter_ops.len(), 2);
        assert!(batch.voter_ops.contains_key("a"));
        assert!(batch.voter_ops.contains_key("b"));
    }

    #[test]
    fn plan_returns_no_batch_for_equal_or_older_versions() {
        let current = policy(DeciderPolicy::OnByDefault, []);
        let desired = desired(current.clone(), 3);

        assert!(
            plan(Some(&current), Some(3), &desired)
                .expect("planning should succeed")
                .is_none()
        );
        assert!(
            plan(Some(&current), Some(4), &desired)
                .expect("planning should succeed")
                .is_none()
        );
    }

    #[test]
    fn plan_returns_version_marker_when_content_is_unchanged() {
        let current = policy(DeciderPolicy::OffByDefault, [voter("a")]);
        let desired = desired(current.clone(), 4);

        let batch = plan(Some(&current), Some(3), &desired)
            .expect("planning should succeed")
            .expect("a newer version should produce a marker batch");
        assert_eq!(batch.expected_current_version, Some(3));
        assert_eq!(batch.new_version, 4);
        assert_eq!(batch.decider_policy, None);
        assert!(batch.voter_ops.is_empty());
    }

    #[test]
    fn plan_only_adds_voters_not_already_present() {
        let current = policy(DeciderPolicy::OffByDefault, [voter("a")]);
        let desired = desired(
            policy(DeciderPolicy::OffByDefault, [voter("a"), voter("b")]),
            2,
        );

        let batch = plan(Some(&current), Some(1), &desired)
            .expect("planning should succeed")
            .expect("a newer desired policy should produce a batch");
        assert_eq!(batch.voter_ops.len(), 1);
        assert!(batch.voter_ops.contains_key("b"));
    }

    #[test]
    fn plan_rejects_changed_config_for_existing_voter() {
        let current = policy(
            DeciderPolicy::OffByDefault,
            [("a".to_string(), voter_config("old"))],
        );
        let desired = desired(
            policy(
                DeciderPolicy::OffByDefault,
                [("a".to_string(), voter_config("new"))],
            ),
            2,
        );

        let error = plan(Some(&current), Some(1), &desired)
            .expect_err("changing the config for an existing voter should fail");
        assert_eq!(
            error.to_string(),
            "voter 'a' is already installed with a different config"
        );
    }

    #[test]
    fn plan_removes_voters_absent_from_desired_policy() {
        let current = policy(DeciderPolicy::OffByDefault, [voter("a"), voter("b")]);
        let desired = desired(policy(DeciderPolicy::OffByDefault, [voter("b")]), 2);

        let batch = plan(Some(&current), Some(1), &desired)
            .expect("planning should succeed")
            .expect("a newer desired policy should produce a batch");
        assert_eq!(batch.voter_ops.len(), 1);
        assert!(matches!(
            batch.voter_ops.get("a").and_then(|op| op.op.as_ref()),
            Some(voter_op::Op::Remove(_))
        ));
    }

    #[test]
    fn repeated_policy_entry_changes_id() {
        let mut state = PerBusEngineState {
            decider: Some(VersionedDecider {
                policy: DeciderPolicy::FirstBooleanWins as i32,
                id: 10,
            }),
            ..Default::default()
        };

        set_decider_policy_in_engine_state(
            &mut state,
            Some(DeciderPolicy::FirstBooleanWins as i32),
            20,
        )
        .expect("recognized policy should be stored");

        assert_eq!(
            state.decider.expect("policy should remain configured").id,
            20
        );
    }

    #[test]
    fn unrecognized_policy_entry_is_rejected_without_mutation() {
        let mut state = PerBusEngineState {
            decider: Some(VersionedDecider {
                policy: DeciderPolicy::FirstBooleanWins as i32,
                id: 10,
            }),
            ..Default::default()
        };
        let original_decider = state.decider.clone();

        let error = set_decider_policy_in_engine_state(&mut state, Some(99), 17)
            .expect_err("unknown policy should be rejected");

        assert!(error.to_string().contains("invalid decider policy"));
        assert_eq!(state.decider, original_decider);
    }

    #[test]
    fn base_engine_control_of_reads_a_batch() {
        let mut voter_ops = HashMap::new();
        voter_ops.insert(
            "v".to_string(),
            VoterOp {
                op: Some(voter_op::Op::Add(AddVoterOp {
                    config: Some(VoterConfig::default()),
                })),
            },
        );
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(payload::Payload::Control(Control {
                    control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(PolicyBatch {
                            decider_policy: None,
                            voter_ops,
                            ..Default::default()
                        })),
                    })),
                })),
            }),
        };
        assert!(matches!(
            base_engine_control_of(entry.payload.as_ref().expect("entry should have a payload")),
            Some(base_engine_control::Control::PolicyBatch(batch)) if batch.voter_ops.len() == 1
        ));
    }

    #[test]
    fn base_engine_control_of_ignores_non_control() {
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(payload::Payload::DeciderPolicy(0)),
            }),
        };
        assert!(
            base_engine_control_of(entry.payload.as_ref().expect("entry should have a payload"))
                .is_none()
        );
    }

    fn constrained_intention(constraint: intention::PolicyVersionConstraint) -> Payload {
        Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("test".to_string())),
                policy_version_constraint: Some(constraint),
            })),
        }
    }

    #[test]
    fn intention_policy_version_constraints_are_enforced() {
        let exact =
            constrained_intention(intention::PolicyVersionConstraint::RequiredPolicyVersion(5));
        assert!(policy_constraint_failure(&exact, Some(5)).is_none());
        assert!(policy_constraint_failure(&exact, Some(6)).is_some());
        assert!(policy_constraint_failure(&exact, None).is_some());

        let minimum =
            constrained_intention(intention::PolicyVersionConstraint::MinimumPolicyVersion(5));
        assert!(policy_constraint_failure(&minimum, Some(5)).is_none());
        assert!(policy_constraint_failure(&minimum, Some(6)).is_none());
        assert!(policy_constraint_failure(&minimum, Some(4)).is_some());
        assert!(policy_constraint_failure(&minimum, None).is_some());
    }
}
