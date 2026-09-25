/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::AddVoterOp;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::RemoveVoterOp;
use agent_bus_proto_rust::agent_bus::VoterOp;
use agent_bus_proto_rust::agent_bus::voter_op;
use anyhow::Context;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;

pub(crate) fn plan(
    current: &PolicyState,
    current_version: Option<i64>,
    desired: &VersionedPolicyState,
) -> anyhow::Result<Option<PolicyBatch>> {
    if current_version.is_some_and(|version| desired.version <= version) {
        return Ok(None);
    }

    let raw_decider = desired
        .state
        .decider_policy
        .context("desired policy has no decider policy")?;
    DeciderPolicy::try_from(raw_decider).map_err(|_| {
        anyhow::anyhow!("desired policy has unrecognized decider policy {raw_decider}")
    })?;

    let mut batch = PolicyBatch {
        expected_current_version: current_version,
        new_version: desired.version,
        ..Default::default()
    };
    if current.decider_policy != desired.state.decider_policy {
        batch.decider_policy = desired.state.decider_policy;
    }
    for (voter_id, desired_config) in &desired.state.voters {
        match current.voters.get(voter_id) {
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
    for voter_id in current.voters.keys() {
        if !desired.state.voters.contains_key(voter_id) {
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

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
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

    fn desired(state: PolicyState, version: i64) -> VersionedPolicyState {
        VersionedPolicyState { state, version }
    }

    #[test]
    fn plan_sets_policy_and_adds_missing_voters() {
        let desired = desired(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                voters: [voter("a"), voter("b")].into_iter().collect(),
            },
            3,
        );

        let batch = plan(&PolicyState::default(), None, &desired)
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
        let desired = desired(PolicyState::default(), 3);

        assert!(
            plan(&PolicyState::default(), Some(3), &desired)
                .expect("planning should succeed")
                .is_none()
        );
        assert!(
            plan(&PolicyState::default(), Some(4), &desired)
                .expect("planning should succeed")
                .is_none()
        );
    }

    #[test]
    fn plan_rejects_missing_or_unrecognized_decider_policy() {
        let missing = plan(
            &PolicyState::default(),
            None,
            &desired(PolicyState::default(), 1),
        )
        .expect_err("a missing desired decider policy should fail planning");
        assert!(missing.to_string().contains("has no decider policy"));

        let unrecognized = plan(
            &PolicyState::default(),
            None,
            &desired(
                PolicyState {
                    decider_policy: Some(99),
                    ..Default::default()
                },
                1,
            ),
        )
        .expect_err("an unrecognized desired decider policy should fail planning");
        assert!(
            unrecognized
                .to_string()
                .contains("unrecognized decider policy 99")
        );
    }

    #[test]
    fn plan_returns_version_marker_when_content_is_unchanged() {
        let state = PolicyState {
            decider_policy: Some(DeciderPolicy::OffByDefault as i32),
            voters: [voter("a")].into_iter().collect(),
        };
        let desired = desired(state.clone(), 4);

        let batch = plan(&state, Some(3), &desired)
            .expect("planning should succeed")
            .expect("a newer version should produce a marker batch");
        assert_eq!(batch.expected_current_version, Some(3));
        assert_eq!(batch.new_version, 4);
        assert_eq!(batch.decider_policy, None);
        assert!(batch.voter_ops.is_empty());
    }

    #[test]
    fn plan_only_adds_voters_not_already_present() {
        let current = PolicyState {
            decider_policy: Some(DeciderPolicy::OffByDefault as i32),
            voters: [voter("a")].into_iter().collect(),
        };
        let desired = desired(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                voters: [voter("a"), voter("b")].into_iter().collect(),
            },
            2,
        );

        let batch = plan(&current, Some(1), &desired)
            .expect("planning should succeed")
            .expect("a newer desired policy should produce a batch");
        assert_eq!(batch.voter_ops.len(), 1);
        assert!(batch.voter_ops.contains_key("b"));
    }

    #[test]
    fn plan_rejects_changed_config_for_existing_voter() {
        let current = PolicyState {
            decider_policy: Some(DeciderPolicy::OffByDefault as i32),
            voters: [("a".to_string(), voter_config("old"))]
                .into_iter()
                .collect(),
        };
        let desired = desired(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                voters: [("a".to_string(), voter_config("new"))]
                    .into_iter()
                    .collect(),
            },
            2,
        );

        let error = plan(&current, Some(1), &desired)
            .expect_err("changing the config for an existing voter should fail");
        assert_eq!(
            error.to_string(),
            "voter 'a' is already installed with a different config"
        );
    }

    #[test]
    fn plan_removes_voters_absent_from_desired_state() {
        let current = PolicyState {
            decider_policy: Some(DeciderPolicy::OffByDefault as i32),
            voters: [voter("a"), voter("b")].into_iter().collect(),
        };
        let desired = desired(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                voters: [voter("b")].into_iter().collect(),
            },
            2,
        );

        let batch = plan(&current, Some(1), &desired)
            .expect("planning should succeed")
            .expect("a newer desired policy should produce a batch");
        assert_eq!(batch.voter_ops.len(), 1);
        assert!(matches!(
            batch.voter_ops.get("a").and_then(|op| op.op.as_ref()),
            Some(voter_op::Op::Remove(_))
        ));
    }
}
