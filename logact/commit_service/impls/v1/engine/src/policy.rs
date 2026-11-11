/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::AddVoterOp;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::RemoveVoterOp;
use agent_bus_proto_rust::agent_bus::VoterOp;
use agent_bus_proto_rust::agent_bus::voter_op;
use logact_commit_service_api::Policy;
use logact_commit_service_api::VersionedPolicy;

/// Plan the `PolicyBatch` that moves a bus from `current`, applied at
/// `current_version`, to `desired`. `None` means no policy is installed yet.
/// Returns `None` if `desired` is not newer than `current_version`.
pub(crate) fn plan(
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
}
