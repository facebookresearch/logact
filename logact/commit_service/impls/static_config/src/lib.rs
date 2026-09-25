/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A [`PolicyProvider`] backed by a fixed configuration chosen at construction.

use std::collections::HashMap;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use anyhow::Result;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;

/// The single generation of a static config: it never changes after construction,
/// so one version covers its whole lifetime.
const STATIC_VERSION: i64 = 0;

#[derive(Clone)]
pub struct StaticConfigPolicyProvider {
    decider_policy: DeciderPolicy,
    voters: HashMap<String, VoterConfig>,
    validate_policy: Rc<dyn Fn(&PolicyState) -> Result<()>>,
}

impl Default for StaticConfigPolicyProvider {
    fn default() -> Self {
        Self::new(DeciderPolicy::OnByDefault, Vec::new(), |_| Ok(()))
    }
}

impl StaticConfigPolicyProvider {
    pub fn new(
        decider_policy: DeciderPolicy,
        voter_configs: Vec<VoterConfig>,
        validate_policy: impl Fn(&PolicyState) -> Result<()> + 'static,
    ) -> Self {
        // Derive stable, distinct voter ids from positions in the provider's fixed
        // configuration once, then reuse them on every commit-path read.
        let voters = voter_configs
            .into_iter()
            .enumerate()
            .map(|(i, config)| (format!("static-voter-{i}"), config))
            .collect();
        Self {
            decider_policy,
            voters,
            validate_policy: Rc::new(validate_policy),
        }
    }
}

impl PolicyProvider for StaticConfigPolicyProvider {
    type Error = anyhow::Error;

    async fn read(&self, _bus_id: &str) -> Result<VersionedPolicyState, Self::Error> {
        let state = PolicyState {
            decider_policy: Some(self.decider_policy as i32),
            voters: self.voters.clone(),
        };
        (self.validate_policy)(&state)?;

        Ok(VersionedPolicyState {
            state,
            version: STATIC_VERSION,
        })
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::voter_config;
    use futures::executor::block_on;
    use rule_based_voter_proto_rust::rule_based_voter::RuleBasedVoterConfig;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRule;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRuleAction;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRuleMatchType;

    use super::*;

    fn rule_voter_config(pattern: &str) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::RuleBased(RuleBasedVoterConfig {
                rules: vec![VoterRule {
                    pattern: pattern.to_string(),
                    action: VoterRuleAction::VoterRuleAllow as i32,
                    match_type: VoterRuleMatchType::VoterRuleSubstring as i32,
                    reason: "test allow".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        }
    }

    #[test]
    fn read_reports_the_configured_state() {
        let provider = StaticConfigPolicyProvider::new(
            DeciderPolicy::FirstBooleanWins,
            vec![rule_voter_config("echo")],
            |_| Ok(()),
        );
        let desired = block_on(provider.read("agent-1")).unwrap();
        let state = desired.state;
        assert_eq!(
            state.decider_policy,
            Some(DeciderPolicy::FirstBooleanWins as i32)
        );
        assert_eq!(
            state.voters,
            [("static-voter-0".to_string(), rule_voter_config("echo"))]
                .into_iter()
                .collect()
        );
        assert_eq!(desired.version, 0);
    }

    #[test]
    fn read_rejects_policy_unsupported_by_this_node() {
        let provider = StaticConfigPolicyProvider::new(
            DeciderPolicy::FirstBooleanWins,
            vec![rule_voter_config("echo")],
            |_| anyhow::bail!("unsupported voter type"),
        );

        let error = block_on(provider.read("agent-1"))
            .expect_err("node capability validation should reject the policy");
        assert!(error.to_string().contains("unsupported voter type"));
    }

    #[test]
    fn default_provider_explicitly_configures_on_by_default() {
        let desired = block_on(StaticConfigPolicyProvider::default().read("agent-1")).unwrap();
        let state = desired.state;

        assert_eq!(
            state.decider_policy,
            Some(DeciderPolicy::OnByDefault as i32)
        );
        assert!(state.voters.is_empty());
        assert_eq!(desired.version, 0);
    }
}
