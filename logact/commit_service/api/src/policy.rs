/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Policy interfaces for commit services.

use std::collections::HashMap;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::VoterConfig;
pub use logact_commit_service_policy_proto_rust::logact_commit_service_policy::PolicyState;
use thiserror::Error;

/// Desired commit-service policy for a bus: its decider policy and voters.
#[derive(Clone, Debug, PartialEq)]
pub struct Policy {
    pub decider_policy: DeciderPolicy,
    pub voters: HashMap<String, VoterConfig>,
}

/// A desired [`Policy`] and its provider-assigned logical version.
#[derive(Clone, Debug, PartialEq)]
pub struct VersionedPolicy {
    pub policy: Policy,
    pub version: i64,
}

/// A stored [`PolicyState`] and its version, as kept by durable policy
/// sources.
#[derive(Clone, Debug, PartialEq)]
pub struct VersionedPolicyState {
    pub state: PolicyState,
    pub version: i64,
}

/// A [`PolicyState`] that is not a valid [`Policy`].
#[derive(Debug, Error)]
#[error("{0}")]
pub struct InvalidPolicyState(pub String);

impl TryFrom<&PolicyState> for Policy {
    type Error = InvalidPolicyState;

    fn try_from(state: &PolicyState) -> Result<Self, Self::Error> {
        let PolicyState {
            decider_policy,
            voters,
        } = state;
        let raw = decider_policy
            .ok_or_else(|| InvalidPolicyState("missing decider policy".to_string()))?;
        let decider_policy = DeciderPolicy::try_from(raw)
            .map_err(|_| InvalidPolicyState(format!("unrecognized decider policy value {raw}")))?;
        for (voter_id, config) in voters {
            if config.config.is_none() {
                return Err(InvalidPolicyState(format!(
                    "voter '{voter_id}' has no configuration"
                )));
            }
        }
        Ok(Policy {
            decider_policy,
            voters: voters.clone(),
        })
    }
}

impl From<Policy> for PolicyState {
    fn from(policy: Policy) -> Self {
        let Policy {
            decider_policy,
            voters,
        } = policy;
        PolicyState {
            decider_policy: Some(decider_policy as i32),
            voters,
        }
    }
}

/// A source of desired commit-service policy, consulted on the commit path.
///
/// For each `bus_id`, the provider owns one logical version sequence regardless
/// of which underlying sources it consults. Every policy revision that
/// supersedes another must have a greater version, and one version must never
/// identify different policies. Providers that outlive a process or are
/// shared across service instances are responsible for preserving that sequence
/// across restarts and concurrent readers. Reads may return an older revision;
/// callers ignore versions they have already applied.
///
/// Every successful read returns a complete, valid `VersionedPolicy`; an
/// unavailable, uninitialized, or malformed policy is an error.
pub trait PolicyProvider {
    type Error;

    fn read(
        &self,
        bus_id: &str,
    ) -> impl std::future::Future<Output = Result<VersionedPolicy, Self::Error>>;
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::voter_config;

    use super::*;

    fn voter_config() -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::Custom(Default::default())),
        }
    }

    #[test]
    fn policy_round_trips_through_policy_state() {
        let policy = Policy {
            decider_policy: DeciderPolicy::FirstBooleanWins,
            voters: [("voter".to_string(), voter_config())]
                .into_iter()
                .collect(),
        };
        let state = PolicyState::from(policy.clone());
        assert_eq!(
            state.decider_policy,
            Some(DeciderPolicy::FirstBooleanWins as i32)
        );
        assert_eq!(Policy::try_from(&state).expect("valid state"), policy);
    }

    #[test]
    fn invalid_policy_states_are_rejected() {
        let error = |state: PolicyState| {
            Policy::try_from(&state)
                .expect_err("state should be invalid")
                .to_string()
        };
        assert_eq!(error(PolicyState::default()), "missing decider policy");
        assert_eq!(
            error(PolicyState {
                decider_policy: Some(99),
                ..Default::default()
            }),
            "unrecognized decider policy value 99"
        );
        assert_eq!(
            error(PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                voters: [("voter".to_string(), VoterConfig::default())]
                    .into_iter()
                    .collect(),
            }),
            "voter 'voter' has no configuration"
        );
    }
}
