/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A standalone, in-memory `CommitSvc` implementation.
//!
//! Unlike `CommitServiceV1`, this has no engine: it wraps an in-memory AgentBus
//! and decides inline. Like the engine-backed service, it consults a
//! [`PolicyProvider`] on the commit path and adopts a returned
//! [`logact_commit_service_api::PolicyState`] when its version is newer. It runs
//! no voters, so it models only the decider-policy slice of the decision.
//!
//! It exists as a structurally independent implementation of the `CommitSvc`
//! contract — the conformance suite runs the same generic scenarios against it as
//! against the engine-backed implementations, which keeps those scenarios honest
//! (i.e. testing the spec, not one implementation's internals).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBusError;
use agentbus_api::AppendRequest;
use agentbus_api::Environment;
use agentbus_simple::InMemoryAgentBus;
use anyhow::Context;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use logact_commit_service_static_config::StaticConfigPolicyProvider;

/// In-memory `CommitSvc` backed by a local in-memory AgentBus.
pub struct InMemCommitService<E: Environment, P = StaticConfigPolicyProvider> {
    bus: InMemoryAgentBus<E>,
    policy_provider: P,
    policy_state: Rc<RefCell<HashMap<String, VersionedPolicyState>>>,
}

impl<E: Environment, P: Clone> Clone for InMemCommitService<E, P> {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            policy_provider: self.policy_provider.clone(),
            policy_state: self.policy_state.clone(),
        }
    }
}

impl<E: Environment> InMemCommitService<E, StaticConfigPolicyProvider> {
    pub fn new(environment: Rc<E>) -> Self {
        Self::with_policy_provider(environment, StaticConfigPolicyProvider::default())
    }
}

impl<E: Environment, P> InMemCommitService<E, P> {
    /// Build a service that consults `policy_provider` on the commit path.
    pub fn with_policy_provider(environment: Rc<E>, policy_provider: P) -> Self {
        Self {
            bus: InMemoryAgentBus::new(environment),
            policy_provider,
            policy_state: Rc::new(RefCell::new(HashMap::new())),
        }
    }
}

impl<E, P> InMemCommitService<E, P>
where
    E: Environment,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
{
    async fn reconcile_policy(&self, bus_id: &str) -> CommitResult<()> {
        let desired = self
            .policy_provider
            .read(bus_id)
            .await
            .map_err(|error| CommitError::Internal(error.into()))?;
        let should_apply = self
            .policy_state
            .borrow()
            .get(bus_id)
            .is_none_or(|applied| desired.version > applied.version);
        if should_apply {
            self.policy_state
                .borrow_mut()
                .insert(bus_id.to_string(), desired);
        }
        Ok(())
    }
}

impl<E, P> CommitSvc for InMemCommitService<E, P>
where
    E: Environment + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
{
    type Bus = InMemoryAgentBus<E>;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        self.reconcile_policy(&request.bus_id.agent_bus_id).await?;
        let response = self
            .bus
            .append(AppendRequest {
                agent_bus_id: request.bus_id.agent_bus_id.clone(),
                bus_id: Some(request.bus_id.clone()),
                payload: Some(intention_to_payload(request.intention)),
            })
            .await
            .map_err(commit_error_from_bus)?;

        let policy = match self.policy_state.borrow().get(&request.bus_id.agent_bus_id) {
            Some(desired) => get_decider_policy(&desired.state).map_err(CommitError::Internal)?,
            None => DeciderPolicy::OnByDefault,
        };
        let (approved, reason) = match policy {
            DeciderPolicy::OnByDefault => (
                true,
                "approved: ON_BY_DEFAULT policy; in-memory service does not run voters".to_string(),
            ),
            DeciderPolicy::OffByDefault => (
                false,
                "denied: OFF_BY_DEFAULT policy; in-memory service does not run voters".to_string(),
            ),
            DeciderPolicy::FirstBooleanWins => (
                false,
                "denied: FIRST_BOOLEAN_WINS policy; in-memory service does not run voters"
                    .to_string(),
            ),
        };

        Ok(CommitIntentionOutcome {
            approved,
            reason,
            log_position: response.log_position,
        })
    }
}

fn commit_error_from_bus(error: AgentBusError) -> CommitError {
    match error {
        error @ AgentBusError::InvalidArgument(_) => {
            CommitError::InvalidArgument(anyhow::Error::new(error))
        }
        error @ AgentBusError::Timeout(_) => CommitError::Timeout(anyhow::Error::new(error)),
        error @ AgentBusError::Unavailable(_) => {
            CommitError::Unavailable(anyhow::Error::new(error))
        }
        error @ AgentBusError::Internal(_) => CommitError::Internal(anyhow::Error::new(error)),
    }
}

fn get_decider_policy(state: &PolicyState) -> anyhow::Result<DeciderPolicy> {
    let raw = state
        .decider_policy
        .context("policy provider returned a policy without a decider policy")?;
    DeciderPolicy::try_from(raw)
        .map_err(|_| anyhow::anyhow!("policy provider returned unrecognized decider policy {raw}"))
}

fn intention_to_payload(intention: intention::Intention) -> Payload {
    Payload {
        payload: Some(payload::Payload::Intention(Intention {
            intention: Some(intention),
            ..Default::default()
        })),
    }
}

#[cfg(test)]
mod tests {
    use agentbus_api::environment::RealEnvironment;
    use futures::executor::block_on;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::PolicyState;

    use super::*;

    fn intention(agent_id: &str) -> CommitIntentionCommand {
        CommitIntentionCommand {
            bus_id: agentbus_api::BusId {
                agent_bus_id: agent_id.to_string(),
            },
            intention: intention::Intention::StringIntention("x".to_string()),
        }
    }

    fn assert_mapped_error(error: CommitError, expected_category: &str, expected_message: &str) {
        let category = match &error {
            CommitError::InvalidArgument(_) => "invalid argument",
            CommitError::Concurrency(_) => "concurrency",
            CommitError::Timeout(_) => "timeout",
            CommitError::Unavailable(_) => "unavailable",
            CommitError::Internal(_) => "internal",
        };
        assert_eq!(
            category, expected_category,
            "the source category should be preserved"
        );
        assert_eq!(
            format!("{error:#}"),
            expected_message,
            "the source message should remain in the error chain"
        );
    }

    #[test]
    fn bus_errors_preserve_category_and_context() {
        for (error, category, message) in [
            (
                AgentBusError::InvalidArgument(anyhow::anyhow!("invalid")),
                "invalid argument",
                "invalid",
            ),
            (
                AgentBusError::Timeout(anyhow::anyhow!("timed out")),
                "timeout",
                "timed out",
            ),
            (
                AgentBusError::Unavailable(anyhow::anyhow!("unavailable")),
                "unavailable",
                "unavailable",
            ),
            (
                AgentBusError::Internal(anyhow::anyhow!("internal")),
                "internal",
                "internal",
            ),
        ] {
            assert_mapped_error(commit_error_from_bus(error), category, message);
        }
    }

    #[test]
    fn positions_increase_per_agent_and_isolate() {
        let svc = InMemCommitService::new(Rc::new(RealEnvironment::new()));
        block_on(async {
            assert_eq!(
                svc.commit_intention(intention("a"))
                    .await
                    .unwrap()
                    .log_position,
                0
            );
            assert_eq!(
                svc.commit_intention(intention("a"))
                    .await
                    .unwrap()
                    .log_position,
                1
            );
            // A second agent starts its own sequence at 0.
            assert_eq!(
                svc.commit_intention(intention("b"))
                    .await
                    .unwrap()
                    .log_position,
                0
            );
        });
    }

    #[test]
    fn intention_is_approved_by_default() {
        let svc = InMemCommitService::new(Rc::new(RealEnvironment::new()));
        let resp = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(resp.approved);
    }

    #[test]
    fn off_by_default_policy_denies() {
        let svc = InMemCommitService::with_policy_provider(
            Rc::new(RealEnvironment::new()),
            StaticConfigPolicyProvider::new(DeciderPolicy::OffByDefault, Vec::new(), |_| Ok(())),
        );
        let resp = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(!resp.approved);
    }

    #[test]
    fn first_boolean_wins_denies_without_a_voter() {
        let svc = InMemCommitService::with_policy_provider(
            Rc::new(RealEnvironment::new()),
            StaticConfigPolicyProvider::new(
                DeciderPolicy::FirstBooleanWins,
                Vec::new(),
                |_| Ok(()),
            ),
        );
        let resp = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(!resp.approved);
    }

    #[test]
    fn malformed_provider_policy_returns_an_error() {
        let missing = get_decider_policy(&PolicyState::default())
            .expect_err("a missing decider policy should be rejected");
        assert!(missing.to_string().contains("without a decider policy"));

        let unrecognized = get_decider_policy(&PolicyState {
            decider_policy: Some(99),
            ..Default::default()
        })
        .expect_err("an unrecognized decider policy should be rejected");
        assert!(
            unrecognized
                .to_string()
                .contains("unrecognized decider policy 99")
        );
    }

    #[derive(Clone)]
    struct ControllablePolicyProvider {
        desired: Rc<RefCell<VersionedPolicyState>>,
    }

    impl ControllablePolicyProvider {
        fn new(decider_policy: DeciderPolicy, version: i64) -> Self {
            Self {
                desired: Rc::new(RefCell::new(VersionedPolicyState {
                    state: PolicyState {
                        decider_policy: Some(decider_policy as i32),
                        ..Default::default()
                    },
                    version,
                })),
            }
        }

        fn set(&self, decider_policy: DeciderPolicy, version: i64) {
            *self.desired.borrow_mut() = VersionedPolicyState {
                state: PolicyState {
                    decider_policy: Some(decider_policy as i32),
                    ..Default::default()
                },
                version,
            };
        }
    }

    impl PolicyProvider for ControllablePolicyProvider {
        type Error = anyhow::Error;

        async fn read(&self, _bus_id: &str) -> anyhow::Result<VersionedPolicyState> {
            Ok(self.desired.borrow().clone())
        }
    }

    #[test]
    fn generic_provider_applies_only_newer_versions() {
        let provider = ControllablePolicyProvider::new(DeciderPolicy::OffByDefault, 2);
        let svc = InMemCommitService::with_policy_provider(
            Rc::new(RealEnvironment::new()),
            provider.clone(),
        );

        let initial = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(!initial.approved);

        provider.set(DeciderPolicy::OnByDefault, 1);
        let stale = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(!stale.approved);

        provider.set(DeciderPolicy::OnByDefault, 3);
        let newer = block_on(svc.commit_intention(intention("a"))).unwrap();
        assert!(newer.approved);
    }

    #[test]
    fn clones_share_one_log() {
        let svc = InMemCommitService::new(Rc::new(RealEnvironment::new()));
        let clone = svc.clone();
        block_on(async {
            assert_eq!(
                svc.commit_intention(intention("a"))
                    .await
                    .unwrap()
                    .log_position,
                0
            );
            // The clone sees the shared state, so it continues the sequence.
            assert_eq!(
                clone
                    .commit_intention(intention("a"))
                    .await
                    .unwrap()
                    .log_position,
                1
            );
        });
    }
}
