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

use agent_bus_proto_rust::agent_bus::Abort;
use agent_bus_proto_rust::agent_bus::AddVoterOp;
use agent_bus_proto_rust::agent_bus::BaseEngineControl;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::Commit;
use agent_bus_proto_rust::agent_bus::Control;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::VoterOp;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;
use agent_bus_proto_rust::agent_bus::voter_op;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use agentbus_api::AppendRequest;
use agentbus_api::CheckTailRequest;
use agentbus_api::Environment;
use agentbus_api::ReadNextRequest;
use agentbus_simple::InMemoryAgentBus;
use anyhow::Context;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use logact_commit_service_static_config::StaticConfigPolicyProvider;

/// In-memory `CommitSvc` backed by a local in-memory AgentBus.
pub struct InMemCommitService<E: Environment, P = StaticConfigPolicyProvider> {
    bus: InMemoryAgentBus<E>,
    policy_provider: P,
    policy_state: Rc<RefCell<HashMap<String, VersionedPolicyState>>>,
    replay_state: Rc<RefCell<HashMap<String, ReplayState>>>,
}

impl<E: Environment, P: Clone> Clone for InMemCommitService<E, P> {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            policy_provider: self.policy_provider.clone(),
            policy_state: self.policy_state.clone(),
            replay_state: self.replay_state.clone(),
        }
    }
}

impl<E: Environment> InMemCommitService<E, StaticConfigPolicyProvider> {
    pub fn new(environment: Rc<E>) -> Self {
        Self::with_policy_provider(environment, StaticConfigPolicyProvider::default())
    }
}

impl<E: Environment, P> InMemCommitService<E, P> {
    /// Build a service that consults `policy_provider` on creation and commits.
    pub fn with_policy_provider(environment: Rc<E>, policy_provider: P) -> Self {
        Self {
            bus: InMemoryAgentBus::new(environment),
            policy_provider,
            policy_state: Rc::new(RefCell::new(HashMap::new())),
            replay_state: Rc::default(),
        }
    }
}

impl<E, P> InMemCommitService<E, P>
where
    E: Environment,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
{
    async fn reconcile_policy(&self, bus_id: &str) -> CommitResult<i64> {
        let desired = self
            .policy_provider
            .read(bus_id)
            .await
            .map_err(|error| CommitError::Internal(error.into()))?;
        let desired_version = desired.version;
        get_decider_policy(&desired.state).map_err(CommitError::Internal)?;
        let expected_current_version = self
            .policy_state
            .borrow()
            .get(bus_id)
            .map(|applied| applied.version);
        let should_apply = expected_current_version.is_none_or(|version| desired.version > version);
        if should_apply {
            self.bus
                .append(AppendRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.to_string(),
                    }),
                    payload: Some(policy_batch_payload(PolicyBatch {
                        expected_current_version,
                        new_version: desired.version,
                        decider_policy: desired.state.decider_policy,
                        ..Default::default()
                    })),
                })
                .await
                .map_err(commit_error_from_bus)?;
            self.policy_state
                .borrow_mut()
                .insert(bus_id.to_string(), desired);
        }
        Ok(desired_version)
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

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        let initial = self
            .policy_provider
            .read(&request.bus_id)
            .await
            .map_err(|error| CommitError::Internal(error.into()))?;
        get_decider_policy(&initial.state).map_err(CommitError::Internal)?;
        let batch = PolicyBatch {
            expected_current_version: None,
            new_version: initial.version,
            decider_policy: initial.state.decider_policy,
            voter_ops: initial
                .state
                .voters
                .iter()
                .map(|(id, config)| {
                    (
                        id.clone(),
                        VoterOp {
                            op: Some(voter_op::Op::Add(AddVoterOp {
                                config: Some(config.clone()),
                            })),
                        },
                    )
                })
                .collect(),
        };
        let position = self
            .bus
            .append(AppendRequest {
                bus_id: Some(agentbus_api::BusId {
                    agent_bus_id: request.bus_id.clone(),
                }),
                payload: Some(Payload {
                    payload: Some(payload::Payload::Control(Control {
                        control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                            control: Some(base_engine_control::Control::PolicyBatch(batch)),
                        })),
                    })),
                }),
            })
            .await
            .map_err(commit_error_from_bus)?
            .log_position;
        if position == 0 {
            self.policy_state
                .borrow_mut()
                .entry(request.bus_id)
                .or_insert(initial);
        }
        Ok(())
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let policy_version = self.reconcile_policy(&request.bus_id.agent_bus_id).await?;
        let response = self
            .bus
            .append(AppendRequest {
                bus_id: Some(request.bus_id.clone()),
                payload: Some(intention_to_payload(request.intention, policy_version)),
            })
            .await
            .map_err(commit_error_from_bus)?;

        self.replay(&request.bus_id.agent_bus_id, response.log_position)
            .await
    }
}

#[derive(Clone, Default)]
struct ReplayState {
    next_position: i64,
    policy: Option<(i64, DeciderPolicy)>,
    pending: Option<i64>,
}

impl ReplayState {
    fn process_entry(&mut self, entry: &BusEntry, position: i64) -> CommitResult<Option<Payload>> {
        let output = match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
            Some(payload::Payload::Control(Control {
                control:
                    Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(batch)),
                    })),
            })) if batch.expected_current_version == self.policy.map(|(version, _)| version) => {
                let raw = batch
                    .decider_policy
                    .context("policy batch has no decider policy")
                    .map_err(CommitError::Internal)?;
                let policy = DeciderPolicy::try_from(raw)
                    .context("policy batch has unrecognized decider policy")
                    .map_err(CommitError::Internal)?;
                self.policy = Some((batch.new_version, policy));
                self.pending = None;
                None
            }
            Some(payload::Payload::Intention(intention)) => {
                let version = self.policy.map(|(version, _)| version);
                let valid = match intention.policy_version_constraint {
                    Some(intention::PolicyVersionConstraint::RequiredPolicyVersion(required)) => {
                        version == Some(required)
                    }
                    Some(intention::PolicyVersionConstraint::MinimumPolicyVersion(minimum)) => {
                        version.is_some_and(|version| version >= minimum)
                    }
                    None => true,
                };
                if !valid {
                    Some(decision_payload(
                        position,
                        false,
                        "policy version constraint not satisfied".to_string(),
                    ))
                } else {
                    match self
                        .policy
                        .map_or(DeciderPolicy::OnByDefault, |(_, policy)| policy)
                    {
                        DeciderPolicy::OnByDefault => Some(decision_payload(
                            position,
                            true,
                            "approved: ON_BY_DEFAULT policy; in-memory service does not run voters"
                                .to_string(),
                        )),
                        DeciderPolicy::OffByDefault => Some(decision_payload(
                            position,
                            false,
                            "denied: OFF_BY_DEFAULT policy; in-memory service does not run voters"
                                .to_string(),
                        )),
                        DeciderPolicy::FirstBooleanWins => {
                            // If supersession adopts explicit aborts, emit one for the previous intention here.
                            self.pending = Some(position);
                            None
                        }
                    }
                }
            }
            Some(payload::Payload::Vote(vote)) if self.pending == Some(vote.intention_id) => {
                if let Some(vote_type::VoteType::BooleanVote(approved)) = vote
                    .abstract_vote
                    .as_ref()
                    .and_then(|v| v.vote_type.as_ref())
                {
                    self.pending = None;
                    let reason = if vote.reason.is_empty() {
                        "decided by policy".to_string()
                    } else {
                        vote.reason.clone()
                    };
                    Some(decision_payload(vote.intention_id, *approved, reason))
                } else {
                    None
                }
            }
            Some(payload::Payload::Commit(commit)) if self.pending == Some(commit.intention_id) => {
                self.pending = None;
                None
            }
            Some(payload::Payload::Abort(abort)) if self.pending == Some(abort.intention_id) => {
                self.pending = None;
                None
            }
            _ => None,
        };
        Ok(output)
    }
}

impl<E, P> InMemCommitService<E, P>
where
    E: Environment + 'static,
{
    async fn replay(
        &self,
        bus_id: &str,
        intention_position: i64,
    ) -> CommitResult<CommitIntentionOutcome> {
        let mut position = self
            .replay_state
            .borrow_mut()
            .entry(bus_id.to_string())
            .or_default()
            .next_position
            .min(intention_position);
        loop {
            let tail = self
                .bus
                .check_tail(CheckTailRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.to_string(),
                    }),
                })
                .await
                .map_err(commit_error_from_bus)?
                .tail_position;
            if position >= tail {
                self.append_output(
                    bus_id,
                    decision_payload(
                        intention_position,
                        false,
                        "no decision before the log was drained".to_string(),
                    ),
                )
                .await?;
                let mut states = self.replay_state.borrow_mut();
                let state = states.get_mut(bus_id).expect("replay state exists");
                if state.pending == Some(intention_position) {
                    state.pending = None;
                }
                continue;
            }
            while position < tail {
                let response = self
                    .bus
                    .read_next(ReadNextRequest {
                        bus_id: Some(BusId {
                            agent_bus_id: bus_id.to_string(),
                        }),
                        start_log_position: position,
                        end_log_position: tail,
                        max_entries: 64,
                        filter: None,
                    })
                    .await
                    .map_err(commit_error_from_bus)?;
                for entry in response.entries {
                    let entry_position = entry
                        .header
                        .as_ref()
                        .context("replayed entry has no header")
                        .map_err(CommitError::Internal)?
                        .log_position;
                    let mut next_state = self.replay_state.borrow()[bus_id].clone();
                    let expected_position = next_state.next_position;
                    if entry_position >= expected_position {
                        if let Some(output) = next_state.process_entry(&entry, entry_position)? {
                            self.append_output(bus_id, output).await?;
                        }
                        next_state.next_position = entry_position + 1;
                        let mut states = self.replay_state.borrow_mut();
                        let state = states.get_mut(bus_id).expect("replay state exists");
                        if state.next_position == expected_position {
                            *state = next_state;
                        }
                    }
                    if let Some(outcome) = logged_outcome(&entry, intention_position) {
                        return Ok(outcome);
                    }
                    position = entry_position + 1;
                }
            }
        }
    }

    async fn append_output(&self, bus_id: &str, payload: Payload) -> CommitResult<()> {
        self.bus
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_string(),
                }),
                payload: Some(payload),
            })
            .await
            .map_err(commit_error_from_bus)?;
        Ok(())
    }
}

fn logged_outcome(entry: &BusEntry, position: i64) -> Option<CommitIntentionOutcome> {
    let (approved, reason) = match entry.payload.as_ref()?.payload.as_ref()? {
        payload::Payload::Commit(commit) if commit.intention_id == position => {
            (true, commit.reason.clone())
        }
        payload::Payload::Abort(abort) if abort.intention_id == position => {
            (false, abort.reason.clone())
        }
        _ => return None,
    };
    Some(CommitIntentionOutcome {
        approved,
        reason,
        log_position: position,
    })
}

fn decision_payload(position: i64, approved: bool, reason: String) -> Payload {
    Payload {
        payload: Some(if approved {
            payload::Payload::Commit(Commit {
                intention_id: position,
                reason,
            })
        } else {
            payload::Payload::Abort(Abort {
                intention_id: position,
                reason,
            })
        }),
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

fn policy_batch_payload(batch: PolicyBatch) -> Payload {
    Payload {
        payload: Some(payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::PolicyBatch(batch)),
            })),
        })),
    }
}

fn get_decider_policy(state: &PolicyState) -> anyhow::Result<DeciderPolicy> {
    let raw = state
        .decider_policy
        .context("policy provider returned a policy without a decider policy")?;
    DeciderPolicy::try_from(raw)
        .map_err(|_| anyhow::anyhow!("policy provider returned unrecognized decider policy {raw}"))
}

fn intention_to_payload(intention: intention::Intention, policy_version: i64) -> Payload {
    Payload {
        payload: Some(payload::Payload::Intention(Intention {
            intention: Some(intention),
            policy_version_constraint: Some(
                intention::PolicyVersionConstraint::MinimumPolicyVersion(policy_version),
            ),
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
}
