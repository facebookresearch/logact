/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Header;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::SpeculativeDecision;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_api::CreateBusCommand;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::VersionedPolicy;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::VoterFactory;
use logact_commit_service_engine::installed_policy;
use logact_commit_service_engine::plan_policy_batch;
use logact_commit_service_engine::policy_batch_payload;

use crate::Checkpoint;
use crate::CheckpointError;
use crate::CheckpointStore;
use crate::ReplayState;
use crate::drive::Drive;
use crate::drive::apply_error;
use crate::player::append;

/// A commit service that materializes the log through V1's voters and decider,
/// resuming from whatever checkpoint `C` holds.
#[derive(Clone)]
pub struct ReplayCommitService<T, F, P, C> {
    bus: T,
    voter_factory: F,
    policy_provider: P,
    checkpoints: C,
    speculative_decisions: bool,
}

impl<T, F, P, C> ReplayCommitService<T, F, P, C> {
    pub fn new(bus: T, voter_factory: F, policy_provider: P, checkpoints: C) -> Self {
        Self {
            bus,
            voter_factory,
            policy_provider,
            checkpoints,
            speculative_decisions: false,
        }
    }

    /// Attach a speculative decision to intentions under a decider that needs
    /// no votes. Readers honor correct speculations either way.
    pub fn with_speculative_decisions(mut self, enabled: bool) -> Self {
        self.speculative_decisions = enabled;
        self
    }

    pub fn checkpoints(&self) -> &C {
        &self.checkpoints
    }
}

impl<T, F, P, C> ReplayCommitService<T, F, P, C>
where
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
{
    /// Saving is best-effort: a failed save only lengthens a later drive.
    async fn save_checkpoint(&self, bus_id: &str, checkpoint: Checkpoint)
    where
        C: CheckpointStore,
    {
        if let Err(error) = self.checkpoints.save(bus_id, &checkpoint).await {
            tracing::warn!(bus_id, position = checkpoint.position, error = %error, "saving checkpoint failed");
        }
    }

    async fn desired_policy(&self, bus_id: &str) -> CommitResult<VersionedPolicy> {
        self.policy_provider
            .read(bus_id)
            .await
            .map_err(|error| CommitError::Internal(error.into()))
    }
}

impl<T, F, P, C> CommitSvc for ReplayCommitService<T, F, P, C>
where
    T: AgentBus,
    F: VoterFactory,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    C: CheckpointStore,
{
    type Bus = T;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
        let desired = self.desired_policy(&request.bus_id).await?;
        let batch = plan_policy_batch(None, None, &desired)
            .map_err(CommitError::Internal)?
            .ok_or_else(|| {
                CommitError::Internal(anyhow::anyhow!(
                    "CreateBus initial policy planning produced no batch"
                ))
            })?;
        let payload = policy_batch_payload(batch);
        let position = append(&self.bus, &request.bus_id, payload.clone()).await?;
        // As in V1, checkpoint a fresh bus at its initial policy, so the first
        // drive plans against it.
        if position == 0 {
            let mut state = ReplayState::default();
            let entry = BusEntry {
                header: Some(Header {
                    log_position: position,
                    ..Default::default()
                }),
                payload: Some(payload),
            };
            state
                .materialize(&entry, &|config| self.voter_factory.validate_config(config))
                .map_err(apply_error)?;
            self.save_checkpoint(&request.bus_id, Checkpoint { state, position })
                .await;
        }
        Ok(())
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let bus_id = request.bus_id.agent_bus_id.as_str();
        let desired = self.desired_policy(bus_id).await?;
        let checkpoint = self
            .checkpoints
            .load(bus_id)
            .await
            .map_err(CommitError::from)?;
        let checkpoint_position = checkpoint.as_ref().map(|checkpoint| checkpoint.position);
        let start = checkpoint_position.map_or(0, |position| position + 1);
        let state = checkpoint.map_or_else(ReplayState::default, |checkpoint| checkpoint.state);
        let current = installed_policy(&state.policy).map_err(CommitError::Internal)?;
        let mut first_append = None;
        if let Some(batch) = plan_policy_batch(
            Some(&current),
            state.policy.applied_policy_version,
            &desired,
        )
        .map_err(CommitError::Internal)?
        {
            first_append = Some(append(&self.bus, bus_id, policy_batch_payload(batch)).await?);
        }
        let speculative_decision = if self.speculative_decisions {
            compute_speculation(&desired)
        } else {
            None
        };
        let intention = Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(request.intention),
                policy_version_constraint: Some(
                    intention::PolicyVersionConstraint::MinimumPolicyVersion(desired.version),
                ),
                speculative_decision,
            })),
        };
        let proposed_position = append(&self.bus, bus_id, intention).await?;
        if let Some(checkpoint_position) = checkpoint_position
            && first_append.unwrap_or(proposed_position) <= checkpoint_position
        {
            return Err(CommitError::BusDataLoss(anyhow::anyhow!(
                "bus '{bus_id}' reused a checkpointed log position"
            )));
        }

        let mut drive = Drive::new(&self.bus, bus_id, start, &self.voter_factory, state);
        let (outcome, decided_at) = drive.until_decided(proposed_position).await?;
        let checkpoint = Checkpoint {
            state: drive.state,
            position: decided_at,
        };
        self.save_checkpoint(bus_id, checkpoint).await;
        Ok(outcome)
    }
}

/// The speculative decision for an intention proposed under `desired`, if its
/// decider needs no votes. It is correct iff `desired` is still the applied
/// policy when the intention lands.
fn compute_speculation(desired: &VersionedPolicy) -> Option<SpeculativeDecision> {
    let (approved, reason) = match desired.policy.decider_policy {
        DeciderPolicy::OnByDefault => (true, "ON_BY_DEFAULT policy"),
        DeciderPolicy::OffByDefault => (false, "OFF_BY_DEFAULT policy"),
        DeciderPolicy::FirstBooleanWins => return None,
    };
    Some(SpeculativeDecision {
        policy_version: desired.version,
        approved,
        reason: reason.to_string(),
    })
}

impl From<CheckpointError> for CommitError {
    fn from(error: CheckpointError) -> Self {
        match &error {
            CheckpointError::Storage(StorageError::TransactionConflict(_)) => {
                CommitError::Concurrency(error.into())
            }
            CheckpointError::Storage(StorageError::Timeout(_))
            | CheckpointError::Storage(StorageError::BackendUnavailable(_))
            | CheckpointError::Unavailable(_) => {
                CommitError::CheckpointStoreUnavailable(error.into())
            }
            CheckpointError::Storage(StorageError::InvalidPosition(_))
            | CheckpointError::Storage(StorageError::InternalError(_))
            | CheckpointError::Internal(_) => CommitError::Internal(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_errors_preserve_service_categories_and_sources() {
        let source = || anyhow::Error::new(std::io::Error::other("checkpoint backend failed"));
        let cases = [
            (
                CheckpointError::Storage(StorageError::TransactionConflict(source())),
                "Concurrency",
            ),
            (
                CheckpointError::Storage(StorageError::Timeout(source())),
                "CheckpointStoreUnavailable",
            ),
            (
                CheckpointError::Storage(StorageError::BackendUnavailable(source())),
                "CheckpointStoreUnavailable",
            ),
            (
                CheckpointError::Storage(StorageError::InternalError(source())),
                "Internal",
            ),
            (
                CheckpointError::Unavailable(source()),
                "CheckpointStoreUnavailable",
            ),
            (CheckpointError::Internal(source()), "Internal"),
        ];
        for (error, expected_category) in cases {
            let (category, source) = match CommitError::from(error) {
                CommitError::BusDataLoss(source) => ("BusDataLoss", source),
                CommitError::InvalidArgument(source) => ("InvalidArgument", source),
                CommitError::Concurrency(source) => ("Concurrency", source),
                CommitError::Timeout(source) => ("Timeout", source),
                CommitError::Unavailable(source) => ("Unavailable", source),
                CommitError::CheckpointStoreUnavailable(source) => {
                    ("CheckpointStoreUnavailable", source)
                }
                CommitError::Internal(source) => ("Internal", source),
            };
            assert_eq!(category, expected_category);
            let checkpoint_error = source
                .downcast_ref::<CheckpointError>()
                .expect("commit errors should retain the typed checkpoint cause");
            let backend_source = match checkpoint_error {
                CheckpointError::Storage(StorageError::TransactionConflict(source))
                | CheckpointError::Storage(StorageError::Timeout(source))
                | CheckpointError::Storage(StorageError::BackendUnavailable(source))
                | CheckpointError::Storage(StorageError::InternalError(source))
                | CheckpointError::Unavailable(source)
                | CheckpointError::Internal(source) => source,
                CheckpointError::Storage(StorageError::InvalidPosition(_)) => {
                    panic!("unexpected invalid position")
                }
            };
            assert!(backend_source.downcast_ref::<std::io::Error>().is_some());
        }
        let error = CommitError::from(CheckpointError::Storage(StorageError::InvalidPosition(-1)));
        let CommitError::Internal(source) = error else {
            panic!("invalid checkpoint positions indicate a commit-service implementation bug");
        };
        assert!(matches!(
            source.downcast_ref::<CheckpointError>(),
            Some(CheckpointError::Storage(StorageError::InvalidPosition(-1)))
        ));
    }
}
