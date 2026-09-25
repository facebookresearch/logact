/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use logact_commit_service_api::CommitError;
use logact_commit_service_api::CommitIntentionCommand;
use logact_commit_service_api::CommitIntentionOutcome;
use logact_commit_service_api::CommitResult;
use logact_commit_service_api::CommitSvc;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactory;
use logact_commit_service_engine::EngineError;
use logact_commit_service_engine::PolicyProvider;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::VoterFactory;

pub struct CommitServiceV1<T, F, S, P, D, E> {
    bus: T,
    engine: BaseEngine<T, F, S, P, D, E>,
}

impl<T, F, S, P, D, E> Clone for CommitServiceV1<T, F, S, P, D, E>
where
    T: Clone,
    F: Clone,
    P: Clone,
    D: Clone,
{
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            engine: self.engine.clone(),
        }
    }
}

impl<T, F, S, P, D, E> CommitServiceV1<T, F, S, P, D, E>
where
    T: Clone,
{
    /// Build the engine from a clone of the same bus this service exposes.
    pub fn new<BuildEngine>(bus: T, build_engine: BuildEngine) -> Self
    where
        BuildEngine: FnOnce(T) -> BaseEngine<T, F, S, P, D, E>,
    {
        let engine = build_engine(bus.clone());
        Self { bus, engine }
    }
}

impl<T, F, S, P, D, E> CommitSvc for CommitServiceV1<T, F, S, P, D, E>
where
    T: AgentBus + 'static,
    F: VoterFactory,
    S: Storage + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    D: DeciderFactory,
{
    type Bus = T;

    fn agent_bus(&self) -> &Self::Bus {
        &self.bus
    }

    async fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> CommitResult<CommitIntentionOutcome> {
        let intention_payload = intention_to_payload(request.intention);
        let outcome = self
            .engine
            .propose_intention(&request.bus_id.agent_bus_id, intention_payload)
            .await
            .map_err(commit_error_from_engine)?;

        Ok(CommitIntentionOutcome {
            approved: outcome.approved,
            reason: outcome.reason,
            log_position: outcome.log_position,
        })
    }
}

fn commit_error_from_engine(error: EngineError) -> CommitError {
    match error {
        EngineError::Storage(error) => commit_error_from_storage(error),
        EngineError::InvalidEngineState(source) => CommitError::Internal(source),
        EngineError::Playback(error) => commit_error_from_apply(error),
        EngineError::Bus(error) => commit_error_from_bus(error),
        EngineError::PolicyProvider(source) => CommitError::Internal(source),
        EngineError::Internal(source) => CommitError::Internal(source),
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

fn commit_error_from_storage(error: StorageError) -> CommitError {
    match error {
        error @ StorageError::TransactionConflict(_) => {
            CommitError::Concurrency(anyhow::Error::new(error))
        }
        error @ StorageError::Timeout(_) => CommitError::Timeout(anyhow::Error::new(error)),
        error @ StorageError::BackendUnavailable(_) => {
            CommitError::Unavailable(anyhow::Error::new(error))
        }
        error @ StorageError::InternalError(_) => CommitError::Internal(anyhow::Error::new(error)),
    }
}

fn commit_error_from_apply(error: ApplyError) -> CommitError {
    match error {
        error @ ApplyError::Bus(AgentBusError::Timeout(_)) => {
            CommitError::Timeout(anyhow::Error::new(error))
        }
        error @ ApplyError::Bus(AgentBusError::Unavailable(_)) => {
            CommitError::Unavailable(anyhow::Error::new(error))
        }
        error @ (ApplyError::Storage(StorageError::TransactionConflict(_))
        | ApplyError::Concurrency(_)) => CommitError::Concurrency(anyhow::Error::new(error)),
        error @ ApplyError::Storage(StorageError::Timeout(_)) => {
            CommitError::Timeout(anyhow::Error::new(error))
        }
        error @ ApplyError::Storage(StorageError::BackendUnavailable(_)) => {
            CommitError::Unavailable(anyhow::Error::new(error))
        }
        error @ (ApplyError::StalePosition { .. }
        | ApplyError::MissingHeader
        | ApplyError::InvalidEngineState { .. }
        | ApplyError::MalformedPolicy { .. }
        | ApplyError::DeprecatedOperation { .. }
        | ApplyError::Storage(StorageError::InternalError(_))
        | ApplyError::Bus(AgentBusError::InvalidArgument(_))
        | ApplyError::Bus(AgentBusError::Internal(_))
        | ApplyError::Backend(_)) => CommitError::Internal(anyhow::Error::new(error)),
    }
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
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn engine_errors_map_to_commit_categories() {
        assert!(matches!(
            commit_error_from_engine(EngineError::Bus(AgentBusError::InvalidArgument(anyhow!(
                "invalid"
            )))),
            CommitError::InvalidArgument(_)
        ));
        assert!(matches!(
            commit_error_from_engine(EngineError::Storage(StorageError::TransactionConflict(
                anyhow!("conflict")
            ))),
            CommitError::Concurrency(_)
        ));
        assert!(matches!(
            commit_error_from_engine(EngineError::Storage(StorageError::Timeout(anyhow!(
                "timeout"
            )))),
            CommitError::Timeout(_)
        ));
        assert!(matches!(
            commit_error_from_engine(EngineError::Bus(AgentBusError::Unavailable(anyhow!(
                "unavailable"
            )))),
            CommitError::Unavailable(_)
        ));
        assert!(matches!(
            commit_error_from_engine(EngineError::Internal(anyhow!("internal"))),
            CommitError::Internal(_)
        ));
    }

    #[test]
    fn engine_error_context_is_preserved() {
        let error = commit_error_from_engine(EngineError::Internal(
            anyhow!("write raced").context("drive failed"),
        ));
        assert_eq!(
            format!("{error:#}"),
            "drive failed: write raced",
            "alternate formatting should retain the anyhow context and source"
        );
    }
}
