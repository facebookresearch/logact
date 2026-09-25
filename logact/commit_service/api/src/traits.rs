/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::intention;
use agentbus_api::AgentBus;
use agentbus_api::BusId;

/// A validated request to commit one durable AgentBus intention.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitIntentionCommand {
    /// AgentBus receiving the intention.
    pub bus_id: BusId,

    /// Durable protobuf intention value.
    pub intention: intention::Intention,
}

/// Result of committing an intention.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitIntentionOutcome {
    /// Whether the safety policy approved the intention.
    pub approved: bool,

    /// Human-readable explanation of the decision.
    pub reason: String,

    /// AgentBus position where the intention was durably appended.
    pub log_position: i64,
}

/// Failure returned by a commit-service implementation.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// The caller supplied an invalid argument.
    #[error(transparent)]
    InvalidArgument(anyhow::Error),

    /// A concurrency failure occurred.
    #[error(transparent)]
    Concurrency(anyhow::Error),

    /// The operation timed out.
    #[error(transparent)]
    Timeout(anyhow::Error),

    /// A required service was unavailable.
    #[error(transparent)]
    Unavailable(anyhow::Error),

    /// An uncategorized failure inside the service implementation.
    #[error(transparent)]
    Internal(anyhow::Error),
}

/// Result returned by commit-service operations.
pub type CommitResult<T> = Result<T, CommitError>;

/// Core trait for LogAct commit service implementations.
pub trait CommitSvc {
    /// The authoritative AgentBus exposed by this commit service.
    type Bus: AgentBus;

    /// Return this service's AgentBus surface.
    fn agent_bus(&self) -> &Self::Bus;

    /// Commit an intention. Blocks on the safety pipeline; the returned
    /// `approved` reflects the decider's verdict.
    fn commit_intention(
        &self,
        request: CommitIntentionCommand,
    ) -> impl std::future::Future<Output = CommitResult<CommitIntentionOutcome>>;
}
