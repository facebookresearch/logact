/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Import the generated proto types
pub use agent_bus_proto_rust::agent_bus::*;
use thiserror::Error;

/// Error returned by [`AgentBus`] operations.
#[derive(Debug, Error)]
pub enum AgentBusError {
    /// A requested log position has no entry in storage.
    #[error(transparent)]
    PositionNotFound(anyhow::Error),

    /// The request contained an invalid argument.
    #[error(transparent)]
    InvalidArgument(anyhow::Error),

    /// The operation did not complete before its deadline.
    #[error(transparent)]
    Timeout(anyhow::Error),

    /// The bus backend or remote service was unavailable.
    #[error(transparent)]
    Unavailable(anyhow::Error),

    /// The operation failed for another internal reason.
    #[error(transparent)]
    Internal(anyhow::Error),
}

/// Result returned by [`AgentBus`] operations.
pub type BusResult<T> = std::result::Result<T, AgentBusError>;

/// Core trait that all AgentBus implementations must implement
/// This allows pluggable implementations with different state management approaches
pub trait AgentBus {
    /// Handle an append request - add an entry to the log
    fn append(
        &self,
        request: AppendRequest,
    ) -> impl std::future::Future<Output = BusResult<AppendResponse>>;

    /// Retrieve entries from the log. Missing storage entries below the tail
    /// return [`AgentBusError::PositionNotFound`]; filtered-out entries are skipped.
    fn poll(
        &self,
        request: PollRequest,
    ) -> impl std::future::Future<Output = BusResult<PollResponse>>;

    /// Read a range below the known tail. A missing storage entry is reported as
    /// [`AgentBusError::PositionNotFound`]; filtered-out entries are skipped.
    fn read_next(
        &self,
        request: ReadNextRequest,
    ) -> impl std::future::Future<Output = BusResult<ReadNextResponse>>;

    /// Returns the tail position (next position to be written) for the given bus.
    /// Returns 0 for an empty bus.
    fn check_tail(
        &self,
        request: CheckTailRequest,
    ) -> impl std::future::Future<Output = BusResult<CheckTailResponse>>;

    /// Blocking poll: wait up to timeout_ms for new entries from start_log_position,
    /// then return up to max_entries matching the optional filter.
    /// Combines blocking_check_tail + looped read_next internally.
    /// Missing storage entries below the tail return [`AgentBusError::PositionNotFound`].
    fn blocking_poll(
        &self,
        request: BlockingPollRequest,
    ) -> impl std::future::Future<Output = BusResult<BlockingPollResponse>>;
}
