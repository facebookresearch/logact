/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::Payload;
use agentbus_api::AgentBusError;

use crate::StorageError;

mod storage_write_result;
pub use storage_write_result::StorageWriteResultExt;

/// Policy operation whose parameters were rejected during engine playback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr)]
pub enum MalformedPolicyBatchKind {
    #[strum(serialize = "PolicyBatch")]
    PolicyBatch,
    #[strum(serialize = "AddVoter")]
    AddVoter,
    #[strum(serialize = "RemoveVoter")]
    RemoveVoter,
    #[strum(serialize = "DeciderPolicy")]
    DeciderPolicy,
}

/// Legacy singleton policy operation rejected outside a `PolicyBatch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr)]
pub enum DeprecatedOperationKind {
    #[strum(serialize = "AddVoter")]
    AddVoter,
    #[strum(serialize = "RemoveVoter")]
    RemoveVoter,
    #[strum(serialize = "SetDeciderPolicy")]
    SetDeciderPolicy,
}

/// A concurrent state transition that prevented a storage operation from
/// completing. `source: None` means the storage request completed but its CAS
/// precondition was not met. `source: Some(error)` means storage returned an
/// error classified as a concurrency failure; currently this is
/// [`StorageError::TransactionConflict`].
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("engine storage conflict for bus '{bus_id}' at position {position}")]
    Engine {
        bus_id: String,
        position: i64,
        #[source]
        source: Option<StorageError>,
    },

    #[error("voter storage conflict for bus '{bus_id}' at position {position}")]
    Voter {
        bus_id: String,
        position: i64,
        #[source]
        source: Option<StorageError>,
    },

    #[error("decider storage conflict for bus '{bus_id}' at position {position}")]
    Decider {
        bus_id: String,
        position: i64,
        #[source]
        source: Option<StorageError>,
    },
}

/// Error returned by [`Applicator::apply`].
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The caller re-issued `apply` for an entry strictly older than the last
    /// one this applicator applied for the bus. Applicators retain only enough
    /// state to reproduce their single most-recent result, so older replays
    /// cannot be served and indicate a caller (engine) bug.
    #[error("stale apply for position {requested}: last applied position was {last}")]
    StalePosition { requested: i64, last: i64 },

    /// The entry has no header, so its log position can't be determined. The bus
    /// stamps a header on every appended entry, so this indicates a malformed
    /// entry rather than a normal condition.
    #[error("bus entry has no header; cannot determine its log position")]
    MissingHeader,

    /// Persisted engine state contains a value this binary cannot interpret.
    #[error("invalid engine state: {message}")]
    InvalidEngineState { message: String },

    /// A policy operation contains parameters that cannot be interpreted.
    #[error("malformed policy: {message}")]
    MalformedPolicy {
        kind: MalformedPolicyBatchKind,
        message: String,
    },

    /// A legacy singleton policy operation appeared outside a `PolicyBatch`.
    #[error("standalone {operation:?} is disabled; policy must use PolicyBatch")]
    DeprecatedOperation { operation: DeprecatedOperationKind },

    /// A typed storage failure while loading or saving applicator state.
    #[error(transparent)]
    Storage(StorageError),

    /// A storage CAS rejection or transaction conflict, classified by the
    /// state owner whose operation can be retried.
    #[error(transparent)]
    Concurrency(ConcurrencyError),

    /// A typed AgentBus failure while applying an entry.
    #[error(transparent)]
    Bus(AgentBusError),

    /// A non-storage backend or internal failure while applying.
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

/// An applicator consumes a single `BusEntry` and may produce a follow-up
/// `Payload` to be appended to the bus. The engine dispatches every polled
/// entry that matches the applicator's bound payload types to `apply`.
///
/// # Duplication tolerance contract
///
/// The engine writes its cursor, each applicator's state, and the produced
/// entry to storage / the bus non-atomically, so a crash can leave `apply`
/// having run without its output appended. To make recovery safe, `apply` must
/// honor the following per-bus contract:
///
/// - The engine calls `apply` with **non-decreasing** entry positions.
/// - The engine MAY re-issue `apply` for the **most recently applied** position
///   (the same `BusEntry`). In that case the applicator MUST return the
///   identical `Option<Payload>` it returned the first time, and MUST NOT apply
///   any side effect a second time.
/// - The engine MUST NOT re-issue `apply` for any position **older** than the
///   last applied one. An applicator detecting this returns
///   [`ApplyError::StalePosition`].
///
/// Concretely: an applicator only needs to retain enough state to reproduce the
/// result for its single last position. The generic conformance suite in
/// `logact_commit_service_engine_tests` (`applicator_tests!`) exercises this.
#[async_trait::async_trait(?Send)]
pub trait Applicator {
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError>;
}
