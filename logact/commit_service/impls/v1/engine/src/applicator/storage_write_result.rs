/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use super::ApplyError;
use super::ConcurrencyError;
use crate::StorageError;
use crate::StorageResult;

/// Converts storage writes into apply-layer results with concurrency failures
/// attributed to the state owner performing the write.
pub trait StorageWriteResultExt {
    /// Convert an engine-state write result.
    fn into_engine_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError>;

    /// Convert a voter-state write result.
    fn into_voter_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError>;

    /// Convert a decider-state write result.
    fn into_decider_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError>;
}

impl StorageWriteResultExt for StorageResult<bool> {
    fn into_engine_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError> {
        into_apply_result(self, StorageOwner::Engine, bus_id, position)
    }

    fn into_voter_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError> {
        into_apply_result(self, StorageOwner::Voter, bus_id, position)
    }

    fn into_decider_apply_result(self, bus_id: &str, position: i64) -> Result<(), ApplyError> {
        into_apply_result(self, StorageOwner::Decider, bus_id, position)
    }
}

#[derive(Clone, Copy)]
enum StorageOwner {
    Engine,
    Voter,
    Decider,
}

impl StorageOwner {
    fn conflict(self, bus_id: &str, position: i64, source: Option<StorageError>) -> ApplyError {
        let error = match self {
            Self::Engine => ConcurrencyError::Engine {
                bus_id: bus_id.to_string(),
                position,
                source,
            },
            Self::Voter => ConcurrencyError::Voter {
                bus_id: bus_id.to_string(),
                position,
                source,
            },
            Self::Decider => ConcurrencyError::Decider {
                bus_id: bus_id.to_string(),
                position,
                source,
            },
        };
        ApplyError::Concurrency(error)
    }
}

fn into_apply_result(
    result: StorageResult<bool>,
    owner: StorageOwner,
    bus_id: &str,
    position: i64,
) -> Result<(), ApplyError> {
    match result {
        Ok(true) => Ok(()),
        Ok(false) => Err(owner.conflict(bus_id, position, None)),
        Err(error @ StorageError::TransactionConflict(_)) => {
            Err(owner.conflict(bus_id, position, Some(error)))
        }
        Err(error) => Err(ApplyError::Storage(error)),
    }
}
