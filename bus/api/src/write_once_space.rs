/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Write-Once Address Space API
//!
//! Defines a trait for an address space where each address can only be written once.
//! Subsequent writes to the same address return an error.

use bytes::Bytes;
use thiserror::Error;

use crate::tailable_space::TailableSpace;

/// Error type for write-once address space operations
#[derive(Error, Debug)]
pub enum WriteOnceError {
    #[error("address already exists: {0}")]
    AddressAlreadyExists(u64),
    #[error("transaction conflict")]
    TransactionConflict(#[source] anyhow::Error),
    #[error("timeout")]
    Timeout(#[source] anyhow::Error),
    #[error("backend unavailable")]
    BackendUnavailable(#[source] anyhow::Error),
    #[error("internal error")]
    InternalError(#[source] anyhow::Error),
}

/// Result type alias for write-once operations
pub type WriteOnceResult<T> = std::result::Result<T, WriteOnceError>;

/// A write-once address space where each address can only be written to once.
///
/// Implementations provide storage where:
/// - `write` succeeds only if the address is empty
/// - `write` fails with `AddressAlreadyExists` if the address has a value
/// - `read` returns the value at an address, or None if not present
///
/// The `space_id` parameter allows a single implementation to handle multiple
/// logical spaces. Implementations route operations based on space_id.
pub trait WriteOnceSpace: TailableSpace {
    /// Write a value to the given address within a space.
    ///
    /// Returns `Ok(())` if the write succeeds (address was empty).
    /// Returns `Err(WriteOnceError::AddressAlreadyExists)` if the address already has a value.
    fn write(
        &mut self,
        space_id: &str,
        address: u64,
        value: Bytes,
    ) -> impl std::future::Future<Output = WriteOnceResult<()>>;

    /// Read the value at the given address within a space.
    ///
    /// Returns `Some(value)` if the address has a value.
    /// Returns `None` if the address is empty.
    fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> impl std::future::Future<Output = Option<Bytes>>;
}
