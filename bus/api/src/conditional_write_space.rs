/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Conditional Write Space API
//!
//! A versioned address space with conditional write (compare-and-swap) semantics.
//! Each successful write gives an address a new version that differs from every
//! earlier version at that address. Versions are opaque: callers compare them
//! only for equality.

use bytes::Bytes;
use thiserror::Error;

use crate::tailable_space::TailableSpace;

#[derive(Error, Debug)]
pub enum ConditionalWriteError {
    #[error("transaction conflict")]
    TransactionConflict(#[source] anyhow::Error),
    #[error("timeout")]
    Timeout(#[source] anyhow::Error),
    #[error("backend unavailable")]
    BackendUnavailable(#[source] anyhow::Error),
    #[error("internal error")]
    InternalError(#[source] anyhow::Error),
}

pub type ConditionalWriteResult<T> = std::result::Result<T, ConditionalWriteError>;

/// Opaque version token returned by read, passed back on write.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Version(pub Bytes);

/// A versioned value stored at an address in a `ConditionalWriteSpace`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VersionedValue {
    pub version: Version,
    pub value: Bytes,
}

/// A versioned address space with conditional write (compare-and-swap) semantics.
pub trait ConditionalWriteSpace: TailableSpace {
    /// Conditionally write a value to the given address within a space.
    ///
    /// Succeeds only if the current version at `address` matches
    /// `expected_version`, returning the new version. Use `None` to write to an
    /// empty address. Returns `None` on version mismatch; the existing value is
    /// unchanged.
    fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> impl std::future::Future<Output = ConditionalWriteResult<Option<Version>>>;

    /// Read the versioned value at the given address within a space.
    ///
    /// Returns `Some(VersionedValue)` if the address has been written, `None` if empty.
    fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> impl std::future::Future<Output = ConditionalWriteResult<Option<VersionedValue>>>;
}
