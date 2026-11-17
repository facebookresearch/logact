/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The storage contract of the lease protocol: one versioned compare-and-swap
//! cell that stores a payload with its [`LeaseMetadata`].

use std::future::Future;
use std::time::Duration;

/// Lease-protocol metadata stored with a value. Each cell chooses how to store
/// it, including how to encode `visible_version`.
#[derive(Clone, Debug, PartialEq)]
pub struct LeaseMetadata<V> {
    /// Set while a writer revokes the lease on this value and waits for
    /// outstanding leases to expire. Readers must not cache it.
    pub revoked: bool,
    /// The version reported to callers instead of the stored version. Set when
    /// a reservation or recovery rewrites a value without changing it.
    pub visible_version: Option<V>,
    /// The longest lease readers may grant on this value, which its writer
    /// and anyone replacing it drain for. Zero if unknown, so readers do not
    /// cache the value.
    pub lease_duration: Duration,
}

impl<V> Default for LeaseMetadata<V> {
    fn default() -> Self {
        Self {
            revoked: false,
            visible_version: None,
            lease_duration: Duration::ZERO,
        }
    }
}

/// The contents of a [`CasCell`]: a payload, its lease metadata, and the
/// stored version. Values written without the protocol have no metadata and
/// are never cached.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredValue<P, V> {
    pub payload: P,
    pub leasing: Option<LeaseMetadata<V>>,
    pub version: V,
}

/// One versioned compare-and-swap cell that stores a payload with its lease
/// metadata: the only storage contract the lease protocol needs.
pub trait CasCell {
    type Payload: Clone;
    type Version: Clone + PartialEq;
    type Error;

    /// Return the stored value, or `None` if the cell is empty. Stored
    /// metadata the cell cannot decode is reported as `Self::Error`.
    fn read(
        &self,
    ) -> impl Future<Output = Result<Option<StoredValue<Self::Payload, Self::Version>>, Self::Error>>;

    /// Store `payload` with `leasing` if the stored version is `expected`, and
    /// return the new version, or `None` on a version mismatch.
    /// `expected: None` succeeds only if the cell is empty.
    fn write(
        &self,
        payload: Self::Payload,
        leasing: LeaseMetadata<Self::Version>,
        expected: Option<Self::Version>,
    ) -> impl Future<Output = Result<Option<Self::Version>, Self::Error>>;
}
