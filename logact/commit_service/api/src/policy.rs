/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Policy interfaces for commit services.

pub use logact_commit_service_policy_proto_rust::logact_commit_service_policy::PolicyState;

#[derive(Clone, Debug, PartialEq)]
/// Desired policy for one bus and its provider-assigned logical version.
pub struct VersionedPolicyState {
    pub state: PolicyState,
    pub version: i64,
}

/// A source of desired commit-service policy, consulted on the commit path.
///
/// For each `bus_id`, the provider owns one logical version sequence regardless
/// of which underlying sources it consults. Every policy revision that
/// supersedes another must have a greater version, and one version must never
/// identify different policy states. Providers that outlive a process or are
/// shared across service instances are responsible for preserving that sequence
/// across restarts and concurrent readers. Reads may return an older revision;
/// callers ignore versions they have already applied.
///
/// Every successful read returns a complete `VersionedPolicyState`; an
/// unavailable or uninitialized policy is an error.
pub trait PolicyProvider {
    type Error;

    fn read(
        &self,
        bus_id: &str,
    ) -> impl std::future::Future<Output = Result<VersionedPolicyState, Self::Error>>;
}
