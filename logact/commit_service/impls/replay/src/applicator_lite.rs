/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! [`ApplicatorLite`]: V1's `Applicator` without storage.

use agent_bus_proto_rust::agent_bus::BusEntry;
use logact_commit_service_engine::ApplyError;

/// V1's `Applicator` without storage, context or `async`: a deterministic
/// transition whose state lives in memory, so replaying the log rebuilds it.
/// Unlike V1's, its output needn't be a payload to log.
pub trait ApplicatorLite {
    /// What applying one entry produces.
    type Output;

    /// Apply `entry`, returning its output.
    fn apply(&mut self, entry: &BusEntry) -> Result<Self::Output, ApplyError>;
}

impl<A: ApplicatorLite + ?Sized> ApplicatorLite for &mut A {
    type Output = A::Output;

    fn apply(&mut self, entry: &BusEntry) -> Result<Self::Output, ApplyError> {
        (**self).apply(entry)
    }
}
