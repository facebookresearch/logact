/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Feature flags that components read through their [`Environment`](crate::Environment).

/// Feature flags for AgentBus and commit-service components.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureFlags {
    /// Serialize commit intentions per bus.
    pub enable_commit_buffering: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            enable_commit_buffering: true,
        }
    }
}
