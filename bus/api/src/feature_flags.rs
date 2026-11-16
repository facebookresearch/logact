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
    /// Build AgentBus on the main environment instead of a dedicated worker.
    pub enable_single_env: bool,

    /// Serialize commit intentions per bus.
    pub enable_commit_buffering: bool,
    /// Disable direct appends to CommitSvc's AgentBus except agent input,
    /// action output, agent output, and mail. Intentions, votes, decisions,
    /// and policy updates are not allowed.
    pub disable_direct_agentbus_appends: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            enable_single_env: false,
            enable_commit_buffering: true,
            disable_direct_agentbus_appends: false,
        }
    }
}
