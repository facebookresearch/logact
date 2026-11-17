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

    /// Run the replay commit service with shared storage checkpoints instead of
    /// V1.
    pub enable_replay_commit_service: bool,
    /// Save the shared replay service's checkpoints in the background with
    /// `put_if_newer`, so a commit doesn't wait for its checkpoint save.
    pub enable_background_replay_checkpoints: bool,
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
            enable_replay_commit_service: false,
            enable_background_replay_checkpoints: false,
            enable_commit_buffering: true,
            disable_direct_agentbus_appends: false,
        }
    }
}
