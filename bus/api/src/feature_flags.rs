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
    /// Select replay instead of V1 in test fixtures. The production server
    /// always runs replay.
    pub enable_replay_commit_service: bool,
    /// Save the shared replay service's checkpoints in the background with
    /// `put_if_newer`, so a commit doesn't wait for its checkpoint save.
    pub enable_background_replay_checkpoints: bool,
    /// Attach a speculative decision to the replay commit service's intentions
    /// under `ON_BY_DEFAULT` and `OFF_BY_DEFAULT`. Reading correct speculations is
    /// always on.
    pub enable_speculative_decisions: bool,
    /// Serialize commit intentions per bus.
    pub enable_commit_buffering: bool,
    /// Retained for compatibility; the commit service always restricts direct
    /// appends to agent events and mail.
    pub disable_direct_agentbus_appends: bool,
    /// Serve commit-service policy reads from a lease over the policy register
    /// instead of reading the register on every commit.
    pub enable_leased_policy_reads: bool,
    /// Poll the commit-service policy register in the background to renew its lease as it expires.
    pub enable_policy_lease_keepalive: bool,
    /// Serve the commit service and its AgentBus through the `channeled_svc`
    /// channels instead of the legacy ones.
    pub enable_channeled_svc: bool,
    /// Check caller eligibility inside the `channeled_svc` channels instead of
    /// the Thrift authorizer. Requires `enable_channeled_svc`.
    pub enable_eligibility_under_channel: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            enable_replay_commit_service: false,
            enable_background_replay_checkpoints: false,
            enable_speculative_decisions: false,
            enable_commit_buffering: true,
            disable_direct_agentbus_appends: false,
            enable_leased_policy_reads: false,
            enable_policy_lease_keepalive: false,
            enable_channeled_svc: false,
            enable_eligibility_under_channel: false,
        }
    }
}
