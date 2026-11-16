/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Replay-based LogAct commit services.
//!
//! A bus's commit-service state at log position `p` is materialized
//! deterministically from `log[0..=p]` ([`ReplayState`]). A commit loads that
//! state as of some position from a [`CheckpointStore`], appends the policy and
//! its intention, then materializes forward until the intention is decided.
//! [`store_support`] exposes the pieces other checkpoint stores build on.

mod checkpoint;
mod decider;
mod drive;
mod player;
mod service;
mod state;

pub use checkpoint::Checkpoint;
pub use checkpoint::CheckpointStore;
pub use service::ReplayCommitService;
pub use state::ReplayState;

/// Building blocks for checkpoint stores defined outside this crate.
pub mod store_support {
    pub use crate::decider::decided_intention;
    pub use crate::drive::apply_error;
    pub use crate::player::LogPlayer;
    pub use crate::player::entry_position;
    pub use crate::state::Step;
}
