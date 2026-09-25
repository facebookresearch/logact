/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Compile-time constants for structured `event_type` fields used in tracing.
//!
//! Using constants instead of bare strings catches typos at compile time and
//! gives a single place to grep for all event types emitted by the safety
//! pipeline.

pub mod event_type {
    pub const GATE_STARTED: &str = "gate_started";
    pub const GATE_COMPLETED: &str = "gate_completed";
    pub const GATE_TIMEOUT: &str = "gate_timeout";
    pub const VOTER_DECISION: &str = "voter_decision";
    pub const LLM_EVALUATION: &str = "llm_evaluation";
    pub const LLM_EVALUATION_ERROR: &str = "llm_evaluation_error";
    pub const DECIDER_DECISION: &str = "decider_decision";
    pub const DECISION_APPENDED: &str = "decision_appended";
}
