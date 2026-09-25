/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Canonical payload type enum for AgentBus.
//!
//! Maps between payload field names, proto enum values, and display strings.
//! Import this instead of maintaining your own copy — the mapping matches
//! `agent_bus.proto`.

use agent_bus_proto_rust::agent_bus::SelectivePollType;

/// Payload types in the AgentBus log.
///
/// Adding a new payload type? Add a variant here and implement all four methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadType {
    Intention,
    Vote,
    DeciderPolicy,
    Commit,
    Abort,
    VoterPolicy,
    Control,
    InferenceInput,
    InferenceOutput,
    ActionOutput,
    AgentInput,
    AgentOutput,
    Mail,
}

impl PayloadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Intention => "intention",
            Self::Vote => "vote",
            Self::DeciderPolicy => "decider_policy",
            Self::Commit => "commit",
            Self::Abort => "abort",
            Self::VoterPolicy => "voter_policy",
            Self::Control => "control",
            Self::InferenceInput => "inference_input",
            Self::InferenceOutput => "inference_output",
            Self::ActionOutput => "action_output",
            Self::AgentInput => "agent_input",
            Self::AgentOutput => "agent_output",
            Self::Mail => "mail",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "intention" => Some(Self::Intention),
            "vote" => Some(Self::Vote),
            "decider_policy" => Some(Self::DeciderPolicy),
            "commit" => Some(Self::Commit),
            "abort" => Some(Self::Abort),
            "voter_policy" => Some(Self::VoterPolicy),
            "control" => Some(Self::Control),
            "inference_input" => Some(Self::InferenceInput),
            "inference_output" => Some(Self::InferenceOutput),
            "action_output" => Some(Self::ActionOutput),
            "agent_input" => Some(Self::AgentInput),
            "agent_output" => Some(Self::AgentOutput),
            "mail" => Some(Self::Mail),
            _ => None,
        }
    }

    /// Proto i32 value matching `SelectivePollType` enum in agent_bus.proto.
    pub fn to_proto_i32(&self) -> i32 {
        match self {
            Self::Intention => SelectivePollType::Intention as i32,
            Self::Vote => SelectivePollType::Vote as i32,
            Self::DeciderPolicy => SelectivePollType::DeciderPolicy as i32,
            Self::Commit => SelectivePollType::Commit as i32,
            Self::Abort => SelectivePollType::Abort as i32,
            Self::VoterPolicy => SelectivePollType::VoterPolicy as i32,
            Self::Control => SelectivePollType::Control as i32,
            Self::InferenceInput => SelectivePollType::InferenceInput as i32,
            Self::InferenceOutput => SelectivePollType::InferenceOutput as i32,
            Self::ActionOutput => SelectivePollType::ActionOutput as i32,
            Self::AgentInput => SelectivePollType::AgentInput as i32,
            Self::AgentOutput => SelectivePollType::AgentOutput as i32,
            Self::Mail => SelectivePollType::Mail as i32,
        }
    }

    /// Whether this payload type carries a string content body that the CLI
    /// truncates in non-full-payload mode.
    pub fn has_string_payload(&self) -> bool {
        matches!(
            self,
            Self::Intention
                | Self::InferenceInput
                | Self::InferenceOutput
                | Self::ActionOutput
                | Self::AgentInput
                | Self::AgentOutput
                | Self::Control
        )
    }
}
