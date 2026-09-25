/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared construction helpers for configured voters.

use agentbus_core::config::InitialVoterConfig;
use agentbus_core::voter::Voter;

#[derive(Clone)]
pub struct VoterFactory {
    configs: Vec<InitialVoterConfig>,
    api_key: Option<String>,
}

impl VoterFactory {
    pub fn new(configs: Vec<InitialVoterConfig>, api_key: Option<String>) -> Self {
        Self { configs, api_key }
    }

    pub fn build(&self) -> Vec<Box<dyn Voter>> {
        build_voters(&self.configs, self.api_key.clone())
    }
}

fn build_voters(configs: &[InitialVoterConfig], api_key: Option<String>) -> Vec<Box<dyn Voter>> {
    configs
        .iter()
        .cloned()
        .map(|config| -> Box<dyn Voter> {
            match config {
                InitialVoterConfig::Llm {
                    config,
                    model,
                    api_endpoint,
                } => Box::new(agentbus_voter_llm::from_typed_config(
                    config,
                    api_key.clone(),
                    model,
                    api_endpoint,
                )),
                InitialVoterConfig::RuleBased(config) => Box::new(
                    agentbus_voter_rule_based::from_typed_config(config)
                        .unwrap_or_else(|e| panic!("Failed to build rule-based voter: {}", e)),
                ),
            }
        })
        .collect()
}
