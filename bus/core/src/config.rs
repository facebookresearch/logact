/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! YAML config file parsing for AppServer.
//!
//! Parses a YAML config into `AppServerConfig` plus a list of initial local
//! voter definitions. These are the static voter configs loaded from YAML/CLI
//! before runtime `VoterPolicy` updates are applied. There is no bus-visible
//! voter ID: `VoterPolicy` updates continue to fan out by config type.

use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use llm_voter_proto_rust::llm_voter::LlmVoterConfig;
use rule_based_voter_proto_rust::rule_based_voter::RuleBasedVoterConfig;
use rule_based_voter_proto_rust::rule_based_voter::VoterRule;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleAction;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleMatchType;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleSubject;
use rule_based_voter_proto_rust::rule_based_voter::voter_rule_subject;
use serde::Deserialize;
use serde::Deserializer;

use crate::appserver::AppServerConfig;

#[derive(Deserialize)]
pub struct AppServerConfigFile {
    pub bus_id: String,
    #[serde(default = "default_gate_timeout")]
    pub gate_check_timeout_ms: u64,
    /// Internal polling interval (ms) for WriteOnce-backed blocking_poll.
    pub blocking_poll_interval_ms: Option<u64>,
    #[serde(default)]
    pub voters: Vec<InitialVoterConfig>,
}

fn default_gate_timeout() -> u64 {
    30000
}

/// Initial voter config loaded from YAML/CLI before runtime voter policy
/// overrides are applied.
#[derive(Debug, Clone, PartialEq)]
pub enum InitialVoterConfig {
    Llm {
        config: LlmVoterConfig,
        model: Option<String>,
        api_endpoint: Option<String>,
    },
    RuleBased(RuleBasedVoterConfig),
}

impl<'de> Deserialize<'de> for InitialVoterConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(VoterConfigEntry::deserialize(deserializer)?.into_initial_config())
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum VoterConfigEntry {
    #[serde(rename = "llm")]
    Llm {
        #[serde(default)]
        prompt_override: String,
        model: Option<String>,
        api_endpoint: Option<String>,
    },
    #[serde(rename = "rule-based")]
    RuleBased {
        #[serde(default)]
        default_action: RuleAction,
        rules: Vec<RuleEntry>,
    },
}

#[derive(Deserialize)]
struct RuleEntry {
    pattern: String,
    action: RuleAction,
    #[serde(default)]
    match_type: RuleMatchType,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    subjects: Vec<RuleSubjectEntry>,
}

#[derive(Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum RuleAction {
    Allow,
    #[default]
    Deny,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum RuleMatchType {
    Exact,
    #[default]
    Substring,
    Regex,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RuleSubjectEntry {
    ExactBusId(ExactBusIdSubject),
    BusIdPrefix(BusIdPrefixSubject),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactBusIdSubject {
    exact_bus_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BusIdPrefixSubject {
    bus_id_prefix: String,
}

impl AppServerConfigFile {
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        serde_yaml::from_str(yaml).context("failed to parse config YAML")
    }

    pub fn from_path(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path))?;
        Self::from_yaml(&contents)
    }

    pub fn into_config(self) -> Result<(AppServerConfig, Vec<InitialVoterConfig>)> {
        let config = AppServerConfig {
            bus_id: self.bus_id,
            gate_check_timeout: Duration::from_millis(self.gate_check_timeout_ms),
            blocking_poll_interval: self.blocking_poll_interval_ms.map(Duration::from_millis),
        };
        Ok((config, self.voters))
    }
}

impl VoterConfigEntry {
    fn into_initial_config(self) -> InitialVoterConfig {
        match self {
            VoterConfigEntry::Llm {
                prompt_override,
                model,
                api_endpoint,
            } => InitialVoterConfig::Llm {
                config: LlmVoterConfig { prompt_override },
                model,
                api_endpoint,
            },
            VoterConfigEntry::RuleBased {
                default_action,
                rules,
            } => {
                let rules = rules
                    .into_iter()
                    .map(|r| VoterRule {
                        pattern: r.pattern,
                        action: match r.action {
                            RuleAction::Allow => VoterRuleAction::VoterRuleAllow as i32,
                            RuleAction::Deny => VoterRuleAction::VoterRuleDeny as i32,
                        },
                        match_type: match r.match_type {
                            RuleMatchType::Exact => VoterRuleMatchType::VoterRuleExact as i32,
                            RuleMatchType::Substring => {
                                VoterRuleMatchType::VoterRuleSubstring as i32
                            }
                            RuleMatchType::Regex => VoterRuleMatchType::VoterRuleRegex as i32,
                        },
                        reason: r.reason,
                        subjects: r
                            .subjects
                            .into_iter()
                            .map(|subject| VoterRuleSubject {
                                kind: Some(match subject {
                                    RuleSubjectEntry::ExactBusId(subject) => {
                                        voter_rule_subject::Kind::ExactBusId(subject.exact_bus_id)
                                    }
                                    RuleSubjectEntry::BusIdPrefix(subject) => {
                                        voter_rule_subject::Kind::BusIdPrefix(subject.bus_id_prefix)
                                    }
                                }),
                            })
                            .collect(),
                    })
                    .collect();
                InitialVoterConfig::RuleBased(RuleBasedVoterConfig {
                    rules,
                    default_action: match default_action {
                        RuleAction::Allow => VoterRuleAction::VoterRuleAllow as i32,
                        RuleAction::Deny => VoterRuleAction::VoterRuleDeny as i32,
                    },
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
bus_id: "test-bus"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        assert_eq!(config.bus_id, "test-bus");
        assert!(config.voters.is_empty());

        let (domain, voters) = config.into_config().unwrap();
        assert_eq!(domain.bus_id, "test-bus");
        assert!(voters.is_empty());
    }

    #[test]
    fn test_parse_llm_voter() {
        let yaml = r#"
bus_id: "test-bus"
voters:
  - type: llm
    prompt_override: "custom prompt"
    model: "test-model"
    api_endpoint: "http://127.0.0.1:8080"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        let (_, voters) = config.into_config().unwrap();
        assert_eq!(voters.len(), 1);
        match &voters[0] {
            InitialVoterConfig::Llm {
                config,
                model,
                api_endpoint,
            } => {
                assert_eq!(config.prompt_override, "custom prompt");
                assert_eq!(model.as_deref(), Some("test-model"));
                assert_eq!(api_endpoint.as_deref(), Some("http://127.0.0.1:8080"));
            }
            InitialVoterConfig::RuleBased(_) => panic!("expected llm voter"),
        }
    }

    #[test]
    fn test_parse_rule_based_voter() {
        let yaml = r#"
bus_id: "test-bus"
voters:
  - type: rule-based
    default_action: allow
    rules:
      - pattern: "rm -rf"
        action: deny
        match_type: substring
        reason: "destructive"
        subjects:
          - exact_bus_id: "fenced-bus"
          - bus_id_prefix: "quarantined/"
      - pattern: "echo"
        action: allow
        match_type: exact
        reason: "safe"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        let (_, voters) = config.into_config().unwrap();
        assert_eq!(voters.len(), 1);
        match &voters[0] {
            InitialVoterConfig::RuleBased(rb) => {
                assert_eq!(rb.rules.len(), 2);
                assert_eq!(rb.default_action(), VoterRuleAction::VoterRuleAllow);
                assert_eq!(rb.rules[0].subjects.len(), 2);
                assert!(matches!(
                    rb.rules[0].subjects[0].kind.as_ref(),
                    Some(voter_rule_subject::Kind::ExactBusId(bus_id))
                        if bus_id == "fenced-bus"
                ));
                assert!(matches!(
                    rb.rules[0].subjects[1].kind.as_ref(),
                    Some(voter_rule_subject::Kind::BusIdPrefix(prefix))
                        if prefix == "quarantined/"
                ));
                assert!(rb.rules[1].subjects.is_empty());
            }
            InitialVoterConfig::Llm { .. } => panic!("expected rule-based voter"),
        }
    }

    #[test]
    fn test_defaults_applied() {
        let yaml = r#"
bus_id: "test-bus"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        assert_eq!(config.gate_check_timeout_ms, 30000);

        let (domain, _) = config.into_config().unwrap();
        assert_eq!(domain.gate_check_timeout, Duration::from_millis(30000));
    }

    #[test]
    fn test_parse_mixed_voters() {
        let yaml = r#"
bus_id: "test-bus"
voters:
  - type: llm
    model: "model-a"
  - type: rule-based
    rules:
      - pattern: "rm"
        action: deny
        reason: "no delete"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        let (_, voters) = config.into_config().unwrap();
        assert_eq!(voters.len(), 2);
        assert!(matches!(voters[0], InitialVoterConfig::Llm { .. }));
        assert!(matches!(voters[1], InitialVoterConfig::RuleBased(_)));
    }

    #[test]
    fn test_multiple_voters_of_same_type_allowed() {
        let yaml = r#"
bus_id: "test-bus"
voters:
  - type: llm
    model: "model-a"
  - type: llm
    model: "model-b"
"#;
        let config = AppServerConfigFile::from_yaml(yaml).unwrap();
        let (_, voters) = config.into_config().unwrap();
        assert_eq!(voters.len(), 2);
    }
}
