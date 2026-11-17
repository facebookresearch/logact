/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Initial voter configurations loaded by the commit service.

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
    fn test_parse_llm_voter() {
        let yaml = r#"
- type: llm
  prompt_override: "custom prompt"
  model: "test-model"
  api_endpoint: "http://127.0.0.1:8080"
"#;
        let voters: Vec<InitialVoterConfig> = serde_yaml::from_str(yaml).unwrap();
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
        let voters: Vec<InitialVoterConfig> = serde_yaml::from_str(yaml).unwrap();
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
    fn test_parse_mixed_voters() {
        let yaml = r#"
- type: llm
  model: "model-a"
- type: rule-based
  rules:
    - pattern: "rm"
      action: deny
      reason: "no delete"
"#;
        let voters: Vec<InitialVoterConfig> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(voters.len(), 2);
        assert!(matches!(voters[0], InitialVoterConfig::Llm { .. }));
        assert!(matches!(voters[1], InitialVoterConfig::RuleBased(_)));
    }

    #[test]
    fn test_multiple_voters_of_same_type_allowed() {
        let yaml = r#"
- type: llm
  model: "model-a"
- type: llm
  model: "model-b"
"#;
        let voters: Vec<InitialVoterConfig> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(voters.len(), 2);
    }
}
