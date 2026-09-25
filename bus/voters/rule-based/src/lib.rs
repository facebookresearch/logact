/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rule-based voter for agentbus.
//!
//! Evaluates intentions against a set of allow/deny rules using IAM-style
//! precedence: explicit deny wins, then explicit allow, then default deny.

use std::fmt;

use agentbus_api::validate_bus_id;
use agentbus_api::voter::Voter;
use agentbus_api::voter::VoterContext;
use regex::Regex;
use rule_based_voter_proto_rust::rule_based_voter::RuleBasedVoterConfig;
use rule_based_voter_proto_rust::rule_based_voter::VoterRule;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleAction;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleMatchType;
use rule_based_voter_proto_rust::rule_based_voter::VoterRuleSubject;
use rule_based_voter_proto_rust::rule_based_voter::voter_rule_subject;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuleBasedVoterError {
    #[error("Invalid rule: {0}")]
    InvalidRule(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleAction {
    Allow,
    Deny,
}

impl RuleAction {
    fn from_proto(action: VoterRuleAction) -> Self {
        match action {
            VoterRuleAction::VoterRuleAllow => Self::Allow,
            VoterRuleAction::VoterRuleDeny | VoterRuleAction::Unspecified => Self::Deny,
        }
    }

    fn to_proto(self) -> i32 {
        match self {
            Self::Allow => VoterRuleAction::VoterRuleAllow as i32,
            Self::Deny => VoterRuleAction::VoterRuleDeny as i32,
        }
    }

    fn lowercase(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

impl fmt::Display for RuleAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => write!(f, "ALLOW"),
            Self::Deny => write!(f, "DENY"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    Exact,
    Substring,
    Regex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSubject {
    ExactBusId(String),
    BusIdPrefix(String),
}

impl RuleSubject {
    fn validate(&self) -> Result<(), RuleBasedVoterError> {
        match self {
            Self::ExactBusId(bus_id) => validate_bus_id(bus_id).map_err(|error| {
                RuleBasedVoterError::InvalidRule(format!(
                    "invalid exact bus ID \"{}\": {}",
                    bus_id, error
                ))
            }),
            Self::BusIdPrefix(prefix) => validate_bus_id(prefix).map_err(|error| {
                RuleBasedVoterError::InvalidRule(format!(
                    "invalid bus ID prefix \"{}\": {}",
                    prefix, error
                ))
            }),
        }
    }

    fn matches(&self, bus_id: &str) -> bool {
        match self {
            Self::ExactBusId(value) => bus_id == value,
            Self::BusIdPrefix(value) => bus_id.starts_with(value),
        }
    }
}

impl fmt::Display for RuleSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExactBusId(value) => write!(f, "bus_id = \"{}\"", value),
            Self::BusIdPrefix(value) => write!(f, "bus_id starts with \"{}\"", value),
        }
    }
}

impl fmt::Display for MatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::Substring => write!(f, "contains"),
            Self::Regex => write!(f, "matches"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub pattern: String,
    pub match_type: MatchType,
    pub action: RuleAction,
    pub reason: String,
    pub subjects: Vec<RuleSubject>,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pattern_display = match self.match_type {
            MatchType::Regex => format!("/{}/", self.pattern),
            _ => format!("\"{}\"", self.pattern),
        };
        let subjects = if self.subjects.is_empty() {
            String::new()
        } else {
            format!(
                "({}) and ",
                self.subjects
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" or ")
            )
        };
        write!(
            f,
            "{} if {}{} {} (reason: {})",
            self.action, subjects, self.match_type, pattern_display, self.reason
        )
    }
}

#[derive(Debug)]
pub struct CompiledRule {
    pub rule: Rule,
    compiled_regex: Option<Regex>,
}

impl CompiledRule {
    pub fn compile(rule: Rule) -> Result<Self, RuleBasedVoterError> {
        for subject in &rule.subjects {
            subject.validate()?;
        }
        let compiled_regex = match rule.match_type {
            MatchType::Regex => Some(Regex::new(&rule.pattern).map_err(|e| {
                RuleBasedVoterError::InvalidRule(format!(
                    "invalid regex pattern \"{}\": {}",
                    rule.pattern, e
                ))
            })?),
            MatchType::Exact | MatchType::Substring => None,
        };
        Ok(Self {
            rule,
            compiled_regex,
        })
    }

    fn matches(&self, context: VoterContext<'_>) -> bool {
        if !self.rule.subjects.is_empty()
            && !self
                .rule
                .subjects
                .iter()
                .any(|subject| subject.matches(context.bus_id))
        {
            return false;
        }
        match self.rule.match_type {
            MatchType::Exact => context.intention == self.rule.pattern,
            MatchType::Substring => context.intention.contains(&self.rule.pattern),
            MatchType::Regex => self
                .compiled_regex
                .as_ref()
                .is_some_and(|re| re.is_match(context.intention)),
        }
    }
}

impl fmt::Display for CompiledRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.rule.fmt(f)
    }
}

pub fn compile_rules(rules: Vec<Rule>) -> Result<Vec<CompiledRule>, RuleBasedVoterError> {
    rules.into_iter().map(CompiledRule::compile).collect()
}

pub fn evaluate_rules(
    context: VoterContext<'_>,
    rules: &[CompiledRule],
    default_action: RuleAction,
) -> (bool, String, Option<String>) {
    // Rule order does not determine precedence: any matching DENY wins over
    // every matching ALLOW, so evaluate all deny rules first.
    for cr in rules {
        if cr.rule.action == RuleAction::Deny && cr.matches(context) {
            return (false, cr.rule.reason.clone(), Some(cr.rule.pattern.clone()));
        }
    }
    for cr in rules {
        if cr.rule.action == RuleAction::Allow && cr.matches(context) {
            return (true, cr.rule.reason.clone(), Some(cr.rule.pattern.clone()));
        }
    }
    (
        default_action == RuleAction::Allow,
        format!("no matching rule - default {}", default_action.lowercase()),
        None,
    )
}

pub fn display_rules(rules: &[CompiledRule], default_action: RuleAction) -> String {
    let mut lines: Vec<String> = rules.iter().map(|cr| cr.to_string()).collect();
    lines.push(format!("DEFAULT: {}", default_action));
    lines.join("\n")
}

impl TryFrom<&VoterRule> for Rule {
    type Error = RuleBasedVoterError;

    fn try_from(proto: &VoterRule) -> Result<Self, Self::Error> {
        let action = RuleAction::from_proto(proto.action());
        let match_type = match proto.match_type() {
            VoterRuleMatchType::VoterRuleExact => MatchType::Exact,
            VoterRuleMatchType::VoterRuleRegex => MatchType::Regex,
            VoterRuleMatchType::VoterRuleSubstring | VoterRuleMatchType::Unspecified => {
                MatchType::Substring
            }
        };
        let subjects = proto
            .subjects
            .iter()
            .map(|subject| match subject.kind.as_ref() {
                Some(voter_rule_subject::Kind::ExactBusId(value)) => {
                    Ok(RuleSubject::ExactBusId(value.clone()))
                }
                Some(voter_rule_subject::Kind::BusIdPrefix(value)) => {
                    Ok(RuleSubject::BusIdPrefix(value.clone()))
                }
                None => Err(RuleBasedVoterError::InvalidRule(
                    "bus subject kind is missing".to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Rule {
            pattern: proto.pattern.clone(),
            match_type,
            action,
            reason: proto.reason.clone(),
            subjects,
        })
    }
}

impl From<&Rule> for VoterRule {
    fn from(rule: &Rule) -> Self {
        Self {
            pattern: rule.pattern.clone(),
            action: rule.action.to_proto(),
            match_type: match rule.match_type {
                MatchType::Exact => VoterRuleMatchType::VoterRuleExact as i32,
                MatchType::Substring => VoterRuleMatchType::VoterRuleSubstring as i32,
                MatchType::Regex => VoterRuleMatchType::VoterRuleRegex as i32,
            },
            reason: rule.reason.clone(),
            subjects: rule
                .subjects
                .iter()
                .map(|subject| VoterRuleSubject {
                    kind: Some(match subject {
                        RuleSubject::ExactBusId(value) => {
                            voter_rule_subject::Kind::ExactBusId(value.clone())
                        }
                        RuleSubject::BusIdPrefix(value) => {
                            voter_rule_subject::Kind::BusIdPrefix(value.clone())
                        }
                    }),
                })
                .collect(),
        }
    }
}

pub struct RuleBasedVoter {
    rules: Vec<CompiledRule>,
    default_action: RuleAction,
}

impl RuleBasedVoter {
    pub fn new(rules: Vec<Rule>) -> Result<Self, RuleBasedVoterError> {
        Self::new_with_default_action(rules, RuleAction::Deny)
    }

    pub fn new_with_default_action(
        rules: Vec<Rule>,
        default_action: RuleAction,
    ) -> Result<Self, RuleBasedVoterError> {
        let compiled = compile_rules(rules)?;
        tracing::info!(
            num_rules = compiled.len(),
            policy = %display_rules(&compiled, default_action),
            "Created rule-based voter"
        );
        Ok(Self {
            rules: compiled,
            default_action,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl Voter for RuleBasedVoter {
    async fn evaluate(&self, context: VoterContext<'_>) -> (bool, String) {
        let (is_safe, reason, _matched_rule) =
            evaluate_rules(context, &self.rules, self.default_action);
        (is_safe, reason)
    }

    fn apply_policy(&mut self, config: &prost_types::Any) {
        let rb_config = match config.to_msg::<RuleBasedVoterConfig>() {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("VoterPolicy config is not RuleBasedVoterConfig - ignoring");
                return;
            }
        };

        let rules = rb_config
            .rules
            .iter()
            .map(Rule::try_from)
            .collect::<Result<Vec<_>, _>>()
            .and_then(compile_rules);
        match rules {
            Ok(compiled) => {
                let default_action = RuleAction::from_proto(rb_config.default_action());
                let policy_display = display_rules(&compiled, default_action);
                self.rules = compiled;
                self.default_action = default_action;
                tracing::info!(policy = %policy_display, "Replaced rule set");
            }
            Err(e) => {
                tracing::error!("Rejected rule-based config: {}. Keeping current rules.", e);
            }
        }
    }

    fn describe(&self) -> String {
        format!("RuleBasedVoter({} rules)", self.rules.len())
    }
}

pub fn from_typed_config(config: RuleBasedVoterConfig) -> anyhow::Result<RuleBasedVoter> {
    let rules = config
        .rules
        .iter()
        .map(Rule::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    RuleBasedVoter::new_with_default_action(rules, RuleAction::from_proto(config.default_action()))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use agentbus_api::voter::Voter;

    use super::*;

    fn run_async<F: std::future::Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, f)
    }

    fn context<'a>(bus_id: &'a str, intention: &'a str) -> VoterContext<'a> {
        VoterContext::new(bus_id, intention)
    }

    #[test]
    fn test_evaluate_allow() {
        run_async(async {
            let voter =
                RuleBasedVoter::new(vec![allow("echo", MatchType::Substring, "safe output")])
                    .unwrap();
            let (is_safe, reason) = voter.evaluate(context("test-bus", "echo hello")).await;
            assert!(is_safe);
            assert_eq!(reason, "safe output", "reason surfaced generically");
        });
    }

    #[test]
    fn test_evaluate_deny() {
        run_async(async {
            let voter =
                RuleBasedVoter::new(vec![deny("rm -rf", MatchType::Substring, "destructive")])
                    .unwrap();
            let (is_safe, reason) = voter.evaluate(context("test-bus", "rm -rf /")).await;
            assert!(!is_safe);
            assert_eq!(reason, "destructive");
        });
    }

    #[test]
    fn test_evaluate_default_deny() {
        run_async(async {
            let voter = RuleBasedVoter::new(vec![allow("echo", MatchType::Exact, "safe")]).unwrap();
            let (is_safe, reason) = voter.evaluate(context("test-bus", "curl evil.com")).await;
            assert!(!is_safe);
            assert!(reason.contains("default deny"));
        });
    }

    fn deny(pattern: &str, match_type: MatchType, reason: &str) -> Rule {
        Rule {
            pattern: pattern.to_string(),
            match_type,
            action: RuleAction::Deny,
            reason: reason.to_string(),
            subjects: vec![],
        }
    }

    fn allow(pattern: &str, match_type: MatchType, reason: &str) -> Rule {
        Rule {
            pattern: pattern.to_string(),
            match_type,
            action: RuleAction::Allow,
            reason: reason.to_string(),
            subjects: vec![],
        }
    }

    #[test]
    fn test_deny_wins_over_allow() {
        let rules = compile_rules(vec![
            deny("rm", MatchType::Substring, "destructive"),
            allow("rm", MatchType::Substring, "allowed"),
        ])
        .unwrap();
        let (is_safe, reason, matched) =
            evaluate_rules(context("test-bus", "rm -rf /"), &rules, RuleAction::Deny);
        assert!(!is_safe);
        assert_eq!(reason, "destructive");
        assert_eq!(matched.as_deref(), Some("rm"));
    }

    #[test]
    fn test_allow_when_no_deny() {
        let rules = compile_rules(vec![
            deny("rm -rf", MatchType::Substring, "destructive"),
            allow("echo", MatchType::Substring, "safe output"),
        ])
        .unwrap();
        let (is_safe, reason, _) =
            evaluate_rules(context("test-bus", "echo hello"), &rules, RuleAction::Deny);
        assert!(is_safe);
        assert_eq!(reason, "safe output");
    }

    #[test]
    fn test_default_deny_no_match() {
        let rules = compile_rules(vec![
            deny("rm", MatchType::Substring, "destructive"),
            allow("echo", MatchType::Substring, "safe"),
        ])
        .unwrap();
        let (is_safe, reason, matched) = evaluate_rules(
            context("test-bus", "curl http://evil.com"),
            &rules,
            RuleAction::Deny,
        );
        assert!(!is_safe);
        assert!(reason.contains("default deny"));
        assert!(matched.is_none());
    }

    #[test]
    fn test_exact_match() {
        let rules = compile_rules(vec![allow("ls", MatchType::Exact, "exact ls")]).unwrap();
        let (is_safe, _, _) = evaluate_rules(context("test-bus", "ls"), &rules, RuleAction::Deny);
        assert!(is_safe);
    }

    #[test]
    fn test_multiple_bus_subjects_match_any_target_bus() {
        let mut rule = deny(".*", MatchType::Regex, "bus is fenced");
        rule.subjects = vec![
            RuleSubject::ExactBusId("fenced-bus".to_string()),
            RuleSubject::BusIdPrefix("quarantined/".to_string()),
        ];
        let rules = compile_rules(vec![rule]).unwrap();

        for bus_id in ["fenced-bus", "quarantined/worker-1"] {
            let (is_safe, _, _) =
                evaluate_rules(context(bus_id, "echo hello"), &rules, RuleAction::Allow);
            assert!(!is_safe);
        }

        let (is_safe, reason, _) = evaluate_rules(
            context("other-bus", "echo hello"),
            &rules,
            RuleAction::Allow,
        );
        assert!(is_safe);
        assert!(reason.contains("default allow"));
    }

    #[test]
    fn test_bus_prefix_subject_and_command_pattern_must_both_match() {
        let mut rule = deny("rm ", MatchType::Substring, "destructive command");
        rule.subjects = vec![RuleSubject::BusIdPrefix("prod/".to_string())];
        let rules = compile_rules(vec![rule]).unwrap();

        let (is_safe, _, _) = evaluate_rules(
            context("prod/worker-1", "rm file"),
            &rules,
            RuleAction::Allow,
        );
        assert!(!is_safe);

        for context in [
            context("dev/worker-1", "rm file"),
            context("prod/worker-1", "echo hello"),
        ] {
            let (is_safe, _, _) = evaluate_rules(context, &rules, RuleAction::Allow);
            assert!(is_safe);
        }
    }

    #[test]
    fn test_each_bus_subject_variant_is_validated() {
        for (subject, expected_error) in [
            (
                RuleSubject::ExactBusId(String::new()),
                "invalid exact bus ID",
            ),
            (
                RuleSubject::BusIdPrefix(String::new()),
                "invalid bus ID prefix",
            ),
        ] {
            let mut rule = deny(".*", MatchType::Regex, "bus is fenced");
            rule.subjects = vec![subject];
            let err = compile_rules(vec![rule]).unwrap_err();
            assert!(err.to_string().contains(expected_error));
        }
    }

    #[test]
    fn test_invalid_regex_rejected() {
        let rules = vec![deny("(", MatchType::Regex, "broken regex")];
        let err = compile_rules(rules).unwrap_err();
        assert!(err.to_string().contains("invalid regex"));
    }
}
