/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! V1 implementation of the engine `VoterFactory` trait.

use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::voter_config;
use agentbus_api::Environment;
use agentbus_api::NoopLogger;
use agentbus_api::NoopMetrics;
use agentbus_api::RealEnvironment;
use agentbus_api::logger::AgentbusLogger;
use agentbus_api::voter::Voter;
use anyhow::Context;
use anyhow::Result;
use llm_voter_proto_rust::llm_voter::LlmVoterConfig;
use logact_commit_service_engine::Applicator;
use logact_commit_service_engine::ImmutableVoter;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::ObservableApplicator;
use logact_commit_service_engine::StateMachineSpec;
use logact_commit_service_engine::StatelessVoterAdapter;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::VoterFactory;
use prost::Name;
use rule_based_voter_proto_rust::rule_based_voter::RuleBasedVoterConfig;

fn unsupported_config(config: &VoterConfig, expected_type_url: &str) -> anyhow::Error {
    let got = match config.config.as_ref() {
        Some(voter_config::Config::Custom(any)) => any.type_url.clone(),
        Some(voter_config::Config::Llm(_)) => LlmVoterConfig::type_url(),
        Some(voter_config::Config::RuleBased(_)) => RuleBasedVoterConfig::type_url(),
        None => "<missing>".to_string(),
    };
    anyhow::anyhow!("unsupported voter type: expected {expected_type_url}, got {got}")
}

fn llm_config(config: &VoterConfig) -> Result<LlmVoterConfig> {
    match config.config.as_ref() {
        Some(voter_config::Config::Llm(llm)) => Ok(llm.clone()),
        _ => Err(unsupported_config(config, &LlmVoterConfig::type_url())),
    }
}

fn rule_based_config(config: &VoterConfig) -> Result<RuleBasedVoterConfig> {
    match config.config.as_ref() {
        Some(voter_config::Config::RuleBased(rule_based)) => Ok(rule_based.clone()),
        _ => Err(unsupported_config(
            config,
            &RuleBasedVoterConfig::type_url(),
        )),
    }
}

fn immutable_voter(voter_id: String, config: &VoterConfig, voter: Rc<dyn Voter>) -> ImmutableVoter {
    ImmutableVoter::new(voter_id, config.clone(), voter)
}

pub struct DelegatingVoterFactory<L = NoopLogger, E = RealEnvironment, S = InMemoryStorage> {
    llm_factory: LlmVoterFactory<S>,
    rule_based_factory: RuleBasedVoterFactory<S>,
    observability: Observability<L, E>,
}

impl<L, E, S> DelegatingVoterFactory<L, E, S> {
    pub fn new(
        llm_factory: LlmVoterFactory<S>,
        rule_based_factory: RuleBasedVoterFactory<S>,
        observability: Observability<L, E>,
    ) -> Self {
        Self {
            llm_factory,
            rule_based_factory,
            observability,
        }
    }
}

impl Default for DelegatingVoterFactory {
    fn default() -> Self {
        Self::with_storage(Rc::new(InMemoryStorage::new()))
    }
}

impl<S> DelegatingVoterFactory<NoopLogger, RealEnvironment, S> {
    pub fn with_storage(storage: Rc<S>) -> Self {
        Self::new(
            LlmVoterFactory::new(storage.clone(), None, None, None),
            RuleBasedVoterFactory::new(storage),
            Observability {
                metrics: Rc::new(NoopMetrics),
                logger: Rc::new(NoopLogger),
                environment: Rc::new(RealEnvironment::new()),
            },
        )
    }
}

impl<L, E, S> Clone for DelegatingVoterFactory<L, E, S> {
    fn clone(&self) -> Self {
        Self {
            llm_factory: self.llm_factory.clone(),
            rule_based_factory: self.rule_based_factory.clone(),
            observability: self.observability.clone(),
        }
    }
}

impl<S, L, E> VoterFactory for DelegatingVoterFactory<L, E, S>
where
    S: Storage + 'static,
    L: AgentbusLogger,
    E: Environment + 'static,
{
    fn validate_config(&self, config: Option<&VoterConfig>) -> Result<VoterConfig> {
        let config = config.context("missing voter config")?;
        match config.config.as_ref() {
            Some(voter_config::Config::Llm(_)) => self.llm_factory.validate_config(Some(config)),
            Some(voter_config::Config::RuleBased(_)) => {
                self.rule_based_factory.validate_config(Some(config))
            }
            Some(voter_config::Config::Custom(any)) => {
                Err(anyhow::anyhow!("unknown voter type: {}", any.type_url))
            }
            None => Err(anyhow::anyhow!("missing voter config")),
        }
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> Result<Rc<dyn Applicator>> {
        let (voter, applicator_kind) = match spec.config.config.as_ref() {
            Some(voter_config::Config::Llm(_)) => {
                (self.llm_factory.create_voter(spec)?, "llm-voter")
            }
            Some(voter_config::Config::RuleBased(_)) => (
                self.rule_based_factory.create_voter(spec)?,
                "rule-based-voter",
            ),
            Some(voter_config::Config::Custom(any)) => {
                return Err(anyhow::anyhow!("unknown voter type: {}", any.type_url));
            }
            None => return Err(anyhow::anyhow!("missing voter config")),
        };

        Ok(Rc::new(ObservableApplicator::new(
            voter,
            applicator_kind,
            self.observability.clone(),
        )))
    }
}

pub struct LlmVoterFactory<S = InMemoryStorage> {
    storage: Rc<S>,
    llm_api_key: Option<String>,
    llm_model: Option<String>,
    llm_api_endpoint: Option<String>,
}

impl<S> Clone for LlmVoterFactory<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            llm_api_key: self.llm_api_key.clone(),
            llm_model: self.llm_model.clone(),
            llm_api_endpoint: self.llm_api_endpoint.clone(),
        }
    }
}

impl<S> LlmVoterFactory<S> {
    pub fn new(
        storage: Rc<S>,
        llm_api_key: Option<String>,
        llm_model: Option<String>,
        llm_api_endpoint: Option<String>,
    ) -> Self {
        Self {
            storage,
            llm_api_key,
            llm_model,
            llm_api_endpoint,
        }
    }
}

impl Default for LlmVoterFactory {
    fn default() -> Self {
        Self::new(Rc::new(InMemoryStorage::new()), None, None, None)
    }
}

impl<S: Storage + 'static> VoterFactory for LlmVoterFactory<S> {
    fn validate_config(&self, config: Option<&VoterConfig>) -> Result<VoterConfig> {
        let config = config.context("missing voter config")?;
        llm_config(config)?;
        Ok(config.clone())
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> Result<Rc<dyn Applicator>> {
        let llm_config = llm_config(&spec.config)?;
        let voter: Rc<dyn Voter> = Rc::new(agentbus_voter_llm::from_typed_config(
            llm_config,
            self.llm_api_key.clone(),
            self.llm_model.clone(),
            self.llm_api_endpoint.clone(),
        ));
        Ok(Rc::new(StatelessVoterAdapter::new(
            immutable_voter(spec.id.clone(), &spec.config, voter),
            self.storage.clone(),
        )))
    }
}

pub struct RuleBasedVoterFactory<S = InMemoryStorage> {
    storage: Rc<S>,
}

impl<S> Clone for RuleBasedVoterFactory<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<S> RuleBasedVoterFactory<S> {
    pub fn new(storage: Rc<S>) -> Self {
        Self { storage }
    }
}

impl Default for RuleBasedVoterFactory {
    fn default() -> Self {
        Self::new(Rc::new(InMemoryStorage::new()))
    }
}

impl<S: Storage + 'static> VoterFactory for RuleBasedVoterFactory<S> {
    fn validate_config(&self, config: Option<&VoterConfig>) -> Result<VoterConfig> {
        let config = config.context("missing voter config")?;
        agentbus_voter_rule_based::from_typed_config(rule_based_config(config)?)?;
        Ok(config.clone())
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> Result<Rc<dyn Applicator>> {
        let voter: Rc<dyn Voter> = Rc::new(agentbus_voter_rule_based::from_typed_config(
            rule_based_config(&spec.config)?,
        )?);
        Ok(Rc::new(StatelessVoterAdapter::new(
            immutable_voter(spec.id.clone(), &spec.config, voter),
            self.storage.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::BusEntry;
    use agent_bus_proto_rust::agent_bus::Header;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Payload;
    use agent_bus_proto_rust::agent_bus::intention;
    use agent_bus_proto_rust::agent_bus::payload;
    use agentbus_api::InMemoryLogger;
    use agentbus_api::InMemoryMetrics;
    use agentbus_api::LogFieldValue;
    use agentbus_api::RealEnvironment;
    use futures::executor::block_on;
    use llm_voter_proto_rust::llm_voter::LlmVoterConfig;
    use logact_commit_service_engine::InMemoryStorage;
    use logact_commit_service_engine::Observability;
    use logact_commit_service_engine::VoterFactory;
    use prost_types::Any;
    use rule_based_voter_proto_rust::rule_based_voter::RuleBasedVoterConfig;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRule;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRuleAction;
    use rule_based_voter_proto_rust::rule_based_voter::VoterRuleMatchType;

    use super::*;

    fn llm_voter_config(config: LlmVoterConfig) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::Llm(config)),
        }
    }

    fn rule_based_voter_config(config: RuleBasedVoterConfig) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::RuleBased(config)),
        }
    }

    fn custom_voter_config(config: Any) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::Custom(config)),
        }
    }

    fn voter_spec(id: &str, config: VoterConfig) -> StateMachineSpec<String, VoterConfig> {
        StateMachineSpec::new(id.to_string(), config)
    }

    fn intention_entry(position: i64, body: &str) -> BusEntry {
        BusEntry {
            header: Some(Header {
                log_position: position,
                ..Default::default()
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention(body.to_string())),
                    ..Default::default()
                })),
            }),
        }
    }

    #[test]
    fn llm_voter_factory_configures_factory_voter() {
        let config = LlmVoterConfig {
            prompt_override: "check carefully".to_string(),
        };
        let factory = LlmVoterFactory::new(
            Rc::new(InMemoryStorage::new()),
            Some("test-key".to_string()),
            Some("test-model".to_string()),
            Some("http://127.0.0.1:1234".to_string()),
        );

        factory
            .create_voter(&voter_spec("1", llm_voter_config(config)))
            .unwrap();
    }

    #[test]
    fn concrete_factory_rejects_wrong_type_url() {
        let factory = LlmVoterFactory::default();
        let err = match factory.create_voter(&voter_spec(
            "1",
            rule_based_voter_config(RuleBasedVoterConfig::default()),
        )) {
            Ok(_) => panic!("wrong voter type should be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("unsupported voter type"));
    }

    #[test]
    fn rule_based_voter_config_is_supported() {
        let config = RuleBasedVoterConfig {
            rules: vec![VoterRule {
                pattern: "echo".to_string(),
                action: VoterRuleAction::VoterRuleAllow as i32,
                match_type: VoterRuleMatchType::VoterRuleSubstring as i32,
                reason: "safe echo".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let factory = RuleBasedVoterFactory::new(Rc::new(InMemoryStorage::new()));

        factory
            .create_voter(&voter_spec("1", rule_based_voter_config(config)))
            .unwrap();
    }

    #[test]
    fn delegating_voter_factory_routes_by_type_url() {
        let llm_config = LlmVoterConfig {
            prompt_override: "check carefully".to_string(),
        };
        let rule_based_config = RuleBasedVoterConfig::default();
        let factory = DelegatingVoterFactory::default();

        factory
            .create_voter(&voter_spec("1", llm_voter_config(llm_config)))
            .unwrap();
        factory
            .create_voter(&voter_spec("2", rule_based_voter_config(rule_based_config)))
            .unwrap();
    }

    #[test]
    fn delegating_voter_factory_observes_created_voters() {
        let metrics = Rc::new(InMemoryMetrics::new());
        let logger = InMemoryLogger::new();
        let storage = Rc::new(InMemoryStorage::new());
        let factory = DelegatingVoterFactory::new(
            LlmVoterFactory::new(storage.clone(), None, None, None),
            RuleBasedVoterFactory::new(storage),
            Observability {
                metrics: metrics.clone(),
                logger: Rc::new(logger.clone()),
                environment: Rc::new(RealEnvironment::new()),
            },
        );
        let config = RuleBasedVoterConfig {
            rules: vec![VoterRule {
                pattern: "echo".to_string(),
                action: VoterRuleAction::VoterRuleAllow as i32,
                match_type: VoterRuleMatchType::VoterRuleSubstring as i32,
                reason: "safe echo".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let voter = factory
            .create_voter(&voter_spec("1", rule_based_voter_config(config)))
            .unwrap();
        block_on(voter.apply("bus-1", &intention_entry(7, "echo this"))).unwrap();

        assert_eq!(
            metrics.counter("apply.applicator.num_calls"),
            1,
            "observed voter should record apply throughput"
        );
        assert_eq!(
            metrics.counter("apply.applicator.num_errors"),
            0,
            "healthy voter apply should not record errors"
        );

        let rows = logger.rows();
        assert_eq!(rows.len(), 1, "observed voter should log one apply row");
        assert_eq!(rows[0].api_call_name, "apply");
        assert!(
            rows[0].additional_fields.iter().any(|(key, value)| {
                key == "payload_type"
                    && matches!(
                        value,
                        LogFieldValue::String(payload_type) if payload_type == "intention"
                    )
            }),
            "logged row should include the applied payload type"
        );
    }

    #[test]
    fn unknown_voter_type_is_rejected() {
        let factory = DelegatingVoterFactory::default();
        let err = match factory.create_voter(&voter_spec(
            "1",
            custom_voter_config(Any {
                type_url: "test/unknown".to_string(),
                value: vec![],
            }),
        )) {
            Ok(_) => panic!("unknown voter type should be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("unknown voter type"));
    }
}
