/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! BaseEngine and Applicator framework for the LogAct commit service.

pub mod applicator;
pub mod base_engine;
pub mod decider_state;
pub mod engine_state;
pub mod observable_applicator;
pub mod observable_storage;
mod policy;
pub mod policy_decider_applicator;
pub mod policy_register;
pub mod scoped_storage;
pub mod state_machine_spec;
pub mod stateless_voter_adapter;
pub mod storage;
mod storage_concurrency_retry;
pub mod synchronous_register_provider;

pub use agentbus_api::RetryConfig;
pub use applicator::Applicator;
pub use applicator::ApplyError;
pub use applicator::ConcurrencyError;
pub use applicator::DeprecatedOperationKind;
pub use applicator::MalformedPolicyBatchKind;
pub use applicator::StorageWriteResultExt;
pub use base_engine::ApplicatorBinding;
pub use base_engine::BaseEngine;
pub use base_engine::BaseEngineConfig;
pub use base_engine::DeciderFactory;
pub use base_engine::DeciderFactoryImpl;
pub use base_engine::EngineError;
pub use base_engine::EngineResult;
pub use base_engine::ProposalOutcome;
pub use base_engine::VoterFactory;
pub use base_engine::validate_voter_configs;
pub use decider_state::DeciderState;
pub use engine_state::ENGINE_STORAGE_PREFIX;
pub use engine_state::EngineStateLoadError;
pub use engine_state::PerBusEngineState;
pub use engine_state::VersionedPolicy;
pub use engine_state::engine_state_key;
// The policy contract lives in the `api` crate (bus-agnostic); re-exported here so
// existing `logact_commit_service_engine::` paths keep resolving.
pub use logact_commit_service_api::PolicyProvider;
pub use logact_commit_service_api::PolicyState;
pub use logact_commit_service_api::VersionedPolicyState;
pub use logact_commit_service_static_config::StaticConfigPolicyProvider;
pub use observable_applicator::Observability;
pub use observable_applicator::ObservableApplicator;
pub use observable_applicator::ObservedDeciderFactory;
pub use observable_storage::ObservableStorage;
pub use policy_decider_applicator::FirstBooleanWinsApplicator;
pub use policy_decider_applicator::OffByDefaultApplicator;
pub use policy_decider_applicator::OnByDefaultApplicator;
pub use policy_register::PolicyRegister;
pub use policy_register::PolicyRegisterError;
pub use policy_register::PolicyRegisterResult;
pub use scoped_storage::ScopedStorage;
pub use state_machine_spec::StateMachineSpec;
pub use stateless_voter_adapter::ImmutableVoter;
pub use stateless_voter_adapter::StatelessVoterAdapter;
pub use storage::InMemoryStorage;
pub use storage::Storage;
pub use storage::StorageError;
pub use storage::StorageResult;
pub use synchronous_register_provider::SynchronousRegisterProvider;
pub use synchronous_register_provider::SynchronousRegisterProviderError;
