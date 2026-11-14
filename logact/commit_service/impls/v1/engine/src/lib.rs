/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! BaseEngine and Applicator framework for the LogAct commit service.

pub mod applicator;
pub mod base_engine;
mod caching_storage;
pub mod decider_state;
pub mod decider_transitions;
pub mod engine_state;
pub mod observable_applicator;
pub mod observable_storage;
mod policy;
pub mod policy_decider_applicator;
pub mod retrying_storage;
pub mod scoped_storage;
pub mod state_machine_spec;
pub mod stateless_voter_adapter;
pub mod storage;
mod storage_concurrency_retry;

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
pub use base_engine::EngineContext;
pub use base_engine::EngineError;
pub use base_engine::EngineResult;
pub use base_engine::ProposalOutcome;
pub use base_engine::VoterFactory;
pub use base_engine::abort_payload;
pub use base_engine::forced_abort;
pub use base_engine::load_decider_spec_from_state;
pub use base_engine::outcome_for;
pub use base_engine::state_voter_configs;
pub use base_engine::validate_voter_configs;
pub use caching_storage::CachingStorage;
pub use caching_storage::CachingStorageConfig;
pub use decider_state::DeciderState;
pub use engine_state::ENGINE_STORAGE_PREFIX;
pub use engine_state::EngineStateLoadError;
pub use engine_state::PerBusEngineState;
pub use engine_state::VersionedDecider;
pub use engine_state::engine_state_key;
// The policy contract lives in the `api` crate (bus-agnostic); re-exported here so
// existing `logact_commit_service_engine::` paths keep resolving.
pub use logact_commit_service_api::Policy;
pub use logact_commit_service_api::PolicyProvider;
pub use logact_commit_service_api::PolicyState;
pub use logact_commit_service_api::VersionedPolicy;
pub use logact_commit_service_api::VersionedPolicyState;
pub use logact_commit_service_static_config::StaticConfigPolicyProvider;
pub use observable_applicator::Observability;
pub use observable_applicator::ObservableApplicator;
pub use observable_applicator::ObservedDeciderFactory;
pub use observable_storage::ObservableStorage;
pub use policy::apply_policy_entry;
pub use policy::installed_policy;
pub use policy::plan as plan_policy_batch;
pub use policy::policy_batch_payload;
pub use policy::policy_constraint_failure;
pub use policy_decider_applicator::FirstBooleanWinsApplicator;
pub use policy_decider_applicator::OffByDefaultApplicator;
pub use policy_decider_applicator::OnByDefaultApplicator;
pub use retrying_storage::RetryingStorage;
pub use scoped_storage::ScopedStorage;
pub use state_machine_spec::StateMachineSpec;
pub use stateless_voter_adapter::ImmutableVoter;
pub use stateless_voter_adapter::StatelessVoterAdapter;
pub use storage::InMemoryStorage;
pub use storage::Storage;
pub use storage::StorageError;
pub use storage::StorageResult;
