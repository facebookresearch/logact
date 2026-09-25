/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! V1 implementation of the LogAct commit service.

pub mod commit_service_v1;
pub mod voter_factory;

pub use commit_service_v1::CommitServiceV1;
pub use logact_commit_service_engine::StaticConfigPolicyProvider;
pub use logact_commit_service_engine::SynchronousRegisterProvider;
pub use voter_factory::DelegatingVoterFactory;
pub use voter_factory::LlmVoterFactory;
pub use voter_factory::RuleBasedVoterFactory;
