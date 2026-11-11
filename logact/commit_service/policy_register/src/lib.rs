/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The policy register API: a versioned, structurally validated desired-policy
//! register, with a [`logact_commit_service_engine::Storage`]-backed implementation.

use std::time::Duration;

use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use logact_commit_service_api::InvalidPolicyState;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use logact_commit_service_engine::StorageError;
use thiserror::Error;

pub mod storage;
pub mod synchronous_provider;

pub use storage::StoragePolicyRegister;
pub use synchronous_provider::SynchronousRegisterProvider;
pub use synchronous_provider::SynchronousRegisterProviderError;

/// Failures while reading, validating, or mutating a policy register.
#[derive(Debug, Error)]
pub enum PolicyRegisterError {
    #[error("policy register '{key}' is not initialized")]
    Uninitialized { key: String },
    #[error("policy register storage operation failed")]
    Storage(#[from] StorageError),
    #[error("policy register contains malformed policy: {message}")]
    MalformedPolicy { message: String },
    #[error("policy register generation {generation} cannot be incremented")]
    GenerationOverflow { generation: i64 },
    #[error("policy register '{key}' changed concurrently")]
    ConcurrentUpdate { key: String },
}

impl From<InvalidPolicyState> for PolicyRegisterError {
    fn from(error: InvalidPolicyState) -> Self {
        PolicyRegisterError::MalformedPolicy { message: error.0 }
    }
}

/// Result returned by [`PolicyRegister`] operations.
pub type PolicyRegisterResult<T> = std::result::Result<T, PolicyRegisterError>;

const DEFAULT_POLICY_REGISTER_BOOTSTRAP_MAX_RETRIES: usize = 7;

/// Return the default retry policy for initializing a policy register.
pub fn default_policy_register_bootstrap_retry_config() -> RetryConfig {
    RetryConfig::try_new(
        DEFAULT_POLICY_REGISTER_BOOTSTRAP_MAX_RETRIES,
        Duration::from_millis(10),
        Duration::from_millis(100),
    )
    .expect("default policy register bootstrap retry configuration should be valid")
}

/// A structurally validated, versioned desired-policy register.
pub trait PolicyRegister {
    /// The key that identifies this register in errors.
    fn key(&self) -> &str;

    /// Read and validate the complete desired policy and its version. Versions
    /// may advance without a change to the policy values.
    fn read_validated(
        &self,
    ) -> impl std::future::Future<Output = PolicyRegisterResult<VersionedPolicyState>>;

    /// Conditionally replace the complete desired policy and return its new
    /// version. `None` requires an absent register; `Some(version)` requires
    /// the register to be at that version.
    fn set_policy(
        &self,
        state: &PolicyState,
        expected_version: Option<i64>,
    ) -> impl std::future::Future<Output = PolicyRegisterResult<i64>>;
}

/// Install `state` if `register` is absent, retrying storage transaction
/// conflicts according to `retry_config`. Returns whether this caller installed
/// the initial policy.
pub async fn bootstrap_policy_register_if_absent<R, E>(
    register: &R,
    state: &PolicyState,
    environment: &E,
    retry_config: RetryConfig,
) -> PolicyRegisterResult<bool>
where
    R: PolicyRegister,
    E: Environment,
{
    retry(
        environment,
        retry_config,
        || async {
            match register.set_policy(state, None).await {
                Ok(_) => Ok(true),
                Err(PolicyRegisterError::ConcurrentUpdate { .. }) => Ok(false),
                Err(error) => Err(error),
            }
        },
        |error| match error {
            PolicyRegisterError::Storage(StorageError::TransactionConflict(_)) => {
                RetryDecision::RetryWithBackoff
            }
            _ => RetryDecision::Stop,
        },
    )
    .await
    .map_err(RetryFailure::into_last_error)
}
