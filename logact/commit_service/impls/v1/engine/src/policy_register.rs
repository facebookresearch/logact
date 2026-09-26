/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Durable desired policy stored as one versioned compare-and-swap register.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use bytes::Bytes;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use prost::Message;
use thiserror::Error;

use crate::Storage;
use crate::StorageError;

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

/// A structurally validated, versioned policy register backed by [`Storage`].
pub struct PolicyRegister<S> {
    storage: Rc<S>,
    key: String,
}

impl<S> Clone for PolicyRegister<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            key: self.key.clone(),
        }
    }
}

impl<S: Storage> PolicyRegister<S> {
    /// Create a policy register over `key` in `storage`.
    pub fn new(storage: Rc<S>, key: impl Into<String>) -> Self {
        Self {
            storage,
            key: key.into(),
        }
    }

    /// Read and validate the complete desired policy and its register generation.
    pub async fn read_validated(&self) -> PolicyRegisterResult<VersionedPolicyState> {
        let Some((value, version)) = self.storage.get(&self.key).await? else {
            return Err(PolicyRegisterError::Uninitialized {
                key: self.key.clone(),
            });
        };
        let state = decode_policy_state(&value)?;
        validate_policy_state(&state)?;
        Ok(VersionedPolicyState { state, version })
    }

    /// Conditionally replace the complete desired policy and return its new
    /// generation. `None` requires an absent register; `Some(version)` requires
    /// the register to be at that generation.
    pub async fn set_policy(
        &self,
        state: &PolicyState,
        expected_version: Option<i64>,
    ) -> PolicyRegisterResult<i64> {
        validate_policy_state(state)?;
        let new_version = match expected_version {
            Some(version) => {
                version
                    .checked_add(1)
                    .ok_or(PolicyRegisterError::GenerationOverflow {
                        generation: version,
                    })?
            }
            None => 0,
        };
        let updated = self
            .storage
            .put(
                &self.key,
                Bytes::from(state.encode_to_vec()),
                expected_version,
                new_version,
            )
            .await?;
        if !updated {
            return Err(PolicyRegisterError::ConcurrentUpdate {
                key: self.key.clone(),
            });
        }
        Ok(new_version)
    }
}

/// Install `state` if `register` is absent, retrying storage transaction
/// conflicts according to `retry_config`. Returns whether this caller installed
/// the initial policy.
pub async fn bootstrap_policy_register_if_absent<S, E>(
    register: &PolicyRegister<S>,
    state: &PolicyState,
    environment: &E,
    retry_config: RetryConfig,
) -> PolicyRegisterResult<bool>
where
    S: Storage,
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

fn decode_policy_state(value: &Bytes) -> PolicyRegisterResult<PolicyState> {
    PolicyState::decode(value.as_ref()).map_err(|error| PolicyRegisterError::MalformedPolicy {
        message: format!("failed to decode policy: {error}"),
    })
}

fn validate_policy_state(state: &PolicyState) -> PolicyRegisterResult<()> {
    let raw = state
        .decider_policy
        .ok_or_else(|| PolicyRegisterError::MalformedPolicy {
            message: "missing decider policy".to_string(),
        })?;
    DeciderPolicy::try_from(raw).map_err(|_| PolicyRegisterError::MalformedPolicy {
        message: format!("unrecognized decider policy value {raw}"),
    })?;

    for (voter_id, config) in &state.voters {
        if config.config.is_none() {
            return Err(PolicyRegisterError::MalformedPolicy {
                message: format!("voter '{voter_id}' has no configuration"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::VoterConfig;
    use agent_bus_proto_rust::agent_bus::voter_config;
    use agentbus_simulator::Simulator;
    use futures::executor::block_on;
    use prost_types::Any;

    use super::*;
    use crate::InMemoryStorage;
    use crate::StorageResult;

    fn register() -> (Rc<InMemoryStorage>, PolicyRegister<InMemoryStorage>) {
        let storage = Rc::new(InMemoryStorage::new());
        let register = PolicyRegister::new(storage.clone(), "test-policy");
        (storage, register)
    }

    fn policy(decider_policy: DeciderPolicy) -> PolicyState {
        PolicyState {
            decider_policy: Some(decider_policy as i32),
            ..Default::default()
        }
    }

    fn retry_config(max_retries: usize) -> RetryConfig {
        RetryConfig::try_new(
            max_retries,
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .expect("test retry configuration should be valid")
    }

    fn bootstrap<S: Storage + 'static>(
        register: PolicyRegister<S>,
        state: PolicyState,
        max_retries: usize,
    ) -> PolicyRegisterResult<bool> {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            bootstrap_policy_register_if_absent(
                &register,
                &state,
                task_environment.as_ref(),
                retry_config(max_retries),
            )
            .await
        });
        environment.run();
        block_on(handle).expect("bootstrap task should complete")
    }

    #[test]
    fn uninitialized_register_is_an_error() {
        let (_, register) = register();

        let error =
            block_on(register.read_validated()).expect_err("an absent policy register should fail");
        assert!(matches!(error, PolicyRegisterError::Uninitialized { .. }));
    }

    #[test]
    fn bootstrap_initializes_an_absent_register() {
        let (_, register) = register();

        assert!(
            bootstrap(register.clone(), policy(DeciderPolicy::OnByDefault), 1)
                .expect("bootstrap policy should be installed")
        );

        let stored = block_on(register.read_validated()).expect("policy should be readable");
        assert_eq!(
            stored.state.decider_policy,
            Some(DeciderPolicy::OnByDefault as i32)
        );
        assert_eq!(stored.version, 0);
    }

    #[test]
    fn bootstrap_preserves_an_existing_policy() {
        let (_, register) = register();
        block_on(register.set_policy(&policy(DeciderPolicy::OnByDefault), None))
            .expect("initial policy should be installed");

        assert!(
            !bootstrap(register.clone(), policy(DeciderPolicy::OffByDefault), 1)
                .expect("existing policy should be preserved")
        );

        let stored = block_on(register.read_validated()).expect("policy should be readable");
        assert_eq!(
            stored.state.decider_policy,
            Some(DeciderPolicy::OnByDefault as i32)
        );
        assert_eq!(stored.version, 0);
    }

    #[test]
    fn set_policy_advances_the_generation() {
        let (_, register) = register();
        assert_eq!(
            block_on(register.set_policy(&policy(DeciderPolicy::OnByDefault), None))
                .expect("initial policy should be installed"),
            0
        );
        let mut current =
            block_on(register.read_validated()).expect("initial policy should be readable");
        current.state.decider_policy = Some(DeciderPolicy::OffByDefault as i32);
        assert_eq!(
            block_on(register.set_policy(&current.state, Some(current.version)))
                .expect("decider update should succeed"),
            1
        );
    }

    struct ContendedStorage {
        inner: InMemoryStorage,
        put_calls: Cell<usize>,
        conflicts_remaining: Cell<usize>,
        reject_put: bool,
    }

    #[async_trait::async_trait(?Send)]
    impl Storage for ContendedStorage {
        async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
            self.inner.get(key).await
        }

        async fn put(
            &self,
            key: &str,
            value: Bytes,
            expected: Option<i64>,
            new_position: i64,
        ) -> StorageResult<bool> {
            self.put_calls.set(self.put_calls.get() + 1);
            let conflicts_remaining = self.conflicts_remaining.get();
            if conflicts_remaining > 0 {
                self.conflicts_remaining.set(conflicts_remaining - 1);
                return Err(StorageError::TransactionConflict(anyhow::anyhow!(
                    "test transaction conflict"
                )));
            }
            if self.reject_put {
                return Ok(false);
            }
            self.inner.put(key, value, expected, new_position).await
        }
    }

    fn contended_register(
        conflicts: usize,
        reject_put: bool,
    ) -> (Rc<ContendedStorage>, PolicyRegister<ContendedStorage>) {
        let storage = Rc::new(ContendedStorage {
            inner: InMemoryStorage::new(),
            put_calls: Cell::new(0),
            conflicts_remaining: Cell::new(conflicts),
            reject_put,
        });
        let register = PolicyRegister::new(storage.clone(), "test-policy");
        (storage, register)
    }

    #[test]
    fn condition_failure_is_a_concurrent_update() {
        let (storage, register) = contended_register(0, true);

        let error = block_on(register.set_policy(&policy(DeciderPolicy::OffByDefault), Some(0)))
            .expect_err("a concurrent update should be returned to the caller");

        assert!(matches!(
            error,
            PolicyRegisterError::ConcurrentUpdate { key } if key == "test-policy"
        ));
        assert_eq!(storage.put_calls.get(), 1);
    }

    #[test]
    fn transaction_conflict_remains_a_storage_error() {
        let (storage, register) = contended_register(1, false);

        let error = block_on(register.set_policy(&policy(DeciderPolicy::OffByDefault), Some(0)))
            .expect_err("transaction conflict should be returned to the caller");

        assert!(matches!(
            error,
            PolicyRegisterError::Storage(StorageError::TransactionConflict(_))
        ));
        assert_eq!(storage.put_calls.get(), 1);
    }

    #[test]
    fn bootstrap_retries_a_transaction_conflict() {
        let (storage, register) = contended_register(1, false);

        assert!(
            bootstrap(register.clone(), policy(DeciderPolicy::OnByDefault), 1)
                .expect("bootstrap should retry the transaction conflict")
        );

        assert_eq!(storage.put_calls.get(), 2);
        assert_eq!(
            block_on(register.read_validated())
                .expect("bootstrap policy should be readable")
                .version,
            0
        );
    }

    #[test]
    fn bootstrap_returns_an_exhausted_transaction_conflict() {
        let (storage, register) = contended_register(2, false);

        let error = bootstrap(register, policy(DeciderPolicy::OnByDefault), 1)
            .expect_err("bootstrap should return its final transaction conflict");

        assert!(matches!(
            error,
            PolicyRegisterError::Storage(StorageError::TransactionConflict(_))
        ));
        assert_eq!(storage.put_calls.get(), 2);
    }

    #[test]
    fn write_rejects_structurally_invalid_policy() {
        let (storage, register) = register();
        let error = block_on(register.set_policy(&PolicyState::default(), None))
            .expect_err("structural validation should reject the write");
        assert!(matches!(
            error,
            PolicyRegisterError::MalformedPolicy { message }
                if message.contains("missing decider policy")
        ));
        assert_eq!(
            block_on(storage.get("test-policy")).expect("storage read should succeed"),
            None
        );
    }

    #[test]
    fn decider_update_preserves_voters() {
        let (_, register) = register();
        let mut state = policy(DeciderPolicy::OnByDefault);
        state.voters.insert(
            "voter-a".to_string(),
            VoterConfig {
                config: Some(voter_config::Config::Custom(Any {
                    type_url: "test/voter".to_string(),
                    value: Vec::new(),
                })),
            },
        );
        block_on(register.set_policy(&state, None)).expect("initial policy should be installed");

        let mut current =
            block_on(register.read_validated()).expect("initial policy should be readable");
        current.state.decider_policy = Some(DeciderPolicy::OffByDefault as i32);
        block_on(register.set_policy(&current.state, Some(current.version)))
            .expect("decider update should succeed");
        let updated =
            block_on(register.read_validated()).expect("updated policy should be readable");
        assert!(updated.state.voters.contains_key("voter-a"));
    }

    #[test]
    fn read_rejects_structurally_invalid_policy() {
        let (storage, register) = register();
        block_on(storage.put(
            "test-policy",
            Bytes::from(PolicyState::default().encode_to_vec()),
            None,
            0,
        ))
        .expect("storage write should succeed");

        let error = block_on(register.read_validated())
            .expect_err("missing decider policy should fail validation");
        assert!(matches!(
            error,
            PolicyRegisterError::MalformedPolicy { message }
                if message.contains("missing decider policy")
        ));
    }

    #[test]
    fn read_rejects_invalid_encoding_as_malformed_policy() {
        let (storage, register) = register();
        block_on(storage.put("test-policy", Bytes::from_static(&[0xff]), None, 0))
            .expect("storage write should succeed");

        let error = block_on(register.read_validated())
            .expect_err("invalid policy encoding should fail validation");
        assert!(matches!(
            error,
            PolicyRegisterError::MalformedPolicy { message }
                if message.contains("failed to decode policy")
        ));
    }

    #[test]
    fn read_rejects_unrecognized_decider_policy() {
        let (storage, register) = register();
        let invalid = PolicyState {
            decider_policy: Some(99),
            ..Default::default()
        };
        block_on(storage.put("test-policy", Bytes::from(invalid.encode_to_vec()), None, 0))
            .expect("storage write should succeed");

        let error = block_on(register.read_validated())
            .expect_err("an unrecognized decider policy should fail validation");
        assert!(matches!(
            error,
            PolicyRegisterError::MalformedPolicy { message }
                if message.contains("unrecognized decider policy value 99")
        ));
    }

    #[test]
    fn read_rejects_voter_without_configuration() {
        let (storage, register) = register();
        let mut invalid = policy(DeciderPolicy::OnByDefault);
        invalid
            .voters
            .insert("voter-a".to_string(), VoterConfig::default());
        block_on(storage.put("test-policy", Bytes::from(invalid.encode_to_vec()), None, 0))
            .expect("storage write should succeed");

        let error = block_on(register.read_validated())
            .expect_err("a voter without configuration should fail validation");
        assert!(matches!(
            error,
            PolicyRegisterError::MalformedPolicy { message }
                if message.contains("voter 'voter-a' has no configuration")
        ));
    }
}
