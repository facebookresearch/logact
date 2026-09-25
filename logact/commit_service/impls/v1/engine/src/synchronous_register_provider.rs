/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A [`PolicyProvider`] that synchronously reads a [`PolicyRegister`].
//!
//! The provider delegates storage, versioning, serialization, and structural
//! validation to the register, then checks that this node can construct the
//! policy's runtime types.

use std::rc::Rc;

use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use thiserror::Error;

use crate::PolicyRegister;
use crate::PolicyRegisterError;
use crate::Storage;

/// Failures while reading a policy for use by this node.
#[derive(Debug, Error)]
pub enum SynchronousRegisterProviderError {
    #[error(transparent)]
    Register(#[from] PolicyRegisterError),
    #[error("policy is unsupported by this node: {message}")]
    UnsupportedPolicy { message: String },
}

/// A synchronous read-consistency mechanism over a policy register.
pub struct SynchronousRegisterProvider<S> {
    register: PolicyRegister<S>,
    validate_policy: Rc<dyn Fn(&PolicyState) -> anyhow::Result<()>>,
}

impl<S> Clone for SynchronousRegisterProvider<S> {
    fn clone(&self) -> Self {
        Self {
            register: self.register.clone(),
            validate_policy: self.validate_policy.clone(),
        }
    }
}

impl<S: Storage> SynchronousRegisterProvider<S> {
    /// Create a provider that reads `register` and validates node support on
    /// every policy lookup.
    pub fn new(
        register: PolicyRegister<S>,
        validate_policy: impl Fn(&PolicyState) -> anyhow::Result<()> + 'static,
    ) -> Self {
        Self {
            register,
            validate_policy: Rc::new(validate_policy),
        }
    }
}

impl<S: Storage> PolicyProvider for SynchronousRegisterProvider<S> {
    type Error = SynchronousRegisterProviderError;

    async fn read(&self, _bus_id: &str) -> Result<VersionedPolicyState, Self::Error> {
        let desired = self.register.read_validated().await?;
        (self.validate_policy)(&desired.state).map_err(|error| {
            SynchronousRegisterProviderError::UnsupportedPolicy {
                message: format!("{error:#}"),
            }
        })?;
        Ok(desired)
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use futures::executor::block_on;

    use super::*;
    use crate::InMemoryStorage;

    fn register_and_provider(
        key: &str,
    ) -> (
        PolicyRegister<InMemoryStorage>,
        SynchronousRegisterProvider<InMemoryStorage>,
    ) {
        let register = PolicyRegister::new(Rc::new(InMemoryStorage::new()), key);
        let provider = SynchronousRegisterProvider::new(register.clone(), |_| Ok(()));
        (register, provider)
    }

    fn read_for(
        provider: &SynchronousRegisterProvider<InMemoryStorage>,
        bus_id: &str,
    ) -> VersionedPolicyState {
        block_on(provider.read(bus_id)).expect("register read should succeed")
    }

    fn policy(decider_policy: DeciderPolicy) -> PolicyState {
        PolicyState {
            decider_policy: Some(decider_policy as i32),
            ..Default::default()
        }
    }

    #[test]
    fn unset_register_returns_an_error() {
        let (_, provider) = register_and_provider("test-policy");

        let error =
            block_on(provider.read("agent-1")).expect_err("an absent policy register should fail");
        assert!(matches!(
            error,
            SynchronousRegisterProviderError::Register(PolicyRegisterError::Uninitialized { .. })
        ));
    }

    #[test]
    fn installed_policy_applies_to_every_bus() {
        let (register, provider) = register_and_provider("test-policy");
        block_on(register.set_policy(&policy(DeciderPolicy::OffByDefault), None))
            .expect("policy update should succeed");

        let first = read_for(&provider, "agent-1");
        let second = read_for(&provider, "agent-2");
        assert_eq!(first, second);
        assert_eq!(
            first.state.decider_policy,
            Some(DeciderPolicy::OffByDefault as i32)
        );
        assert_eq!(first.version, 0);
    }

    #[test]
    fn register_updates_advance_the_provider_generation() {
        let (register, provider) = register_and_provider("test-policy");
        block_on(register.set_policy(&policy(DeciderPolicy::OnByDefault), None))
            .expect("initial policy update should succeed");
        let mut current = read_for(&provider, "agent-1");
        assert_eq!(current.version, 0);

        current.state.decider_policy = Some(DeciderPolicy::OffByDefault as i32);
        block_on(register.set_policy(&current.state, Some(current.version)))
            .expect("second policy update should succeed");
        assert_eq!(read_for(&provider, "agent-1").version, 1);
    }

    #[test]
    fn clones_share_one_register() {
        let (register, provider) = register_and_provider("test-policy");
        let handle = provider.clone();

        block_on(register.set_policy(&policy(DeciderPolicy::OffByDefault), None))
            .expect("policy update should succeed");
        assert_eq!(read_for(&provider, "agent-1"), read_for(&handle, "agent-1"));
    }

    #[test]
    fn distinct_registers_are_isolated_in_shared_storage() {
        let storage = Rc::new(InMemoryStorage::new());
        let first_register = PolicyRegister::new(storage.clone(), "first-policy");
        let second_register = PolicyRegister::new(storage, "second-policy");
        let first = SynchronousRegisterProvider::new(first_register.clone(), |_| Ok(()));
        let second = SynchronousRegisterProvider::new(second_register, |_| Ok(()));

        block_on(first_register.set_policy(&policy(DeciderPolicy::OffByDefault), None))
            .expect("policy update should succeed");

        assert_eq!(read_for(&first, "agent-1").version, 0);
        block_on(second.read("agent-1"))
            .expect_err("the second register should remain uninitialized");
    }

    #[test]
    fn rejects_policy_unsupported_by_this_node() {
        let (register, _) = register_and_provider("test-policy");
        block_on(register.set_policy(&policy(DeciderPolicy::OnByDefault), None))
            .expect("policy update should succeed");
        let provider =
            SynchronousRegisterProvider::new(register, |_| anyhow::bail!("unsupported voter type"));

        let error = block_on(provider.read("agent-1"))
            .expect_err("node capability validation should reject the policy");
        assert!(matches!(
            error,
            SynchronousRegisterProviderError::UnsupportedPolicy { .. }
        ));
    }
}
