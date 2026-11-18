/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Lease-backed policies for the existing commit-service fixture matrix.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use logact_commit_service_api::PolicyState;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_policy_register::LeaseBasedPolicyRegister;
use logact_commit_service_policy_register::PolicyLeaseConfig;
use logact_commit_service_policy_register::PolicyRegister;
use logact_commit_service_policy_register::PolicyRegisterError;
use logact_commit_service_policy_register::RegisterPolicyProvider;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_v1::DelegatingVoterFactory;

use crate::fixtures::SettableDeciderPolicy;
use crate::fixtures::TestPolicyProvider;

type TestLeaseRegister =
    LeaseBasedPolicyRegister<StoragePolicyRegister<InMemoryStorage>, Simulator>;

/// A provider over a lease-based policy register, sharing its lease cache with
/// the controller.
pub type LeaseBasedTestPolicyProvider = RegisterPolicyProvider<TestLeaseRegister>;

impl TestPolicyProvider for LeaseBasedTestPolicyProvider {
    type Controller = TestLeaseRegister;

    fn for_test(environment: Rc<Simulator>) -> (Self, Self::Controller) {
        let config = PolicyLeaseConfig {
            lease_duration: Duration::from_millis(10),
            drift_allowance: Duration::from_millis(2),
            recovery_grace: Duration::from_millis(2),
        };
        let voter_factory = DelegatingVoterFactory::default();
        let register = LeaseBasedPolicyRegister::new(
            StoragePolicyRegister::new(Rc::new(InMemoryStorage::new()), "test-lease-policy"),
            environment,
            config,
        );
        futures::executor::block_on(register.set_policy(
            &PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
            None,
        ))
        .expect("test lease register should initialize");
        (
            RegisterPolicyProvider::new(register.clone(), move |policy| {
                validate_voter_configs(&voter_factory, policy)
            }),
            register,
        )
    }
}

impl<R: PolicyRegister, E: Environment> SettableDeciderPolicy for LeaseBasedPolicyRegister<R, E> {
    async fn set_decider_policy(&self, policy: DeciderPolicy) -> anyhow::Result<i64> {
        loop {
            let mut current = self.read_validated().await?;
            current.state.decider_policy = Some(policy as i32);
            match self.set_policy(&current.state, Some(current.version)).await {
                Err(PolicyRegisterError::ConcurrentUpdate { .. }) => continue,
                result => return Ok(result?),
            }
        }
    }

    async fn read_decider_policy(&self) -> anyhow::Result<(DeciderPolicy, i64)> {
        let current = self.read_validated().await?;
        let raw = current
            .state
            .decider_policy
            .expect("lease register validation requires a decider policy");
        Ok((DeciderPolicy::try_from(raw)?, current.version))
    }
}
