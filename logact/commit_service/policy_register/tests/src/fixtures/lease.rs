/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::time::Duration;

use agentbus_simulator::Simulator;
use anyhow::Result;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use logact_commit_service_policy_register::LeaseBasedPolicyRegister;
use logact_commit_service_policy_register::PolicyLeaseConfig;
use logact_commit_service_policy_register::PolicyRegister;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;

pub(crate) const CONFIG: PolicyLeaseConfig = PolicyLeaseConfig {
    lease_duration: Duration::from_millis(10),
    drift_allowance: Duration::from_millis(2),
    recovery_grace: Duration::from_millis(2),
};

/// A fixture whose register layers the lease protocol over an inner register.
pub trait LeasedFixture: ConformanceFixture {
    type Inner: PolicyRegister;

    /// A handle to the inner register that bypasses the lease protocol.
    fn create_inner(&self) -> Self::Inner;
}

/// Layers the lease protocol over the register of any other fixture. Each
/// handle has its own reader cache over the shared inner register.
pub struct LeaseBasedPolicyRegisterFixture<F> {
    inner: F,
}

impl<F> ConformanceFixture for LeaseBasedPolicyRegisterFixture<F>
where
    F: ConformanceFixture<Impl: PolicyRegister>,
{
    type Env = F::Env;
    type Impl = LeaseBasedPolicyRegister<F::Impl, F::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        LeaseBasedPolicyRegister::new(self.inner.create_impl(), self.inner.get_env(), CONFIG)
    }
}

impl<F> LeasedFixture for LeaseBasedPolicyRegisterFixture<F>
where
    F: ConformanceFixture<Impl: PolicyRegister>,
{
    type Inner = F::Impl;

    fn create_inner(&self) -> Self::Inner {
        self.inner.create_impl()
    }
}

impl<F: SimulatorFixture> SimulatorFixture for LeaseBasedPolicyRegisterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}

impl<F: IntegrationFixture> IntegrationFixture for LeaseBasedPolicyRegisterFixture<F> {
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
        })
    }
}
