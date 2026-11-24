/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use logact_commit_service_policy_register::KeepalivePolicyRegister;
use logact_commit_service_policy_register::PolicyRegister;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::LeasedFixture;
use crate::fixtures::SimulatorFixture;
use crate::fixtures::lease::CONFIG;

/// Each handle polls a handle of the inner fixture's register ten times per
/// lease of the lease fixture's `CONFIG`, for ten leases.
pub struct KeepalivePolicyRegisterFixture<F> {
    inner: F,
}

impl<F> ConformanceFixture for KeepalivePolicyRegisterFixture<F>
where
    F: ConformanceFixture<Impl: PolicyRegister + 'static, Env: Environment + 'static>,
{
    type Env = F::Env;
    type Impl = KeepalivePolicyRegister<F::Impl, F::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        KeepalivePolicyRegister::with_timing(
            self.inner.create_impl(),
            self.inner.get_env(),
            CONFIG.lease_duration / 10,
            CONFIG.lease_duration * 10,
        )
    }
}

impl<F> LeasedFixture for KeepalivePolicyRegisterFixture<F>
where
    F: LeasedFixture<Impl: PolicyRegister + 'static, Env: Environment + 'static>,
{
    type Inner = F::Inner;

    fn create_inner(&self) -> Self::Inner {
        self.inner.create_inner()
    }
}

impl<F: SimulatorFixture> SimulatorFixture for KeepalivePolicyRegisterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}
