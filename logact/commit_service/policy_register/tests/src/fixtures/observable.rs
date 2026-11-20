/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_api::InMemoryLogger;
use agentbus_api::InMemoryMetrics;
use agentbus_simulator::Simulator;
use logact_commit_service_engine::Observability;
use logact_commit_service_policy_register::ObservablePolicyRegister;
use logact_commit_service_policy_register::PolicyRegister;

use crate::fixtures::ConformanceFixture;
use crate::fixtures::SimulatorFixture;

/// Wraps every register handle with observability backed by shared in-memory sinks.
pub struct ObservablePolicyRegisterFixture<F> {
    inner: F,
    metrics: Rc<InMemoryMetrics>,
    logger: InMemoryLogger,
}

impl<F> ConformanceFixture for ObservablePolicyRegisterFixture<F>
where
    F: ConformanceFixture<Impl: PolicyRegister>,
{
    type Env = F::Env;
    type Impl = ObservablePolicyRegister<F::Impl, InMemoryLogger, F::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        ObservablePolicyRegister::new(
            self.inner.create_impl(),
            Observability {
                metrics: self.metrics.clone(),
                logger: Rc::new(self.logger.clone()),
                environment: self.inner.get_env(),
            },
        )
    }
}

impl<F: SimulatorFixture> SimulatorFixture for ObservablePolicyRegisterFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
            metrics: Rc::new(InMemoryMetrics::new()),
            logger: InMemoryLogger::new(),
        }
    }
}
