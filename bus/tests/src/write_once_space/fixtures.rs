/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common test fixtures for WriteOnceSpace implementations

use std::rc::Rc;

use agentbus_api::RetryConfig;
use agentbus_api::WriteOnceSpace;
use agentbus_conditional_write_space::WriteOnceSpaceAdapter;
use agentbus_writeonce::RetryingWriteOnceSpace;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;
use fbinit::FacebookInit;

use crate::conditional_write_space::fixtures::ConditionalWriteSpaceTestFixture;
use crate::fixtures::IntegrationFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;

/// The `WriteOnceSpace`-specific view of a fixture: a `ConformanceFixture` whose
/// `Impl` is a `WriteOnceSpace`. Backends implement `ConformanceFixture`; the
/// blanket impl gives them this trait for free.
pub trait WriteOnceSpaceTestFixture: ConformanceFixture<Impl: WriteOnceSpace + 'static> {}
impl<F: ConformanceFixture<Impl: WriteOnceSpace + 'static>> WriteOnceSpaceTestFixture for F {}

/// Generic fixture that adapts any ConditionalWriteSpaceTestFixture into a
/// WriteOnceSpaceTestFixture via WriteOnceSpaceAdapter, the bottom-most
/// write-once space, so it applies the scenario's `retry`.
pub struct WriteOnceAdapterFixture<F> {
    inner: F,
    retry: RetryConfig,
}

impl<F> ConformanceFixture for WriteOnceAdapterFixture<F>
where
    F: ConditionalWriteSpaceTestFixture,
{
    type Env = F::Env;
    type Impl = RetryingWriteOnceSpace<WriteOnceSpaceAdapter<F::Impl>, F::Env>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        RetryingWriteOnceSpace::new(
            WriteOnceSpaceAdapter::new(self.inner.create_impl()),
            self.inner.get_env(),
            self.retry,
        )
    }
}

impl<F> SimulatorFixture for WriteOnceAdapterFixture<F>
where
    F: ConditionalWriteSpaceTestFixture + SimulatorFixture,
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
            retry: TestConfig::default().retry_config(),
        }
    }

    /// The backing conditional-write space applies the rest of the config.
    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            inner: F::try_new_with_config(simulator, config)?,
            retry: config.retry_config(),
        })
    }
}

impl<F> IntegrationFixture for WriteOnceAdapterFixture<F>
where
    F: ConditionalWriteSpaceTestFixture + IntegrationFixture,
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
            retry: TestConfig::default().retry_config(),
        })
    }
}

pub mod simtest;
