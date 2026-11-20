/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test fixtures for eligibility `RequestGate` implementations

use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::CachingEligibilityGate;
use agentbus_api::EligibilityGate;
use agentbus_api::RequestGate;
use agentbus_api::RetryConfig;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;

use crate::eligibility::EXEMPT_SERVICE;
use crate::eligibility::InMemoryEligibilityLookup;
use crate::eligibility::InMemoryRequestGate;
use crate::fixtures::SimulatorFixture;
use crate::impls::latency_wrapper::LatencyWrapper;
use crate::impls::retrying_request_gate::RetryingRequestGate;
use crate::simulator::Simulator;

/// The eligibility-gate view of a fixture: a `ConformanceFixture` whose `Impl`
/// is a `RequestGate` over an eligibility table the scenario controls.
pub trait EligibilityGateTestFixture:
    ConformanceFixture<Env = Simulator, Impl: RequestGate + 'static>
{
    /// The eligibility table the gate's lookups read.
    fn eligibility(&self) -> InMemoryEligibilityLookup;

    /// How long the gate may keep answering from a decision after it changes.
    fn cache_ttl(&self) -> Duration {
        Duration::ZERO
    }
}

/// A gate behind the scenario's injected latency and, with `retry`, retries.
type Backend<G> = RetryingRequestGate<LatencyWrapper<G>, Simulator>;

fn backend<G>(gate: G, env: &Rc<Simulator>, retry: RetryConfig) -> Backend<G> {
    RetryingRequestGate::new(LatencyWrapper::new(gate, env.clone()), env.clone(), retry)
}

fn retry_config(config: &TestConfig) -> Result<RetryConfig> {
    anyhow::ensure!(config.ttl.is_none(), "eligibility decisions do not expire");
    Ok(config.retry_config())
}

/// Fixture for the reference `InMemoryRequestGate`.
pub struct InMemoryRequestGateFixture {
    eligibility: InMemoryEligibilityLookup,
    env: Rc<Simulator>,
    retry: RetryConfig,
}

impl ConformanceFixture for InMemoryRequestGateFixture {
    type Env = Simulator;
    type Impl = Backend<InMemoryRequestGate>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        let gate = InMemoryRequestGate::new(self.eligibility.clone(), [EXEMPT_SERVICE.to_owned()]);
        backend(gate, &self.env, self.retry)
    }
}

impl EligibilityGateTestFixture for InMemoryRequestGateFixture {
    fn eligibility(&self) -> InMemoryEligibilityLookup {
        self.eligibility.clone()
    }
}

impl SimulatorFixture for InMemoryRequestGateFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            eligibility: InMemoryEligibilityLookup::default(),
            env: Rc::new(simulator),
            retry: retry_config(config)?,
        })
    }
}

/// Fixture for `EligibilityGate` looking up the table directly.
pub struct EligibilityGateFixture {
    eligibility: InMemoryEligibilityLookup,
    env: Rc<Simulator>,
    retry: RetryConfig,
}

impl EligibilityGateFixture {
    fn gate(&self) -> EligibilityGate<Simulator> {
        let eligibility = self.eligibility.clone();
        EligibilityGate::new(
            move |unixname| {
                let eligibility = eligibility.clone();
                async move { eligibility.check(&unixname).await }
            },
            self.env.clone(),
        )
        .with_exempt_services([EXEMPT_SERVICE.to_owned()])
    }
}

impl ConformanceFixture for EligibilityGateFixture {
    type Env = Simulator;
    type Impl = Backend<EligibilityGate<Simulator>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        backend(self.gate(), &self.env, self.retry)
    }
}

impl EligibilityGateTestFixture for EligibilityGateFixture {
    fn eligibility(&self) -> InMemoryEligibilityLookup {
        self.eligibility.clone()
    }
}

impl SimulatorFixture for EligibilityGateFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            eligibility: InMemoryEligibilityLookup::default(),
            env: Rc::new(simulator),
            retry: retry_config(config)?,
        })
    }
}

/// Fixture for a `CachingEligibilityGate` over `EligibilityGateFixture`'s gate.
/// Each `create_impl` is a new cache, as each process has its own cache.
pub struct CachingEligibilityGateFixture {
    inner: EligibilityGateFixture,
}

impl CachingEligibilityGateFixture {
    const CACHE_TTL: Duration = Duration::from_secs(1);
}

impl ConformanceFixture for CachingEligibilityGateFixture {
    type Env = Simulator;
    type Impl = Backend<CachingEligibilityGate<Simulator>>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let gate = CachingEligibilityGate::new(
            self.inner.gate(),
            NonZeroUsize::new(1024).expect("capacity should be nonzero"),
            Self::CACHE_TTL,
        );
        backend(gate, &self.inner.env, self.inner.retry)
    }
}

impl EligibilityGateTestFixture for CachingEligibilityGateFixture {
    fn eligibility(&self) -> InMemoryEligibilityLookup {
        self.inner.eligibility()
    }

    fn cache_ttl(&self) -> Duration {
        Self::CACHE_TTL
    }
}

impl SimulatorFixture for CachingEligibilityGateFixture {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: EligibilityGateFixture::new(simulator),
        }
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        Ok(Self {
            inner: EligibilityGateFixture::try_new_with_config(simulator, config)?,
        })
    }
}
