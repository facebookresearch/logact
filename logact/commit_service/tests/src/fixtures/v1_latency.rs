/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A deterministic simulated-latency fixture for `CommitServiceV1`.

use std::rc::Rc;
use std::time::Duration;

use agentbus_tests::common::fault_config::FaultConfig;
use agentbus_tests::fixtures::SimulatorFixture;
use agentbus_tests::fixtures::simtest::FaultInjectingFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use agentbus_tests::simulator::Simulator;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::TestConfig;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine_tests::fixtures::FaultInjectingStorageFixture;
use logact_commit_service_engine_tests::fixtures::InMemoryStorageFixture;
use logact_commit_service_engine_tests::fixtures::fault_injecting::StorageFaultConfig;
use logact_commit_service_engine_tests::fixtures::fault_injecting::StorageFaults;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::distr::Uniform;

use crate::fixtures::CommitServiceV1Fixture;

const OPERATION_LATENCY_MS: u64 = 50;

/// `CommitServiceV1` with deterministic latency and no injected failures.
pub type CommitServiceV1LatencyFixture = CommitServiceV1Fixture<
    FixedLatencyBusFixture,
    SynchronousRegisterProvider<InMemoryStorage>,
    FaultInjectingStorageFixture<InMemoryStorageFixture, FixedStorageLatency>,
>;

/// The simple in-memory bus with every operation taking `OPERATION_LATENCY_MS`.
pub struct FixedLatencyBusFixture(FaultInjectingFixture<SimpleMemoryFixture>);

impl ConformanceFixture for FixedLatencyBusFixture {
    type Env = Simulator;
    type Impl = <FaultInjectingFixture<SimpleMemoryFixture> as ConformanceFixture>::Impl;

    fn get_env(&self) -> Rc<Self::Env> {
        self.0.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.0.create_impl()
    }
}

impl SimulatorFixture for FixedLatencyBusFixture {
    fn new(simulator: Simulator) -> Self {
        Self::try_new_with_config(simulator, &TestConfig::default())
            .expect("the default config should be valid")
    }

    fn try_new_with_config(simulator: Simulator, config: &TestConfig) -> Result<Self> {
        let fixed_latency = Uniform::new(OPERATION_LATENCY_MS, OPERATION_LATENCY_MS + 1)
            .expect("fixed latency range should be valid");
        Ok(Self(FaultInjectingFixture::new_with_fixture_and_latency(
            SimpleMemoryFixture::try_new_with_config(simulator, config)?,
            FaultConfig {
                prob_lost: 0.0,
                prob_commit_then_error: 0.0,
                prob_error_then_commit: 0.0,
            },
            fixed_latency,
            fixed_latency,
        )))
    }
}

/// Every storage operation takes `OPERATION_LATENCY_MS`.
pub struct FixedStorageLatency;

impl StorageFaults for FixedStorageLatency {
    fn config() -> StorageFaultConfig {
        StorageFaultConfig {
            get_delay: Duration::from_millis(OPERATION_LATENCY_MS),
            put_delay: Duration::from_millis(OPERATION_LATENCY_MS),
            ..Default::default()
        }
    }
}
