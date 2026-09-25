/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A deterministic simulated-latency fixture for `CommitServiceV1`.

use std::time::Duration;

use agentbus_tests::common::fault_config::FaultConfig;
use agentbus_tests::fixtures::SimulatorFixture;
use agentbus_tests::fixtures::simtest::FaultInjectingFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use agentbus_tests::simulator::Simulator;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine_tests::fixtures::fault_injecting::FaultInjectingStorage;
use logact_commit_service_engine_tests::fixtures::fault_injecting::StorageFaultConfig;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::distr::Uniform;

use crate::fixtures::CommitServiceV1Fixture;

const OPERATION_LATENCY_MS: u64 = 50;

/// `CommitServiceV1` with deterministic latency and no injected failures.
pub type CommitServiceV1LatencyFixture = CommitServiceV1Fixture<
    FaultInjectingFixture<SimpleMemoryFixture>,
    SynchronousRegisterProvider<InMemoryStorage>,
    FaultInjectingStorage<InMemoryStorage, Simulator>,
>;

impl SimulatorFixture for CommitServiceV1LatencyFixture {
    fn new(simulator: Simulator) -> Self {
        let fixed_latency = Uniform::new(OPERATION_LATENCY_MS, OPERATION_LATENCY_MS + 1)
            .expect("fixed latency range should be valid");
        let bus_fixture = FaultInjectingFixture::new_with_config_and_latency(
            simulator,
            FaultConfig {
                prob_lost: 0.0,
                prob_commit_then_error: 0.0,
                prob_error_then_commit: 0.0,
            },
            fixed_latency,
            fixed_latency,
        );
        CommitServiceV1Fixture::new_with_bus_fixture_and_storage(bus_fixture, |environment| {
            FaultInjectingStorage::new(
                InMemoryStorage::new(),
                environment,
                StorageFaultConfig {
                    get_delay: Duration::from_millis(OPERATION_LATENCY_MS),
                    put_delay: Duration::from_millis(OPERATION_LATENCY_MS),
                    ..Default::default()
                },
            )
        })
    }
}
