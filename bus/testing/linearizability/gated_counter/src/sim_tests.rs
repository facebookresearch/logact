/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_tests::fixtures::simtest::FaultInjectingFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use gated_counter_suite::gated_counter_sim_suite;
use logact_commit_service_tests::fixtures::ChanneledCommitServiceFixture;
use logact_commit_service_tests::fixtures::CommitServiceV1Fixture;
use logact_commit_service_tests::fixtures::CommitServiceV1LatencyFixture;
use logact_commit_service_tests::fixtures::InMemCommitServiceFixture;
use logact_commit_service_tests::fixtures::RandomRoutingCommitServiceFixture;

gated_counter_sim_suite!();
