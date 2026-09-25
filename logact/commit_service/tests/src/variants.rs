/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The concrete `CommitSvc` implementations the conformance suite runs against.
//!
//! V1 is run over the full AgentBus backend matrix — reusing agentbus's
//! `agentbus_fixtures!` registry (with a forwarded callback) so the matrix stays in
//! one place — plus a channeled
//! stack over the simple in-memory bus and the random-routing fixtures. The
//! standalone `InMemCommitService` runs as a single fixture. The local gRPC and
//! SQLite stack runs in both legacy-ID and typed-ID modes in the integration suite.
//! `define_driver!` calls `commit_service_fixtures!` with a per-fixture callback;
//! `cs_bus_bridge` adapts each AgentBus fixture into the V1 commit-service fixture.

/// Registry of all `CommitSvc` fixtures under test. Invoked by the generated suite
/// entry with a per-fixture callback (`$cb`).
#[macro_export]
macro_rules! commit_service_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::fixtures::LegacyBusIdFixture<$crate::fixtures::GrpcCommitServiceFixture>,
            grpc_sqlite_legacy_bus_id,
            integration
        ]);
        $cb!([
            $crate::fixtures::TypedBusIdFixture<$crate::fixtures::GrpcCommitServiceFixture>,
            grpc_sqlite_typed_bus_id,
            integration
        ]);
        // V1 over every AgentBus backend; `$cb` is forwarded through the registry to
        // `cs_bus_bridge`, which emits the V1 fixture per sim bus.
        agentbus_tests::agentbus_fixtures!($crate::cs_bus_bridge, ($cb));
        // Deterministic latency-only V1 fixture. Registering it here runs every
        // generic commit-service scenario against the same fixture used by the
        // replay-latency scenario.
        $cb!([CommitServiceV1LatencyFixture, v1_latency, sim]);
        // Channeled wrapper over the simple in-memory bus.
        $cb!(
            [
                ChanneledCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                channeled_v1_simple_memory,
                sim
            ]
        );
        // Router over several independent v1 services (one shared bus + storage),
        // dispatching each request to a randomly-picked one.
        $cb!([RandomRoutingCommitServiceFixture<SimpleMemoryFixture>, random_routing, sim]);
        $cb!(
            [
                RandomRoutingCommitServiceFixture<FaultInjectingFixture<SimpleMemoryFixture>>,
                random_routing_fault,
                sim
            ]
        );
        // Standalone in-memory implementation with its own local bus.
        $cb!([InMemCommitServiceFixture, inmem, sim]);
    };
}

/// Bridge: turn one AgentBus *sim* fixture (with its suffix) into the V1
/// commit-service fixture, invoking the forwarded `$cb`. The AgentBus registry's
/// `integration` entry is skipped — commit_service bridges the simulator buses
/// only.
#[macro_export]
macro_rules! cs_bus_bridge {
    ([$bus:ty, $sfx:ident, sim], ($cb:path)) => {
        $cb!([CommitServiceV1Fixture<$bus>, $sfx, sim]);
    };
    ([$bus:ty, $sfx:ident, integration], ($cb:path)) => {};
}
