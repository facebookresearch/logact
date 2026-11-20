/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The concrete `CommitSvc` implementations the conformance suite runs against.
//!
//! V1 runs over the full AgentBus backend matrix — reusing agentbus's
//! `agentbus_fixtures!` registry (with a forwarded callback) so the matrix stays in
//! one place — plus channeled and buffered stacks over the simple in-memory bus,
//! the random-routing fixtures, and the observability decorator over a single V1
//! service and over a router. The standalone `InMemCommitService` runs as a single
//! fixture. The local gRPC and SQLite stack runs in the integration suite.
//! `ReplayCommitService` runs over the AgentBus sim matrix once per checkpoint store.
//! `define_driver!` calls `commit_service_fixtures!` with a per-fixture callback;
//! `cs_bus_bridge` adapts each AgentBus fixture into the V1 commit-service fixture.

/// Registry of all `CommitSvc` fixtures under test. Invoked by the generated suite
/// entry with a per-fixture callback (`$cb`).
#[macro_export]
macro_rules! commit_service_fixtures {
    ($cb:path $(, $ctx:tt)?) => {
        $cb!([
            $crate::fixtures::GrpcCommitServiceFixture,
            grpc_sqlite,
            integration
        ] $(, $ctx)?);
        // V1 over every AgentBus backend; `$cb` is forwarded through the registry to
        // `cs_bus_bridge`, which emits the V1 fixture per sim bus.
        agentbus_tests::agentbus_fixtures!($crate::cs_bus_bridge, ($cb $(, $ctx)?));
        agentbus_tests::agentbus_fixtures!($crate::cs_lease_bus_bridge, ($cb $(, $ctx)?));
        // Deterministic latency-only V1 fixture. Registering it here runs every
        // generic commit-service scenario against the same fixture used by the
        // replay-latency scenario.
        $cb!([CommitServiceV1LatencyFixture, v1_latency, sim] $(, $ctx)?);
        // V1 deployed as production deploys it over ZippyDB, with in-memory
        // stand-ins: requests routed across replicas, each with its own channeled
        // write-once AgentBus over a caching adapter over one shared
        // conditional-write space, and shared scoped engine storage.
        $cb!([$crate::fixtures::CommitServiceV1ProductionShape, v1_production_shape, sim] $(, $ctx)?);
        // The commit service as production builds it from the simulator's
        // feature flags; the defaults are production's.
        $cb!([$crate::fixtures::CommitServiceProductionShape, production_shape, sim] $(, $ctx)?);
        // Channeled wrapper over the simple in-memory bus.
        $cb!(
            [
                ChanneledCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                channeled_v1_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        // The same over the `channeled_svc` frontend, and in the production
        // topology with the bus behind its own `channeled_svc` channel.
        $cb!(
            [
                $crate::fixtures::ChanneledSvcCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                channeled_svc_v1_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        $cb!(
            [
                $crate::fixtures::ChanneledSvcCommitServiceFixture<CommitServiceV1Fixture<agentbus_tests::fixtures::simtest::BusChanneledSvc>>,
                channeled_svc_v1_channeled_svc_bus,
                sim
            ]
            $(, $ctx)?
        );
        // Replay behind the `channeled_svc` frontend, with shared checkpoint
        // storage, over the same two buses.
        $cb!(
            [
                $crate::fixtures::ChanneledSvcCommitServiceFixture<$crate::fixtures::ReplayCommitServiceFixture<FaultInjectingFixture<SimpleMemoryFixture>, $crate::fixtures::replay::SharedStorage>>,
                channeled_svc_replay_shared_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        $cb!(
            [
                $crate::fixtures::ChanneledSvcCommitServiceFixture<$crate::fixtures::ReplayCommitServiceFixture<agentbus_tests::fixtures::simtest::BusChanneledSvc, $crate::fixtures::replay::SharedStorage>>,
                channeled_svc_replay_shared_channeled_svc_bus,
                sim
            ]
            $(, $ctx)?
        );
        $cb!(
            [
                $crate::fixtures::BufferingCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                buffered_v1_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        // Buffered V1 replicas behind a router. Clients of a bus mostly reach its
        // home replica, so they contend in that replica's queues.
        $cb!(
            [
                $crate::fixtures::RandomRoutingFixture<
                    $crate::fixtures::BufferingCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                >,
                random_routing_buffered_v1_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        // Observability decorator over a single V1 service and over a router.
        $cb!(
            [
                ObservableCommitServiceFixture<CommitServiceV1Fixture<FaultInjectingFixture<SimpleMemoryFixture>>>,
                observable_v1_simple_memory,
                sim
            ]
            $(, $ctx)?
        );
        $cb!(
            [
                ObservableCommitServiceFixture<RandomRoutingCommitServiceFixture<SimpleMemoryFixture>>,
                observable_random_routing,
                sim
            ]
            $(, $ctx)?
        );
        // Router over several independent v1 services (one shared bus + storage),
        // dispatching each request to a randomly-picked one.
        $cb!([RandomRoutingCommitServiceFixture<SimpleMemoryFixture>, random_routing, sim] $(, $ctx)?);
        $cb!(
            [
                RandomRoutingCommitServiceFixture<FaultInjectingFixture<SimpleMemoryFixture>>,
                random_routing_fault,
                sim
            ]
            $(, $ctx)?
        );
        // Standalone in-memory implementation with its own local bus.
        $cb!([InMemCommitServiceFixture, inmem, sim] $(, $ctx)?);
        // Replay-based implementations over every AgentBus backend, one per
        // checkpoint store. Without storage, every load replays from 0.
        agentbus_tests::agentbus_fixtures!(
            $crate::cs_replay_bus_bridge,
            ($crate::fixtures::replay::NoStore, nostorage, $cb $(, $ctx)?)
        );
        agentbus_tests::agentbus_fixtures!(
            $crate::cs_replay_bus_bridge,
            ($crate::fixtures::replay::LocalCheckpoints, independent, $cb $(, $ctx)?)
        );
        agentbus_tests::agentbus_fixtures!(
            $crate::cs_replay_bus_bridge,
            ($crate::fixtures::replay::IndependentStorage, independent_storage, $cb $(, $ctx)?)
        );
        agentbus_tests::agentbus_fixtures!(
            $crate::cs_replay_bus_bridge,
            ($crate::fixtures::replay::SharedStorage, shared, $cb $(, $ctx)?)
        );
        agentbus_tests::agentbus_fixtures!(
            $crate::cs_replay_bus_bridge,
            ($crate::fixtures::replay::BackgroundSharedStorage, shared_background, $cb $(, $ctx)?)
        );
        // A random mix of V1 and replay servers over every AgentBus backend.
        agentbus_tests::agentbus_fixtures!($crate::cs_hybrid_bus_bridge, ($cb $(, $ctx)?));
        $cb!([
            $crate::fixtures::InMemCommitServiceFixture<$crate::fixtures::LeaseBasedTestPolicyProvider>,
            lease_inmem,
            sim
        ] $(, $ctx)?);
        $cb!([
            $crate::fixtures::RandomRoutingCommitServiceFixture<
                SimpleMemoryFixture,
                $crate::fixtures::LeaseBasedTestPolicyProvider
            >,
            lease_random_routing,
            sim
        ] $(, $ctx)?);
        $cb!([
            $crate::fixtures::RandomRoutingCommitServiceFixture<
                FaultInjectingFixture<SimpleMemoryFixture>,
                $crate::fixtures::LeaseBasedTestPolicyProvider
            >,
            lease_random_routing_fault,
            sim
        ] $(, $ctx)?);
    };
}

/// Bridge: turn one AgentBus *sim* fixture (with its suffix) into the V1
/// commit-service fixture, invoking the forwarded `$cb`. The AgentBus registry's
/// `integration` entry is skipped — commit_service bridges the simulator buses
/// only.
#[macro_export]
macro_rules! cs_bus_bridge {
    ([$bus:ty, $sfx:ident, sim], ($cb:path $(, $ctx:tt)?)) => {
        $cb!([CommitServiceV1Fixture<$bus>, $sfx, sim] $(, $ctx)?);
    };
    ([$bus:ty, $sfx:ident, integration], ($cb:path $(, $ctx:tt)?)) => {};
}

/// Bridge: like `cs_bus_bridge`, but emits a `ReplayCommitServiceFixture` with
/// checkpoint store `$store`, prefixing its suffix with `$prefix`.
#[macro_export]
macro_rules! cs_replay_bus_bridge {
    ([$bus:ty, $sfx:ident, sim], ($store:ty, $prefix:ident, $cb:path $(, $ctx:tt)?)) => {
        paste::paste! {
            $cb!([$crate::fixtures::ReplayCommitServiceFixture<$bus, $store>, [<$prefix _ $sfx>], sim] $(, $ctx)?);
        }
    };
    ([$bus:ty, $sfx:ident, integration], ($store:ty, $prefix:ident, $cb:path $(, $ctx:tt)?)) => {};
}

/// Bridge: like `cs_bus_bridge`, but emits a `HybridCommitServiceFixture`.
#[macro_export]
macro_rules! cs_hybrid_bus_bridge {
    ([$bus:ty, $sfx:ident, sim], ($cb:path $(, $ctx:tt)?)) => {
        paste::paste! {
            $cb!([$crate::fixtures::HybridCommitServiceFixture<$bus>, [<hybrid_ $sfx>], sim] $(, $ctx)?);
        }
    };
    ([$bus:ty, $sfx:ident, integration], ($cb:path $(, $ctx:tt)?)) => {};
}

/// Run the same simulator scenarios with independent lease-based reader caches.
#[macro_export]
macro_rules! cs_lease_bus_bridge {
    ([$bus:ty, $sfx:ident, sim], ($cb:path $(, $ctx:tt)?)) => {
        paste::paste! {
            $cb!([
                $crate::fixtures::CommitServiceV1Fixture<
                    $bus,
                    $crate::fixtures::LeaseBasedTestPolicyProvider
                >,
                [<lease_ $sfx>],
                sim
            ] $(, $ctx)?);
        }
    };
    ([$bus:ty, $sfx:ident, integration], ($cb:path $(, $ctx:tt)?)) => {};
}
