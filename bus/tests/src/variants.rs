/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registry of the `AgentBus` fixtures the suite runs against, in the shape
//! `define_driver!` expects: one `$cb!([Fixture, suffix, env])` per fixture. The
//! five simulator fixtures are tagged `sim`; the SQLite and in-process transport
//! fixtures are tagged `integration` so the generated
//! `agentbus_integration_suite!` drives them. Fixtures are written with absolute
//! `$crate` paths so callers need no imports. Adding a fixture = one line here.
//!
//! An optional trailing `$ctx` is forwarded to each callback, so another suite can
//! compose this matrix — e.g. commit_service wraps each AgentBus fixture in its V1
//! bridge via `agentbus_fixtures!(cs_bus_bridge, ($cb))`.
//!
//! Additional integration fixtures can live in downstream crates, which drive
//! them via the generated `agentbus_emit_integration`.

/// Registry of all `AgentBus` fixtures under test.
#[macro_export]
macro_rules! agentbus_fixtures {
    ($cb:path $(, $ctx:tt)?) => {
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::simtest::BusSimpleMemory>, bus_simple_memory_legacy_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::simtest::BusSimpleMemory>, bus_simple_memory_typed_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::simtest::BusChanneled>, bus_channeled_agentbus_legacy_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::simtest::BusChanneled>, bus_channeled_agentbus_typed_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::simtest::BusChained>, bus_chained_agentbus_legacy_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::simtest::BusChained>, bus_chained_agentbus_typed_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::simtest::SpaceInMemory>, bus_write_once_in_memory_legacy_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::simtest::SpaceInMemory>, bus_write_once_in_memory_typed_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::simtest::SpaceChanneled>, bus_write_once_channeled_legacy_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::simtest::SpaceChanneled>, bus_write_once_channeled_typed_bus_id, sim] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::integration::SqliteAgentBusFixture>, bus_write_once_sqlite_legacy_bus_id, integration] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::integration::SqliteAgentBusFixture>, bus_write_once_sqlite_typed_bus_id, integration] $(, $ctx)?);
        $cb!([$crate::fixtures::LegacyBusIdFixture<$crate::fixtures::integration::IntegrationTestFixture>, integration_legacy_bus_id, integration] $(, $ctx)?);
        $cb!([$crate::fixtures::TypedBusIdFixture<$crate::fixtures::integration::IntegrationTestFixture>, integration_typed_bus_id, integration] $(, $ctx)?);
    };
}
