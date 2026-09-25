/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common test fixtures for AgentBus implementations

/// Re-exported from the shared `conformance` crate so this crate's fixtures, the
/// benchmark binary, and the shared conformance test codegen share one trait.
pub use conformance::ConformanceFixture;
/// Construction trait for fixtures that require async initialization. Re-exported
/// from the shared `conformance` crate so this crate's fixtures and the shared
/// conformance test codegen share one trait.
pub use conformance::IntegrationFixture;
/// Construction trait for fixtures that can be created with a Simulator. Re-exported
/// from the shared `conformance` crate so this crate's fixtures and the shared
/// conformance test codegen share one trait.
pub use conformance::SimulatorFixture;

/// The `AgentBus`-specific view of a fixture: a [`ConformanceFixture`] whose
/// `Impl` is an `AgentBus`. Backends implement `ConformanceFixture`; the blanket
/// impl below gives them this trait for free.
pub trait AgentBusTestFixture: ConformanceFixture<Impl: agentbus_api::AgentBus + 'static> {}
impl<F: ConformanceFixture<Impl: agentbus_api::AgentBus + 'static>> AgentBusTestFixture for F {}

pub mod bus_id_encoding;
pub mod integration;
pub mod simtest;
pub mod write_once_agentbus_fixture;

pub use bus_id_encoding::BusIdEncodingFixture;
pub use bus_id_encoding::LegacyBusIdEncoding;
pub use bus_id_encoding::TypedBusIdEncoding;
pub use write_once_agentbus_fixture::WriteOnceAgentBusGenericFixture;

pub type LegacyBusIdFixture<F> = BusIdEncodingFixture<F, LegacyBusIdEncoding>;
pub type TypedBusIdFixture<F> = BusIdEncodingFixture<F, TypedBusIdEncoding>;
