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

/// A fixture whose buses look for the tail `tail_window_size` positions back,
/// so they may take for the tail an unwritten position up to that far below the
/// highest written one, including one whose entry expired.
pub trait TailWindowFixture: AgentBusTestFixture {
    fn tail_window_size(&self) -> u64 {
        1
    }
}

pub mod integration;
pub mod simtest;
pub mod write_once_agentbus_fixture;
pub mod write_once_agentbus_v2_fixture;
pub mod write_once_mixed_version_fixture;

pub use write_once_agentbus_fixture::WriteOnceAgentBusGenericFixture;
pub use write_once_agentbus_v2_fixture::WriteOnceAgentBusV2GenericFixture;
