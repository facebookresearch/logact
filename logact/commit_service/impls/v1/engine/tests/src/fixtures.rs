/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `Storage` test fixtures — one implementation per file under `fixtures/`.
//!
//! Each submodule constructs one concrete `Storage` in a given environment. Which
//! fixtures the suite actually runs against is the registry in `variants.rs` —
//! this file only defines them and the trait they implement.

use logact_commit_service_engine::Storage;

pub mod fault_injecting;
pub mod in_memory;
pub mod scoped;
pub mod sqlite;

// The portable fixture traits are shared across every conformance suite, so
// they live in the `conformance` crate. Re-exported here so the submodules and
// scenarios can name them at `crate::fixtures::*`.
pub use conformance::ConformanceFixture;
pub use conformance::SimulatorFixture;
pub use fault_injecting::FaultInjectingStorageFixture;
pub use in_memory::InMemoryStorageFixture;
pub use scoped::ScopedStorageFixture;
pub use sqlite::SqliteFileStorageFixture;

/// The `Storage`-specific view of a fixture: a `ConformanceFixture` whose `Impl`
/// is a `Storage`. Scenarios bind on this so they can call `Storage` methods. The
/// blanket impl means every `ConformanceFixture<Impl: Storage>` qualifies — no
/// fixture implements it directly.
pub trait StorageTestFixture: ConformanceFixture<Impl: Storage> {}
impl<F: ConformanceFixture<Impl: Storage>> StorageTestFixture for F {}
