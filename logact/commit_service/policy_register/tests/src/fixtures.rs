/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `PolicyRegister` test fixtures — one implementation per file under `fixtures/`.
//!
//! Each fixture holds one register; every `create_impl` returns another handle
//! to it, so scenarios can model independent clients.

use logact_commit_service_policy_register::PolicyRegister;

pub mod in_memory;
pub mod sqlite;

pub use conformance::ConformanceFixture;
pub use conformance::SimulatorFixture;
pub use in_memory::InMemoryPolicyRegisterFixture;
pub use sqlite::SqlitePolicyRegisterFixture;

/// The `PolicyRegister`-specific view of a fixture, which scenarios bind on.
pub trait PolicyRegisterTestFixture: ConformanceFixture<Impl: PolicyRegister> {}
impl<F: ConformanceFixture<Impl: PolicyRegister>> PolicyRegisterTestFixture for F {}
