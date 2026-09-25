/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common test fixtures for ConditionalWriteSpace implementations

use agentbus_api::ConditionalWriteSpace;
use conformance::ConformanceFixture;

/// The `ConditionalWriteSpace`-specific view of a fixture: a `ConformanceFixture`
/// whose `Impl` is a `ConditionalWriteSpace`. Backends implement
/// `ConformanceFixture`; the blanket impl gives them this trait for free.
pub trait ConditionalWriteSpaceTestFixture:
    ConformanceFixture<Impl: ConditionalWriteSpace + Clone + 'static>
{
}
impl<F: ConformanceFixture<Impl: ConditionalWriteSpace + Clone + 'static>>
    ConditionalWriteSpaceTestFixture for F
{
}

pub mod simtest;
pub mod sqlite;
