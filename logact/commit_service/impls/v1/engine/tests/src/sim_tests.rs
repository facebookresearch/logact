/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Simulator-based `Storage` conformance tests.
//!
//! Every `all` and `sim_only` scenario runs against every `sim` fixture.
//! The entire suite is generated from the `#[scenario(...)]` annotations in
//! `scenarios.rs` and the fixture registry in `variants.rs`.

use logact_commit_service_engine_tests::storage_sim_suite;

storage_sim_suite!();
