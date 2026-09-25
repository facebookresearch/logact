/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic `Applicator` conformance tests — runs the duplication-tolerance
//! contract suite (new / replay-last / stale) against every applicator.

use logact_commit_service_engine_tests::applicator::fixtures::DeciderApplicatorFixture;
use logact_commit_service_engine_tests::applicator::fixtures::StatelessVoterAdapterFixture;
use logact_commit_service_engine_tests::applicator_tests;

#[rustfmt::skip]
mod tests {
use super::*;

applicator_tests!(DeciderApplicatorFixture, decider);
applicator_tests!(StatelessVoterAdapterFixture, adapter);
}
