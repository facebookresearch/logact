/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registry of the eligibility `RequestGate` fixtures the generated simulator
//! suite runs against.
//!
//! The `gate_`-prefixed suffixes disambiguate the generated test names from the
//! other suites in the same `sim_tests` binary.

/// Registry of portable eligibility `RequestGate` fixtures under test.
#[macro_export]
macro_rules! eligibility_gate_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::eligibility_gate::fixtures::InMemoryRequestGateFixture,
            gate_in_memory,
            sim
        ]);
        $cb!([
            $crate::eligibility_gate::fixtures::EligibilityGateFixture,
            gate_eligibility,
            sim
        ]);
        $cb!([
            $crate::eligibility_gate::fixtures::CachingEligibilityGateFixture,
            gate_caching,
            sim
        ]);
    };
}
