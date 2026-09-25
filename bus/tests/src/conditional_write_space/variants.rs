/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registry of the `ConditionalWriteSpace` fixtures the generated simulator and
//! integration conformance suites run against. Portable fixtures live here;
//! additional backend fixtures can live in downstream test crates.
//!
//! The `cws_`-prefixed suffixes disambiguate the generated test names from the
//! identically-named WriteOnceSpace scenarios in the same `sim_tests` binary.

/// Registry of portable `ConditionalWriteSpace` fixtures under test.
#[macro_export]
macro_rules! conditional_write_space_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::conditional_write_space::fixtures::simtest::InMemoryConditionalWriteSpaceFixture,
            cws_in_memory,
            sim
        ]);
        $cb!([
            $crate::conditional_write_space::fixtures::sqlite::SqliteConditionalWriteSpaceFixture,
            cws_sqlite,
            integration
        ]);
    };
}
