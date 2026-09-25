/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registry of the `WriteOnceSpace` fixtures the generated simulator and integration
//! conformance suites run against. Portable fixtures live here; additional backend
//! fixtures can live in downstream test crates.
//!
//! The `wos_`-prefixed suffixes disambiguate the generated test names from the
//! identically-named scenarios of sibling suites in the same `sim_tests` binary
//! (matching the `cws_` prefix the `ConditionalWriteSpace` suite uses).

/// Registry of portable `WriteOnceSpace` fixtures under test.
#[macro_export]
macro_rules! write_once_space_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::write_once_space::fixtures::simtest::InMemoryWriteOnceSpaceFixture,
            wos_in_memory,
            sim
        ]);
        $cb!([
            $crate::write_once_space::fixtures::WriteOnceAdapterFixture<
                $crate::conditional_write_space::fixtures::simtest::InMemoryConditionalWriteSpaceFixture,
            >,
            wos_in_memory_adapter,
            sim
        ]);
        $cb!([
            $crate::write_once_space::fixtures::WriteOnceAdapterFixture<
                $crate::conditional_write_space::fixtures::sqlite::SqliteConditionalWriteSpaceFixture,
            >,
            wos_sqlite_adapter,
            integration
        ]);
        $cb!([
            $crate::write_once_space::fixtures::simtest::ChanneledWriteOnceSpaceFixture,
            wos_channeled,
            sim
        ]);
    };
}
