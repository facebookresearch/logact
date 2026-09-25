/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Registry of the `TailableSpace` fixtures the generated simulator and integration
//! conformance suites run against. Portable fixtures live here; additional backend
//! fixtures can live in downstream test crates.
//!
//! TailableSpace is exercised through two adapters — `WosTailableFixture` (over a
//! WriteOnceSpace) and `CwsTailableFixture` (over a ConditionalWriteSpace). The
//! `tail_`-prefixed suffixes disambiguate the generated test names from the sibling
//! suites in the shared `sim_tests` binary.

/// Registry of portable `TailableSpace` fixtures under test.
#[macro_export]
macro_rules! tailable_space_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::tailable_space::fixtures::WosTailableFixture<
                $crate::write_once_space::fixtures::simtest::InMemoryWriteOnceSpaceFixture,
            >,
            tail_wos_in_memory,
            sim
        ]);
        $cb!([
            $crate::tailable_space::fixtures::WosTailableFixture<
                $crate::write_once_space::fixtures::WriteOnceAdapterFixture<
                    $crate::conditional_write_space::fixtures::simtest::InMemoryConditionalWriteSpaceFixture,
                >,
            >,
            tail_wos_in_memory_adapter,
            sim
        ]);
        $cb!([
            $crate::tailable_space::fixtures::WosTailableFixture<
                $crate::write_once_space::fixtures::WriteOnceAdapterFixture<
                    $crate::conditional_write_space::fixtures::sqlite::SqliteConditionalWriteSpaceFixture,
                >,
            >,
            tail_wos_sqlite_adapter,
            integration
        ]);
        $cb!([
            $crate::tailable_space::fixtures::WosTailableFixture<
                $crate::write_once_space::fixtures::simtest::ChanneledWriteOnceSpaceFixture,
            >,
            tail_wos_channeled,
            sim
        ]);
        $cb!([
            $crate::tailable_space::fixtures::CwsTailableFixture<
                $crate::conditional_write_space::fixtures::simtest::InMemoryConditionalWriteSpaceFixture,
            >,
            tail_cws_in_memory,
            sim
        ]);
        $cb!([
            $crate::tailable_space::fixtures::CwsTailableFixture<
                $crate::conditional_write_space::fixtures::sqlite::SqliteConditionalWriteSpaceFixture,
            >,
            tail_cws_sqlite,
            integration
        ]);
    };
}
