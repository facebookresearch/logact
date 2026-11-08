/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The `PolicyRegister` implementations the conformance suite runs against.

/// Registry of `PolicyRegister` fixtures. Invoked by the suite entry points with
/// a per-fixture callback (`$cb`) that the driver supplies.
#[macro_export]
macro_rules! policy_register_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::fixtures::InMemoryPolicyRegisterFixture,
            in_memory,
            sim
        ]);
        $cb!([
            $crate::fixtures::SqlitePolicyRegisterFixture,
            sqlite_file,
            integration
        ]);
    };
}
