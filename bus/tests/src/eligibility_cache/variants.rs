/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Eligibility gates with controllable cache lookups.
#[macro_export]
macro_rules! eligibility_cache_fixtures {
    ($cb:path) => {
        $cb!([
            $crate::eligibility_cache::fixtures::ControlledCachingEligibilityGateFixture,
            eligibility_cache,
            sim
        ]);
    };
}
