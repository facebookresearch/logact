/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[macro_export]
macro_rules! wrap_agentbus_counter_fixture {
    ([$fixture:ty, $suffix:ident, sim], ($cb:path)) => {
        paste::paste! {
            $cb!([
                $crate::fixtures::AgentBusCounterFixture<$fixture>,
                [<agentbus_ $suffix>],
                sim
            ]);
            $cb!([
                $crate::fixtures::SkipCommitsCounterFixture<$fixture>,
                [<agentbus_skip_commits_ $suffix>],
                sim
            ]);
            $cb!([
                $crate::fixtures::RandomPollCounterFixture<$fixture>,
                [<agentbus_random_poll_ $suffix>],
                sim
            ]);
            $cb!([
                $crate::fixtures::SmallPollCounterFixture<$fixture>,
                [<agentbus_small_poll_ $suffix>],
                sim
            ]);
        }
    };
    ([$fixture:ty, $suffix:ident, integration], ($cb:path)) => {};
}

#[macro_export]
macro_rules! counter_fixtures {
    ($cb:path) => {
        agentbus_tests::agentbus_fixtures!($crate::wrap_agentbus_counter_fixture, ($cb));
    };
}
