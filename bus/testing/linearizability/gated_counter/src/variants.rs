/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[macro_export]
macro_rules! wrap_agentbus_gated_counter_fixture {
    ([$fixture:ty, $suffix:ident, sim], ($cb:path)) => {
        paste::paste! {
            $cb!([
                $crate::fixtures::AgentBusGatedCounterFixture<$fixture>,
                [<agentbus_ $suffix>],
                sim
            ]);
        }
    };
    ([$fixture:ty, $suffix:ident, integration], ($cb:path)) => {};
}

#[macro_export]
macro_rules! wrap_commit_service_gated_counter_fixture {
    ([$fixture:ty, $suffix:ident, sim], ($cb:path)) => {
        paste::paste! {
            $cb!([
                $crate::fixtures::CommitServiceGatedCounterFixture<$fixture>,
                [<commit_service_ $suffix>],
                sim
            ]);
        }
    };
    ([$fixture:ty, $suffix:ident, integration], ($cb:path)) => {};
}

#[macro_export]
macro_rules! gated_counter_fixtures {
    ($cb:path) => {
        agentbus_tests::agentbus_fixtures!($crate::wrap_agentbus_gated_counter_fixture, ($cb));
        logact_commit_service_tests::commit_service_fixtures!(
            $crate::wrap_commit_service_gated_counter_fixture,
            ($cb)
        );
    };
}
