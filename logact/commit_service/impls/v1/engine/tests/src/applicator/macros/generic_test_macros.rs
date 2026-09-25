/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[macro_export]
macro_rules! gen_applicator_test {
    ($test_name:ident, $suffix:ident, $fixture:ty) => {
        paste::paste! {
            #[test]
            fn [<$test_name _ $suffix>]() {
                use $crate::applicator::fixtures::ApplicatorTestFixture;
                use $crate::fixtures::SimulatorFixture;
                let seed: u64 = rand::random();
                eprintln!("Simulator seed: {seed}");
                let simulator = agentbus_simulator::Simulator::new(seed);
                let fixture = <$fixture as SimulatorFixture>::new(simulator);
                let env_rc = fixture.get_env();
                let handle = env_rc.spawn(async move {
                    $crate::applicator::scenarios::[<run_ $test_name>](&fixture).await
                });
                env_rc.run();
                futures::executor::block_on(handle)
                    .expect("scenario task should complete")
                    .expect("scenario should succeed");
            }
        }
    };
}

/// Instantiate the full `Applicator` duplication-tolerance contract suite for a
/// fixture: new positions succeed, the last position replays identically, and
/// older positions are rejected as stale.
#[macro_export]
macro_rules! applicator_tests {
    ($fixture:ty, $suffix:ident) => {
        $crate::gen_applicator_test!(test_new_positions_succeed, $suffix, $fixture);
        $crate::gen_applicator_test!(test_replay_of_last_returns_same_result, $suffix, $fixture);
        $crate::gen_applicator_test!(test_stale_apply_is_rejected, $suffix, $fixture);
    };
}
