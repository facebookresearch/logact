/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Macros for generating integration test suites for ConditionalRegister

#[macro_export]
macro_rules! conditional_register_integration_test {
    ($test_name:ident, $suffix:ident, $fixture:ty) => {
        paste::paste! {
            #[fbinit::test]
            async fn [<cr_ $test_name _ $suffix>](fb: fbinit::FacebookInit) -> anyhow::Result<()> {
                let fixture = <$fixture as $crate::fixtures::IntegrationFixture>::new_async(fb).await?;
                $crate::conditional_register::scenarios::[<run_ $test_name>](&fixture).await?;
                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! conditional_register_integration_tests {
    ($fixture:ty, $suffix:ident) => {
        $crate::conditional_register_integration_test!(
            get_nonexistent_returns_none,
            $suffix,
            $fixture
        );
        $crate::conditional_register_integration_test!(put_then_get, $suffix, $fixture);
        $crate::conditional_register_integration_test!(
            put_version_mismatch_fails,
            $suffix,
            $fixture
        );
        $crate::conditional_register_integration_test!(put_cas_update, $suffix, $fixture);
        $crate::conditional_register_integration_test!(
            put_to_nonexistent_with_some_version_fails,
            $suffix,
            $fixture
        );
        $crate::conditional_register_integration_test!(
            multiple_keys_in_same_namespace,
            $suffix,
            $fixture
        );
        $crate::conditional_register_integration_test!(
            different_namespaces_are_isolated,
            $suffix,
            $fixture
        );
        $crate::conditional_register_integration_test!(
            version_increments_correctly,
            $suffix,
            $fixture
        );
    };
}
