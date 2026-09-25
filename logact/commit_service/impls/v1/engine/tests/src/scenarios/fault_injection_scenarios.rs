/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fault injection `Storage` conformance scenarios (sim-only).
//!
//! Each scenario builds its `FaultInjectingStorage` wrapper explicitly with the
//! failures, delay, or overlapping-write behavior it needs. Tests that verify
//! corruption resistance also retain a clean handle to the wrapped storage.

#[conformance_macros::scenarios(storage_fault_injection_scenarios_list)]
mod defs {
    use std::rc::Rc;
    use std::time::Duration;

    use anyhow::Result;
    use bytes::Bytes;
    use futures::future::join;
    use logact_commit_service_engine::Storage;
    use logact_commit_service_engine::StorageError;

    use crate::fixtures::StorageTestFixture;
    use crate::fixtures::fault_injecting::FaultInjectingStorage;
    use crate::fixtures::fault_injecting::StorageFaultConfig;

    const OVERLAPPING_PUT_DELAY: Duration = Duration::from_millis(50);

    #[scenario(sim_only)]
    pub async fn run_test_get_error_is_propagated<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        // Wrap the backend so every operation fails, then assert the error surfaces.
        let storage = FaultInjectingStorage::new(
            fixture.create_impl(),
            fixture.get_env(),
            StorageFaultConfig {
                get_failure_rate: 1.0,
                ..Default::default()
            },
        );
        assert!(
            storage.get("key").await.is_err(),
            "injected fault should propagate as Err"
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_put_error_is_propagated<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = FaultInjectingStorage::new(
            fixture.create_impl(),
            fixture.get_env(),
            StorageFaultConfig {
                put_failure_rate: 1.0,
                ..Default::default()
            },
        );
        assert!(
            storage
                .put("key", Bytes::from("value"), None, 0)
                .await
                .is_err(),
            "injected fault should propagate as Err"
        );
        Ok(())
    }

    // Annotated `sim_only` with no `fault_rate`, i.e. as a plain
    // (non-fault-injecting) scenario, even though it exercises faults. The
    // `fault_rate` annotation wraps the *whole* fixture so every `create_impl()`
    // handle is faulty; this test instead needs both variants at once — a faulty
    // handle to operate through and a clean handle (sharing the same backing
    // store) to set up and verify the ground-truth value — so it builds the
    // faulty wrapper explicitly in the body rather than via the annotation.
    #[scenario(sim_only)]
    pub async fn run_test_intermittent_faults_dont_corrupt<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        // Share one backing store between the direct handle and the faulty
        // wrapper via `Rc` (the `Impl` need not be `Clone`).
        let storage = Rc::new(fixture.create_impl());
        storage.put("key", Bytes::from("v0"), None, 0).await?;

        let faulty = FaultInjectingStorage::new(
            storage.clone(),
            fixture.get_env(),
            StorageFaultConfig {
                get_failure_rate: 0.5,
                put_failure_rate: 0.5,
                ..Default::default()
            },
        );

        let mut successful_writes = 0u64;
        let mut faulted_calls = 0u64;
        let mut last_known_value = "v0".to_string();

        for i in 1..20i64 {
            let (_, expected_pos) = match faulty.get("key").await {
                Ok(Some(entry)) => entry,
                Ok(None) => panic!("key should always exist"),
                Err(_) => {
                    faulted_calls += 1;
                    continue;
                }
            };

            let new_value = format!("v{i}");
            match faulty
                .put(
                    "key",
                    Bytes::from(new_value.clone()),
                    Some(expected_pos),
                    expected_pos + 1,
                )
                .await
            {
                Ok(true) => {
                    successful_writes += 1;
                    last_known_value = new_value;
                }
                Ok(false) => {}
                Err(_) => {
                    faulted_calls += 1;
                }
            }
        }

        assert!(
            faulted_calls > 0,
            "at least one call should have been faulted"
        );
        assert!(
            successful_writes > 0,
            "at least one write should have succeeded"
        );

        let (val, _) = storage
            .get("key")
            .await?
            .expect("key should exist after mixed faults");
        let val_str = String::from_utf8(val.to_vec())?;
        assert_eq!(
            val_str, last_known_value,
            "stored value must match the last successful write"
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_overlapping_put_reports_conflict<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = FaultInjectingStorage::new(
            fixture.create_impl(),
            fixture.get_env(),
            StorageFaultConfig {
                put_delay: OVERLAPPING_PUT_DELAY,
                conflict_on_overlapping_put: true,
                ..Default::default()
            },
        );
        let first_value = Bytes::from_static(b"first");
        let second_value = Bytes::from_static(b"second");

        let (first, second) = join(
            storage.put("key", first_value.clone(), None, 0),
            storage.put("key", second_value.clone(), None, 1),
        )
        .await;

        let (winning_value, winning_position) = match (first, second) {
            (Ok(true), Err(StorageError::TransactionConflict(_))) => (first_value, 0),
            (Err(StorageError::TransactionConflict(_)), Ok(true)) => (second_value, 1),
            results => panic!("expected one successful put and one conflict, got {results:?}"),
        };
        let stored = storage
            .get("key")
            .await?
            .expect("winning put should persist");
        assert_eq!(stored, (winning_value, winning_position));
        assert_eq!(storage.fault_counts().transaction_conflicts, 1);

        assert!(
            storage
                .put(
                    "key",
                    Bytes::from_static(b"retry"),
                    Some(winning_position),
                    winning_position + 1,
                )
                .await?,
            "retry after the winning put becomes visible should succeed"
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_test_overlapping_puts_to_different_keys_succeed<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = FaultInjectingStorage::new(
            fixture.create_impl(),
            fixture.get_env(),
            StorageFaultConfig {
                put_delay: OVERLAPPING_PUT_DELAY,
                conflict_on_overlapping_put: true,
                ..Default::default()
            },
        );

        let (first, second) = join(
            storage.put("first", Bytes::from_static(b"first"), None, 0),
            storage.put("second", Bytes::from_static(b"second"), None, 0),
        )
        .await;

        assert!(first?, "first key should be written");
        assert!(second?, "second key should be written");
        assert_eq!(storage.fault_counts().transaction_conflicts, 0);
        Ok(())
    }
}

// See `test_scenarios.rs` for why scenarios live in `mod defs` and are re-exported.
pub use defs::*;
