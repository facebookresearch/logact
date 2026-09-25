/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic `Storage` conformance scenarios (no fault injection).
//!
//! `general` scenarios run in sim and against real backends; `deterministic`
//! ones are sim-only. The `#[scenarios(storage_test_scenarios_list)]` proc-macro
//! emits the `storage_test_scenarios_list!` callback that the `define_driver!`-
//! generated `storage_scenarios!` combiner folds into the suite.

#[conformance_macros::scenarios(storage_test_scenarios_list)]
mod defs {
    use std::rc::Rc;

    use anyhow::Result;
    use bytes::Bytes;
    use logact_commit_service_engine::Storage;

    use crate::fixtures::ConformanceFixture;
    use crate::fixtures::StorageTestFixture;

    #[scenario]
    pub async fn run_test_get_missing_key<F: StorageTestFixture>(fixture: &F) -> Result<()> {
        let storage = fixture.create_impl();
        assert!(
            storage.get("nonexistent").await?.is_none(),
            "missing key should return None"
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_and_get<F: StorageTestFixture>(fixture: &F) -> Result<()> {
        let storage = fixture.create_impl();
        assert!(
            storage.put("key1", Bytes::from("hello"), None, 0).await?,
            "initial put with expected=None should succeed"
        );

        let (value, position) = storage.get("key1").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("hello"));
        assert_eq!(position, 0);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_advances_position<F: StorageTestFixture>(fixture: &F) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("key1", Bytes::from("v1"), None, 0).await?;
        storage.put("key1", Bytes::from("v2"), Some(0), 5).await?;

        let (value, position) = storage.get("key1").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("v2"));
        assert_eq!(position, 5);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_rejects_wrong_expected<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("key1", Bytes::from("v1"), None, 10).await?;

        let result = storage
            .put("key1", Bytes::from("v2"), Some(999), 20)
            .await?;
        assert!(!result, "wrong expected position should be rejected");

        let (value, _) = storage.get("key1").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("v1"));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_rejects_none_when_exists<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("key1", Bytes::from("v1"), None, 10).await?;

        let result = storage.put("key1", Bytes::from("v2"), None, 20).await?;
        assert!(!result, "expected=None should fail when key exists");

        let (value, _) = storage.get("key1").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("v1"));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_rejects_some_when_absent<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = fixture.create_impl();

        let result = storage.put("key1", Bytes::from("v1"), Some(0), 10).await?;
        assert!(!result, "expected=Some should fail when key absent");

        assert!(storage.get("key1").await?.is_none());
        Ok(())
    }

    #[scenario]
    pub async fn run_test_independent_keys<F: StorageTestFixture>(fixture: &F) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("a", Bytes::from("1"), None, 0).await?;
        storage.put("b", Bytes::from("2"), None, 0).await?;

        let (va, _) = storage.get("a").await?.expect("a should exist");
        let (vb, _) = storage.get("b").await?.expect("b should exist");
        assert_eq!(va, Bytes::from("1"));
        assert_eq!(vb, Bytes::from("2"));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_overwrite_preserves_other_keys<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("a", Bytes::from("1"), None, 0).await?;
        storage.put("b", Bytes::from("2"), None, 0).await?;
        storage.put("a", Bytes::from("3"), Some(0), 1).await?;

        let (va, _) = storage.get("a").await?.expect("a should exist");
        let (vb, _) = storage.get("b").await?.expect("b should exist");
        assert_eq!(va, Bytes::from("3"));
        assert_eq!(vb, Bytes::from("2"));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_cas_sequence<F: StorageTestFixture>(fixture: &F) -> Result<()> {
        let storage = fixture.create_impl();

        // Each put uses the position from the previous state
        storage.put("counter", Bytes::from("v0"), None, 0).await?;
        for i in 1..10i64 {
            let (_, prev_pos) = storage.get("counter").await?.expect("key should exist");
            let result = storage
                .put("counter", Bytes::from(format!("v{i}")), Some(prev_pos), i)
                .await?;
            assert!(result, "CAS put {i} should succeed");
        }

        let (value, position) = storage.get("counter").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("v9"));
        assert_eq!(position, 9);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_put_rejects_stale_version<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let storage = fixture.create_impl();
        storage.put("key", Bytes::from("v1"), None, 10).await?;

        // new_position < current — should fail even with correct expected
        let result = storage.put("key", Bytes::from("v2"), Some(10), 5).await?;
        assert!(!result, "put with new_position < current should fail");

        // new_position == current — should also fail
        let result = storage.put("key", Bytes::from("v3"), Some(10), 10).await?;
        assert!(!result, "put with new_position == current should fail");

        // Value should be unchanged
        let (value, position) = storage.get("key").await?.expect("key should exist");
        assert_eq!(value, Bytes::from("v1"));
        assert_eq!(position, 10);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_concurrent_get_then_put<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        // Wrap in `Rc` so the concurrent writers below can share one handle
        // without requiring `Impl: Clone`.
        let storage = Rc::new(fixture.create_impl());
        storage.put("key", Bytes::from("initial"), None, 0).await?;

        // All writers read the SAME state before any writes (simulates concurrent reads)
        let existing = storage.get("key").await?;
        let expected = existing.map(|(_, pos)| pos);

        // Then all try to CAS with the same expected position
        let futs: Vec<_> = (1..=5i64)
            .map(|i| {
                let s = storage.clone();
                async move {
                    s.put("key", Bytes::from(format!("writer-{i}")), expected, i)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;

        let succeeded = results.iter().filter(|r| matches!(r, Ok(true))).count();
        let failed_cas = results.iter().filter(|r| matches!(r, Ok(false))).count();
        let errors = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(errors, 0, "no backend errors expected");
        assert_eq!(succeeded, 1, "exactly one writer should win the CAS");
        assert_eq!(failed_cas, 4, "other writers should fail CAS");

        let (_, _) = storage
            .get("key")
            .await?
            .expect("key should exist after concurrent writes");
        Ok(())
    }

    /// Multiple tasks spawned via `Simulator::spawn` race to CAS the same key.
    /// Sim-only: depends on the simulator's deterministic task scheduling.
    #[scenario(sim_only)]
    pub async fn run_test_spawned_concurrent_cas<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = agentbus_simulator::Simulator>,
        F::Impl: Storage + 'static,
    {
        let env = fixture.get_env();
        let storage = Rc::new(fixture.create_impl());
        let num_writers = 5;
        let rounds_per_writer = 3;

        storage.put("counter", Bytes::from("0"), None, 0).await?;

        let mut handles = Vec::new();
        for _ in 0..num_writers {
            let s = Rc::clone(&storage);
            handles.push(env.spawn(async move {
                let mut wins = 0u64;
                for _ in 0..rounds_per_writer {
                    let (val, pos) = s.get("counter").await.unwrap().expect("counter exists");
                    let count: u64 = String::from_utf8(val.to_vec()).unwrap().parse().unwrap();
                    if s.put(
                        "counter",
                        Bytes::from((count + 1).to_string()),
                        Some(pos),
                        pos + 1,
                    )
                    .await
                    .unwrap()
                    {
                        wins += 1;
                    }
                }
                wins
            }));
        }

        let mut total_wins = 0u64;
        for handle in handles {
            total_wins += handle.await.expect("writer task should complete");
        }

        let (val, _) = storage.get("counter").await?.expect("counter should exist");
        let final_count: u64 = String::from_utf8(val.to_vec())?.parse()?;
        assert_eq!(
            final_count, total_wins,
            "final counter must equal total successful CAS writes"
        );
        Ok(())
    }
}

// The `#[scenarios(..)]` proc-macro requires the scenarios to live inside an
// (otherwise private) `mod defs` so it can scan and rewrite them as a unit. This
// re-export lifts them back out to `crate::scenarios::run_test_*`, the path the
// driver references, without the `defs::` layer leaking out.
pub use defs::*;
