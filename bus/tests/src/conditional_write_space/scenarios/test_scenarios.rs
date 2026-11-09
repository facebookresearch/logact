/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(conditional_write_space_test_scenarios_list)]
mod defs {
    //! Test scenarios for ConditionalWriteSpace implementations

    use agentbus_api::ConditionalWriteSpace;
    use agentbus_api::TailableSpace;
    use agentbus_api::Version;
    use agentbus_api::VersionedValue;
    use agentbus_api::environment::Environment;
    use anyhow::Result;
    use bytes::Bytes;
    use conformance::ConformanceFixture;
    use rand::RngExt as _;

    use crate::conditional_write_space::fixtures::ConditionalWriteSpaceTestFixture;

    const DEFAULT_SPACE: &str = "default";
    const DEFAULT_ADDRESS: u64 = 0;

    /// Test basic write then read cycle.
    #[scenario]
    pub async fn run_test_write_then_read<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        let ok = client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("hello"))
            .await?;
        assert!(ok.is_some(), "first write should succeed");

        let entry = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?;
        let entry = entry.expect("should have a value");
        assert_eq!(entry.value, Bytes::from("hello"));
        assert_eq!(
            ok,
            Some(entry.version.clone()),
            "write should return the stored version"
        );

        Ok(())
    }

    /// Test reading an empty address returns None.
    #[scenario]
    pub async fn run_test_read_nonexistent<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let client = fixture.create_impl();

        let entry = client.read(DEFAULT_SPACE, 999).await?;
        assert_eq!(entry, None);

        Ok(())
    }

    /// Test that write with expected_version=None writes to an empty address.
    #[scenario]
    pub async fn run_test_conditional_write_create<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        let ok = client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("value"))
            .await?;
        assert!(
            ok.is_some(),
            "create should succeed with expected_version=None"
        );

        let entry = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?;
        let entry = entry.expect("should have a value");
        assert_eq!(entry.value, Bytes::from("value"));

        // Second write with expected_version=None should fail (address occupied)
        let ok = client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("value2"))
            .await?;
        assert!(ok.is_none(), "duplicate create should fail");

        // Original value unchanged
        let entry = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?;
        let entry = entry.expect("should have a value");
        assert_eq!(entry.value, Bytes::from("value"));

        Ok(())
    }

    /// Test that write fails when expected_version doesn't match.
    #[scenario]
    pub async fn run_test_conditional_write_version_mismatch<
        F: ConditionalWriteSpaceTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("v1"))
            .await?;

        // Write with None should fail (address is occupied)
        let ok = client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("v2"))
            .await?;
        assert!(
            ok.is_none(),
            "write with None to occupied address should fail"
        );

        // Read the current version, then use a wrong-but-well-formed version.
        // (Implementations may reject malformed versions with an error rather than
        // returning false, so we use an 8-byte value that simply won't match.)
        let entry = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        let mut bad_version = entry.version.clone();
        bad_version.0 = Bytes::from(999u64.to_be_bytes().to_vec());
        let ok = client
            .write(
                DEFAULT_SPACE,
                DEFAULT_ADDRESS,
                Some(bad_version),
                Bytes::from("v2"),
            )
            .await?;
        assert!(ok.is_none(), "write with wrong version should fail");

        // Value unchanged
        let entry = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(entry.value, Bytes::from("v1"));

        Ok(())
    }

    /// Test that write with the correct version updates the value and version.
    #[scenario]
    pub async fn run_test_conditional_write_update<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        client
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("v1"))
            .await?;

        let v1 = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        let ok = client
            .write(
                DEFAULT_SPACE,
                DEFAULT_ADDRESS,
                Some(v1.version.clone()),
                Bytes::from("v2"),
            )
            .await?;
        assert!(ok.is_some(), "update with correct version should succeed");

        let v2 = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(v2.value, Bytes::from("v2"));
        assert_eq!(ok, Some(v2.version.clone()));
        assert_ne!(
            v2.version, v1.version,
            "an update should change the version"
        );

        let ok = client
            .write(
                DEFAULT_SPACE,
                DEFAULT_ADDRESS,
                Some(v2.version.clone()),
                Bytes::from("v3"),
            )
            .await?;
        assert!(
            ok.is_some(),
            "second update with correct version should succeed"
        );

        let v3 = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(v3.value, Bytes::from("v3"));
        assert_eq!(ok, Some(v3.version.clone()));
        assert_ne!(
            v3.version, v2.version,
            "an update should change the version"
        );

        Ok(())
    }

    /// Versions returned by `write` are the stored versions, can be used for the
    /// next write without reading, and never repeat at an address. A superseded
    /// version is rejected.
    #[scenario]
    pub async fn run_test_write_returns_usable_versions<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();
        let mut versions: Vec<Version> = Vec::new();
        for round in 0..5 {
            let value = Bytes::from(format!("v{round}"));
            let version = client
                .write(
                    DEFAULT_SPACE,
                    DEFAULT_ADDRESS,
                    versions.last().cloned(),
                    value.clone(),
                )
                .await?
                .expect("a write with the returned version should succeed");
            let stored = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
            assert_eq!(
                stored,
                VersionedValue {
                    version: version.clone(),
                    value
                }
            );
            assert!(!versions.contains(&version), "versions must not repeat");
            versions.push(version);
        }

        let stale = versions[versions.len() - 2].clone();
        let ok = client
            .write(
                DEFAULT_SPACE,
                DEFAULT_ADDRESS,
                Some(stale),
                Bytes::from("stale"),
            )
            .await?;
        assert!(ok.is_none(), "a superseded version should be rejected");
        assert_eq!(
            client
                .read(DEFAULT_SPACE, DEFAULT_ADDRESS)
                .await?
                .unwrap()
                .version,
            versions[versions.len() - 1]
        );
        Ok(())
    }

    /// Test that two clients sharing state see each other's writes.
    #[scenario]
    pub async fn run_test_different_clients<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client1 = fixture.create_impl();
        let mut client2 = fixture.create_impl();

        client1
            .write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("from_c1"))
            .await?;

        // client2 sees client1's write
        let entry = client2.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(entry.value, Bytes::from("from_c1"));

        // client2 can update
        let ok = client2
            .write(
                DEFAULT_SPACE,
                DEFAULT_ADDRESS,
                Some(entry.version),
                Bytes::from("from_c2"),
            )
            .await?;
        assert!(ok.is_some());

        // client1 sees client2's update
        let entry = client1.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(entry.value, Bytes::from("from_c2"));

        Ok(())
    }

    /// Two tasks race to create the same address; exactly one succeeds.
    /// Sim-only: spawns via the simulator's deterministic scheduler (`Simulator::spawn`).
    #[scenario(sim_only)]
    pub async fn run_test_concurrent_writes<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = agentbus_simulator::Simulator>,
        F::Impl: ConditionalWriteSpace + Clone + 'static,
    {
        let reg = fixture.create_impl();
        let mut reg1 = reg.clone();
        let mut reg2 = reg.clone();
        let env = fixture.get_env();

        let h1 = env.spawn(async move {
            reg1.write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("r1"))
                .await
                .unwrap()
                .is_some()
        });
        let h2 = env.spawn(async move {
            reg2.write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("r2"))
                .await
                .unwrap()
                .is_some()
        });

        let ok1 = h1.await.expect("task 1 should complete");
        let ok2 = h2.await.expect("task 2 should complete");
        assert_concurrent_create(&reg, ok1, ok2).await
    }

    /// Integration counterpart of `run_test_concurrent_writes`: two real writers
    /// race via a `LocalSet` + `tokio::task::spawn_local` (the backends are
    /// `!Send`), exercising the real backend's compare-and-swap under contention.
    #[scenario(int_only)]
    pub async fn run_test_concurrent_writes_integration<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let reg = fixture.create_impl();
        let mut reg1 = reg.clone();
        let mut reg2 = reg.clone();

        let (ok1, ok2) = tokio::task::LocalSet::new()
            .run_until(async move {
                let h1 = tokio::task::spawn_local(async move {
                    reg1.write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("r1"))
                        .await
                        .unwrap()
                        .is_some()
                });
                let h2 = tokio::task::spawn_local(async move {
                    reg2.write(DEFAULT_SPACE, DEFAULT_ADDRESS, None, Bytes::from("r2"))
                        .await
                        .unwrap()
                        .is_some()
                });
                (
                    h1.await.expect("task 1 should complete"),
                    h2.await.expect("task 2 should complete"),
                )
            })
            .await;

        assert_concurrent_create(&reg, ok1, ok2).await
    }

    /// Shared assertion for the concurrent-create race: exactly one writer wins,
    /// and the stored value is the winner's.
    async fn assert_concurrent_create<I: ConditionalWriteSpace>(
        reg: &I,
        ok1: bool,
        ok2: bool,
    ) -> Result<()> {
        assert!(ok1 ^ ok2, "exactly one concurrent create should succeed");

        let entry = reg
            .read(DEFAULT_SPACE, DEFAULT_ADDRESS)
            .await?
            .expect("address should have a value");
        if ok1 {
            assert_eq!(entry.value, Bytes::from("r1"));
        } else {
            assert_eq!(entry.value, Bytes::from("r2"));
        }
        Ok(())
    }

    /// Multiple clients concurrently do read-then-conditional-write, racing on the
    /// same address. Invariant: the final payload equals the total successful writes
    /// across all clients, proving no write was lost or double-counted.
    /// Sim-only: spawns via the simulator's deterministic scheduler (`Simulator::spawn`).
    #[scenario(sim_only)]
    pub async fn run_test_multi_task_version_consistency<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = agentbus_simulator::Simulator>,
        F::Impl: ConditionalWriteSpace + Clone + 'static,
    {
        let reg = fixture.create_impl();
        let env = fixture.get_env();
        let num_clients = 5;
        let rounds: u64 = 10;

        let mut handles = Vec::new();
        for _ in 0..num_clients {
            handles.push(env.spawn(run_version_client(reg.clone(), rounds)));
        }

        let mut total_successes = 0u64;
        for handle in handles {
            total_successes += handle.await.expect("client task should complete");
        }
        assert_version_consistency(&reg, total_successes).await
    }

    /// Integration counterpart of `run_test_multi_task_version_consistency`: the
    /// same read-then-conditional-write race against a real backend, driven on a
    /// `LocalSet` via `tokio::task::spawn_local` (the backends are `!Send`).
    #[scenario(int_only)]
    pub async fn run_test_multi_task_version_consistency_integration<
        F: ConditionalWriteSpaceTestFixture,
    >(
        fixture: &F,
    ) -> Result<()> {
        let reg = fixture.create_impl();
        let num_clients = 5;
        let rounds: u64 = 10;

        let clients: Vec<_> = (0..num_clients).map(|_| reg.clone()).collect();
        let total_successes = tokio::task::LocalSet::new()
            .run_until(async move {
                let handles: Vec<_> = clients
                    .into_iter()
                    .map(|client| tokio::task::spawn_local(run_version_client(client, rounds)))
                    .collect();
                let mut total = 0u64;
                for handle in handles {
                    total += handle.await.expect("client task should complete");
                }
                total
            })
            .await;
        assert_version_consistency(&reg, total_successes).await
    }

    /// One client's read-then-conditional-write loop; returns how many of its writes
    /// won the CAS. Shared by the sim and integration variants above.
    async fn run_version_client<I: ConditionalWriteSpace>(mut client: I, rounds: u64) -> u64 {
        let mut successes = 0u64;
        for _ in 0..rounds {
            let current = client.read(DEFAULT_SPACE, DEFAULT_ADDRESS).await.unwrap();
            let (version, count) = match &current {
                Some(v) => {
                    let c: u64 = std::str::from_utf8(&v.value).unwrap().parse().unwrap();
                    (Some(v.version.clone()), c)
                }
                None => (None, 0),
            };
            let ok = client
                .write(
                    DEFAULT_SPACE,
                    DEFAULT_ADDRESS,
                    version,
                    Bytes::from(format!("{}", count + 1)),
                )
                .await
                .unwrap();
            if ok.is_some() {
                successes += 1;
            }
        }
        successes
    }

    /// Shared assertion: the stored count equals the total successful writes.
    async fn assert_version_consistency<I: ConditionalWriteSpace>(
        reg: &I,
        total_successes: u64,
    ) -> Result<()> {
        let entry = reg
            .read(DEFAULT_SPACE, DEFAULT_ADDRESS)
            .await?
            .expect("at least one write should have succeeded");
        let final_count: u64 = std::str::from_utf8(&entry.value).unwrap().parse().unwrap();
        assert_eq!(
            final_count, total_successes,
            "stored count must equal total successful writes"
        );
        Ok(())
    }

    /// Test that different space_ids are isolated: writes in one space do not
    /// affect reads in another, even at the same address.
    #[scenario]
    pub async fn run_test_space_id_isolation<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        let ok = client
            .write("space_a", DEFAULT_ADDRESS, None, Bytes::from("in_a"))
            .await?;
        assert!(ok.is_some(), "write to space_a should succeed");

        // Same address in space_b is unaffected
        let entry = client.read("space_b", DEFAULT_ADDRESS).await?;
        assert_eq!(entry, None, "space_b should be empty");

        // Writing to space_b with expected_version=None must succeed
        let ok = client
            .write("space_b", DEFAULT_ADDRESS, None, Bytes::from("in_b"))
            .await?;
        assert!(
            ok.is_some(),
            "write to space_b should succeed independently"
        );

        // Each space holds its own value
        let entry_a = client.read("space_a", DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(entry_a.value, Bytes::from("in_a"));

        let entry_b = client.read("space_b", DEFAULT_ADDRESS).await?.unwrap();
        assert_eq!(entry_b.value, Bytes::from("in_b"));

        Ok(())
    }

    pub async fn run_test_tail<F: ConditionalWriteSpaceTestFixture>(fixture: &F) -> Result<()> {
        let mut client = fixture.create_impl();

        let tail = client.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 0, "empty space should have tail 0");

        client
            .write(DEFAULT_SPACE, 0, None, Bytes::from("a"))
            .await?;
        let tail = client.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 1, "after writing addr 0, tail should be 1");

        client
            .write(DEFAULT_SPACE, 1, None, Bytes::from("b"))
            .await?;
        let tail = client.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "after writing addrs 0 and 1, tail should be 2");

        // Overwriting addr 0 should not change the tail (addr 0 still present)
        let v = client.read(DEFAULT_SPACE, 0).await?.unwrap().version;
        client
            .write(DEFAULT_SPACE, 0, Some(v), Bytes::from("a2"))
            .await?;
        let tail = client.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "overwriting existing addr should not change tail");

        // Skip addr 2, write addr 3 — tail stops at first gap
        client
            .write(DEFAULT_SPACE, 3, None, Bytes::from("d"))
            .await?;
        let tail = client.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "tail should stop at first gap (addr 2)");

        Ok(())
    }

    /// Test tail with random writes and a computed window_size.
    ///
    /// Writes to a random subset of addresses, then computes the correct
    /// window_size from the resulting pattern and verifies tail() returns
    /// the expected contiguous tail.
    pub async fn run_test_tail_with_holes<F: ConditionalWriteSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();
        let env = fixture.get_env();

        const NUM_ADDRESSES: u64 = 50;

        let mut written = vec![false; NUM_ADDRESSES as usize];
        for addr in 0..NUM_ADDRESSES {
            if env.with_rng(|rng| rng.random_bool(0.7)) {
                client
                    .write(DEFAULT_SPACE, addr, None, Bytes::from(format!("{}", addr)))
                    .await?;
                written[addr as usize] = true;
            }
        }

        let contiguous_tail = written.iter().position(|&w| !w).unwrap_or(written.len()) as u64;
        let non_contiguous_tail = written
            .iter()
            .rposition(|&w| w)
            .map(|i| i as u64 + 1)
            .unwrap_or(0);
        let window_size = non_contiguous_tail.saturating_sub(contiguous_tail);

        let tail = client.tail(DEFAULT_SPACE, window_size).await?;
        assert_eq!(tail, contiguous_tail);

        Ok(())
    }
}
pub use defs::*;
