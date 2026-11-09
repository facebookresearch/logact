/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(write_once_space_test_scenarios_list)]
mod defs {
    //! Test scenarios for WriteOnceSpace implementations

    use std::collections::HashMap;
    use std::time::Duration;

    use agentbus_api::WriteOnceError;
    use agentbus_api::WriteOnceResult;
    use agentbus_api::WriteOnceSpace;
    use agentbus_api::environment::Environment;
    use anyhow::Result;
    use anyhow::bail;
    use anyhow::ensure;
    use bytes::Bytes;

    use crate::simulator::Simulator;
    use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;

    const DEFAULT_SPACE: &str = "default";

    /// Writes `value`, which should win `address`, resolving an `AmbiguousWrite`
    /// by reading the address back.
    async fn write_to_win<W: WriteOnceSpace>(
        space: &mut W,
        space_id: &str,
        address: u64,
        value: &'static str,
    ) -> Result<()> {
        match space.write(space_id, address, Bytes::from(value)).await {
            Ok(()) => Ok(()),
            Err(WriteOnceError::AmbiguousWrite(_)) => {
                let stored = space.read(space_id, address).await?;
                ensure!(
                    stored == Some(Bytes::from(value)),
                    "another write won address {address}"
                );
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Asserts that a write to the already-written `address` lost.
    fn assert_lost(result: WriteOnceResult<()>, address: u64) {
        assert!(
            matches!(
                result,
                Err(WriteOnceError::AddressAlreadyExists(a) | WriteOnceError::AmbiguousWrite(a))
                    if a == address
            ),
            "a write to written address {address} should lose, got {result:?}"
        );
    }

    /// Test basic write then read cycle.
    #[scenario]
    pub async fn run_test_write_then_read<F: WriteOnceSpaceTestFixture>(fixture: &F) -> Result<()> {
        let mut client = fixture.create_impl();

        write_to_win(&mut client, DEFAULT_SPACE, 0, "hello").await?;
        let value = client.read(DEFAULT_SPACE, 0).await?;
        assert_eq!(value, Some(Bytes::from("hello")));

        Ok(())
    }

    /// Test reading from a non-existent address returns None.
    #[scenario]
    pub async fn run_test_read_nonexistent<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let client = fixture.create_impl();

        let value = client.read(DEFAULT_SPACE, 999).await?;
        assert_eq!(value, None);

        Ok(())
    }

    /// Test that a missing value becomes visible after another client writes it.
    #[scenario]
    pub async fn run_test_read_miss_then_write<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let reader = fixture.create_impl();
        let mut writer = fixture.create_impl();

        assert_eq!(reader.read(DEFAULT_SPACE, 0).await?, None);
        assert_eq!(reader.read(DEFAULT_SPACE, 0).await?, None);
        write_to_win(&mut writer, DEFAULT_SPACE, 0, "later").await?;
        assert_eq!(
            reader.read(DEFAULT_SPACE, 0).await?,
            Some(Bytes::from("later"))
        );
        assert_eq!(
            reader.read(DEFAULT_SPACE, 0).await?,
            Some(Bytes::from("later"))
        );

        Ok(())
    }

    /// Test that the same address in different spaces holds independent values.
    #[scenario]
    pub async fn run_test_space_isolation<F: WriteOnceSpaceTestFixture>(fixture: &F) -> Result<()> {
        let mut client1 = fixture.create_impl();
        let mut client2 = fixture.create_impl();

        write_to_win(&mut client1, "a", 7, "first").await?;
        assert_eq!(client2.read("b", 7).await?, None);
        write_to_win(&mut client2, "b", 7, "second").await?;

        for client in [&client1, &client2] {
            assert_eq!(client.read("a", 7).await?, Some(Bytes::from("first")));
            assert_eq!(client.read("b", 7).await?, Some(Bytes::from("second")));
        }

        Ok(())
    }

    /// Test write-once semantics: first write succeeds, second fails with AddressAlreadyExists.
    #[scenario]
    pub async fn run_test_write_once_semantics<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        write_to_win(&mut client, DEFAULT_SPACE, 0, "first").await?;

        let result = client.write(DEFAULT_SPACE, 0, Bytes::from("second")).await;
        assert_lost(result, 0);

        let value = client.read(DEFAULT_SPACE, 0).await?;
        assert_eq!(value, Some(Bytes::from("first")));

        Ok(())
    }

    #[scenario]
    pub async fn run_test_different_clients<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client1 = fixture.create_impl();
        let mut client2 = fixture.create_impl();

        write_to_win(&mut client1, DEFAULT_SPACE, 0, "val-0").await?;
        write_to_win(&mut client2, DEFAULT_SPACE, 1, "val-1").await?;

        for client in [&mut client1, &mut client2] {
            assert_eq!(
                client.read(DEFAULT_SPACE, 0).await?,
                Some(Bytes::from("val-0"))
            );
            assert_eq!(
                client.read(DEFAULT_SPACE, 1).await?,
                Some(Bytes::from("val-1"))
            );
            for i in 0..2 {
                let result = client.write(DEFAULT_SPACE, i, Bytes::from("fail")).await;
                assert_lost(result, i);
            }
        }

        Ok(())
    }

    async fn assert_ttl_state<W: WriteOnceSpace>(
        space: &W,
        expected: [Option<&'static str>; 5],
        tail: u64,
    ) -> Result<()> {
        for (address, value) in (0..).zip(expected) {
            assert_eq!(
                space.read(DEFAULT_SPACE, address).await?,
                value.map(Bytes::from),
                "address {address}"
            );
        }
        assert_eq!(space.tail(DEFAULT_SPACE, 8).await?, tail);
        Ok(())
    }

    /// Expired writes read as empty and can be rewritten, while live ones still
    /// conflict. `tail` ignores expired entries, so it can fall back to the first
    /// expired address.
    #[scenario(sim_only, ttl_ms = 100)]
    pub async fn run_test_ttl_expiry<F>(fixture: &F) -> Result<()>
    where
        F: WriteOnceSpaceTestFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let mut space = fixture.create_impl();
        for address in 0..3 {
            space
                .write(DEFAULT_SPACE, address, Bytes::from("a"))
                .await?;
        }
        assert_ttl_state(&space, [Some("a"), Some("a"), Some("a"), None, None], 3).await?;

        env.sleep(Duration::from_millis(60)).await;
        space.write(DEFAULT_SPACE, 3, Bytes::from("b")).await?;
        assert_ttl_state(
            &space,
            [Some("a"), Some("a"), Some("a"), Some("b"), None],
            4,
        )
        .await?;

        env.sleep(Duration::from_millis(60)).await;
        assert_ttl_state(&space, [None, None, None, Some("b"), None], 0).await?;
        space.write(DEFAULT_SPACE, 0, Bytes::from("c")).await?;
        assert!(matches!(
            space.write(DEFAULT_SPACE, 3, Bytes::from("d")).await,
            Err(WriteOnceError::AddressAlreadyExists(3))
        ));
        assert_ttl_state(&space, [Some("c"), None, None, Some("b"), None], 1).await?;

        env.sleep(Duration::from_millis(150)).await;
        assert_ttl_state(&space, [None; 5], 0).await?;
        space.write(DEFAULT_SPACE, 4, Bytes::from("e")).await?;
        assert_ttl_state(&space, [None, None, None, None, Some("e")], 0).await?;
        Ok(())
    }

    // The scenarios above, through lossy storage with retries: no transient
    // error may reach the caller.

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_write_then_read_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_write_then_read(fixture).await
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_read_nonexistent_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_read_nonexistent(fixture).await
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_read_miss_then_write_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_read_miss_then_write(fixture).await
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_space_isolation_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_space_isolation(fixture).await
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_write_once_semantics_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_write_once_semantics(fixture).await
    }

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_different_clients_with_retry<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        run_test_different_clients(fixture).await
    }

    /// Writers race for the same addresses, each with its own values, through
    /// lossy storage with retries. A writer that sees `AmbiguousWrite` reads the
    /// address back to learn whether it won. Each address has exactly one winner,
    /// whose value is stored, and no transient error reaches a writer.
    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_racing_writers_with_retry<F>(fixture: &F) -> Result<()>
    where
        F: WriteOnceSpaceTestFixture<Env = Simulator>,
    {
        const WRITERS: usize = 4;
        const ADDRESSES: u64 = 32;
        let value = |writer: usize, address: u64| Bytes::from(format!("{writer}-{address}"));

        let env = fixture.get_env();
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let mut space = fixture.create_impl();
                env.spawn(async move {
                    let mut won = Vec::new();
                    for address in 0..ADDRESSES {
                        match space
                            .write(DEFAULT_SPACE, address, value(writer, address))
                            .await
                        {
                            Ok(()) => won.push(address),
                            Err(WriteOnceError::AddressAlreadyExists(_)) => {}
                            Err(WriteOnceError::AmbiguousWrite(_)) => {
                                if space.read(DEFAULT_SPACE, address).await?
                                    == Some(value(writer, address))
                                {
                                    won.push(address);
                                }
                            }
                            Err(error) => bail!("writer {writer}: {error:#}"),
                        }
                    }
                    Ok(won)
                })
            })
            .collect();

        let mut winners = HashMap::new();
        for (writer, handle) in writers.into_iter().enumerate() {
            for address in handle.await.expect("writer task should complete")? {
                if let Some(other) = winners.insert(address, writer) {
                    bail!("writers {other} and {writer} both won address {address}");
                }
            }
        }

        let reader = fixture.create_impl();
        for address in 0..ADDRESSES {
            let Some(&writer) = winners.get(&address) else {
                bail!("no writer won address {address}");
            };
            assert_eq!(
                reader.read(DEFAULT_SPACE, address).await?,
                Some(value(writer, address)),
                "address {address}"
            );
        }

        Ok(())
    }
}
pub use defs::*;
