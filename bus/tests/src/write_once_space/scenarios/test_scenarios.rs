/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(write_once_space_test_scenarios_list)]
mod defs {
    //! Test scenarios for WriteOnceSpace implementations

    use agentbus_api::WriteOnceError;
    use agentbus_api::WriteOnceSpace;
    use anyhow::Result;
    use bytes::Bytes;

    use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;

    const DEFAULT_SPACE: &str = "default";

    /// Test basic write then read cycle.
    #[scenario]
    pub async fn run_test_write_then_read<F: WriteOnceSpaceTestFixture>(fixture: &F) -> Result<()> {
        let mut client = fixture.create_impl();

        client.write(DEFAULT_SPACE, 0, Bytes::from("hello")).await?;
        let value = client.read(DEFAULT_SPACE, 0).await;
        assert_eq!(value, Some(Bytes::from("hello")));

        Ok(())
    }

    /// Test reading from a non-existent address returns None.
    #[scenario]
    pub async fn run_test_read_nonexistent<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let client = fixture.create_impl();

        let value = client.read(DEFAULT_SPACE, 999).await;
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

        assert_eq!(reader.read(DEFAULT_SPACE, 0).await, None);
        assert_eq!(reader.read(DEFAULT_SPACE, 0).await, None);
        writer.write(DEFAULT_SPACE, 0, Bytes::from("later")).await?;
        assert_eq!(
            reader.read(DEFAULT_SPACE, 0).await,
            Some(Bytes::from("later"))
        );
        assert_eq!(
            reader.read(DEFAULT_SPACE, 0).await,
            Some(Bytes::from("later"))
        );

        Ok(())
    }

    /// Test that the same address in different spaces holds independent values.
    #[scenario]
    pub async fn run_test_space_isolation<F: WriteOnceSpaceTestFixture>(fixture: &F) -> Result<()> {
        let mut client1 = fixture.create_impl();
        let mut client2 = fixture.create_impl();

        client1.write("a", 7, Bytes::from("first")).await?;
        assert_eq!(client2.read("b", 7).await, None);
        client2.write("b", 7, Bytes::from("second")).await?;

        for client in [&client1, &client2] {
            assert_eq!(client.read("a", 7).await, Some(Bytes::from("first")));
            assert_eq!(client.read("b", 7).await, Some(Bytes::from("second")));
        }

        Ok(())
    }

    /// Test write-once semantics: first write succeeds, second fails with AddressAlreadyExists.
    #[scenario]
    pub async fn run_test_write_once_semantics<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client = fixture.create_impl();

        client.write(DEFAULT_SPACE, 0, Bytes::from("first")).await?;

        let result = client.write(DEFAULT_SPACE, 0, Bytes::from("second")).await;
        assert!(matches!(
            result,
            Err(WriteOnceError::AddressAlreadyExists(0))
        ));

        let value = client.read(DEFAULT_SPACE, 0).await;
        assert_eq!(value, Some(Bytes::from("first")));

        Ok(())
    }

    #[scenario]
    pub async fn run_test_different_clients<F: WriteOnceSpaceTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let mut client1 = fixture.create_impl();
        let mut client2 = fixture.create_impl();

        client1
            .write(DEFAULT_SPACE, 0, Bytes::from("val-0"))
            .await?;
        client2
            .write(DEFAULT_SPACE, 1, Bytes::from("val-1"))
            .await?;

        for client in [&mut client1, &mut client2] {
            assert_eq!(
                client.read(DEFAULT_SPACE, 0).await,
                Some(Bytes::from("val-0"))
            );
            assert_eq!(
                client.read(DEFAULT_SPACE, 1).await,
                Some(Bytes::from("val-1"))
            );
            for i in 0..2 {
                let result = client.write(DEFAULT_SPACE, i, Bytes::from("fail")).await;
                assert!(matches!(
                    result,
                    Err(WriteOnceError::AddressAlreadyExists(addr)) if addr == i
                ));
            }
        }

        Ok(())
    }
}
pub use defs::*;
