/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(storage_retry_list)]
mod defs {
    //! `Storage` behavior under injected latency and loss with retries: no
    //! operation fails, and a rejected `put` is one whose earlier attempt landed.

    use anyhow::Result;
    use bytes::Bytes;
    use logact_commit_service_engine::Storage;

    use crate::fixtures::StorageTestFixture;

    #[scenario(sim_only, latency_ms = 65000, zipf = 1.2, loss = 0.05, retry)]
    pub async fn run_test_storage_under_loss_succeeds<F: StorageTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        const KEYS: usize = 4;
        const WRITES: i64 = 8;
        let storage = fixture.create_impl();
        for key in 0..KEYS {
            let key = format!("retry-{key}");
            let mut expected = None;
            for position in 0..WRITES {
                let value = Bytes::from(format!("{key}@{position}"));
                if !storage.put(&key, value.clone(), expected, position).await? {
                    assert_eq!(
                        storage.get(&key).await?,
                        Some((value, position)),
                        "with no competing writer, only a retried put that landed is rejected"
                    );
                }
                expected = Some(position);
            }
        }
        Ok(())
    }
}
pub use defs::*;
