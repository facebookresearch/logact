/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(tailable_space_test_scenarios_list)]
mod defs {
    //! Test scenarios for TailableSpace implementations

    use agentbus_api::TailableSpace;
    use agentbus_api::environment::Environment;
    use anyhow::Result;
    use rand::RngExt as _;

    use crate::tailable_space::fixtures::TailableSpaceTestFixture;

    const DEFAULT_SPACE: &str = "default";

    /// Test that tail returns the first unwritten slot with contiguous writes,
    /// overwrites, and gaps.
    #[scenario]
    pub async fn run_test_tail<F: TailableSpaceTestFixture>(fixture: &F) -> Result<()> {
        let space = fixture.create_impl();

        let tail = space.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 0, "empty space should have tail 0");

        fixture.write_at(DEFAULT_SPACE, 0).await?;
        let tail = space.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 1, "after writing addr 0, tail should be 1");

        fixture.write_at(DEFAULT_SPACE, 1).await?;
        let tail = space.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "after writing addrs 0 and 1, tail should be 2");

        // Overwriting addr 0 should not change the tail (addr 0 still present)
        fixture.write_at(DEFAULT_SPACE, 0).await?;
        let tail = space.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "overwriting existing addr should not change tail");

        // Skip addr 2, write addr 3 — tail stops at first gap
        fixture.write_at(DEFAULT_SPACE, 3).await?;
        let tail = space.tail(DEFAULT_SPACE, 10).await?;
        assert_eq!(tail, 2, "tail should stop at first gap (addr 2)");

        Ok(())
    }

    /// Test tail with random writes and a computed window_size.
    ///
    /// Writes to a random subset of addresses, then computes the correct
    /// window_size from the resulting pattern and verifies tail() returns
    /// the expected contiguous tail.
    #[scenario]
    pub async fn run_test_tail_with_holes<F: TailableSpaceTestFixture>(fixture: &F) -> Result<()> {
        let space = fixture.create_impl();
        let env = fixture.get_env();

        const NUM_ADDRESSES: u64 = 50;

        let mut written = vec![false; NUM_ADDRESSES as usize];
        for addr in 0..NUM_ADDRESSES {
            if env.with_rng(|rng| rng.random_bool(0.7)) {
                fixture.write_at(DEFAULT_SPACE, addr).await?;
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

        let tail = space.tail(DEFAULT_SPACE, window_size).await?;
        assert_eq!(tail, contiguous_tail);

        Ok(())
    }
}
pub use defs::*;
