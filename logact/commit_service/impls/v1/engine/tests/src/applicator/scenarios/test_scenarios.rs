/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic scenarios for the `Applicator` duplication-tolerance contract.

use anyhow::Result;
use logact_commit_service_engine::Applicator;
use logact_commit_service_engine::ApplyError;

use crate::applicator::fixtures::ApplicatorTestFixture;

/// C) Applying at new, increasing positions succeeds.
pub async fn run_test_new_positions_succeed<F: ApplicatorTestFixture>(fixture: &F) -> Result<()> {
    let applicator = fixture.create_impl();
    applicator.apply("bus", &fixture.make_entry(10)).await?;
    applicator.apply("bus", &fixture.make_entry(11)).await?;
    Ok(())
}

/// B) Re-applying the last position returns the identical result.
pub async fn run_test_replay_of_last_returns_same_result<F: ApplicatorTestFixture>(
    fixture: &F,
) -> Result<()> {
    let applicator = fixture.create_impl();
    applicator.apply("bus", &fixture.make_entry(10)).await?;
    let first = applicator.apply("bus", &fixture.make_entry(11)).await?;
    let replay = applicator.apply("bus", &fixture.make_entry(11)).await?;
    assert_eq!(
        first, replay,
        "replay of the last position must return the identical result"
    );
    Ok(())
}

/// A) Re-applying a position older than the last is rejected as stale.
pub async fn run_test_stale_apply_is_rejected<F: ApplicatorTestFixture>(fixture: &F) -> Result<()> {
    let applicator = fixture.create_impl();
    applicator.apply("bus", &fixture.make_entry(10)).await?;
    applicator.apply("bus", &fixture.make_entry(11)).await?;
    let err = applicator
        .apply("bus", &fixture.make_entry(5))
        .await
        .expect_err("re-applying an older position must be rejected");
    assert!(
        matches!(
            err,
            ApplyError::StalePosition {
                requested: 5,
                last: 11
            }
        ),
        "expected StalePosition {{ requested: 5, last: 11 }}, got {err:?}"
    );
    Ok(())
}
