/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Retry asynchronous operations using environment-controlled randomness and sleep.

use std::future::Future;
use std::time::Duration;

use rand::RngExt as _;

use crate::Environment;

/// Attempt and timing parameters for bounded equal-jitter exponential backoff.
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    max_retries: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RetryConfig {
    /// Construct a retry configuration. `initial_backoff` is the upper bound
    /// of the first equal-jitter window; its lower bound is half that value.
    ///
    /// # Errors
    ///
    /// Returns an error if `initial_backoff` is zero or exceeds `max_backoff`.
    pub fn try_new(
        max_retries: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !initial_backoff.is_zero(),
            "initial backoff must be nonzero"
        );
        anyhow::ensure!(
            initial_backoff <= max_backoff,
            "initial backoff {initial_backoff:?} must not exceed maximum backoff {max_backoff:?}"
        );
        Ok(Self {
            max_retries,
            initial_backoff,
            max_backoff,
        })
    }
}

/// The action to take after an operation fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Return the operation's error.
    Stop,
    /// Retry without sleeping. This counts toward the configured retry limit.
    RetryImmediately,
    /// Retry after bounded equal-jitter exponential backoff.
    RetryWithBackoff,
}

/// Why a retried operation ultimately failed.
#[derive(Debug)]
pub enum RetryFailure<Err> {
    /// The caller classified the error as terminal.
    Stopped { last_error: Err },
    /// The configured retry count was exhausted.
    Exhausted {
        /// Number of retries performed after the initial attempt.
        retries: usize,
        /// Error returned by the final operation attempt.
        last_error: Err,
    },
}

impl<Err> RetryFailure<Err> {
    /// Consume the failure and return the operation's final error.
    pub fn into_last_error(self) -> Err {
        match self {
            Self::Stopped { last_error } | Self::Exhausted { last_error, .. } => last_error,
        }
    }
}

/// Run `operation` until it succeeds, `decide` stops retrying, or the retry
/// count is exhausted.
///
/// Each invocation of `operation` creates a fresh future. Backoff uses the
/// supplied environment so simulated randomness and sleeps remain deterministic.
pub async fn retry<E, Op, Fut, T, Err, Decide>(
    environment: &E,
    config: RetryConfig,
    mut operation: Op,
    mut decide: Decide,
) -> Result<T, RetryFailure<Err>>
where
    E: Environment,
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Err>>,
    Decide: FnMut(&Err) -> RetryDecision,
{
    let mut next_backoff_upper = config.initial_backoff;
    let mut retries = 0;

    loop {
        let error = match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => error,
        };

        let decision = decide(&error);
        match decision {
            RetryDecision::Stop => {
                return Err(RetryFailure::Stopped { last_error: error });
            }
            RetryDecision::RetryImmediately | RetryDecision::RetryWithBackoff => {}
        }

        if retries == config.max_retries {
            return Err(RetryFailure::Exhausted {
                retries,
                last_error: error,
            });
        }
        retries += 1;

        if decision == RetryDecision::RetryImmediately {
            continue;
        }

        let upper = next_backoff_upper;
        let delay = upper
            .mul_f64(environment.with_rng(|rng| rng.random_range(0.5..=1.0)))
            .max(Duration::from_nanos(1));
        next_backoff_upper = upper.saturating_mul(2).min(config.max_backoff);
        environment.sleep(delay).await;
    }
}
