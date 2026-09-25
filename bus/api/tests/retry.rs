/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Clock;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::retry;
use agentbus_simulator::Simulator;
use agentbus_simulator::generate_seed;
use futures::executor::block_on;
use rand::distr::Uniform;

fn retry_config(max_retries: usize) -> RetryConfig {
    RetryConfig::try_new(
        max_retries,
        Duration::from_millis(10),
        Duration::from_millis(100),
    )
    .expect("test retry configuration should be valid")
}

fn run_in_simulator<T, F, Fut>(seed: u64, make_future: F) -> (T, Duration)
where
    T: 'static,
    F: FnOnce(Rc<Simulator>) -> Fut,
    Fut: Future<Output = T> + 'static,
{
    let environment = Rc::new(Simulator::with_jitter(
        seed,
        Uniform::new(0, 1).expect("zero-jitter distribution should be valid"),
    ));
    let start_time = environment.clock.monotonic_time();
    let handle = environment.spawn(make_future(environment.clone()));
    environment.run();
    let result = block_on(handle).expect("task should complete");
    let elapsed = environment.clock.monotonic_time() - start_time;
    (result, elapsed)
}

#[test]
fn rejects_invalid_configuration() {
    assert!(RetryConfig::try_new(1, Duration::from_nanos(1), Duration::from_nanos(1)).is_ok());
    assert_eq!(
        RetryConfig::try_new(1, Duration::ZERO, Duration::from_secs(1))
            .expect_err("zero initial backoff should be rejected")
            .to_string(),
        "initial backoff must be nonzero"
    );
    assert_eq!(
        RetryConfig::try_new(1, Duration::from_secs(2), Duration::from_secs(1))
            .expect_err("initial backoff above the maximum should be rejected")
            .to_string(),
        "initial backoff 2s must not exceed maximum backoff 1s"
    );
}

#[test]
fn succeeds_without_retrying() {
    let attempts = Rc::new(Cell::new(0));
    let task_attempts = attempts.clone();
    let (result, elapsed) = run_in_simulator(0, move |environment| async move {
        retry(
            environment.as_ref(),
            retry_config(7),
            || {
                task_attempts.set(task_attempts.get() + 1);
                async { Ok::<_, &'static str>(7) }
            },
            |_| RetryDecision::Stop,
        )
        .await
    });

    assert_eq!(result.expect("operation should succeed"), 7);
    assert_eq!(attempts.get(), 1);
    assert_eq!(elapsed, Duration::ZERO);
}

#[test]
fn retries_immediately_without_sleeping() {
    let attempts = Rc::new(Cell::new(0));
    let task_attempts = attempts.clone();
    let (result, elapsed) = run_in_simulator(1, move |environment| async move {
        retry(
            environment.as_ref(),
            retry_config(7),
            || {
                let attempt = task_attempts.get() + 1;
                task_attempts.set(attempt);
                async move {
                    if attempt == 1 {
                        Err("retry")
                    } else {
                        Ok(attempt)
                    }
                }
            },
            |_| RetryDecision::RetryImmediately,
        )
        .await
    });

    assert_eq!(result.expect("second attempt should succeed"), 2);
    assert_eq!(attempts.get(), 2);
    assert_eq!(elapsed, Duration::ZERO);
}

#[test]
fn counts_immediate_and_backed_off_retries_together() {
    let attempts = Rc::new(Cell::new(0));
    let task_attempts = attempts.clone();
    let (result, elapsed) = run_in_simulator(2, move |environment| async move {
        retry(
            environment.as_ref(),
            retry_config(2),
            || {
                let attempt = task_attempts.get() + 1;
                task_attempts.set(attempt);
                async move { Err::<(), _>(attempt) }
            },
            |attempt| {
                if *attempt == 1 {
                    RetryDecision::RetryImmediately
                } else {
                    RetryDecision::RetryWithBackoff
                }
            },
        )
        .await
    });
    let failure = result.expect_err("the retry limit should be exhausted");

    assert!(matches!(
        failure,
        RetryFailure::Exhausted {
            retries: 2,
            last_error: 3,
        }
    ));
    assert_eq!(attempts.get(), 3);
    assert!((Duration::from_millis(5)..=Duration::from_millis(10)).contains(&elapsed));
}

#[test]
fn returns_an_error_the_decider_will_not_retry() {
    let (result, elapsed) = run_in_simulator(3, move |environment| async move {
        retry(
            environment.as_ref(),
            retry_config(7),
            || async { Err::<(), _>("terminal") },
            |_| RetryDecision::Stop,
        )
        .await
    });
    let failure = result.expect_err("the error should escape");

    assert!(matches!(
        failure,
        RetryFailure::Stopped {
            last_error: "terminal"
        }
    ));
    assert_eq!(elapsed, Duration::ZERO);
}

#[test]
fn retry_behavior_is_deterministic_in_the_simulator() {
    fn run_scenario(seed: u64) -> (usize, Duration) {
        let attempts = Rc::new(Cell::new(0));
        let task_attempts = attempts.clone();
        let (result, elapsed) = run_in_simulator(seed, move |environment| async move {
            retry(
                environment.as_ref(),
                RetryConfig::try_new(3, Duration::from_millis(10), Duration::from_millis(40))
                    .expect("test retry configuration should be valid"),
                || {
                    let attempt = task_attempts.get() + 1;
                    task_attempts.set(attempt);
                    async move {
                        if attempt < 4 {
                            Err(attempt)
                        } else {
                            Ok(attempt)
                        }
                    }
                },
                |_| RetryDecision::RetryWithBackoff,
            )
            .await
        });

        assert_eq!(result.expect("fourth attempt should succeed"), 4);
        (attempts.get(), elapsed)
    }

    let seed = generate_seed();
    let first = run_scenario(seed);
    let second = run_scenario(seed);

    assert_eq!(
        first, second,
        "retry behavior should be deterministic for seed {seed}"
    );
    assert!(
        (Duration::from_millis(35)..=Duration::from_millis(70)).contains(&first.1),
        "three equal-jitter sleeps should advance simulated time within their combined range for seed {seed}"
    );
}
