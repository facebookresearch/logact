/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The simulator's network model: latency and loss that wrappers inject into
//! the bottom-most APIs they wrap.

use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use agentbus_api::environment::Environment;
use futures::FutureExt as _;
use futures::future;
use futures::select_biased;
use rand::RngExt as _;
use rand::rngs::StdRng;
use rand_distr::Distribution as _;
use rand_distr::Zipf;

use crate::Simulator;

/// Latency injected into every request and every response that crosses a
/// bottom-most API. A caller whose response takes longer than
/// [`LatencyConfig::TIMEOUT`] gets a timeout error, while its request still
/// lands. A failure is modeled as a lost hop: a lost request never reaches the
/// backend, and a lost response never reaches the caller, who times out.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencyConfig {
    /// Each hop's latency is drawn from `distribution`, up to `max_latency`...
    pub max_latency: Duration,
    /// ...unless the hop is lost, which happens with this probability.
    pub loss_fraction: f64,
    pub distribution: LatencyDistribution,
}

/// How a hop's latency is drawn, up to [`LatencyConfig::max_latency`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LatencyDistribution {
    /// Uniform in `[0, max_latency]`.
    Uniform,
    /// `k` milliseconds for `k` in `[1, max_latency]`, with probability
    /// proportional to `1 / k^exponent`: mostly fast hops with a heavy tail.
    Zipf { exponent: f64 },
}

impl LatencyConfig {
    /// Not set by scenario; roughly matching real client timeout settings.
    pub const TIMEOUT: Duration = Duration::from_secs(60);

    /// A hop's latency, or `None` if the hop is lost.
    fn sample(&self, rng: &mut StdRng) -> Option<Duration> {
        if rng.random_bool(self.loss_fraction) {
            return None;
        }
        Some(match self.distribution {
            LatencyDistribution::Uniform => rng.random_range(Duration::ZERO..=self.max_latency),
            LatencyDistribution::Zipf { exponent } => {
                let max_ms = self.max_latency.as_millis() as f64;
                if max_ms < 1.0 {
                    return Some(Duration::ZERO);
                }
                let zipf = Zipf::new(max_ms, exponent).expect("the Zipf exponent should be valid");
                Duration::from_millis(zipf.sample(rng) as u64)
            }
        })
    }
}

/// The latency a simulator injects, which a running scenario can disable and
/// re-enable through [`Simulator::latency_injector`].
pub struct LatencyInjector {
    config: Option<LatencyConfig>,
    enabled: Cell<bool>,
}

impl LatencyInjector {
    pub(crate) fn new(config: Option<LatencyConfig>) -> Self {
        Self {
            config,
            enabled: Cell::new(true),
        }
    }

    /// Stops injecting latency into hops that start from now on. Hops already
    /// in flight keep their latency.
    pub fn disable(&self) {
        self.enabled.set(false);
    }

    /// Resumes injecting latency, undoing [`Self::disable`].
    pub fn enable(&self) {
        self.enabled.set(true);
    }

    fn active(&self) -> Option<&LatencyConfig> {
        self.config.as_ref().filter(|_| self.enabled.get())
    }
}

impl Simulator {
    /// Sets the latency [`Self::inject`] adds. `None` adds none.
    pub fn with_latency(mut self, config: Option<LatencyConfig>) -> Self {
        self.latency_injector = LatencyInjector::new(config);
        self
    }

    pub fn latency_injector(&self) -> &LatencyInjector {
        &self.latency_injector
    }

    /// Issues `op` behind a request delay, then delays its response. Returns
    /// `None` if the response misses [`LatencyConfig::TIMEOUT`]. An issued request
    /// is in flight independently of its caller, so it can land after the caller
    /// times out. A lost request never lands, and a lost response never arrives.
    pub async fn inject<T: 'static>(&self, op: impl Future<Output = T> + 'static) -> Option<T> {
        self.inject_waiting(Duration::ZERO, op).await
    }

    /// [`Self::inject`] for an operation that may legitimately wait up to `wait`
    /// before responding (e.g. a long poll), which extends its timeout.
    pub async fn inject_waiting<T: 'static>(
        &self,
        wait: Duration,
        op: impl Future<Output = T> + 'static,
    ) -> Option<T> {
        if self.latency_injector.active().is_none() {
            return Some(op.await);
        }
        let issued = self.issue(op);
        self.within(LatencyConfig::TIMEOUT + wait, async {
            let output = issued.await;
            self.delay().await;
            output
        })
        .await
    }

    /// Runs `op` behind a request delay, as its own task, unless the request is
    /// lost, in which case `op` never runs and the result never arrives.
    fn issue<T: 'static>(&self, op: impl Future<Output = T> + 'static) -> impl Future<Output = T> {
        let Some(latency) = self.sample_latency() else {
            return future::pending().right_future();
        };
        let delay = self.sleep(latency);
        let request = self.spawn(async move {
            delay.await;
            op.await
        });
        async move { request.await.expect("the issued request should complete") }.left_future()
    }

    /// Delays a response, forever if it is lost.
    async fn delay(&self) {
        match self.sample_latency() {
            Some(latency) => self.sleep(latency).await,
            None => future::pending().await,
        }
    }

    /// A hop's latency, or `None` if the hop is lost.
    fn sample_latency(&self) -> Option<Duration> {
        match self.latency_injector.active() {
            Some(config) => self.with_rng(|rng| config.sample(rng)),
            None => Some(Duration::ZERO),
        }
    }

    async fn within<T>(&self, timeout: Duration, fut: impl Future<Output = T>) -> Option<T> {
        let mut fut = pin!(fut.fuse());
        let mut deadline = pin!(self.sleep(timeout).fuse());
        select_biased! {
            output = fut => Some(output),
            () = deadline => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use agentbus_api::environment::Clock;
    use rand::SeedableRng as _;

    use super::*;
    use crate::generate_seed;

    /// Runs one injected operation and returns whether its response arrived,
    /// when it was applied (if ever), and how long its caller waited.
    fn run_injected(loss_fraction: f64, enabled: bool) -> (bool, Option<Duration>, Duration) {
        let config = LatencyConfig {
            max_latency: Duration::from_millis(10),
            loss_fraction,
            distribution: LatencyDistribution::Uniform,
        };
        let env = Rc::new(Simulator::new(generate_seed()).with_latency(Some(config)));
        if !enabled {
            env.latency_injector().disable();
        }
        let start = env.with_clock(Clock::monotonic_time);
        let applied = Rc::new(Cell::new(None));
        let outcome = Rc::new(Cell::new(None));
        env.spawn({
            let (env, applied, outcome) = (env.clone(), applied.clone(), outcome.clone());
            async move {
                let op = {
                    let (env, applied) = (env.clone(), applied.clone());
                    async move { applied.set(Some(env.with_clock(Clock::monotonic_time) - start)) }
                };
                let delivered = env.inject(op).await.is_some();
                outcome.set(Some((
                    delivered,
                    env.with_clock(Clock::monotonic_time) - start,
                )));
            }
        });
        env.run();
        let (delivered, waited) = outcome.get().expect("the caller should finish");
        (delivered, applied.get(), waited)
    }

    #[test]
    fn delivers_responses_within_the_timeout() {
        let (delivered, applied, waited) = run_injected(0.0, true);
        assert!(delivered);
        assert!(applied.is_some_and(|applied| applied <= waited));
        assert!(waited < LatencyConfig::TIMEOUT);
    }

    #[test]
    fn times_out_lost_requests_without_running_them() {
        let (delivered, applied, waited) = run_injected(1.0, true);
        assert!(!delivered, "a lost hop should time out the caller");
        assert!(
            (LatencyConfig::TIMEOUT..LatencyConfig::TIMEOUT * 2).contains(&waited),
            "the caller should time out at the timeout, got {waited:?}"
        );
        assert!(applied.is_none(), "a lost request should never run");
    }

    #[test]
    fn zipf_latency_is_mostly_fast_with_a_tail_up_to_the_max() {
        let config = LatencyConfig {
            max_latency: Duration::from_millis(100),
            loss_fraction: 0.0,
            distribution: LatencyDistribution::Zipf { exponent: 1.5 },
        };
        let mut rng = StdRng::seed_from_u64(generate_seed());
        let mut samples: Vec<_> = (0..1000)
            .map(|_| config.sample(&mut rng).expect("no hop should be lost"))
            .collect();
        samples.sort();
        assert!(
            samples
                .iter()
                .all(|sample| (Duration::from_millis(1)..=config.max_latency).contains(sample)),
            "every hop should take between 1ms and the max"
        );
        assert!(
            samples[samples.len() / 2] <= Duration::from_millis(5),
            "most hops should be fast, got median {:?}",
            samples[samples.len() / 2]
        );
        assert!(
            samples[samples.len() - 1] >= Duration::from_millis(20),
            "the tail should reach well past the median"
        );
    }

    #[test]
    fn injects_nothing_while_disabled() {
        let (delivered, applied, waited) = run_injected(1.0, false);
        assert!(delivered && applied.is_some());
        assert!(waited < LatencyConfig::TIMEOUT);
    }
}
