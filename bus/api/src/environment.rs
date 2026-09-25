/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rand::SeedableRng as _;
use rand::rngs::StdRng;
use rand::rngs::SysRng;

/// A wall-clock timestamp that may jump backward or forward.
///
/// Never use this value for deadlines, elapsed-time measurement, ordering,
/// versioning, or any other correctness decision. It exists only for
/// informational timestamps that must correspond approximately to civil time.
/// The type intentionally provides no comparison or arithmetic operations.
#[derive(Clone, Copy, Debug)]
pub struct UnsafeWallTime(Duration);

impl UnsafeWallTime {
    /// Construct a wall-clock timestamp from a duration since the clock's epoch.
    pub const fn from_duration_since_epoch(duration: Duration) -> Self {
        Self(duration)
    }

    /// Return this timestamp as milliseconds since the clock's epoch.
    pub fn as_millis(&self) -> u128 {
        self.0.as_millis()
    }
}

/// A point on a monotonic clock's implementation-defined timeline.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonotonicInstant(Duration);

impl MonotonicInstant {
    /// Construct an instant from elapsed time since a monotonic clock's origin.
    pub const fn from_duration_since_clock_origin(duration: Duration) -> Self {
        Self(duration)
    }

    /// Return the nonnegative duration elapsed since `earlier`.
    pub fn duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

impl std::ops::Add<Duration> for MonotonicInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(self.0 + duration)
    }
}

impl std::ops::Sub for MonotonicInstant {
    type Output = Duration;

    fn sub(self, earlier: Self) -> Self::Output {
        self.duration_since(earlier)
    }
}

/// Clock abstraction for wall-clock timestamps and elapsed-time measurement.
pub trait Clock {
    /// Return the current non-monotonic wall-clock time.
    ///
    /// This value is unsafe for correctness decisions; use `monotonic_time`
    /// unless producing an informational timestamp.
    fn unsafe_wall_time(&self) -> UnsafeWallTime;

    /// Return the current point on a monotonic timeline.
    fn monotonic_time(&self) -> MonotonicInstant;
}

/// Real-time clock that uses system time
pub struct RealClock {
    monotonic_origin: Instant,
}

impl RealClock {
    pub fn new() -> Self {
        Self {
            monotonic_origin: Instant::now(),
        }
    }
}

impl Clock for RealClock {
    fn unsafe_wall_time(&self) -> UnsafeWallTime {
        UnsafeWallTime::from_duration_since_epoch(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("SystemTime should be after UNIX_EPOCH"),
        )
    }

    fn monotonic_time(&self) -> MonotonicInstant {
        MonotonicInstant(self.monotonic_origin.elapsed())
    }
}

/// Environment trait that allows the same code to run in production or simulation tests
/// Provides access to clock and RNG for deterministic testing
pub trait Environment {
    type Clock: Clock;

    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut StdRng) -> R;

    fn with_clock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Self::Clock) -> R;

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + 'static;

    fn spawn_local(&self, fut: impl std::future::Future<Output = ()> + 'static);
}

/// Production environment using real clock and entropy-seeded RNG
pub struct RealEnvironment {
    rng: std::cell::RefCell<StdRng>,
    clock: RealClock,
}

impl RealEnvironment {
    pub fn new() -> Self {
        Self {
            rng: std::cell::RefCell::new(StdRng::try_from_rng(&mut SysRng).unwrap()),
            clock: RealClock::new(),
        }
    }
}

impl Environment for RealEnvironment {
    type Clock = RealClock;

    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut StdRng) -> R,
    {
        f(&mut self.rng.borrow_mut())
    }

    fn with_clock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Self::Clock) -> R,
    {
        f(&self.clock)
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + 'static {
        tokio::time::sleep(duration)
    }

    fn spawn_local(&self, fut: impl std::future::Future<Output = ()> + 'static) {
        tokio::task::spawn_local(fut);
    }
}
