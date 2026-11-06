/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Entry expiry by wall-clock time, as a storage server expires entries by its own
//! clock.

use std::rc::Rc;
use std::time::Duration;

use crate::Clock;
use crate::Environment;
use crate::UnsafeWallTime;

/// Expires stored entries `ttl` after they are written, by the environment's
/// unsafe wall clock.
#[derive(Clone)]
pub struct WallClockTtl {
    ttl: Duration,
    now: Rc<dyn Fn() -> UnsafeWallTime>,
}

impl WallClockTtl {
    pub fn new<E: Environment + 'static>(environment: Rc<E>, ttl: Duration) -> Self {
        Self {
            ttl,
            now: Rc::new(move || environment.with_clock(|clock| clock.unsafe_wall_time())),
        }
    }

    /// Read the wall clock once for an operation.
    pub fn check(&self) -> TtlCheck {
        TtlCheck {
            now: (self.now)(),
            ttl: self.ttl,
        }
    }
}

/// One operation's view of the wall clock.
#[derive(Clone, Copy, Debug)]
pub struct TtlCheck {
    now: UnsafeWallTime,
    ttl: Duration,
}

impl TtlCheck {
    /// The wall time to record as an entry's write time.
    pub fn now(&self) -> UnsafeWallTime {
        self.now
    }

    /// Whether an entry written at `written_at` has not yet expired.
    pub fn is_live(&self, written_at: UnsafeWallTime) -> bool {
        self.now.as_millis() < written_at.as_millis() + self.ttl.as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_expire_ttl_after_their_write_time() {
        let at = |ms| UnsafeWallTime::from_duration_since_epoch(Duration::from_millis(ms));
        let check = |now| TtlCheck {
            now: at(now),
            ttl: Duration::from_millis(100),
        };
        assert!(check(1099).is_live(at(1000)));
        assert!(!check(1100).is_live(at(1000)));
    }
}
