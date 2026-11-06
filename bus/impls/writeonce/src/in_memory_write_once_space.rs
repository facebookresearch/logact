/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::MonotonicInstant;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;

use crate::in_memory_write_once_space_state::InMemoryWriteOnceSpaceState;

/// In-memory implementation of a write-once address space.
///
/// This is a cloneable wrapper around `InMemoryWriteOnceSpaceState` using `Rc<RefCell<>>`.
/// All clones share the same underlying state. With a TTL, each write expires that
/// long after it is stored, measured on the environment's monotonic clock.
#[derive(Clone)]
pub struct InMemoryWriteOnceSpace {
    state: Rc<RefCell<InMemoryWriteOnceSpaceState>>,
    expiry: Option<Expiry>,
}

#[derive(Clone)]
struct Expiry {
    ttl: Duration,
    now: Rc<dyn Fn() -> MonotonicInstant>,
}

impl Default for InMemoryWriteOnceSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWriteOnceSpace {
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(InMemoryWriteOnceSpaceState::new())),
            expiry: None,
        }
    }

    /// Expire each write `ttl` after it is stored.
    pub fn with_ttl<E: Environment + 'static>(environment: Rc<E>, ttl: Duration) -> Self {
        Self {
            expiry: Some(Expiry {
                ttl,
                now: Rc::new(move || environment.with_clock(|clock| clock.monotonic_time())),
            }),
            ..Self::new()
        }
    }

    fn now(&self) -> Option<MonotonicInstant> {
        self.expiry.as_ref().map(|expiry| (expiry.now)())
    }
}

impl TailableSpace for InMemoryWriteOnceSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        if window_size == 0 {
            return Err(TailError::ZeroWindowSize);
        }

        Ok(self.state.borrow().tail(space_id, window_size, self.now()))
    }
}

impl WriteOnceSpace for InMemoryWriteOnceSpace {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let now = self.now();
        let expires_at = now
            .zip(self.expiry.as_ref())
            .map(|(now, expiry)| now + expiry.ttl);
        self.state
            .borrow_mut()
            .write(space_id, address, value, now, expires_at)
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        Ok(self.state.borrow().read(space_id, address, self.now()))
    }
}
