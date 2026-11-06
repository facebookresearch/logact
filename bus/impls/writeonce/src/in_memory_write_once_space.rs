/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agentbus_api::Environment;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::TtlCheck;
use agentbus_api::WallClockTtl;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;

use crate::in_memory_write_once_space_state::InMemoryWriteOnceSpaceState;

/// In-memory implementation of a write-once address space.
///
/// This is a cloneable wrapper around `InMemoryWriteOnceSpaceState` using `Rc<RefCell<>>`.
/// All clones share the same underlying state. With a TTL, each write expires that
/// long after it is stored, by the environment's unsafe wall clock.
#[derive(Clone)]
pub struct InMemoryWriteOnceSpace {
    state: Rc<RefCell<InMemoryWriteOnceSpaceState>>,
    ttl: Option<WallClockTtl>,
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
            ttl: None,
        }
    }

    /// Expire each write `ttl` after it is stored.
    pub fn with_ttl<E: Environment + 'static>(environment: Rc<E>, ttl: Duration) -> Self {
        Self {
            ttl: Some(WallClockTtl::new(environment, ttl)),
            ..Self::new()
        }
    }

    fn check(&self) -> Option<TtlCheck> {
        self.ttl.as_ref().map(WallClockTtl::check)
    }
}

impl TailableSpace for InMemoryWriteOnceSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        if window_size == 0 {
            return Err(TailError::ZeroWindowSize);
        }

        Ok(self
            .state
            .borrow()
            .tail(space_id, window_size, self.check()))
    }
}

impl WriteOnceSpace for InMemoryWriteOnceSpace {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        self.state
            .borrow_mut()
            .write(space_id, address, value, self.check())
    }

    async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
        Ok(self.state.borrow().read(space_id, address, self.check()))
    }
}
