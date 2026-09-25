/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;

use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use bytes::Bytes;

use crate::in_memory_write_once_space_state::InMemoryWriteOnceSpaceState;

/// In-memory implementation of a write-once address space.
///
/// This is a cloneable wrapper around `InMemoryWriteOnceSpaceState` using `Rc<RefCell<>>`.
/// All clones share the same underlying state.
#[derive(Clone)]
pub struct InMemoryWriteOnceSpace {
    state: Rc<RefCell<InMemoryWriteOnceSpaceState>>,
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
        }
    }
}

impl TailableSpace for InMemoryWriteOnceSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        Ok(self.state.borrow().tail(space_id, window_size))
    }
}

impl WriteOnceSpace for InMemoryWriteOnceSpace {
    async fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        self.state.borrow_mut().write(space_id, address, value)
    }

    async fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        self.state.borrow().read(space_id, address)
    }
}
