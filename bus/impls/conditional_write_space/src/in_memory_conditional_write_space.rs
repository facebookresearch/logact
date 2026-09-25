/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use bytes::Bytes;

/// In-memory implementation of a conditional write space.
///
/// All clones share the same underlying state.
#[derive(Clone)]
pub struct InMemoryConditionalWriteSpace {
    state: Rc<RefCell<HashMap<String, HashMap<u64, VersionedValue>>>>,
}

impl Default for InMemoryConditionalWriteSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryConditionalWriteSpace {
    pub fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(HashMap::new())),
        }
    }
}

fn version_from_u64(n: u64) -> Version {
    Version(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn version_to_u64(v: &Version) -> u64 {
    let bytes: [u8; 8] = v.0[..]
        .try_into()
        .expect("in-memory version is always 8 bytes");
    u64::from_be_bytes(bytes)
}

impl TailableSpace for InMemoryConditionalWriteSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let state = self.state.borrow();
        let Some(space) = state.get(space_id) else {
            return Ok(0);
        };

        let Some(&max_addr) = space.keys().max() else {
            return Ok(0);
        };
        let end = max_addr + 1;

        let window_start = end.saturating_sub(window_size);
        for addr in window_start..end {
            if !space.contains_key(&addr) {
                return Ok(addr);
            }
        }
        Ok(end)
    }
}

impl ConditionalWriteSpace for InMemoryConditionalWriteSpace {
    async fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> ConditionalWriteResult<bool> {
        let mut state = self.state.borrow_mut();
        let space = state.entry(space_id.to_string()).or_default();
        let existing = space.get(&address);

        match (&expected_version, existing) {
            (None, None) => {}
            (Some(expected), Some(actual)) if *expected == actual.version => {}
            _ => return Ok(false),
        }

        let next_version = match expected_version {
            None => 0u64,
            Some(v) => version_to_u64(&v) + 1,
        };

        space.insert(
            address,
            VersionedValue {
                version: version_from_u64(next_version),
                value,
            },
        );
        Ok(true)
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        Ok(self
            .state
            .borrow()
            .get(space_id)
            .and_then(|space| space.get(&address))
            .cloned())
    }
}
