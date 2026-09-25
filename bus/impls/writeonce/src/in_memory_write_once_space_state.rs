/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use bytes::Bytes;

/// In-memory state for a write-once address space.
///
/// Uses a nested HashMap internally. Each (space_id, address) pair can only be
/// written once; subsequent writes return `WriteOnceError::AddressAlreadyExists`.
#[derive(Default)]
pub struct InMemoryWriteOnceSpaceState {
    spaces: HashMap<String, HashMap<u64, Bytes>>,
}

impl InMemoryWriteOnceSpaceState {
    pub fn new() -> Self {
        Self {
            spaces: HashMap::new(),
        }
    }

    pub fn write(&mut self, space_id: &str, address: u64, value: Bytes) -> WriteOnceResult<()> {
        let space = self.spaces.entry(space_id.to_string()).or_default();
        if space.contains_key(&address) {
            return Err(WriteOnceError::AddressAlreadyExists(address));
        }
        space.insert(address, value);
        Ok(())
    }

    pub fn read(&self, space_id: &str, address: u64) -> Option<Bytes> {
        self.spaces.get(space_id)?.get(&address).cloned()
    }

    pub fn tail(&self, space_id: &str, window_size: u64) -> u64 {
        let Some(space) = self.spaces.get(space_id) else {
            return 0;
        };

        let Some(&max_addr) = space.keys().max() else {
            return 0;
        };
        let end = max_addr + 1;

        let window_start = end.saturating_sub(window_size);
        for addr in window_start..end {
            if !space.contains_key(&addr) {
                return addr;
            }
        }
        end
    }
}
