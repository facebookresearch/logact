/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use agentbus_api::MonotonicInstant;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use bytes::Bytes;

/// In-memory state for a write-once address space.
///
/// Uses a nested HashMap internally. Each (space_id, address) pair can only be
/// written once; subsequent writes return `WriteOnceError::AddressAlreadyExists`.
/// An entry past its expiry is treated as absent, so its address can be written
/// again.
#[derive(Default)]
pub struct InMemoryWriteOnceSpaceState {
    spaces: HashMap<String, HashMap<u64, Entry>>,
}

struct Entry {
    value: Bytes,
    expires_at: Option<MonotonicInstant>,
}

impl Entry {
    fn is_live(&self, now: Option<MonotonicInstant>) -> bool {
        match (self.expires_at, now) {
            (Some(deadline), Some(now)) => now < deadline,
            _ => true,
        }
    }
}

impl InMemoryWriteOnceSpaceState {
    pub fn new() -> Self {
        Self {
            spaces: HashMap::new(),
        }
    }

    pub fn write(
        &mut self,
        space_id: &str,
        address: u64,
        value: Bytes,
        now: Option<MonotonicInstant>,
        expires_at: Option<MonotonicInstant>,
    ) -> WriteOnceResult<()> {
        let space = self.spaces.entry(space_id.to_string()).or_default();
        if space.get(&address).is_some_and(|entry| entry.is_live(now)) {
            return Err(WriteOnceError::AddressAlreadyExists(address));
        }
        space.insert(address, Entry { value, expires_at });
        Ok(())
    }

    pub fn read(
        &self,
        space_id: &str,
        address: u64,
        now: Option<MonotonicInstant>,
    ) -> Option<Bytes> {
        self.spaces
            .get(space_id)?
            .get(&address)
            .filter(|entry| entry.is_live(now))
            .map(|entry| entry.value.clone())
    }

    pub fn tail(&self, space_id: &str, window_size: u64, now: Option<MonotonicInstant>) -> u64 {
        let Some(space) = self.spaces.get(space_id) else {
            return 0;
        };
        let is_live = |address: &u64| space.get(address).is_some_and(|entry| entry.is_live(now));

        let Some(max_addr) = space.keys().copied().filter(is_live).max() else {
            return 0;
        };
        let end = max_addr + 1;

        let window_start = end.saturating_sub(window_size);
        for addr in window_start..end {
            if !is_live(&addr) {
                return addr;
            }
        }
        end
    }
}
