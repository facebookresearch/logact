/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use agentbus_api::TtlCheck;
use agentbus_api::UnsafeWallTime;
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
    written_at: Option<UnsafeWallTime>,
}

impl Entry {
    fn is_live(&self, check: Option<TtlCheck>) -> bool {
        match (self.written_at, check) {
            (Some(written_at), Some(check)) => check.is_live(written_at),
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
        check: Option<TtlCheck>,
    ) -> WriteOnceResult<()> {
        let space = self.spaces.entry(space_id.to_string()).or_default();
        if space
            .get(&address)
            .is_some_and(|entry| entry.is_live(check))
        {
            return Err(WriteOnceError::AddressAlreadyExists(address));
        }
        let written_at = check.map(|check| check.now());
        space.insert(address, Entry { value, written_at });
        Ok(())
    }

    pub fn read(&self, space_id: &str, address: u64, check: Option<TtlCheck>) -> Option<Bytes> {
        self.spaces
            .get(space_id)?
            .get(&address)
            .filter(|entry| entry.is_live(check))
            .map(|entry| entry.value.clone())
    }

    pub fn tail(&self, space_id: &str, window_size: u64, check: Option<TtlCheck>) -> u64 {
        let Some(space) = self.spaces.get(space_id) else {
            return 0;
        };
        let is_live = |address: &u64| space.get(address).is_some_and(|entry| entry.is_live(check));

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
