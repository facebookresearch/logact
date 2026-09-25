/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! WriteOnce-based AgentBus implementation.
//!
//! This crate provides an AgentBus implementation backed by write-once address spaces.
//! It includes:
//! - `WriteOnceAgentBus`: An AgentBus that uses WriteOnceSpace for storage
//! - `ChanneledWriteOnceSpace`: A channel-based wrapper for async WriteOnceSpace access
//! - `InMemoryWriteOnceSpace`: A simple in-memory WriteOnceSpace implementation

use agentbus_api::WriteOnceError;

mod channeled_write_once_space;
mod in_memory_write_once_space;
mod in_memory_write_once_space_state;
mod observable_write_once_space;
mod write_once_agentbus;

pub use channeled_write_once_space::ChanneledWriteOnceSpace;
pub use channeled_write_once_space::ChanneledWriteOnceSpaceBackend;
pub use in_memory_write_once_space::InMemoryWriteOnceSpace;
pub use observable_write_once_space::ObservableWriteOnceSpace;
pub use write_once_agentbus::WriteOnceAgentBus;

fn write_once_error_message(error: &WriteOnceError) -> String {
    match error {
        WriteOnceError::AddressAlreadyExists(_) => error.to_string(),
        WriteOnceError::TransactionConflict(source)
        | WriteOnceError::Timeout(source)
        | WriteOnceError::BackendUnavailable(source)
        | WriteOnceError::InternalError(source) => format!("{error}: {source:#}"),
    }
}
