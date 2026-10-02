/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Validation utilities for AgentBus

use agent_bus_proto_rust::agent_bus::BusId;
use anyhow::Result;

use crate::AgentBusError;
use crate::BusResult;

/// Minimum length for a bus ID
pub const MIN_BUS_ID_LEN: usize = 1;

/// Maximum length for a bus ID
pub const MAX_BUS_ID_LEN: usize = 256;

/// Checks if a character is valid for a bus ID.
/// Valid characters are ASCII alphanumeric, hyphens, underscores, dots, and slashes.
pub fn is_valid_bus_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/'
}

/// Validates that a request contains a non-empty, well-formed typed bus ID.
pub fn validate_bus_id(bus_id: Option<&BusId>) -> BusResult<&BusId> {
    let bus_id = bus_id.ok_or_else(|| {
        AgentBusError::InvalidArgument(anyhow::anyhow!("Bus ID must be provided"))
    })?;
    validate_bus_id_string(&bus_id.agent_bus_id).map_err(AgentBusError::InvalidArgument)?;
    Ok(bus_id)
}

/// Validates a string representation of a bus ID.
pub fn validate_bus_id_string(bus_id: &str) -> Result<()> {
    let len = bus_id.len();
    if len < MIN_BUS_ID_LEN || len > MAX_BUS_ID_LEN {
        return Err(anyhow::anyhow!(
            "Bus ID length must be between {} and {} characters, got {}",
            MIN_BUS_ID_LEN,
            MAX_BUS_ID_LEN,
            len
        ));
    }

    if !bus_id.chars().all(is_valid_bus_id_char) {
        return Err(anyhow::anyhow!(
            "Bus ID can only contain ASCII alphanumeric characters, hyphens, underscores, dots, and slashes"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_bus_id_is_required() {
        assert!(matches!(
            validate_bus_id(None),
            Err(AgentBusError::InvalidArgument(_))
        ));
    }

    #[test]
    fn typed_bus_id_must_not_be_empty() {
        assert!(matches!(
            validate_bus_id(Some(&BusId::default())),
            Err(AgentBusError::InvalidArgument(_))
        ));
    }

    #[test]
    fn typed_bus_id_is_returned_after_validation() {
        let bus_id = BusId {
            agent_bus_id: "test-bus".to_owned(),
        };
        assert_eq!(
            validate_bus_id(Some(&bus_id))
                .expect("valid bus ID should pass validation")
                .agent_bus_id,
            "test-bus"
        );
    }
}
