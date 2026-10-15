/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Canonical encoding and decoding for AgentBus identifiers.

use std::fmt;

use anyhow::ensure;

/// A parsed AgentBus identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AgentBusId<'a> {
    namespace: &'a str,
    user: &'a str,
    session_id: &'a str,
    subagent_id: Option<&'a str>,
}

impl<'a> AgentBusId<'a> {
    /// Construct an AgentBus identifier from its components.
    pub fn try_new(
        namespace: &'a str,
        user: &'a str,
        session_id: &'a str,
        subagent_id: Option<&'a str>,
    ) -> anyhow::Result<Self> {
        ensure_component("namespace", namespace)?;
        ensure_component("user", user)?;
        ensure_component("session ID", session_id)?;
        if let Some(subagent_id) = subagent_id {
            ensure_component("subagent ID", subagent_id)?;
        }

        Ok(Self {
            namespace,
            user,
            session_id,
            subagent_id,
        })
    }

    /// Decode an AgentBus identifier in its canonical wire format.
    pub fn parse(raw: &'a str) -> anyhow::Result<Self> {
        let mut parts = raw.split('.');
        let namespace = parts.next().unwrap_or_default();
        let user = parts.next().unwrap_or_default();
        let session_id = parts.next().unwrap_or_default();
        let subagent_id = match (parts.next(), parts.next(), parts.next()) {
            (None, None, None) => None,
            (Some("agent"), Some(subagent_id), None) => Some(subagent_id),
            _ => return Err(invalid_agent_bus_id(raw)),
        };

        Self::try_new(namespace, user, session_id, subagent_id)
            .map_err(|_| invalid_agent_bus_id(raw))
    }

    pub fn namespace(&self) -> &'a str {
        self.namespace
    }

    pub fn user(&self) -> &'a str {
        self.user
    }

    pub fn session_id(&self) -> &'a str {
        self.session_id
    }

    pub fn subagent_id(&self) -> Option<&'a str> {
        self.subagent_id
    }
}

impl fmt::Display for AgentBusId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}.{}.{}",
            self.namespace, self.user, self.session_id
        )?;
        if let Some(subagent_id) = self.subagent_id {
            write!(formatter, ".agent.{subagent_id}")?;
        }
        Ok(())
    }
}

fn ensure_component(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.is_empty(), "AgentBus {name} must not be empty");
    ensure!(!value.contains('.'), "AgentBus {name} must not contain `.`");
    Ok(())
}

fn invalid_agent_bus_id(raw: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "agent bus id `{raw}` must match `<namespace>.<user>.<session_id>[.agent.<subagent_id>]`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_id_round_trips() {
        let id = AgentBusId::try_new("test", "alice", "session-123", None)
            .expect("valid root AgentBus ID should construct");

        assert_eq!(id.to_string(), "test.alice.session-123");
        assert_eq!(
            AgentBusId::parse(&id.to_string()).expect("encoded ID should parse"),
            id
        );
    }

    #[test]
    fn subagent_id_round_trips() {
        let id = AgentBusId::try_new("claude", "alice", "session-123", Some("agent-abc"))
            .expect("valid subagent AgentBus ID should construct");

        assert_eq!(id.to_string(), "claude.alice.session-123.agent.agent-abc");
        assert_eq!(id.namespace(), "claude");
        assert_eq!(id.user(), "alice");
        assert_eq!(id.session_id(), "session-123");
        assert_eq!(id.subagent_id(), Some("agent-abc"));
    }

    #[test]
    fn malformed_ids_are_rejected() {
        for raw in [
            "",
            "alice",
            ".alice.session-123",
            "claude..session-123",
            "claude.alice.",
            "claude.alice",
            "claude.alice.session-123.foo",
            "claude.alice.session-123.agent",
            "claude.alice.session-123.agent.",
        ] {
            assert!(
                AgentBusId::parse(raw).is_err(),
                "`{raw}` should fail validation"
            );
        }
    }
}
