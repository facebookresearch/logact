/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::intention;

use crate::AgentBusError;
use crate::BusResult;

/// Return action strings in proposal order, rejecting empty groups.
pub fn intention_texts(value: &intention::Intention) -> BusResult<Vec<&str>> {
    match value {
        intention::Intention::StringIntention(text) => Ok(vec![text.as_str()]),
        intention::Intention::GroupIntention(group) => {
            if group.intentions.is_empty() {
                return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                    "group must contain at least one intention"
                )));
            }
            Ok(group.intentions.iter().map(String::as_str).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::GroupIntention;
    use agent_bus_proto_rust::agent_bus::Intention;
    use prost::Message;

    use super::*;

    fn group(members: Vec<String>) -> intention::Intention {
        intention::Intention::GroupIntention(GroupIntention {
            intentions: members,
        })
    }

    #[test]
    fn group_texts_preserve_order_and_reject_empty_groups() {
        let value = group(vec!["first".into(), "second".into()]);
        assert_eq!(intention_texts(&value).unwrap(), ["first", "second"]);
        assert!(matches!(
            intention_texts(&group(vec![])),
            Err(AgentBusError::InvalidArgument(_))
        ));
    }

    #[test]
    fn string_intention_retains_its_text_and_wire_encoding() {
        assert_eq!(
            intention_texts(&intention::Intention::StringIntention("run tool".into())).unwrap(),
            ["run tool"]
        );
        let legacy = Intention {
            intention: Some(intention::Intention::StringIntention("run tool".into())),
            ..Default::default()
        };
        assert_eq!(legacy.encode_to_vec(), b"\x0a\x08run tool");
    }
}
