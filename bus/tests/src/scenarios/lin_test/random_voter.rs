/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Random boolean voter for testing

use std::cell::RefCell;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BlockingPollRequest;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::Vote;
use agentbus_api::AgentBus;
use agentbus_api::environment::Environment;
use rand::RngExt as _;

pub struct RandomVoter<T: AgentBus, E: Environment> {
    agent_bus_impl: T,
    agent_bus_id: String,
    pub env: Rc<E>,
    next_log_position: RefCell<i64>,
}

impl<T: AgentBus, E: Environment> RandomVoter<T, E> {
    pub fn new(agent_bus_impl: T, agent_bus_id: String, env: Rc<E>) -> Self {
        Self {
            agent_bus_impl,
            agent_bus_id,
            env,
            next_log_position: RefCell::new(0),
        }
    }

    pub async fn poll_and_vote_once(&self, timeout_ms: i32) {
        let start_log_position = *self.next_log_position.borrow();
        let payload_types =
            vec![agent_bus_proto_rust::agent_bus::SelectivePollType::Intention as i32];
        let resp = self
            .agent_bus_impl
            .blocking_poll(BlockingPollRequest {
                agent_bus_id: self.agent_bus_id.clone(),
                bus_id: Some(BusId {
                    agent_bus_id: self.agent_bus_id.clone(),
                }),
                start_log_position,
                max_entries: 1000,
                filter: Some(agent_bus_proto_rust::agent_bus::PayloadTypeFilter { payload_types }),
                timeout_ms,
            })
            .await
            .expect("blocking_poll should succeed");

        for entry in resp.entries {
            if let Some(ref payload) = entry.payload {
                if let Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(
                    ref intention,
                )) = payload.payload
                {
                    if let Some(
                        agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(_),
                    ) = intention.intention
                    {
                        let intention_id = entry.header.as_ref().unwrap().log_position;
                        let random_vote = self.env.with_rng(|rng| rng.random_bool(0.5));

                        let vote_payload = Payload {
                                payload: Some(
                                    agent_bus_proto_rust::agent_bus::payload::Payload::Vote(Vote {
                                        abstract_vote: Some(
                                            agent_bus_proto_rust::agent_bus::VoteType {
                                                vote_type: Some(agent_bus_proto_rust::agent_bus::vote_type::VoteType::BooleanVote(random_vote)),
                                            },
                                        ),
                                        intention_id,
                                        ..Default::default()
                                    }),
                                ),
                            };

                        self.agent_bus_impl
                            .append(AppendRequest {
                                agent_bus_id: self.agent_bus_id.clone(),
                                bus_id: Some(BusId {
                                    agent_bus_id: self.agent_bus_id.clone(),
                                }),
                                payload: Some(vote_payload),
                                ..Default::default()
                            })
                            .await
                            .expect("Vote append should succeed");
                    }
                }
            }
        }
        *self.next_log_position.borrow_mut() = resp.next_start_position;
    }
}
