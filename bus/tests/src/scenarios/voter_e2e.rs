/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(agentbus_voter_e2e_list)]
mod defs {
    //! End-to-end voter tests: append intention → VoterLoop.poll_and_vote() → verify vote on bus.
    //!
    //! Tests each voter type through the full bus round-trip using the fixture
    //! pattern, so they run across all AgentBus backends including fault-injecting ones.

    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::*;
    use agentbus_api::AgentBus;
    use agentbus_api::environment::Environment;
    use agentbus_core::voter::VoterLoop;

    use crate::fixtures::AgentBusTestFixture;

    const BUS_ID: &str = "voter-e2e";

    async fn append_intention(bus: &impl AgentBus, intention: &str) -> i64 {
        bus.append(AppendRequest {
            agent_bus_id: BUS_ID.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: BUS_ID.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Intention(Intention {
                    intention: Some(intention::Intention::StringIntention(intention.to_string())),
                    ..Default::default()
                })),
            }),
        })
        .await
        .expect("append intention")
        .log_position
    }

    async fn poll_votes(bus: &impl AgentBus) -> Vec<Vote> {
        bus.poll(PollRequest {
            agent_bus_id: BUS_ID.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: BUS_ID.to_string(),
            }),
            start_log_position: 0,
            max_entries: 100,
            filter: Some(PayloadTypeFilter {
                payload_types: vec![SelectivePollType::Vote as i32],
            }),
        })
        .await
        .expect("poll votes")
        .entries
        .into_iter()
        .filter_map(|e| match e.payload?.payload? {
            payload::Payload::Vote(v) => Some(v),
            _ => None,
        })
        .collect()
    }

    /// Append intention → poll_and_vote → return the resulting vote for caller assertions.
    async fn run_voter_round_trip<T: AgentBus + Clone, E: Environment>(
        bus: T,
        env: Rc<E>,
        voter: Box<dyn agentbus_api::voter::Voter>,
        intention: &str,
        expect_approve: bool,
    ) -> Vote {
        let intention_pos = append_intention(&bus, intention).await;
        let mut voter_loop = VoterLoop::new(bus.clone(), BUS_ID.to_string(), 0, env, voter);

        let processed: usize = voter_loop.poll_and_vote(None).await.expect("poll_and_vote");
        assert_eq!(processed, 1, "should process exactly 1 entry");

        let votes = poll_votes(&bus).await;
        assert_eq!(votes.len(), 1, "should have exactly 1 vote on bus");

        let vote = votes.into_iter().next().unwrap();
        assert_eq!(vote.intention_id, intention_pos);
        let approved = match vote.abstract_vote.as_ref().unwrap().vote_type {
            Some(vote_type::VoteType::BooleanVote(b)) => b,
            _ => panic!("expected boolean vote"),
        };
        assert_eq!(approved, expect_approve);
        vote
    }

    // -- LLM voter (mock client) ------------------------------------------------

    struct MockLlmClient {
        response: String,
    }

    #[async_trait::async_trait(?Send)]
    impl agentbus_voter_llm::LlmClient for MockLlmClient {
        async fn chat_completion(
            &self,
            _model: &str,
            _api_endpoint: &str,
            _prompt: &str,
        ) -> Result<String, String> {
            Ok(self.response.clone())
        }
    }

    #[scenario]
    pub async fn run_test_voter_llm_e2e<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let client = Rc::new(MockLlmClient {
            response: "<safe>true</safe><reason>harmless</reason><concerns>none</concerns>"
                .to_string(),
        });
        let voter =
            agentbus_voter_llm::LlmVoter::new(client, agentbus_voter_llm::VoterConfig::default());
        let vote = run_voter_round_trip(
            fixture.create_impl(),
            fixture.get_env(),
            Box::new(voter),
            "echo hello",
            true,
        )
        .await;

        assert_eq!(vote.reason, "harmless");
        assert_eq!(vote.voter_config, None);
        Ok(())
    }

    // -- Rule-based voter -------------------------------------------------------

    #[scenario]
    pub async fn run_test_voter_rule_based_allow_e2e<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let voter = agentbus_voter_rule_based::RuleBasedVoter::new(vec![
            agentbus_voter_rule_based::Rule {
                pattern: "echo".to_string(),
                match_type: agentbus_voter_rule_based::MatchType::Substring,
                action: agentbus_voter_rule_based::RuleAction::Allow,
                reason: "safe".to_string(),
                subjects: vec![],
            },
        ])?;
        let vote = run_voter_round_trip(
            fixture.create_impl(),
            fixture.get_env(),
            Box::new(voter),
            "echo hello",
            true,
        )
        .await;

        assert_eq!(vote.reason, "safe");
        assert_eq!(vote.voter_config, None);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_voter_rule_based_deny_e2e<F>(fixture: &F) -> anyhow::Result<()>
    where
        F: AgentBusTestFixture,
        F::Impl: Clone,
    {
        let voter = agentbus_voter_rule_based::RuleBasedVoter::new(vec![
            agentbus_voter_rule_based::Rule {
                pattern: "rm".to_string(),
                match_type: agentbus_voter_rule_based::MatchType::Substring,
                action: agentbus_voter_rule_based::RuleAction::Deny,
                reason: "destructive".to_string(),
                subjects: vec![],
            },
        ])?;
        let vote = run_voter_round_trip(
            fixture.create_impl(),
            fixture.get_env(),
            Box::new(voter),
            "rm -rf /",
            false,
        )
        .await;

        assert_eq!(vote.reason, "destructive");
        assert_eq!(vote.voter_config, None);
        Ok(())
    }
}
pub use defs::*;
