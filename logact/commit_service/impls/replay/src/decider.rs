/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::payload;
use logact_commit_service_engine::decider_transitions::first_boolean_wins;
use logact_commit_service_engine::decider_transitions::off_by_default;
use logact_commit_service_engine::decider_transitions::on_by_default;
use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state::FirstBooleanWinsState;
use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state::ReplayDeciderState;
use logact_commit_service_policy_proto_rust::logact_commit_service_engine_state::replay_decider_state;

/// One decider policy with its in-memory state, using V1's transitions.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum DeciderMachine {
    #[default]
    OnByDefault,
    OffByDefault,
    FirstBooleanWins {
        pending: Option<i64>,
    },
}

impl DeciderMachine {
    pub fn new(policy: DeciderPolicy) -> Self {
        match policy {
            DeciderPolicy::OnByDefault => Self::OnByDefault,
            DeciderPolicy::OffByDefault => Self::OffByDefault,
            DeciderPolicy::FirstBooleanWins => Self::FirstBooleanWins { pending: None },
        }
    }

    /// The decider's state as stored, or `None` for a decider without state.
    pub fn to_stored(&self) -> Option<ReplayDeciderState> {
        match self {
            Self::OnByDefault | Self::OffByDefault => None,
            Self::FirstBooleanWins { pending } => Some(ReplayDeciderState {
                state: Some(replay_decider_state::State::FirstBooleanWins(
                    FirstBooleanWinsState { pending: *pending },
                )),
            }),
        }
    }

    /// The decider `policy` names, with the state `stored` holds, if any.
    pub fn from_stored(
        policy: DeciderPolicy,
        stored: Option<ReplayDeciderState>,
    ) -> anyhow::Result<Self> {
        let mut decider = Self::new(policy);
        match (&mut decider, stored.and_then(|stored| stored.state)) {
            (_, None) => {}
            (
                Self::FirstBooleanWins { pending },
                Some(replay_decider_state::State::FirstBooleanWins(state)),
            ) => *pending = state.pending,
            (decider, Some(state)) => {
                anyhow::bail!(
                    "stored decider state {state:?} doesn't match the {decider:?} decider"
                )
            }
        }
        Ok(decider)
    }

    /// Apply the payload at `position`, returning any decision.
    pub fn step(&mut self, position: i64, payload: &payload::Payload) -> Option<Payload> {
        match self {
            Self::OnByDefault => on_by_default(position, payload),
            Self::OffByDefault => off_by_default(position, payload),
            Self::FirstBooleanWins { pending } => {
                // A logged decision settles the pending intention, so a late
                // vote for it cannot decide it again.
                if pending.is_some() && decided_intention(payload) == *pending {
                    *pending = None;
                    return None;
                }
                first_boolean_wins(pending, position, payload)
            }
        }
    }
}

/// The intention a `Commit` or `Abort` decides.
pub fn decided_intention(payload: &payload::Payload) -> Option<i64> {
    match payload {
        payload::Payload::Commit(commit) => Some(commit.intention_id),
        payload::Payload::Abort(abort) => Some(abort.intention_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use agent_bus_proto_rust::agent_bus::Abort;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::Vote;
    use agent_bus_proto_rust::agent_bus::VoteType;
    use agent_bus_proto_rust::agent_bus::vote_type;

    use super::*;

    fn approving_vote(intention_id: i64) -> payload::Payload {
        payload::Payload::Vote(Vote {
            intention_id,
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(true)),
            }),
            ..Default::default()
        })
    }

    #[test]
    fn a_logged_decision_settles_the_pending_intention() {
        let mut decider = DeciderMachine::new(DeciderPolicy::FirstBooleanWins);
        assert!(
            decider
                .step(5, &payload::Payload::Intention(Intention::default()))
                .is_none()
        );
        assert_eq!(
            decider,
            DeciderMachine::FirstBooleanWins { pending: Some(5) }
        );

        let abort = payload::Payload::Abort(Abort {
            intention_id: 5,
            ..Default::default()
        });
        assert!(decider.step(6, &abort).is_none());
        assert_eq!(decider, DeciderMachine::FirstBooleanWins { pending: None });
        assert!(
            decider.step(7, &approving_vote(5)).is_none(),
            "a late vote must not decide a settled intention"
        );
    }

    #[test]
    fn a_vote_for_the_pending_intention_decides_it() {
        let mut decider = DeciderMachine::new(DeciderPolicy::FirstBooleanWins);
        decider.step(5, &payload::Payload::Intention(Intention::default()));
        let decision = decider
            .step(6, &approving_vote(5))
            .expect("the first boolean vote decides");
        assert!(matches!(
            decision.payload,
            Some(payload::Payload::Commit(ref commit)) if commit.intention_id == 5
        ));
        assert_eq!(decider, DeciderMachine::FirstBooleanWins { pending: None });
    }
}
