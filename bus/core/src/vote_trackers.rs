/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common VoteRecord implementations for use with Decider

use agent_bus_proto_rust::agent_bus::*;

use crate::decider::VoteRecord;

/// OnByDefaultTracker commits intentions immediately upon registration.
/// The decision is sticky - votes do not affect it.
#[derive(Debug, Default)]
pub struct OnByDefaultTracker {
    decision: Option<bool>,
}

impl OnByDefaultTracker {
    pub fn new() -> Self {
        Self { decision: None }
    }
}

impl VoteRecord for OnByDefaultTracker {
    fn register(&mut self) -> Option<(bool, String)> {
        // Commit by default
        self.decision = Some(true);
        Some((true, "ON_BY_DEFAULT policy".to_string()))
    }

    fn apply_vote(&mut self, _vote: &Vote) -> Option<(bool, String)> {
        // Decision is sticky, votes do not affect it
        self.decision
            .map(|d| (d, "ON_BY_DEFAULT policy".to_string()))
    }
}

/// FirstVoterWinsTracker waits for the first boolean vote and uses that as the decision.
/// The decision is sticky - subsequent votes don't change it.
#[derive(Debug, Default)]
pub struct FirstVoterWinsTracker {
    decision: Option<bool>,
    reason: Option<String>,
}

impl FirstVoterWinsTracker {
    pub fn new() -> Self {
        Self {
            decision: None,
            reason: None,
        }
    }
}

impl VoteRecord for FirstVoterWinsTracker {
    fn register(&mut self) -> Option<(bool, String)> {
        // No immediate decision, wait for votes
        None
    }

    fn apply_vote(&mut self, vote: &Vote) -> Option<(bool, String)> {
        // If we already have a decision, return it (sticky)
        if let Some(decision) = self.decision {
            let reason = self
                .reason
                .clone()
                .unwrap_or_else(|| "No reason provided".to_string());
            return Some((decision, reason));
        }

        // Process the vote if it's a boolean vote
        if let Some(ref vote_type) = vote.abstract_vote {
            if let Some(vote_type::VoteType::BooleanVote(b)) = vote_type.vote_type {
                // This is the first vote, record it
                self.decision = Some(b);

                // Propagate the voter's reason, now carried generically on the
                // vote rather than decoded out of the opaque `info` blob.
                if !vote.reason.is_empty() {
                    self.reason = Some(vote.reason.clone());
                }

                let reason = self
                    .reason
                    .clone()
                    .unwrap_or_else(|| "No reason provided".to_string());
                return Some((b, reason));
            }
        }

        None // Not a boolean vote, keep waiting
    }
}

/// OffByDefaultTracker aborts intentions immediately upon registration.
/// The decision is sticky - votes do not affect it.
#[derive(Debug, Default)]
pub struct OffByDefaultTracker {
    decision: Option<bool>,
}

impl OffByDefaultTracker {
    pub fn new() -> Self {
        Self { decision: None }
    }
}

impl VoteRecord for OffByDefaultTracker {
    fn register(&mut self) -> Option<(bool, String)> {
        // Abort by default
        self.decision = Some(false);
        Some((false, "OFF_BY_DEFAULT policy".to_string()))
    }

    fn apply_vote(&mut self, _vote: &Vote) -> Option<(bool, String)> {
        // Decision is sticky, votes do not affect it
        self.decision
            .map(|d| (d, "OFF_BY_DEFAULT policy".to_string()))
    }
}

/// Factory function to create VoteRecords based on DeciderPolicy
pub fn create_vote_tracker_for_decider_policy(decider_policy: i32) -> Box<dyn VoteRecord> {
    match decider_policy {
        x if x == DeciderPolicy::OnByDefault as i32 => Box::new(OnByDefaultTracker::new()),
        x if x == DeciderPolicy::FirstBooleanWins as i32 => Box::new(FirstVoterWinsTracker::new()),
        x if x == DeciderPolicy::OffByDefault as i32 => Box::new(OffByDefaultTracker::new()),
        _ => Box::new(FirstVoterWinsTracker::new()), // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_on_by_default_tracker() {
        let mut tracker = OnByDefaultTracker::new();

        // Registration should commit immediately
        let decision = tracker.register();
        assert_eq!(decision, Some((true, "ON_BY_DEFAULT policy".to_string())));

        // A false vote should NOT override (decision is sticky)
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(false)),
            }),
            intention_id: 0,
            voter_config: None,
            voter_id: String::new(),
            reason: String::new(),
        };
        let decision = tracker.apply_vote(&vote);
        assert_eq!(decision, Some((true, "ON_BY_DEFAULT policy".to_string())));

        // A true vote should also be ignored
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(true)),
            }),
            intention_id: 0,
            voter_config: None,
            voter_id: String::new(),
            reason: String::new(),
        };
        let decision = tracker.apply_vote(&vote);
        assert_eq!(decision, Some((true, "ON_BY_DEFAULT policy".to_string())));
    }

    #[test]
    fn test_first_voter_wins_tracker() {
        let mut tracker = FirstVoterWinsTracker::new();

        // Registration should not decide yet
        let decision = tracker.register();
        assert_eq!(decision, None);

        // First vote should win
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(true)),
            }),
            intention_id: 0,
            voter_config: None,
            voter_id: String::new(),
            reason: String::new(),
        };
        let decision = tracker.apply_vote(&vote);
        assert_eq!(decision, Some((true, "No reason provided".to_string())));

        // Second vote should be ignored (sticky)
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(false)),
            }),
            intention_id: 0,
            voter_config: None,
            voter_id: String::new(),
            reason: String::new(),
        };
        let decision = tracker.apply_vote(&vote);
        assert_eq!(decision, Some((true, "No reason provided".to_string()))); // Still true!
    }

    #[test]
    fn test_first_voter_wins_propagates_vote_reason() {
        let mut tracker = FirstVoterWinsTracker::new();
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(false)),
            }),
            reason: "matched a risk pattern".to_string(),
            ..Default::default()
        };
        // The deciding vote's reason is carried straight through from the
        // generic field — no `info` decoding.
        assert_eq!(
            tracker.apply_vote(&vote),
            Some((false, "matched a risk pattern".to_string()))
        );
    }

    #[test]
    fn test_off_by_default_tracker() {
        let mut tracker = OffByDefaultTracker::new();

        // Registration should abort immediately
        let decision = tracker.register();
        assert_eq!(decision, Some((false, "OFF_BY_DEFAULT policy".to_string())));

        // A true vote should NOT override (decision is sticky)
        let vote = Vote {
            abstract_vote: Some(VoteType {
                vote_type: Some(vote_type::VoteType::BooleanVote(true)),
            }),
            intention_id: 0,
            voter_config: None,
            voter_id: String::new(),
            reason: String::new(),
        };
        let decision = tracker.apply_vote(&vote);
        assert_eq!(decision, Some((false, "OFF_BY_DEFAULT policy".to_string())));
    }

    #[test]
    fn test_factory() {
        // Test that factory creates the right types
        let tracker = create_vote_tracker_for_decider_policy(DeciderPolicy::OnByDefault as i32);
        assert!(format!("{:?}", tracker).contains("OnByDefault"));

        let tracker =
            create_vote_tracker_for_decider_policy(DeciderPolicy::FirstBooleanWins as i32);
        assert!(format!("{:?}", tracker).contains("FirstVoterWins"));

        let tracker = create_vote_tracker_for_decider_policy(DeciderPolicy::OffByDefault as i32);
        assert!(format!("{:?}", tracker).contains("OffByDefault"));
    }
}
