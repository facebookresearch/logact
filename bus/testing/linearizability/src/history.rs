/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;

use agentbus_api::MonotonicInstant;
use anyhow::Result;

/// Stable identity and program-order position for one client operation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationId {
    pub client: usize,
    pub sequence: usize,
}

/// The typed result returned by a worker action.
#[derive(Clone, Debug)]
pub struct CompletedOperation<Operation, Output, Tag> {
    pub operation: Operation,
    pub output: Output,
    /// Implementation-defined evidence used to construct a candidate order.
    pub tag: Tag,
}

/// One completed operation observed during the concurrent execution.
#[derive(Clone, Debug)]
pub struct Observation<Operation, Output, Tag> {
    pub id: OperationId,
    pub start_time: MonotonicInstant,
    pub end_time: MonotonicInstant,
    pub completed: CompletedOperation<Operation, Output, Tag>,
}

/// Completed concurrent operations and their observed results.
pub struct History<Operation, Output, Tag> {
    observations: RefCell<Vec<Observation<Operation, Output, Tag>>>,
}

impl<Operation, Output, Tag> History<Operation, Output, Tag> {
    pub fn new() -> Self {
        Self {
            observations: RefCell::new(Vec::new()),
        }
    }

    /// Add one completed operation to the history.
    pub fn record(&self, observation: Observation<Operation, Output, Tag>) {
        self.observations.borrow_mut().push(observation);
    }

    /// Return a cloned snapshot of the recorded observations.
    pub fn observations(&self) -> Vec<Observation<Operation, Output, Tag>>
    where
        Operation: Clone,
        Output: Clone,
        Tag: Clone,
    {
        self.observations.borrow().clone()
    }

    /// Insert operations omitted from an anchored candidate while preserving
    /// client order and real-time precedence.
    ///
    /// Each missing operation is inserted at its earliest legal position. This
    /// is useful when the anchors identify the intended gap or when alternative
    /// placements are semantically equivalent; it does not search or backtrack
    /// based on operation outputs. For example, given anchored writes
    /// `[write(1), write(2)]`, a missing read that follows `write(1)` but overlaps
    /// `write(2)` is placed before `write(2)`, regardless of the value it read.
    /// Callers that require another placement must anchor that operation or
    /// search alternative candidates themselves.
    pub fn complete_candidate(&self, mut candidate: Vec<OperationId>) -> Result<Vec<OperationId>> {
        let observations = self.observations.borrow();
        let by_id: HashMap<_, _> = observations
            .iter()
            .map(|observation| (observation.id.clone(), observation))
            .collect();
        anyhow::ensure!(
            by_id.len() == observations.len(),
            "history contains duplicate operation IDs"
        );

        let candidate_ids: HashSet<_> = candidate.iter().cloned().collect();
        anyhow::ensure!(
            candidate_ids.len() == candidate.len()
                && candidate.iter().all(|id| by_id.contains_key(id)),
            "candidate contains duplicate or unknown operation IDs"
        );

        let mut missing: Vec<_> = observations
            .iter()
            .filter(|observation| !candidate_ids.contains(&observation.id))
            .collect();
        while !missing.is_empty() {
            let source = missing
                .iter()
                .enumerate()
                .filter(|(_, observation)| {
                    !missing.iter().any(|other| precedes(other, observation))
                })
                .min_by(|(_, left), (_, right)| {
                    left.id
                        .client
                        .cmp(&right.id.client)
                        .then(left.id.sequence.cmp(&right.id.sequence))
                })
                .map(|(index, _)| index)
                .ok_or_else(|| {
                    anyhow::anyhow!("unanchored operations contain an ordering cycle")
                })?;
            let observation = missing.remove(source);

            let mut after = 0;
            let mut before = candidate.len();
            for (index, id) in candidate.iter().enumerate() {
                let placed = by_id[id];
                if precedes(placed, observation) {
                    after = after.max(index + 1);
                }
                if precedes(observation, placed) {
                    before = before.min(index);
                }
            }
            anyhow::ensure!(
                after <= before,
                "no candidate position for {:?}",
                observation.id
            );
            candidate.insert(after, observation.id.clone());
        }
        Ok(candidate)
    }

    /// Verify that `candidate` is a legal linearization of the observed history.
    pub fn verify<State>(
        &self,
        candidate: &[OperationId],
        mut state: State,
        mut verify_and_apply: impl FnMut(&mut State, &Operation, &Output) -> Result<()>,
    ) -> Result<()> {
        let observations = self.observations.borrow();
        let by_id: HashMap<_, _> = observations
            .iter()
            .map(|observation| (observation.id.clone(), observation))
            .collect();
        anyhow::ensure!(
            by_id.len() == observations.len(),
            "history contains duplicate operation IDs"
        );

        let candidate_ids: HashSet<_> = candidate.iter().collect();
        anyhow::ensure!(
            candidate.len() == observations.len()
                && candidate_ids.len() == candidate.len()
                && candidate.iter().all(|id| by_id.contains_key(id)),
            "candidate must contain every observed operation exactly once"
        );

        let mut observations_by_client: HashMap<usize, Vec<_>> = HashMap::new();
        for observation in observations.iter() {
            anyhow::ensure!(
                observation.start_time <= observation.end_time,
                "operation {:?} ended before it started",
                observation.id
            );
            observations_by_client
                .entry(observation.id.client)
                .or_default()
                .push(observation);
        }
        for (client, client_observations) in &mut observations_by_client {
            client_observations.sort_unstable_by_key(|observation| observation.id.sequence);
            anyhow::ensure!(
                client_observations
                    .iter()
                    .map(|observation| observation.id.sequence)
                    .eq(0..client_observations.len()),
                "client {client} has a malformed operation sequence"
            );
            for neighbours in client_observations.windows(2) {
                let before = neighbours[0];
                let after = neighbours[1];
                anyhow::ensure!(
                    before.end_time <= after.start_time,
                    "client {client} operations {} and {} overlap",
                    before.id.sequence,
                    after.id.sequence
                );
            }
        }

        let positions: HashMap<_, _> = candidate
            .iter()
            .enumerate()
            .map(|(position, id)| (id, position))
            .collect();
        let mut last_sequence_by_client = HashMap::new();
        for id in candidate {
            if let Some(previous) = last_sequence_by_client.insert(id.client, id.sequence) {
                anyhow::ensure!(
                    previous < id.sequence,
                    "candidate reverses client {} operations {} and {}",
                    id.client,
                    previous,
                    id.sequence
                );
            }
        }

        for before in observations.iter() {
            for after in observations.iter() {
                if before.end_time < after.start_time {
                    anyhow::ensure!(
                        positions[&before.id] < positions[&after.id],
                        "candidate places {:?} after {:?}, despite real-time precedence",
                        before.id,
                        after.id
                    );
                }
            }
        }

        for id in candidate {
            let observed = by_id[id];
            verify_and_apply(
                &mut state,
                &observed.completed.operation,
                &observed.completed.output,
            )
            .map_err(|error| anyhow::anyhow!("operation {id:?}: {error}"))?;
        }
        Ok(())
    }
}

fn precedes<Operation, Output, Tag>(
    before: &Observation<Operation, Output, Tag>,
    after: &Observation<Operation, Output, Tag>,
) -> bool {
    (before.id.client == after.id.client && before.id.sequence < after.id.sequence)
        || before.end_time < after.start_time
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct TestObservation {
        client: usize,
        sequence: usize,
        start_ms: u64,
        end_ms: u64,
        operation: i64,
        output: i64,
    }

    fn observation(input: TestObservation) -> Observation<i64, i64, ()> {
        Observation {
            id: OperationId {
                client: input.client,
                sequence: input.sequence,
            },
            start_time: MonotonicInstant::from_duration_since_clock_origin(Duration::from_millis(
                input.start_ms,
            )),
            end_time: MonotonicInstant::from_duration_since_clock_origin(Duration::from_millis(
                input.end_ms,
            )),
            completed: CompletedOperation {
                operation: input.operation,
                output: input.output,
                tag: (),
            },
        }
    }

    #[test]
    fn verifies_candidate_against_partial_order_and_specification() {
        let history = History::new();
        let first = observation(TestObservation {
            client: 0,
            sequence: 0,
            start_ms: 0,
            end_ms: 2,
            operation: 1,
            output: 1,
        });
        let concurrent = observation(TestObservation {
            client: 1,
            sequence: 0,
            start_ms: 1,
            end_ms: 4,
            operation: 2,
            output: 3,
        });
        let last = observation(TestObservation {
            client: 0,
            sequence: 1,
            start_ms: 5,
            end_ms: 6,
            operation: 3,
            output: 6,
        });
        history.record(first.clone());
        history.record(concurrent.clone());
        history.record(last.clone());

        history
            .verify(
                &[first.id, concurrent.id, last.id],
                0,
                |state, operation, observed| {
                    *state += operation;
                    anyhow::ensure!(*state == *observed, "unexpected sum");
                    Ok(())
                },
            )
            .expect("candidate should satisfy the observed history");
    }

    #[test]
    fn completes_candidate_at_the_earliest_legal_position() {
        let history = History::new();
        let first_write = observation(TestObservation {
            client: 0,
            sequence: 0,
            start_ms: 0,
            end_ms: 1,
            operation: 1,
            output: 1,
        });
        let second_write = observation(TestObservation {
            client: 0,
            sequence: 1,
            start_ms: 3,
            end_ms: 5,
            operation: 2,
            output: 2,
        });
        let overlapping_read = observation(TestObservation {
            client: 1,
            sequence: 0,
            start_ms: 2,
            end_ms: 4,
            operation: 0,
            output: 2,
        });
        history.record(first_write.clone());
        history.record(second_write.clone());
        history.record(overlapping_read.clone());

        assert_eq!(
            history
                .complete_candidate(vec![first_write.id, second_write.id])
                .expect("the missing read should have a legal position"),
            vec![first_write.id, overlapping_read.id, second_write.id]
        );
    }

    #[test]
    fn rejects_overlapping_operations_from_one_client() {
        let history = History::new();
        let first = observation(TestObservation {
            client: 0,
            sequence: 0,
            start_ms: 0,
            end_ms: 3,
            operation: 1,
            output: 1,
        });
        let overlapping = observation(TestObservation {
            client: 0,
            sequence: 1,
            start_ms: 2,
            end_ms: 4,
            operation: 2,
            output: 3,
        });
        history.record(first.clone());
        history.record(overlapping.clone());

        let error = history
            .verify(
                &[first.id, overlapping.id],
                (),
                |_state, _operation, _output| Ok(()),
            )
            .expect_err("one client cannot execute overlapping operations");

        assert_eq!(error.to_string(), "client 0 operations 0 and 1 overlap");
    }
}
