/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic linearizability checker for testing concurrent data structures.
//!
//! This module provides a reusable linearizability tracker that can verify
//! that concurrent operations on any data structure (counter, queue, etc.)
//! respect linearizability: there exists a total order of operations that
//! is consistent with the partial order imposed by real-time constraints.

use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use agentbus_api::MonotonicInstant;
use anyhow::Result;
use anyhow::bail;

/// A recorded operation from a client.
#[derive(Clone, Debug)]
pub struct Operation<V> {
    pub client_id: String,
    pub client_seq_no: i64,
    /// The log position where the command is executed.
    pub log_position: i64,
    pub start_time: MonotonicInstant,
    pub end_time: MonotonicInstant,
    pub operation: String,
    pub value: V,
    /// True if this operation does not append to the bus (i.e. a non-appending read).
    pub read_only: bool,
}

/// A record of an executed command in the linearization order.
/// This is object-agnostic - it just stores the intention ID and operation string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutedCommand {
    pub log_position: i64,
    pub operation: String,
}

/// A generic linearizability tracker that records operation results and timestamps,
/// then verifies that a proposed total order is consistent with both the return
/// values and the partial order imposed by real-time constraints.
///
/// Type parameter V is the return value type of operations (e.g., i64 for a counter).
pub struct LinearizabilityTracker<V> {
    operations: RefCell<Vec<Operation<V>>>,
}

impl<V: Clone + PartialEq + Debug> LinearizabilityTracker<V> {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            operations: RefCell::new(Vec::new()),
        })
    }

    /// Record an operation.
    pub fn record(&self, op: Operation<V>) {
        self.operations.borrow_mut().push(op);
    }
    /// Verify linearizability given the command histories from all workers.
    ///
    /// This method checks:
    /// 1. All workers observed the same total order of commands
    /// 2. The number of committed operations matches recorded results
    /// 3. Replaying operations in the total order produces the same return values
    /// 4. The total order respects the partial order (real-time constraints)
    ///
    /// The replay function should apply the given command to a sequential
    /// specification and return the expected result value.
    pub fn verify<F>(&self, worker_histories: &[Vec<ExecutedCommand>], mut replay: F) -> Result<()>
    where
        F: FnMut(&str) -> V,
    {
        let command_history = worker_histories
            .first()
            .expect("Should have at least one worker");

        self.verify_consistent_histories(worker_histories, command_history)?;

        // Sanity check that we the operation history is well-formed
        self.verify_well_formed()?;

        // Linearized order — sort by log_position, with ties broken by:
        // 1. Write before reads at the same position (there is at most one write per
        //    position, verified below)
        // 2. Reads at the same position ordered by start_time
        // NOTE: if in the future our tests allow pending operations, we can use the
        // command_history to help order them (easier if requests are unique)
        let mut linearized_ops = self.operations.borrow().clone();
        linearized_ops.sort_by(|a, b| {
            a.log_position
                .cmp(&b.log_position)
                .then(a.read_only.cmp(&b.read_only)) // false (write) < true (read)
                .then(a.start_time.cmp(&b.start_time))
        });

        // Verify the linearized order agrees with the command history from the bus
        self.verify_matches_write_history(&linearized_ops, command_history)?;

        // Defn of linearizability from the Herlihy & Wing paper:
        // 1. The new history is equivalent to the old one -- they agree on each client
        self.verify_client_ordering(&linearized_ops)?;
        // 2. The new history is a legal sequential history
        self.verify_replay_values(&linearized_ops, &mut replay)?;
        // 3. The new history respects real-time constraints (partial order)
        self.verify_partial_order(&linearized_ops)?;

        Ok(())
    }

    fn verify_consistent_histories(
        &self,
        worker_histories: &[Vec<ExecutedCommand>],
        command_history: &[ExecutedCommand],
    ) -> Result<()> {
        for (idx, worker_history) in worker_histories.iter().enumerate() {
            if worker_history.len() != command_history.len() {
                bail!(
                    "Worker {} history length {} differs from expected length {}",
                    idx,
                    worker_history.len(),
                    command_history.len()
                );
            }
            for (cmd_idx, worker_cmd) in worker_history.iter().enumerate() {
                if worker_cmd != &command_history[cmd_idx] {
                    bail!(
                        "Worker {} command at index {} differs: {:?} vs {:?}",
                        idx,
                        cmd_idx,
                        worker_cmd,
                        command_history[cmd_idx]
                    );
                }
            }
        }
        Ok(())
    }

    /// Verify the write (non-read-only) operations match the command history from the bus,
    /// and that each log_position has at most one write.
    fn verify_matches_write_history(
        &self,
        linearized_ops: &[Operation<V>],
        command_history: &[ExecutedCommand],
    ) -> Result<()> {
        let write_ops: Vec<_> = linearized_ops.iter().filter(|op| !op.read_only).collect();
        // Write log_positions must be strictly increasing (at most one write per position)
        if let Some(w) = write_ops
            .windows(2)
            .find(|w| w[0].log_position >= w[1].log_position)
        {
            bail!(
                "Duplicate write log_position {}: {} seq {} and {} seq {}",
                w[0].log_position,
                w[0].client_id,
                w[0].client_seq_no,
                w[1].client_id,
                w[1].client_seq_no,
            );
        }
        if write_ops.len() != command_history.len() {
            bail!(
                "Write ops count {} differs from command history count {}",
                write_ops.len(),
                command_history.len()
            );
        }
        for (i, (op, cmd)) in write_ops.iter().zip(command_history.iter()).enumerate() {
            if op.log_position != cmd.log_position {
                bail!(
                    "Position {}: write op has log_position {} but history has {}",
                    i,
                    op.log_position,
                    cmd.log_position
                );
            }
            if op.operation != cmd.operation {
                bail!(
                    "Position {}: write op has operation '{}' but history has '{}'",
                    i,
                    op.operation,
                    cmd.operation
                );
            }
        }
        Ok(())
    }

    /// Verify that the recorded operations are well-formed:
    /// - For each client_id, seq_nos are 0, 1, 2, ... in recording order
    /// - For each operation, start_time <= end_time
    /// - For consecutive ops from the same client, end_time <= next start_time
    fn verify_well_formed(&self) -> Result<()> {
        let ops = self.operations.borrow();
        let mut last_per_client: std::collections::HashMap<&str, (i64, MonotonicInstant)> =
            std::collections::HashMap::new();
        for op in ops.iter() {
            if op.start_time > op.end_time {
                bail!(
                    "Client {} seq {}: start_time {:?} > end_time {:?}",
                    op.client_id,
                    op.client_seq_no,
                    op.start_time,
                    op.end_time
                );
            }
            let expected_seq = last_per_client
                .get(op.client_id.as_str())
                .map_or(0, |(seq, _)| seq + 1);
            if op.client_seq_no != expected_seq {
                bail!(
                    "Client {} expected seq_no {} but got {}",
                    op.client_id,
                    expected_seq,
                    op.client_seq_no
                );
            }
            if let Some((_, prev_end)) = last_per_client.get(op.client_id.as_str()) {
                if *prev_end > op.start_time {
                    bail!(
                        "Client {} seq {}: start_time {:?} is before previous end_time {:?}",
                        op.client_id,
                        op.client_seq_no,
                        op.start_time,
                        prev_end
                    );
                }
            }
            last_per_client.insert(&op.client_id, (op.client_seq_no, op.end_time));
        }
        Ok(())
    }

    /// Verify that for each client, operations appear in seq_no order.
    fn verify_client_ordering(&self, sorted_ops: &[Operation<V>]) -> Result<()> {
        let mut last_seq: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for op in sorted_ops {
            let prev = last_seq.entry(&op.client_id).or_insert(-1);
            if op.client_seq_no <= *prev {
                bail!(
                    "Client {} seq_no {} appears after {}, violating per-client ordering",
                    op.client_id,
                    op.client_seq_no,
                    prev
                );
            }
            *prev = op.client_seq_no;
        }
        Ok(())
    }

    /// Replay operations in sorted order and check values match.
    fn verify_replay_values<F>(&self, sorted_ops: &[Operation<V>], replay: &mut F) -> Result<()>
    where
        F: FnMut(&str) -> V,
    {
        for op in sorted_ops {
            let replay_value = replay(&op.operation);
            if op.value != replay_value {
                bail!(
                    "Client {} seq {} at log_position {}: observed {:?} but replay produced {:?}",
                    op.client_id,
                    op.client_seq_no,
                    op.log_position,
                    op.value,
                    replay_value
                );
            }
        }
        Ok(())
    }

    /// Verify real-time constraints: if op A finished before op B started,
    /// A must appear earlier in the linearized order (lower index in sorted_ops).
    fn verify_partial_order(&self, sorted_ops: &[Operation<V>]) -> Result<()> {
        for (idx_a, op_a) in sorted_ops.iter().enumerate() {
            for (idx_b, op_b) in sorted_ops.iter().enumerate() {
                if idx_a == idx_b {
                    continue;
                }
                if op_a.end_time < op_b.start_time && idx_a > idx_b {
                    bail!(
                        "Partial order violation: {} seq {} (pos {}) ended before {} seq {} (pos {}) started, but appears later in linearization",
                        op_a.client_id,
                        op_a.client_seq_no,
                        op_a.log_position,
                        op_b.client_id,
                        op_b.client_seq_no,
                        op_b.log_position
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instant(milliseconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_duration_since_clock_origin(std::time::Duration::from_millis(
            milliseconds,
        ))
    }

    fn cmd(pos: i64, operation: &str) -> ExecutedCommand {
        ExecutedCommand {
            log_position: pos,
            operation: operation.to_string(),
        }
    }

    fn counter_replay() -> impl FnMut(&str) -> i64 {
        let mut val = 0i64;
        move |operation| {
            match operation {
                "inc" => val += 1,
                "dec" => val -= 1,
                _ => {}
            }
            val
        }
    }

    #[test]
    fn test_verify_succeeds_for_valid_history() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        // w0 does inc at pos 0 (0-10ms), w1 does inc at pos 1 (10-20ms)
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 0,
            start_time: instant(0),
            end_time: instant(10),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });
        tracker.record(Operation {
            client_id: "w1".into(),
            client_seq_no: 0,
            log_position: 1,
            start_time: instant(10),
            end_time: instant(20),
            operation: "inc".into(),
            value: 2,
            read_only: false,
        });

        let history = vec![cmd(0, "inc"), cmd(1, "inc")];
        assert!(
            tracker
                .verify(&[history.clone(), history], counter_replay())
                .is_ok()
        );
    }

    #[test]
    fn test_verify_fails_on_inconsistent_history_length() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 0,
            start_time: instant(0),
            end_time: instant(10),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });

        let h1 = vec![cmd(0, "inc")];
        let h2 = vec![cmd(0, "inc"), cmd(1, "inc")];
        let result = tracker.verify(&[h1, h2], counter_replay());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length"));
    }

    #[test]
    fn test_verify_fails_on_inconsistent_history_order() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 0,
            start_time: instant(0),
            end_time: instant(10),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });
        tracker.record(Operation {
            client_id: "w1".into(),
            client_seq_no: 0,
            log_position: 1,
            start_time: instant(10),
            end_time: instant(20),
            operation: "inc".into(),
            value: 2,
            read_only: false,
        });

        let h1 = vec![cmd(0, "inc"), cmd(1, "inc")];
        let h2 = vec![cmd(0, "inc"), cmd(1, "dec")];
        let result = tracker.verify(&[h1, h2], counter_replay());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("differs"));
    }

    #[test]
    fn test_verify_fails_on_replay_value_mismatch() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 0,
            start_time: instant(0),
            end_time: instant(10),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });
        tracker.record(Operation {
            client_id: "w1".into(),
            client_seq_no: 0,
            log_position: 1,
            start_time: instant(10),
            end_time: instant(20),
            operation: "inc".into(),
            value: 999,
            read_only: false, // wrong — should be 2
        });

        let history = vec![cmd(0, "inc"), cmd(1, "inc")];
        let result = tracker.verify(&[history], counter_replay());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("replay"));
    }

    #[test]
    fn test_verify_fails_on_partial_order_violation() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        // w0 finishes at 100ms, w1 starts at 200ms — w0 must come first.
        // But w1 has lower log_position (0 < 1) — sorted order puts w1 first.
        // Values match sorted replay: pos 0 → 1, pos 1 → 2.
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 1,
            start_time: instant(0),
            end_time: instant(100),
            operation: "inc".into(),
            value: 2,
            read_only: false,
        });
        tracker.record(Operation {
            client_id: "w1".into(),
            client_seq_no: 0,
            log_position: 0,
            start_time: instant(200),
            end_time: instant(300),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });

        let history = vec![cmd(0, "inc"), cmd(1, "inc")];
        let result = tracker.verify(&[history], counter_replay());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Partial order"));
    }

    #[test]
    fn test_verify_fails_on_client_ordering_violation() {
        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        // w0 does two ops sequentially, but sorted by log_position reverses them
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 0,
            log_position: 1,
            start_time: instant(0),
            end_time: instant(10),
            operation: "inc".into(),
            value: 2,
            read_only: false,
        });
        tracker.record(Operation {
            client_id: "w0".into(),
            client_seq_no: 1,
            log_position: 0,
            start_time: instant(10),
            end_time: instant(20),
            operation: "inc".into(),
            value: 1,
            read_only: false,
        });

        let history = vec![cmd(0, "inc"), cmd(1, "inc")];
        let result = tracker.verify(&[history], counter_replay());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("per-client ordering")
        );
    }
}
