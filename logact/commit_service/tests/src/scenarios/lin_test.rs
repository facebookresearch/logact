/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_lin_test_list)]
mod defs {
    //! Single-client linearizability test for `CommitSvc`.
    //!
    //! `CommitServiceCounter` is correct under a single client. Every operation —
    //! inc, dec, and read — goes through `commit_intention`, which drives the engine
    //! to sync, and takes the operation's log position from the response (the
    //! intention's append slot). Inc/dec apply to the local value only when
    //! approved; a read ignores its verdict (a read is never "denied") and just
    //! returns the local value at the synced position. Tracking the value locally is
    //! sound here because a single client sees all of its own writes. Everything
    //! below the counter is reused from the agentbus linearizability harness
    //! verbatim: the `Counter` trait and `CommandResult`, `SequentialCounter` for
    //! local bookkeeping (and as the replay spec), `TrackingCounter`,
    //! `CounterWorker`, and `LinearizabilityTracker`.

    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::intention;
    use agentbus_api::environment::Environment;
    use agentbus_tests::scenarios::lin_test::counter_impl::SequentialCounter;
    use agentbus_tests::scenarios::lin_test::counter_trait::CommandResult;
    use agentbus_tests::scenarios::lin_test::counter_trait::Counter;
    use agentbus_tests::scenarios::lin_test::counter_worker::CounterWorker;
    use agentbus_tests::scenarios::lin_test::linearizability_tracker::ExecutedCommand;
    use agentbus_tests::scenarios::lin_test::linearizability_tracker::LinearizabilityTracker;
    use agentbus_tests::scenarios::lin_test::test::Op;
    use agentbus_tests::scenarios::lin_test::tracking_counter::TrackingCounter;
    use anyhow::Result;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitSvc;
    use rand::RngExt as _;

    use crate::fixtures::CommitServiceTestFixture;
    use crate::simulator::Simulator;

    /// A counter built on `CommitSvc`, correct under a single client. The value is
    /// tracked locally via an embedded `SequentialCounter`; every operation's log
    /// position comes from the commit response.
    pub struct CommitServiceCounter<S: CommitSvc> {
        svc: S,
        agent_id: String,
        local: SequentialCounter,
    }

    impl<S: CommitSvc> CommitServiceCounter<S> {
        pub fn new(svc: S, agent_id: String) -> Self {
            Self {
                svc,
                agent_id,
                local: SequentialCounter::new(),
            }
        }

        /// Submit `body` as an intention. This drives the engine to sync and returns
        /// the verdict plus the intention's append position (the linearization point).
        async fn submit(&self, body: &str) -> Result<CommitIntentionOutcome, String> {
            self.svc
                .commit_intention(CommitIntentionCommand {
                    bus_id: agentbus_api::BusId {
                        agent_bus_id: self.agent_id.clone(),
                    },
                    intention: intention::Intention::StringIntention(body.to_string()),
                })
                .await
                .map_err(|e| format!("commit_intention failed: {e}"))
        }

        async fn execute_op(&self, op: &str) -> Result<CommandResult, String> {
            let resp = self.submit(op).await?;
            if !resp.approved {
                return Err(format!("intention '{op}' denied: {}", resp.reason));
            }
            let value = self.local.apply_operation(resp.log_position, op);
            Ok(CommandResult {
                log_position: resp.log_position,
                value,
            })
        }
    }

    impl<S: CommitSvc> Counter for CommitServiceCounter<S> {
        async fn increment(&self) -> Result<CommandResult, String> {
            self.execute_op("inc").await
        }

        async fn decrement(&self) -> Result<CommandResult, String> {
            self.execute_op("dec").await
        }

        async fn read(&self) -> Result<CommandResult, String> {
            // A read is committed as an intention purely to drive the engine to sync
            // and obtain a log position; its verdict is irrelevant (a read is never
            // "denied"). The value is the local counter at that synced position.
            let resp = self.submit("read").await?;
            Ok(CommandResult {
                log_position: resp.log_position,
                value: self.local.get_value(),
            })
        }

        async fn get_command_history(&self) -> Vec<ExecutedCommand> {
            self.local.get_command_history().await
        }
    }

    /// Drive `operations` through one `CommitServiceCounter` and verify the recorded
    /// history is linearizable against a sequential counter spec. Denied intentions
    /// surface as `Err` and are skipped by `TrackingCounter`, so a fixture whose
    /// safety pipeline rejects some intentions stays linearizable on the approved
    /// subsequence.
    async fn run_single_client<F>(fixture: &F, operations: Vec<Op>) -> Result<Vec<ExecutedCommand>>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let agent_id = format!("counter-{}", env.with_rng(|rng| rng.random::<u64>()));

        let tracker: Rc<LinearizabilityTracker<i64>> = LinearizabilityTracker::new();
        let counter = CommitServiceCounter::new(fixture.create_impl(), agent_id);
        let tracking =
            TrackingCounter::new(counter, env.clone(), tracker.clone(), "w0".to_string());
        let worker = CounterWorker::new(tracking, operations);

        worker.run_workload().await;
        let history = worker.get_command_history().await;

        let spec = SequentialCounter::new();
        tracker.verify(std::slice::from_ref(&history), |operation| {
            spec.apply_operation(0, operation)
        })?;
        Ok(history)
    }

    /// Run a random workload through one `CommitServiceCounter` and verify the
    /// recorded history is linearizable against a sequential counter spec.
    #[scenario(sim_only)]
    pub async fn run_lin_test_single_client_counter<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let num_ops: usize = env.with_rng(|rng| rng.random_range(3..8));
        let operations: Vec<Op> = (0..num_ops)
            .map(|_| match env.with_rng(|rng| rng.random_range(0..3)) {
                0 => Op::Increment,
                1 => Op::Decrement,
                _ => Op::Read,
            })
            .collect();
        run_single_client(fixture, operations).await?;
        Ok(())
    }

    /// Like `run_lin_test_single_client_counter`, but with an all-writes workload
    /// against a fixture whose voter denies every 2nd intention, and asserting the
    /// exact commit/abort split. Because that split only holds for the counting
    /// voter, this is driven bespoke from `sim_tests.rs` (not a generic scenario).
    ///
    /// Only inc/dec are issued: reads also go through `commit_intention`, so they
    /// would consume the every-2nd-denial slots and perturb the split. With one
    /// sequential client the voter sees intentions in order (counts 1..N) and denies
    /// the even ones, so exactly `ceil(N/2)` commit and `floor(N/2)` abort —
    /// deterministic for any seed, and not satisfiable by an approve-all pipeline.
    pub async fn run_lin_test_counter_with_denials<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let env = fixture.get_env();
        let num_writes: usize = env.with_rng(|rng| rng.random_range(6..12));
        let operations: Vec<Op> = (0..num_writes)
            .map(|_| {
                if env.with_rng(|rng| rng.random_range(0..2)) == 0 {
                    Op::Increment
                } else {
                    Op::Decrement
                }
            })
            .collect();

        let committed = run_single_client(fixture, operations).await?.len();
        let aborted = num_writes - committed;

        anyhow::ensure!(
            committed == num_writes.div_ceil(2) && aborted == num_writes / 2,
            "modulus-2 voter should commit ceil(N/2) and abort floor(N/2) of N={num_writes}: \
             got committed={committed}, aborted={aborted}"
        );
        Ok(())
    }
}
pub use defs::*;
