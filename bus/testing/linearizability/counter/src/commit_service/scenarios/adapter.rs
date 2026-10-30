/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(commit_service_adapter_adapter_list)]
mod defs {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::rc::Rc;

    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use futures::channel::oneshot;
    use linearizability::history::OperationId;
    use logact_commit_service_api::CommitIntentionCommand;
    use logact_commit_service_api::CommitIntentionOutcome;
    use logact_commit_service_api::CommitResult;
    use logact_commit_service_api::CommitSvc;
    use logact_commit_service_api::CreateBusCommand;
    use logact_commit_service_tests::fixtures::CommitServiceTestFixture;

    use crate::commit_service::CommitServiceCounter;
    use crate::counter::CounterCommand;
    use crate::counter::CounterOperation;

    struct ResponseService<S> {
        inner: S,
        gate: RefCell<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        flip_approval: bool,
    }

    impl<S: CommitSvc> CommitSvc for ResponseService<S> {
        type Bus = S::Bus;

        fn agent_bus(&self) -> &Self::Bus {
            self.inner.agent_bus()
        }

        async fn create_bus(&self, request: CreateBusCommand) -> CommitResult<()> {
            self.inner.create_bus(request).await
        }

        async fn commit_intention(
            &self,
            request: CommitIntentionCommand,
        ) -> CommitResult<CommitIntentionOutcome> {
            let mut response = self.inner.commit_intention(request).await?;
            let gate = self.gate.borrow_mut().take();
            if let Some((ready, release)) = gate {
                ready.send(()).expect("decision observer must remain alive");
                release.await.expect("response gate must be released");
            }
            if self.flip_approval {
                response.approved = !response.approved;
            }
            Ok(response)
        }
    }

    #[scenario(sim_only)]
    pub async fn run_clients_discover_decisions_before_the_originating_response_arrives<F>(
        fixture: &F,
    ) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let mut first = CommitServiceCounter::new(
            ResponseService {
                inner: fixture.create_impl(),
                gate: RefCell::new(Some((ready_tx, release_rx))),
                flip_approval: false,
            },
            "counter-response-delay".to_string(),
        );
        let mut second =
            CommitServiceCounter::new(fixture.create_impl(), "counter-response-delay".to_string());
        first.max_poll_entries = 1;
        second.max_poll_entries = 1;
        let first_completed = Rc::new(Cell::new(false));
        let completed = first_completed.clone();
        let first_task = async {
            let result = first
                .execute_counter(CounterCommand::new(
                    OperationId {
                        client: 0,
                        sequence: 0,
                    },
                    CounterOperation::Increment,
                ))
                .await?;
            completed.set(true);
            Ok::<_, anyhow::Error>(result)
        };
        let second_task = async {
            ready_rx.await?;
            let result = second
                .execute_counter(CounterCommand::new(
                    OperationId {
                        client: 1,
                        sequence: 0,
                    },
                    CounterOperation::Read,
                ))
                .await;
            let observed_before_response = !first_completed.get();
            release_tx
                .send(())
                .expect("originating request must remain alive");
            anyhow::ensure!(
                observed_before_response,
                "later client waited for the earlier RPC response"
            );
            result
        };
        let (first_result, second_result) = futures::join!(first_task, second_task);
        let first_result = first_result?;
        let second_result = second_result?;
        let value = first_result.output.unwrap_or_default();
        anyhow::ensure!(
            second_result.output == Ok(value),
            "later client did not replay the earlier decision"
        );
        anyhow::ensure!(
            first_result.tag < second_result.tag,
            "intentions must be ordered"
        );
        anyhow::ensure!(
            first.counter_log().await? == second.counter_log().await?,
            "client histories differ"
        );
        Ok(())
    }

    #[scenario(sim_only)]
    pub async fn run_response_must_agree_with_the_durable_decision<F>(fixture: &F) -> Result<()>
    where
        F: CommitServiceTestFixture<Env = Simulator>,
    {
        let client = CommitServiceCounter::new(
            ResponseService {
                inner: fixture.create_impl(),
                gate: RefCell::new(None),
                flip_approval: true,
            },
            "counter-response-disagreement".to_string(),
        );
        let result = client
            .execute_counter(CounterCommand::new(
                OperationId {
                    client: 0,
                    sequence: 0,
                },
                CounterOperation::Increment,
            ))
            .await;
        let error = result.expect_err("adapter must reject a response that contradicts the log");
        anyhow::ensure!(
            error
                .to_string()
                .contains("disagrees with its logged decision"),
            "unexpected adapter error: {error:#}"
        );
        Ok(())
    }
}
pub use defs::*;
