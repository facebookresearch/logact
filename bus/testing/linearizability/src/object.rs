/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;

use agentbus_simulator::Simulator;
use anyhow::Context as _;
use anyhow::Result;

use crate::history::History;
use crate::history::OperationId;
pub use crate::operation::OperationResult;
use crate::worker::Worker;

const MAX_WORKER_DELAY_MS: u64 = 3;

/// A pure sequential specification used to check an observed execution.
pub trait SequentialSpec: Sized {
    type Operation: Clone + 'static;
    type Output: Clone + Debug + PartialEq + 'static;

    fn new() -> Self;
    fn apply(&mut self, operation: &Self::Operation) -> Self::Output;

    fn verify_and_apply(
        &mut self,
        operation: &Self::Operation,
        observed: &Self::Output,
    ) -> Result<()> {
        let expected = self.apply(operation);
        anyhow::ensure!(
            &expected == observed,
            "observed {observed:?}, but sequential replay produced {expected:?}"
        );
        Ok(())
    }

    fn is_write(operation: &Self::Operation) -> bool;
}

/// One implementation of a linearizable object under test.
#[expect(
    async_fn_in_trait,
    reason = "simulator-only test implementations do not require Send futures"
)]
pub trait LinearizableObject {
    type Spec: SequentialSpec;
    type Tag: Clone + 'static;

    /// Returns `None` when the outcome is unknown (e.g. the operation timed
    /// out): it may or may not have taken effect.
    async fn execute(
        &self,
        id: OperationId,
        operation: <Self::Spec as SequentialSpec>::Operation,
    ) -> Option<OperationResult<<Self::Spec as SequentialSpec>::Output, Self::Tag>>;
}

/// The operation history recorded for a linearizable object.
pub type HistoryFor<Object> = History<
    <<Object as LinearizableObject>::Spec as SequentialSpec>::Operation,
    <<Object as LinearizableObject>::Spec as SequentialSpec>::Output,
    <Object as LinearizableObject>::Tag,
>;

/// A declarative workload generator for one abstract API.
pub trait LinearizabilityWorkload {
    type Operation: Clone + 'static;

    fn generate(self, environment: &Simulator) -> Vec<Client<Self::Operation>>;
}

/// Construction and lifecycle hooks for one concrete implementation under test.
#[expect(
    async_fn_in_trait,
    reason = "simulator-only test implementations do not require Send futures"
)]
pub trait LinearizabilityImplementation {
    type Object: LinearizableObject + 'static;

    fn environment(&self) -> Rc<Simulator>;
    fn create(&self, client: usize) -> Rc<Self::Object>;

    async fn candidate(&self, history: &HistoryFor<Self::Object>) -> Result<Vec<OperationId>>;

    fn on_complete(&self) -> Option<Rc<dyn Fn()>> {
        None
    }
}

/// Run a declarative workload against one concrete implementation.
pub async fn run_linearizability_test<Workload, Implementation>(
    workload: Workload,
    implementation: Rc<Implementation>,
) -> Result<usize>
where
    Workload: LinearizabilityWorkload<
            Operation = <<Implementation::Object as LinearizableObject>::Spec as SequentialSpec>::Operation,
        >,
    Implementation: LinearizabilityImplementation,
{
    let environment = implementation.environment();
    let clients = workload.generate(&environment);
    anyhow::ensure!(
        !clients.is_empty(),
        "linearizability workload should have at least one client"
    );
    let history = Rc::new(History::new());
    let client_count = clients.len();
    let objects: Vec<_> = (0..client_count)
        .map(|client| implementation.create(client))
        .collect();
    let on_complete = implementation.on_complete();
    let handles: Vec<_> = clients
        .into_iter()
        .zip(objects)
        .enumerate()
        .map(|(client, (workload, worker_object))| {
            let worker = Worker::new(history.clone(), client, workload.operations)
                .with_pause(workload.pause);
            let worker_environment = environment.clone();
            environment.spawn_named(
                async move {
                    worker
                        .run(
                            worker_environment,
                            MAX_WORKER_DELAY_MS,
                            move |id, operation| {
                                let object = worker_object.clone();
                                async move { object.execute(id, operation).await }
                            },
                        )
                        .await;
                },
                Some(format!("worker_{}", workload.name)),
            )
        })
        .collect();
    let completion = environment.spawn(async move {
        for handle in handles {
            handle.await.expect("worker task should complete");
        }
        if let Some(on_complete) = on_complete {
            on_complete();
        }
    });

    environment.run();
    completion.await.expect("completion task should complete");
    let completed = history.observations().len();
    let candidate = implementation.candidate(&history).await?;
    history.verify(
        &candidate,
        <<Implementation as LinearizabilityImplementation>::Object as LinearizableObject>::Spec::new(),
        |state, operation, output| match output {
            Some(output) => {
                <Implementation::Object as LinearizableObject>::Spec::verify_and_apply(
                    state, operation, output,
                )
            }
            None => {
                SequentialSpec::apply(state, operation);
                Ok(())
            }
        },
    )?;
    Ok(completed)
}

/// One concurrent client in a generated workload.
#[derive(Clone)]
pub struct Client<Operation> {
    pub name: String,
    pub operations: Vec<Operation>,
    /// Sleep for the duration before the operation at the sequence number.
    pub pause: Option<(usize, Duration)>,
}

impl<Operation> Client<Operation> {
    pub fn new(name: impl Into<String>, operations: Vec<Operation>) -> Self {
        Self {
            name: name.into(),
            operations,
            pause: None,
        }
    }

    /// Sleep for `duration` before the operation at `sequence`.
    pub fn with_pause(self, sequence: usize, duration: Duration) -> Self {
        Self {
            pause: Some((sequence, duration)),
            ..self
        }
    }
}

impl<Operation> LinearizabilityWorkload for Vec<Client<Operation>>
where
    Operation: Clone + 'static,
{
    type Operation = Operation;

    fn generate(self, _environment: &Simulator) -> Vec<Client<Self::Operation>> {
        self
    }
}

/// Construct a candidate order when every operation has a comparable tag.
/// `landed` gives the tags of the operations with an unknown outcome that took
/// effect.
pub fn candidate_from_ordered_tags<Spec, Tag>(
    history: &History<Spec::Operation, Spec::Output, Tag>,
    landed: impl IntoIterator<Item = (OperationId, Tag)>,
) -> Result<Vec<OperationId>>
where
    Spec: SequentialSpec,
    Tag: Clone + Ord,
{
    let unknown: HashMap<_, _> = history
        .unknown_operations()
        .into_iter()
        .map(|operation| (operation.id, operation))
        .collect();
    let mut entries = history
        .observations()
        .into_iter()
        .map(|observation| {
            (
                observation.completed.tag,
                !Spec::is_write(&observation.completed.operation),
                observation.start_time,
                observation.id,
            )
        })
        .collect::<Vec<_>>();
    for (id, tag) in landed {
        let operation = unknown
            .get(&id)
            .with_context(|| format!("{id:?} is not an operation with an unknown outcome"))?;
        entries.push((
            tag,
            !Spec::is_write(&operation.operation),
            operation.start_time,
            id,
        ));
    }
    entries.sort_by(|first, second| {
        first
            .0
            .cmp(&second.0)
            .then(first.1.cmp(&second.1))
            .then(first.2.cmp(&second.2))
            .then(first.3.client.cmp(&second.3.client))
            .then(first.3.sequence.cmp(&second.3.sequence))
    });
    Ok(entries.into_iter().map(|(_, _, _, id)| id).collect())
}
