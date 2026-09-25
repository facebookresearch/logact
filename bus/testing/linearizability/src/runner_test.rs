/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::rc::Rc;

use agentbus_simulator::Simulator;
use agentbus_simulator::SimulatorBarrier;
use anyhow::Result;

use crate::history::History;
use crate::history::OperationId;
use crate::object::Client;
use crate::object::LinearizabilityImplementation;
use crate::object::LinearizableObject;
use crate::object::OperationResult;
use crate::object::SequentialSpec;
use crate::object::candidate_from_ordered_tags;
use crate::object::run_linearizability_test;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToggleOperation {
    Toggle,
}

#[derive(Default)]
struct ToggleSpec(bool);

impl SequentialSpec for ToggleSpec {
    type Operation = ToggleOperation;
    type Output = bool;

    fn new() -> Self {
        Self::default()
    }

    fn apply(&mut self, _operation: &Self::Operation) -> Self::Output {
        self.0 = !self.0;
        self.0
    }

    fn is_write(_operation: &Self::Operation) -> bool {
        true
    }
}

struct TestToggle {
    value: Cell<bool>,
    buggy: bool,
    writers_ready: Rc<SimulatorBarrier>,
    next_tag: Cell<u64>,
}

impl TestToggle {
    fn new(buggy: bool) -> Self {
        Self {
            value: Cell::new(false),
            buggy,
            writers_ready: SimulatorBarrier::new(2),
            next_tag: Cell::new(0),
        }
    }
}

impl LinearizableObject for TestToggle {
    type Spec = ToggleSpec;
    type Tag = u64;

    async fn execute(
        &self,
        _id: OperationId,
        _operation: ToggleOperation,
    ) -> OperationResult<bool, Self::Tag> {
        let previous = self.value.get();
        if self.buggy {
            self.writers_ready.wait().await;
        }
        let output = !previous;
        self.value.set(output);
        let tag = self.next_tag.get();
        self.next_tag.set(tag + 1);
        OperationResult { output, tag }
    }
}

struct TestToggleImplementation {
    environment: Rc<Simulator>,
    toggle: Rc<TestToggle>,
}

impl TestToggleImplementation {
    fn new(environment: Rc<Simulator>, buggy: bool) -> Self {
        Self {
            environment,
            toggle: Rc::new(TestToggle::new(buggy)),
        }
    }
}

impl LinearizabilityImplementation for TestToggleImplementation {
    type Object = TestToggle;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, _client: usize) -> Rc<Self::Object> {
        self.toggle.clone()
    }

    async fn candidate(
        &self,
        history: &History<ToggleOperation, bool, <Self::Object as LinearizableObject>::Tag>,
    ) -> Result<Vec<OperationId>> {
        Ok(candidate_from_ordered_tags::<ToggleSpec, _>(history))
    }
}

fn run_toggle(buggy: bool) -> Result<usize> {
    let environment = Rc::new(Simulator::new(0));
    let implementation = TestToggleImplementation::new(environment, buggy);
    let workload = vec![
        Client::new("first", vec![ToggleOperation::Toggle]),
        Client::new("second", vec![ToggleOperation::Toggle]),
    ];
    futures::executor::block_on(run_linearizability_test(workload, implementation))
}

#[test]
fn run_linearizability_test_accepts_atomic_toggle_and_rejects_lost_update() {
    assert_eq!(
        run_toggle(false).expect("the atomic toggle should be linearizable"),
        2
    );

    let error = run_toggle(true).expect_err("the framework should reject the lost update");

    assert!(
        error.to_string().contains("sequential replay produced"),
        "unexpected verification error: {error:#}"
    );
}

#[test]
fn run_linearizability_test_rejects_an_empty_workload() {
    let environment = Rc::new(Simulator::new(0));
    let implementation = TestToggleImplementation::new(environment, false);
    let workload: Vec<Client<ToggleOperation>> = Vec::new();

    assert!(
        futures::executor::block_on(run_linearizability_test(workload, implementation)).is_err(),
        "an empty workload should be rejected"
    );
}
