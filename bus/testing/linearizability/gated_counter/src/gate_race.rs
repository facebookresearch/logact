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
use conformance::ConformanceFixture;
use conformance::SimulatorFixture;
use counter_suite::counter::CounterOperation;
use linearizability::history::History;
use linearizability::history::OperationId;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::OperationResult;
use linearizability::object::candidate_from_ordered_tags;

use crate::model::GatedCounterOperation;
use crate::model::GatedCounterOutput;
use crate::model::GatedCounterSpec;

#[derive(Clone, Copy)]
struct State {
    gate_open: bool,
    counter: i64,
}

/// A targeted race fixture for one gate write concurrent with one counter mutation.
/// The buggy variant saves a stale composite state when updating the gate.
pub struct GateRaceCounter<const STALE_GATE_WRITE: bool> {
    state: Cell<State>,
    writers_ready: Rc<SimulatorBarrier>,
    counter_written: Rc<SimulatorBarrier>,
    gate_written: Rc<SimulatorBarrier>,
    readers_ready: Rc<SimulatorBarrier>,
    next_tag: Cell<u64>,
}

impl<const STALE_GATE_WRITE: bool> GateRaceCounter<STALE_GATE_WRITE> {
    fn new() -> Self {
        Self {
            state: Cell::new(State {
                gate_open: true,
                counter: 0,
            }),
            writers_ready: SimulatorBarrier::new(2),
            counter_written: SimulatorBarrier::new(2),
            gate_written: SimulatorBarrier::new(2),
            readers_ready: SimulatorBarrier::new(2),
            next_tag: Cell::new(0),
        }
    }

    fn tag(&self) -> u64 {
        let tag = self.next_tag.get();
        self.next_tag.set(tag + 1);
        tag
    }
}

impl<const STALE_GATE_WRITE: bool> LinearizableObject for GateRaceCounter<STALE_GATE_WRITE> {
    type Spec = GatedCounterSpec;
    type Tag = u64;

    async fn execute(
        &self,
        _id: OperationId,
        operation: GatedCounterOperation,
    ) -> OperationResult<GatedCounterOutput, Self::Tag> {
        if !operation.is_write() {
            self.readers_ready.wait().await;
        }
        let mut state = self.state.get();
        match operation {
            GatedCounterOperation::SetGate(open) => {
                self.writers_ready.wait().await;
                self.counter_written.wait().await;
                if !STALE_GATE_WRITE {
                    state = self.state.get();
                }
                state.gate_open = open;
                self.state.set(state);
                let tag = self.tag();
                self.gate_written.wait().await;
                OperationResult {
                    output: GatedCounterOutput::Gate(open),
                    tag,
                }
            }
            GatedCounterOperation::Counter(CounterOperation::Read) => OperationResult {
                output: GatedCounterOutput::Counter(Ok(state.counter)),
                tag: self.tag(),
            },
            GatedCounterOperation::Counter(operation) => {
                self.writers_ready.wait().await;
                if state.gate_open {
                    state.counter += match operation {
                        CounterOperation::Increment => 1,
                        CounterOperation::Decrement => -1,
                        CounterOperation::Read => unreachable!("reads do not mutate the counter"),
                    };
                }
                self.state.set(state);
                let tag = self.tag();
                self.counter_written.wait().await;
                self.gate_written.wait().await;
                OperationResult {
                    output: GatedCounterOutput::Counter(Ok(state.counter)),
                    tag,
                }
            }
            GatedCounterOperation::ReadGate => OperationResult {
                output: GatedCounterOutput::Gate(state.gate_open),
                tag: self.tag(),
            },
        }
    }
}

pub struct GateRaceImplementation<const STALE_GATE_WRITE: bool> {
    environment: Rc<Simulator>,
    counter: Rc<GateRaceCounter<STALE_GATE_WRITE>>,
}

impl<const STALE_GATE_WRITE: bool> LinearizabilityImplementation
    for GateRaceImplementation<STALE_GATE_WRITE>
{
    type Object = GateRaceCounter<STALE_GATE_WRITE>;

    fn environment(&self) -> Rc<Simulator> {
        self.environment.clone()
    }

    fn create(&self, _client: usize) -> Rc<Self::Object> {
        self.counter.clone()
    }

    async fn candidate(
        &self,
        history: &History<GatedCounterOperation, GatedCounterOutput, u64>,
    ) -> Result<Vec<OperationId>> {
        Ok(candidate_from_ordered_tags::<GatedCounterSpec, _>(history))
    }
}

pub struct GateRaceFixture<const STALE_GATE_WRITE: bool> {
    environment: Rc<Simulator>,
}

impl<const STALE_GATE_WRITE: bool> SimulatorFixture for GateRaceFixture<STALE_GATE_WRITE> {
    fn new(simulator: Simulator) -> Self {
        Self {
            environment: Rc::new(simulator),
        }
    }
}

impl<const STALE_GATE_WRITE: bool> ConformanceFixture for GateRaceFixture<STALE_GATE_WRITE> {
    type Env = Simulator;
    type Impl = GateRaceImplementation<STALE_GATE_WRITE>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.environment.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        GateRaceImplementation {
            environment: self.environment.clone(),
            counter: Rc::new(GateRaceCounter::new()),
        }
    }
}

pub type BuggyGateRaceFixture = GateRaceFixture<true>;
pub type CorrectGateRaceFixture = GateRaceFixture<false>;
