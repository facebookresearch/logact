/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_api::Environment;
use agentbus_simulator::Simulator;
use counter_suite::counter::CounterCommand;
use counter_suite::counter::CounterError;
use counter_suite::counter::CounterOperation;
use counter_suite::counter::CounterOutput;
use counter_suite::counter::CounterSpec;
use linearizability::history::OperationId;
use linearizability::object::Client;
use linearizability::object::LinearizabilityWorkload;
use linearizability::object::SequentialSpec;
use rand::Rng;
use rand::RngExt as _;

/// An operation supported by the reusable gated-counter model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatedCounterOperation {
    Counter(CounterOperation),
    SetGate(bool),
    ReadGate,
}

impl GatedCounterOperation {
    pub const fn is_write(self) -> bool {
        match self {
            Self::Counter(operation) => operation.is_write(),
            Self::SetGate(_) => true,
            Self::ReadGate => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatedCounterCommand {
    pub id: OperationId,
    pub operation: GatedCounterOperation,
}

impl GatedCounterCommand {
    pub fn new(id: OperationId, operation: GatedCounterOperation) -> Self {
        Self { id, operation }
    }
}

pub trait GatedCounterCommandExt {
    fn counter_command(self) -> Option<CounterCommand>;
}

impl GatedCounterCommandExt for GatedCounterCommand {
    fn counter_command(self) -> Option<CounterCommand> {
        let GatedCounterOperation::Counter(operation) = self.operation else {
            return None;
        };
        Some(CounterCommand::new(self.id, operation))
    }
}

/// The visible result of one gated-counter operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatedCounterOutput {
    Counter(CounterOutput),
    Gate(bool),
}

/// Pure sequential specification for a counter controlled by an atomic gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatedCounterSpec {
    gate_open: bool,
    counter: CounterSpec,
}

impl GatedCounterSpec {
    pub fn new() -> Self {
        Self {
            gate_open: true,
            counter: CounterSpec::new(),
        }
    }

    pub fn apply(&mut self, operation: GatedCounterOperation) -> GatedCounterOutput {
        match operation {
            GatedCounterOperation::Counter(operation) => {
                let output = if self.gate_open || !operation.is_write() {
                    Ok(self.counter.apply(operation))
                } else {
                    Err(CounterError::Denied)
                };
                GatedCounterOutput::Counter(output)
            }
            GatedCounterOperation::SetGate(open) => {
                self.gate_open = open;
                GatedCounterOutput::Gate(open)
            }
            GatedCounterOperation::ReadGate => GatedCounterOutput::Gate(self.gate_open),
        }
    }
}

impl SequentialSpec for GatedCounterSpec {
    type Operation = GatedCounterOperation;
    type Output = GatedCounterOutput;

    fn new() -> Self {
        Self::new()
    }

    fn apply(&mut self, operation: &Self::Operation) -> Self::Output {
        self.apply(*operation)
    }

    fn is_write(operation: &Self::Operation) -> bool {
        operation.is_write()
    }
}

/// Generate one gate client and the requested number of counter clients.
pub fn random_gated_workload(
    rng: &mut impl Rng,
    counter_clients: usize,
    counter_operations: usize,
    gate_operations: usize,
) -> Vec<Client<GatedCounterOperation>> {
    let gate = Client::new(
        "gate",
        (0..gate_operations)
            .map(|_| {
                if rng.random() {
                    GatedCounterOperation::SetGate(rng.random())
                } else {
                    GatedCounterOperation::ReadGate
                }
            })
            .collect(),
    );
    std::iter::once(gate)
        .chain((0..counter_clients).map(|client| {
            Client::new(
                format!("counter-{client}"),
                (0..counter_operations)
                    .map(|_| {
                        GatedCounterOperation::Counter(if rng.random() {
                            CounterOperation::Increment
                        } else {
                            CounterOperation::Decrement
                        })
                    })
                    .collect(),
            )
        }))
        .collect()
}

/// Declarative mix of gate writes and counter mutations.
pub struct GatedCounterWorkload {
    counter_clients: usize,
    counter_operations: usize,
    gate_operations: usize,
}

impl GatedCounterWorkload {
    pub fn new(counter_clients: usize, counter_operations: usize, gate_operations: usize) -> Self {
        Self {
            counter_clients,
            counter_operations,
            gate_operations,
        }
    }
}

impl LinearizabilityWorkload for GatedCounterWorkload {
    type Operation = GatedCounterOperation;

    fn generate(self, environment: &Simulator) -> Vec<Client<Self::Operation>> {
        environment.with_rng(|rng| {
            random_gated_workload(
                rng,
                self.counter_clients,
                self.counter_operations,
                self.gate_operations,
            )
        })
    }
}
