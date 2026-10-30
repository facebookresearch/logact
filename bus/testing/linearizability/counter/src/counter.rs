/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agent_bus_proto_rust::agent_bus::BusEntry;
use agentbus_simulator::Simulator;
use anyhow::Context as _;
use anyhow::Result;
use linearizability::history::OperationId;
use linearizability::object::Client;
use linearizability::object::LinearizabilityWorkload;
use linearizability::object::SequentialSpec;
use rand::Rng;
use rand::RngExt as _;

const WIRE_PREFIX: &str = "counter|";

/// An operation supported by the reusable counter model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterOperation {
    Increment,
    Decrement,
    Read,
}

/// An error returned by the abstract counter API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterError {
    Denied,
}

/// The observable result of a counter operation.
pub type CounterOutput = std::result::Result<i64, CounterError>;

impl CounterOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Increment => "inc",
            Self::Decrement => "dec",
            Self::Read => "read",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "inc" => Some(Self::Increment),
            "dec" => Some(Self::Decrement),
            "read" => Some(Self::Read),
            _ => None,
        }
    }

    pub const fn is_write(self) -> bool {
        !matches!(self, Self::Read)
    }
}

/// A counter operation and its opaque identity on the shared test wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterCommand {
    pub id: OperationId,
    pub operation: CounterOperation,
}

impl CounterCommand {
    pub fn new(id: OperationId, operation: CounterOperation) -> Self {
        Self { id, operation }
    }
}

pub trait CounterCommandExt: Sized {
    /// Encode the command for transport through an intention body.
    fn encode(self) -> String;

    /// Decode a counter command, returning `None` for unrelated intention bodies.
    fn decode(body: &str) -> Result<Option<Self>>;
}

impl CounterCommandExt for CounterCommand {
    fn encode(self) -> String {
        format!(
            "{WIRE_PREFIX}{}|{}|{}",
            self.id.client,
            self.id.sequence,
            self.operation.name()
        )
    }

    fn decode(body: &str) -> Result<Option<Self>> {
        let Some(body) = body.strip_prefix(WIRE_PREFIX) else {
            return Ok(None);
        };
        let mut parts = body.split('|');
        let client = parts
            .next()
            .context("counter command has no client ID")?
            .parse()
            .context("counter command has an invalid client ID")?;
        let sequence = parts
            .next()
            .context("counter command has no sequence ID")?
            .parse()
            .context("counter command has an invalid sequence ID")?;
        let operation = parts
            .next()
            .and_then(CounterOperation::from_name)
            .context("counter command has an invalid operation")?;
        anyhow::ensure!(parts.next().is_none(), "counter command has extra fields");
        Ok(Some(Self::new(OperationId { client, sequence }, operation)))
    }
}

pub fn counter_command(entry: &BusEntry) -> Result<Option<CounterCommand>> {
    let Some(agent_bus_proto_rust::agent_bus::payload::Payload::Intention(intention)) = entry
        .payload
        .as_ref()
        .and_then(|payload| payload.payload.as_ref())
    else {
        return Ok(None);
    };
    let Some(agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(body)) =
        intention.intention.as_ref()
    else {
        return Ok(None);
    };
    CounterCommand::decode(body)
}

/// Pure sequential specification for an ordinary counter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CounterSpec {
    value: i64,
}

impl CounterSpec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn value(self) -> i64 {
        self.value
    }

    pub fn apply(&mut self, operation: CounterOperation) -> i64 {
        match operation {
            CounterOperation::Increment => self.value += 1,
            CounterOperation::Decrement => self.value -= 1,
            CounterOperation::Read => {}
        }
        self.value
    }
}

impl SequentialSpec for CounterSpec {
    type Operation = CounterOperation;
    type Output = CounterOutput;

    fn new() -> Self {
        Self::default()
    }

    fn apply(&mut self, operation: &Self::Operation) -> Self::Output {
        Ok(self.apply(*operation))
    }

    fn verify_and_apply(
        &mut self,
        operation: &Self::Operation,
        observed: &Self::Output,
    ) -> Result<()> {
        match observed {
            Ok(observed) => {
                let expected = self.apply(*operation);
                anyhow::ensure!(
                    *observed == expected,
                    "observed {observed}, but sequential replay produced {expected}"
                );
            }
            Err(CounterError::Denied) => {
                anyhow::ensure!(operation.is_write(), "a counter read cannot be denied");
            }
        }
        Ok(())
    }

    fn is_write(operation: &Self::Operation) -> bool {
        operation.is_write()
    }
}

pub fn random_operations(rng: &mut impl Rng, count: usize) -> Vec<CounterOperation> {
    (0..count)
        .map(|_| match rng.random_range(0..3) {
            0 => CounterOperation::Increment,
            1 => CounterOperation::Decrement,
            _ => CounterOperation::Read,
        })
        .collect()
}

/// Concrete workload shared by every counter implementation under test.
#[derive(Clone)]
pub struct CounterWorkload {
    clients: Vec<Client<CounterOperation>>,
}

impl CounterWorkload {
    pub fn random(rng: &mut impl Rng, clients: usize, operations: usize) -> Self {
        let clients = (0..clients)
            .map(|client| {
                Client::new(
                    format!("counter-{client}"),
                    random_operations(rng, operations),
                )
            })
            .collect();
        Self { clients }
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn operations_per_client(&self) -> usize {
        self.clients
            .first()
            .map_or(0, |client| client.operations.len())
    }

    pub fn operation_count(&self) -> usize {
        self.clients
            .iter()
            .map(|client| client.operations.len())
            .sum()
    }
}

impl LinearizabilityWorkload for CounterWorkload {
    type Operation = CounterOperation;

    fn generate(self, _environment: &Simulator) -> Vec<Client<Self::Operation>> {
        self.clients
    }
}
