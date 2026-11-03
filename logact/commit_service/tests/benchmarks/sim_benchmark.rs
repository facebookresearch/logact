/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `sim_benchmark [seed] [--workload KIND] [--rows PATH] [--trace PATH] [--sticky] [--no-cache]` runs synchronous workloads.
//! Latencies include commit-service processing and the GatedCounter adapter's
//! replay, but exclude the linearizability check and its final snapshot read.
//! Each service instance has its own AgentBus client and bounded write-once
//! cache over shared backing storage. `--no-cache` disables caching for comparison.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::iter::once;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusMetrics;
use agentbus_api::AgentbusLogger;
use agentbus_api::Clock;
use agentbus_api::Environment;
use agentbus_api::InMemoryMetrics;
use agentbus_api::LogFieldValue;
use agentbus_api::LogRow;
use agentbus_api::MonotonicInstant;
use agentbus_api::PrefixedMetrics;
use agentbus_observable::ObservableAgentBus;
use agentbus_simulator::Simulator;
use agentbus_tests::common::fault_config::FaultConfig;
use agentbus_tests::fixtures::ConformanceFixture;
use agentbus_tests::fixtures::WriteOnceAgentBusGenericFixture;
use agentbus_tests::write_once_space::fixtures::simtest::FaultInjectingWriteOnceSpaceFixture;
use agentbus_tests::write_once_space::fixtures::simtest::InMemoryWriteOnceSpaceFixture;
use agentbus_writeonce::CachingWriteOnceSpace;
use agentbus_writeonce::ObservableWriteOnceSpace;
use anyhow::Result;
use counter_suite::counter::CounterOperation;
use gated_counter_suite::commit_service::CommitServiceGatedCounterImplementation;
use gated_counter_suite::model::GatedCounterOperation;
use gated_counter_suite::model::GatedCounterSpec;
use linearizability::history::OperationId;
use linearizability::object::Client;
use linearizability::object::HistoryFor;
use linearizability::object::LinearizabilityImplementation;
use linearizability::object::LinearizableObject;
use linearizability::object::run_linearizability_test;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::ObservableStorage;
use logact_commit_service_engine::PolicyRegister;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_engine_tests::fixtures::fault_injecting::FaultInjectingStorage;
use logact_commit_service_engine_tests::fixtures::fault_injecting::StorageFaultConfig;
use logact_commit_service_observable::ObservableCommitSvc;
use logact_commit_service_tests::fixtures::SettableDeciderPolicy;
use logact_commit_service_tests::fixtures::random_routing::RandomRoutingCommitService;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::SynchronousRegisterProvider;
use rand::RngExt as _;
use rand::SeedableRng as _;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

const OPERATION_LATENCY_MS: u64 = 50;
const COUNTER_CLIENTS: usize = 3;
const COUNTER_OPERATIONS: usize = 40;
const GATE_WRITES: usize = 4;
const SERVICE_INSTANCES: usize = 3;
const CACHE_MAX_ENTRIES: usize = 1024;
const CACHE_MAX_VALUE_BYTES: usize = 1024 * 1024;

/// Operations each observable wrapper records, keyed by its metric component.
const OBSERVED_OPERATIONS: &[(&str, &[&str])] = &[
    ("commit_service", &["commit_intention"]),
    (
        "agent_bus",
        &["append", "poll", "read_next", "check_tail", "blocking_poll"],
    ),
    ("write_once_space", &["tail", "read", "write"]),
    ("storage", &["get", "put"]),
];

#[derive(Clone, Copy, Default)]
enum WorkloadKind {
    #[default]
    SharedBus,
    IndependentBuses,
}

impl WorkloadKind {
    fn name(self) -> &'static str {
        match self {
            Self::SharedBus => "shared-bus",
            Self::IndependentBuses => "independent-buses",
        }
    }

    fn routing(self, sticky: bool) -> &'static str {
        match self {
            Self::SharedBus => "uniform random service per commit",
            Self::IndependentBuses if sticky => "one fixed service per bus",
            Self::IndependentBuses => "uniform random service per commit",
        }
    }

    fn bus_ids(self) -> Vec<String> {
        match self {
            Self::SharedBus => vec!["gated-counter-benchmark".to_string()],
            Self::IndependentBuses => (0..COUNTER_CLIENTS)
                .map(|client| format!("gated-counter-benchmark-{client}"))
                .collect(),
        }
    }
}

#[derive(Clone)]
struct PerBusPolicy<P> {
    sources: HashMap<String, P>,
}

impl<P: PolicyProvider> PolicyProvider for PerBusPolicy<P> {
    type Error = P::Error;

    async fn read(&self, bus_id: &str) -> Result<VersionedPolicyState, Self::Error> {
        self.sources
            .get(bus_id)
            .expect("benchmark should initialize every request bus")
            .read(bus_id)
            .await
    }
}

struct BenchmarkOptions {
    args: Vec<String>,
    kind: WorkloadKind,
    rows_path: Option<PathBuf>,
    trace_path: Option<PathBuf>,
    sticky: bool,
    cache_enabled: bool,
}

fn parse_options(args: &[String]) -> Result<BenchmarkOptions> {
    let mut positional = Vec::new();
    let mut kind = WorkloadKind::default();
    let mut rows = None;
    let mut trace = None;
    let mut sticky = false;
    let mut cache_enabled = true;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sticky" => sticky = true,
            "--no-cache" => cache_enabled = false,
            "--workload" => {
                kind = match args.next().map(String::as_str) {
                    Some("shared-bus") => WorkloadKind::SharedBus,
                    Some("independent-buses") => WorkloadKind::IndependentBuses,
                    _ => anyhow::bail!("--workload requires shared-bus or independent-buses"),
                };
            }
            "--rows" => {
                rows =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("--rows requires an output path")
                    })?));
            }
            "--trace" => {
                trace =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("--trace requires an output path")
                    })?));
            }
            _ => {
                anyhow::ensure!(!arg.starts_with('-'), "unknown option {arg}");
                positional.push(arg.clone());
            }
        }
    }
    Ok(BenchmarkOptions {
        args: positional,
        kind,
        rows_path: rows,
        trace_path: trace,
        sticky,
        cache_enabled,
    })
}

type BenchStorage = FaultInjectingStorage<InMemoryStorage, Simulator>;
type SpaceFixture = FaultInjectingWriteOnceSpaceFixture<InMemoryWriteOnceSpaceFixture>;

/// One logged row with its scope and simulator interval.
struct TimedRow {
    scope: Rc<str>,
    start_us: u64,
    end_us: u64,
    row: LogRow,
}

/// Rows from every observable wrapper in one run. Each wrapper records its
/// `latency_us` histogram and then logs its row without awaiting in between, so
/// the simulator clock at `log_row` is the call's end and the latest latency
/// sample is its duration.
#[derive(Default)]
struct Timeline {
    last_latency_us: Cell<Option<u64>>,
    rows: RefCell<Vec<TimedRow>>,
}

/// Forwards metrics while noting each latency sample for the next logged row.
struct LatencyTap {
    inner: Rc<InMemoryMetrics>,
    timeline: Rc<Timeline>,
}

impl AgentBusMetrics for LatencyTap {
    fn record_counter(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
        self.inner.record_counter(key, value, tags);
    }

    fn record_histogram(&self, key: &str, value: i64, tags: &[(&str, &str)]) {
        if key.ends_with(".latency_us") {
            self.timeline.last_latency_us.set(Some(value as u64));
        }
        self.inner.record_histogram(key, value, tags);
    }
}

/// Records each row under its scope with simulator start and end times.
struct ScopedLogger {
    scope: Rc<str>,
    environment: Rc<Simulator>,
    timeline: Rc<Timeline>,
}

impl AgentbusLogger for ScopedLogger {
    fn log_row(&self, row: LogRow) {
        let end_us = now_us(&self.environment);
        let latency_us = self
            .timeline
            .last_latency_us
            .take()
            .expect("observable wrappers should record latency before logging a row");
        self.timeline.rows.borrow_mut().push(TimedRow {
            scope: self.scope.clone(),
            start_us: end_us - latency_us,
            end_us,
            row,
        });
    }
}

fn now_us(environment: &Simulator) -> u64 {
    let now = environment.with_clock(|clock| clock.monotonic_time());
    (now - MonotonicInstant::from_duration_since_clock_origin(Duration::ZERO)).as_micros() as u64
}

fn row_json(run: &str, timed: &TimedRow) -> Value {
    let fields: Map<_, _> = timed
        .row
        .additional_fields
        .iter()
        .map(|(field, value)| {
            let value = match value {
                LogFieldValue::String(value) => json!(value),
                LogFieldValue::StringVector(values) => json!(values),
                LogFieldValue::I64(value) => json!(value),
            };
            (field.clone(), value)
        })
        .collect();
    json!({
        "run": run, "scope": &*timed.scope, "api_call_name": timed.row.api_call_name,
        "start_us": timed.start_us, "end_us": timed.end_us, "latency_ms": timed.row.latency_ms,
        "error_code": timed.row.error_code, "error_message": timed.row.error_message, "fields": fields,
    })
}

/// Metric and row sinks shared by every observable wrapper in one run. Each
/// scope prefixes its metric keys and tags its rows.
struct Sinks {
    environment: Rc<Simulator>,
    metrics: Rc<InMemoryMetrics>,
    tap: Rc<LatencyTap>,
    timeline: Rc<Timeline>,
    scopes: RefCell<BTreeSet<String>>,
}

impl Sinks {
    fn new(environment: Rc<Simulator>) -> Rc<Self> {
        let metrics = Rc::new(InMemoryMetrics::new());
        let timeline = Rc::new(Timeline::default());
        Rc::new(Self {
            environment,
            tap: Rc::new(LatencyTap {
                inner: metrics.clone(),
                timeline: timeline.clone(),
            }),
            metrics,
            timeline,
            scopes: RefCell::new(BTreeSet::new()),
        })
    }

    fn scope(&self, scope: impl Into<String>) -> Observability<ScopedLogger, Simulator> {
        let scope = scope.into();
        self.scopes.borrow_mut().insert(scope.clone());
        Observability {
            metrics: Rc::new(PrefixedMetrics::new(scope.clone(), self.tap.clone())),
            logger: Rc::new(ScopedLogger {
                scope: scope.into(),
                environment: self.environment.clone(),
                timeline: self.timeline.clone(),
            }),
            environment: self.environment.clone(),
        }
    }

    fn bus<A: AgentBus>(&self, scope: &str, bus: A) -> ObservableAgentBus<A, Simulator> {
        let Observability {
            metrics,
            logger,
            environment,
        } = self.scope(scope);
        ObservableAgentBus::new(bus, metrics, logger, environment)
    }

    fn report(&self, run: &str) {
        for scope in self.scopes.borrow().iter() {
            for (component, operations) in OBSERVED_OPERATIONS {
                for operation in *operations {
                    let key = |metric: &str| format!("{scope}.{operation}.{component}.{metric}");
                    let calls = self.metrics.counter(&key("num_calls"));
                    if calls == 0 {
                        continue;
                    }
                    let micros = self
                        .metrics
                        .histogram(&key("latency_us"))
                        .into_iter()
                        .map(|micros| micros as u128)
                        .collect();
                    println!(
                        "{run:<12} {scope:<16} {operation:<16} {calls:>6} {:>6} {}",
                        self.metrics.counter(&key("num_errors")),
                        latency_columns(micros),
                    );
                }
            }
        }
    }

    fn export_rows(&self, run: &str) -> Vec<Value> {
        self.timeline
            .rows
            .borrow()
            .iter()
            .map(|timed| row_json(run, timed))
            .collect()
    }

    /// Chrome trace events with one track per scope. Rows carry no parent
    /// links, so overlapping calls on a track may come from different clients.
    fn export_trace(&self, pid: usize, run: &str) -> Vec<Value> {
        let scopes = self.scopes.borrow();
        let tid = |scope: &str| {
            scopes
                .iter()
                .position(|candidate| candidate == scope)
                .expect("every row scope should be registered")
                + 1
        };
        let process = json!({
            "name": "process_name", "ph": "M", "pid": pid, "tid": 0, "args": {"name": run},
        });
        let threads = scopes.iter().map(|scope| {
            json!({"name": "thread_name", "ph": "M", "pid": pid, "tid": tid(scope), "args": {"name": scope}})
        });
        let rows = self.timeline.rows.borrow();
        let spans = rows.iter().enumerate().flat_map(|(id, timed)| {
            let tid = tid(&timed.scope);
            [
                json!({
                    "name": timed.row.api_call_name, "cat": &*timed.scope, "ph": "b", "id": id,
                    "pid": pid, "tid": tid, "ts": timed.start_us, "args": row_json(run, timed),
                }),
                json!({
                    "name": timed.row.api_call_name, "cat": &*timed.scope, "ph": "e", "id": id,
                    "pid": pid, "tid": tid, "ts": timed.end_us,
                }),
            ]
        });
        once(process).chain(threads).chain(spans).collect()
    }
}

struct ObservableSpaceFixture {
    inner: SpaceFixture,
    sinks: Rc<Sinks>,
    cache_enabled: bool,
}

impl ConformanceFixture for ObservableSpaceFixture {
    type Env = Simulator;
    type Impl = CachingWriteOnceSpace<
        ObservableWriteOnceSpace<<SpaceFixture as ConformanceFixture>::Impl, Simulator>,
    >;

    fn get_env(&self) -> Rc<Simulator> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        let Observability {
            metrics,
            logger,
            environment,
        } = self.sinks.scope("write_once");
        CachingWriteOnceSpace::new(
            ObservableWriteOnceSpace::new(self.inner.create_impl(), metrics, logger, environment),
            if self.cache_enabled {
                CACHE_MAX_ENTRIES
            } else {
                0
            },
            CACHE_MAX_VALUE_BYTES,
        )
    }
}

#[derive(Default)]
struct Measurements {
    elapsed: Duration,
    counter: Vec<Duration>,
    gate_write: Vec<Duration>,
}

struct Measured<I> {
    inner: I,
    start: MonotonicInstant,
    measurements: Rc<RefCell<Measurements>>,
}

impl<I> LinearizabilityImplementation for Measured<I>
where
    I: LinearizabilityImplementation,
    I::Object: LinearizableObject<Spec = GatedCounterSpec>,
{
    type Object = I::Object;

    fn environment(&self) -> Rc<Simulator> {
        self.inner.environment()
    }

    fn create(&self, client: usize) -> Rc<Self::Object> {
        self.inner.create(client)
    }

    async fn candidate(&self, history: &HistoryFor<Self::Object>) -> Result<Vec<OperationId>> {
        {
            let mut measurements = self.measurements.borrow_mut();
            for observation in history.observations() {
                measurements.elapsed = measurements.elapsed.max(observation.end_time - self.start);
                let latency = observation.end_time - observation.start_time;
                match observation.completed.operation {
                    GatedCounterOperation::Counter(_) => measurements.counter.push(latency),
                    GatedCounterOperation::SetGate(_) => measurements.gate_write.push(latency),
                    GatedCounterOperation::ReadGate => {}
                }
            }
        }
        self.inner.candidate(history).await
    }

    fn on_complete(&self) -> Option<Rc<dyn Fn()>> {
        self.inner.on_complete()
    }
}

fn storage(environment: Rc<Simulator>) -> Rc<BenchStorage> {
    Rc::new(FaultInjectingStorage::new(
        InMemoryStorage::new(),
        environment,
        StorageFaultConfig {
            get_delay: Duration::from_millis(OPERATION_LATENCY_MS),
            put_delay: Duration::from_millis(OPERATION_LATENCY_MS),
            ..Default::default()
        },
    ))
}

struct BenchmarkRun {
    space_fixture: Rc<ObservableSpaceFixture>,
    workload: Vec<Client<GatedCounterOperation>>,
    kind: WorkloadKind,
    sticky: bool,
    measurements: Rc<RefCell<Measurements>>,
    sinks: Rc<Sinks>,
}

impl BenchmarkRun {
    async fn measure<P, C>(
        self,
        provider: impl Fn() -> P + 'static,
        controllers: Vec<(String, C)>,
    ) -> Result<()>
    where
        P: PolicyProvider<Error: Into<anyhow::Error>> + Clone + 'static,
        C: SettableDeciderPolicy + Clone + 'static,
    {
        let environment = self.space_fixture.get_env();
        let engine_storage = Rc::new(ObservableStorage::new(
            storage(environment.clone()),
            self.sinks.scope("engine_storage"),
        ));
        let service_space = self.space_fixture.clone();
        let service_environment = environment.clone();
        let service_sinks = self.sinks.clone();
        let make_service = move |instance| {
            let bus = WriteOnceAgentBusGenericFixture::new(service_space.clone()).create_impl();
            let service = CommitServiceV1::new(service_sinks.bus("service_bus", bus), |bus| {
                BaseEngine::new(
                    bus,
                    engine_storage.clone(),
                    DelegatingVoterFactory::with_storage(engine_storage.clone()),
                    provider(),
                    DeciderFactoryImpl::new(engine_storage.clone()),
                    service_environment.clone(),
                )
            });
            let Observability {
                metrics,
                logger,
                environment,
            } = service_sinks.scope(format!("service_{instance}"));
            ObservableCommitSvc::new(service, metrics, logger, environment)
        };
        let start = environment.with_clock(|clock| clock.monotonic_time());
        match self.kind {
            WorkloadKind::SharedBus => {
                let services: Vec<_> = (1..=SERVICE_INSTANCES).map(make_service).collect();
                let router = RandomRoutingCommitService::new(services, environment.clone());
                let verification_bus = self.sinks.bus(
                    "client_bus",
                    WriteOnceAgentBusGenericFixture::new(self.space_fixture.clone()).create_impl(),
                );
                let (bus_id, controller) = controllers
                    .into_iter()
                    .next()
                    .expect("shared-bus policy should exist");
                let implementation = CommitServiceGatedCounterImplementation::new(
                    router,
                    verification_bus,
                    controller,
                    bus_id,
                    environment.clone(),
                );
                run_linearizability_test(
                    self.workload,
                    Measured {
                        inner: implementation,
                        start,
                        measurements: self.measurements,
                    },
                )
                .await?;
            }
            WorkloadKind::IndependentBuses => {
                let services: Vec<_> = (1..=SERVICE_INSTANCES).map(make_service).collect();
                let runs = self.workload.into_iter().zip(controllers).enumerate().map(
                    |(client, (workload, (bus_id, controller)))| {
                        let client_services = if self.sticky {
                            vec![services[client % services.len()].clone()]
                        } else {
                            services.clone()
                        };
                        let router =
                            RandomRoutingCommitService::new(client_services, environment.clone());
                        let verification_bus = self.sinks.bus(
                            "client_bus",
                            WriteOnceAgentBusGenericFixture::new(self.space_fixture.clone())
                                .create_impl(),
                        );
                        let implementation = CommitServiceGatedCounterImplementation::new(
                            router,
                            verification_bus,
                            controller,
                            bus_id,
                            environment.clone(),
                        );
                        let measured = Measured {
                            inner: implementation,
                            start,
                            measurements: self.measurements.clone(),
                        };
                        async move { run_linearizability_test(vec![workload], measured).await }
                    },
                );
                let handles: Vec<_> = runs.map(|run| environment.spawn(run)).collect();
                for handle in handles {
                    handle.await??;
                }
            }
        }
        Ok(())
    }
}

struct RunConfig {
    seed: u64,
    kind: WorkloadKind,
    sticky: bool,
    cache_enabled: bool,
}

fn run_one(
    config: RunConfig,
    workload: Vec<Client<GatedCounterOperation>>,
) -> Result<(Measurements, Rc<Sinks>)> {
    let RunConfig {
        seed,
        kind,
        sticky,
        cache_enabled,
    } = config;
    let fixed_latency = Uniform::new(OPERATION_LATENCY_MS, OPERATION_LATENCY_MS + 1)?;
    let space_fixture = SpaceFixture::new_with_config_and_latency(
        Simulator::new(seed),
        FaultConfig {
            prob_lost: 0.0,
            prob_commit_then_error: 0.0,
            prob_error_then_commit: 0.0,
        },
        fixed_latency,
        fixed_latency,
    );
    let environment = space_fixture.get_env();
    let sinks = Sinks::new(environment.clone());
    let space_fixture = Rc::new(ObservableSpaceFixture {
        inner: space_fixture,
        sinks: sinks.clone(),
        cache_enabled,
    });
    let task_sinks = sinks.clone();
    let measurements = Rc::new(RefCell::new(Measurements::default()));
    let task_environment = environment.clone();
    let task_measurements = measurements.clone();
    let task = environment.spawn(async move {
        let initial_policy = PolicyState {
            decider_policy: Some(DeciderPolicy::OnByDefault as i32),
            ..Default::default()
        };
        let policy_storage = storage(task_environment.clone());
        let observed_policy_storage = Rc::new(ObservableStorage::new(
            policy_storage.clone(),
            task_sinks.scope("policy_register"),
        ));
        let mut registers = Vec::new();
        for (index, bus_id) in kind.bus_ids().into_iter().enumerate() {
            let key = match kind {
                WorkloadKind::SharedBus => "benchmark-policy".to_string(),
                WorkloadKind::IndependentBuses => format!("benchmark-policy-{index}"),
            };
            // Initialization bypasses the observable storage so metrics cover only the workload.
            PolicyRegister::new(policy_storage.clone(), key.clone())
                .set_policy(&initial_policy, None)
                .await?;
            registers.push((
                bus_id,
                PolicyRegister::new(observed_policy_storage.clone(), key),
            ));
        }
        let run = BenchmarkRun {
            space_fixture,
            workload,
            kind,
            sticky,
            measurements: task_measurements,
            sinks: task_sinks,
        };
        let provider_registers = registers.clone();
        run.measure(
            move || PerBusPolicy {
                sources: provider_registers
                    .iter()
                    .map(|(bus_id, register)| {
                        let voter_factory = DelegatingVoterFactory::default();
                        let provider =
                            SynchronousRegisterProvider::new(register.clone(), move |policy| {
                                validate_voter_configs(&voter_factory, policy)
                            });
                        (bus_id.clone(), provider)
                    })
                    .collect(),
            },
            registers,
        )
        .await
    });
    environment.run();
    futures::executor::block_on(task)??;
    Ok((
        Rc::try_unwrap(measurements)
            .unwrap_or_else(|_| panic!("benchmark task should release measurements"))
            .into_inner(),
        sinks,
    ))
}

/// Mean, p50, p95 and p99 columns in milliseconds.
fn latency_columns(mut micros: Vec<u128>) -> String {
    micros.sort_unstable();
    let percentile = |percent: usize| micros[(micros.len() * percent).div_ceil(100) - 1];
    format!(
        "{:>10.1} {:>10.1} {:>10.1} {:>10.1}",
        micros.iter().sum::<u128>() as f64 / micros.len() as f64 / 1000.0,
        percentile(50) as f64 / 1000.0,
        percentile(95) as f64 / 1000.0,
        percentile(99) as f64 / 1000.0,
    )
}

fn report(workload: &str, provider: &str, measurements: &Measurements) {
    for (kind, samples) in [
        ("counter", &measurements.counter),
        ("gate-write", &measurements.gate_write),
    ] {
        if samples.is_empty() {
            continue;
        }
        println!(
            "{workload:<12} {provider:<12} {kind:<12} {:>5} {} {:>12.1}",
            samples.len(),
            latency_columns(samples.iter().map(Duration::as_micros).collect()),
            measurements.elapsed.as_secs_f64() * 1000.0,
        );
    }
}

fn run(args: &[String]) -> Result<()> {
    let BenchmarkOptions {
        args,
        kind,
        rows_path,
        trace_path,
        sticky,
        cache_enabled,
    } = parse_options(args)?;
    anyhow::ensure!(
        args.len() <= 1,
        "usage: sim_benchmark [seed] [--workload shared-bus|independent-buses] [--rows PATH] [--trace PATH] [--sticky] [--no-cache]"
    );
    let seed = args
        .first()
        .map(|arg| arg.parse())
        .transpose()?
        .unwrap_or(1);
    let mut rng = StdRng::seed_from_u64(seed);
    let counters: Vec<_> = (0..COUNTER_CLIENTS)
        .map(|client| {
            Client::new(
                format!("counter-{client}"),
                (0..COUNTER_OPERATIONS)
                    .map(|_| {
                        GatedCounterOperation::Counter(if rng.random() {
                            CounterOperation::Increment
                        } else {
                            CounterOperation::Decrement
                        })
                    })
                    .collect(),
            )
        })
        .collect();
    println!(
        "seed={seed}; workload={}; {SERVICE_INSTANCES} service instances; {COUNTER_CLIENTS} counter clients x {COUNTER_OPERATIONS} operations; write-once/storage/policy operations={OPERATION_LATENCY_MS} ms; no faults",
        kind.name()
    );
    println!("Routing: {}", kind.routing(sticky));
    println!(
        "Counter latencies include adapter replay. Initialization and final verification are excluded. The policy provider reads the backing register synchronously on each commit."
    );
    println!(
        "Separate AgentBus clients and caches per service instance; shared backing storage. Cache enabled={cache_enabled}; limits={CACHE_MAX_ENTRIES} entries / {CACHE_MAX_VALUE_BYTES} value bytes; backing read delay only on misses."
    );
    println!(
        "{:<12} {:<12} {:<12} {:>5} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "Workload",
        "Provider",
        "Operation",
        "N",
        "Mean ms",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "Total sim ms"
    );
    let mut observed = Vec::new();
    for mixed in [false, true] {
        let name = if mixed { "gate-writes" } else { "stable-gate" };
        let mut workload = counters.clone();
        if mixed {
            match kind {
                WorkloadKind::SharedBus => workload.push(Client::new(
                    "gate",
                    (0..GATE_WRITES)
                        .map(|index| GatedCounterOperation::SetGate(index % 2 != 0))
                        .collect(),
                )),
                WorkloadKind::IndependentBuses => {
                    for client in &mut workload {
                        client.operations = client
                            .operations
                            .iter()
                            .enumerate()
                            .flat_map(|(index, operation)| {
                                let mut operations = vec![operation.clone()];
                                if (index + 1).is_multiple_of(COUNTER_OPERATIONS / GATE_WRITES) {
                                    operations.push(GatedCounterOperation::SetGate(
                                        ((index + 1) / (COUNTER_OPERATIONS / GATE_WRITES))
                                            .is_multiple_of(2),
                                    ));
                                }
                                operations
                            })
                            .collect();
                    }
                }
            }
        }
        let provider = "synchronous";
        let (measurements, sinks) = run_one(
            RunConfig {
                seed,
                kind,
                sticky,
                cache_enabled,
            },
            workload,
        )?;
        report(name, provider, &measurements);
        observed.push((name, sinks));
    }

    println!();
    println!(
        "Observable metrics. service_bus includes the adapter replay; client_bus is only the final verification read:"
    );
    println!(
        "{:<12} {:<16} {:<16} {:>6} {:>6} {:>10} {:>10} {:>10} {:>10}",
        "Workload",
        "Scope",
        "Operation",
        "Calls",
        "Errors",
        "Mean ms",
        "p50 ms",
        "p95 ms",
        "p99 ms"
    );
    for (name, sinks) in &observed {
        sinks.report(name);
    }

    let metadata = json!({
        "seed": seed, "workload": kind.name(), "service_instances": SERVICE_INSTANCES, "bus_ids": kind.bus_ids(),
        "operation_latency_ms": OPERATION_LATENCY_MS, "counter_clients": COUNTER_CLIENTS,
        "counter_operations_per_client": COUNTER_OPERATIONS, "gate_writes": GATE_WRITES,
        "clock": "simulator monotonic microseconds", "backing_bus": "write-once AgentBus over in-memory WriteOnceSpace with backing-operation delays",
        "services": "separately constructed persistent instances with independent AgentBus clients; shared backing storage",
        "write_once_cache": {"enabled": cache_enabled, "scope": "private per service instance; shared only by that client's clones", "eviction": "LRU", "max_entries": CACHE_MAX_ENTRIES, "max_value_bytes": CACHE_MAX_VALUE_BYTES, "backing_read_delay": "cache misses only"},
        "routing": kind.routing(sticky),
        "timing": "end = simulator clock when the observable wrapper logs its row; start = end - its latency_us sample",
    });
    if let Some(path) = rows_path {
        let rows: Vec<_> = observed
            .iter()
            .flat_map(|(name, sinks)| sinks.export_rows(name))
            .collect();
        write_json(&path, &json!({"rows": rows, "metadata": metadata}))?;
        println!("Observable rows written to {} (JSON)", path.display());
    }
    if let Some(path) = trace_path {
        let events: Vec<_> = observed
            .iter()
            .enumerate()
            .flat_map(|(index, (name, sinks))| {
                sinks.export_trace(index + 1, &format!("{}/{name}/synchronous", kind.name()))
            })
            .collect();
        write_json(
            &path,
            &json!({"traceEvents": events, "displayTimeUnit": "ms", "metadata": metadata}),
        )?;
        println!("Trace written to {} (Chrome/Perfetto JSON)", path.display());
    }
    Ok(())
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut output = BufWriter::new(File::create(path)?);
    serde_json::to_writer(&mut output, value)?;
    output.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    run(&env::args().skip(1).collect::<Vec<_>>())
}
