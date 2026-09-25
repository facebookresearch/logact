/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::AddVoter;
use agent_bus_proto_rust::agent_bus::AddVoterOp;
use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BaseEngineControl;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::Control;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Header;
use agent_bus_proto_rust::agent_bus::Intention;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::PollRequest;
use agent_bus_proto_rust::agent_bus::RemoveVoter;
use agent_bus_proto_rust::agent_bus::RemoveVoterOp;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::VoterOp;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::vote_type;
use agent_bus_proto_rust::agent_bus::voter_config;
use agent_bus_proto_rust::agent_bus::voter_op;
use agentbus_api::Environment;
use agentbus_api::InMemoryLogger;
use agentbus_api::InMemoryMetrics;
use agentbus_api::LogFieldValue;
use agentbus_simulator::generate_seed;
use agentbus_tests::fixtures::ConformanceFixture;
use agentbus_tests::fixtures::SimulatorFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use bytes::Bytes;
use logact_commit_service_engine::Applicator;
use logact_commit_service_engine::ApplyError;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::BaseEngineConfig;
use logact_commit_service_engine::ConcurrencyError;
use logact_commit_service_engine::DeciderFactory;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::DeprecatedOperationKind;
use logact_commit_service_engine::EngineError;
use logact_commit_service_engine::EngineResult;
use logact_commit_service_engine::ImmutableVoter;
use logact_commit_service_engine::InMemoryStorage;
use logact_commit_service_engine::MalformedPolicyBatchKind;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::ObservableApplicator;
use logact_commit_service_engine::ObservedDeciderFactory;
use logact_commit_service_engine::PerBusEngineState;
use logact_commit_service_engine::PolicyProvider;
use logact_commit_service_engine::PolicyRegister;
use logact_commit_service_engine::PolicyState;
use logact_commit_service_engine::ProposalOutcome;
use logact_commit_service_engine::RetryConfig;
use logact_commit_service_engine::StateMachineSpec;
use logact_commit_service_engine::StatelessVoterAdapter;
use logact_commit_service_engine::StaticConfigPolicyProvider;
use logact_commit_service_engine::Storage;
use logact_commit_service_engine::StorageError;
use logact_commit_service_engine::StorageResult;
use logact_commit_service_engine::SynchronousRegisterProvider;
use logact_commit_service_engine::VersionedPolicy;
use logact_commit_service_engine::VersionedPolicyState;
use logact_commit_service_engine::VoterFactory;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_engine_tests::voters::CountingVoter;
use logact_commit_service_engine_tests::voters::CountingVoterConfig;
use logact_commit_service_engine_tests::voters::CountingVoterFactory;
use logact_commit_service_engine_tests::voters::boolean_vote;
use prost::Message;

struct PlaceholderVoter;

#[async_trait::async_trait(?Send)]
impl agentbus_api::voter::Voter for PlaceholderVoter {
    async fn evaluate(&self, _context: agentbus_api::voter::VoterContext<'_>) -> (bool, String) {
        (true, String::new())
    }

    fn apply_policy(&mut self, _config: &prost_types::Any) {}

    fn describe(&self) -> String {
        "PlaceholderVoter".to_string()
    }
}

struct RetryTestStorage {
    inner: InMemoryStorage,
    engine_cas_pending: Cell<bool>,
    engine_transaction_conflict_pending: Cell<bool>,
    voter_cas_pending: Cell<bool>,
    decider_transaction_conflict_pending: Cell<bool>,
}

impl RetryTestStorage {
    fn new() -> Self {
        Self {
            inner: InMemoryStorage::new(),
            engine_cas_pending: Cell::new(true),
            engine_transaction_conflict_pending: Cell::new(true),
            voter_cas_pending: Cell::new(true),
            decider_transaction_conflict_pending: Cell::new(true),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Storage for RetryTestStorage {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        if key.starts_with("engine:state:")
            && self.engine_transaction_conflict_pending.replace(false)
        {
            return Err(StorageError::TransactionConflict(anyhow::anyhow!(
                "injected engine transaction conflict"
            )));
        }
        if key.starts_with("engine:state:")
            && expected.is_some()
            && self.engine_cas_pending.replace(false)
        {
            // Model the in-flight write that caused the preceding transaction
            // conflict becoming visible before this retry's conditional write.
            if !self.inner.put(key, value, expected, new_position).await? {
                return Err(StorageError::InternalError(anyhow::anyhow!(
                    "injected competing engine write was rejected"
                )));
            }
            return Ok(false);
        }
        if key.starts_with("decider:state:")
            && self.decider_transaction_conflict_pending.replace(false)
        {
            return Err(StorageError::TransactionConflict(anyhow::anyhow!(
                "injected decider transaction conflict"
            )));
        }
        if key.starts_with("voter:") && self.voter_cas_pending.replace(false) {
            // Model a competing writer winning with this transition before this
            // caller learns that its CAS was rejected.
            if !self.inner.put(key, value, expected, new_position).await? {
                return Err(StorageError::InternalError(anyhow::anyhow!(
                    "injected competing voter write was rejected"
                )));
            }
            return Ok(false);
        }
        self.inner.put(key, value, expected, new_position).await
    }
}

struct PersistentConflictStorage {
    inner: InMemoryStorage,
    key_prefix: &'static str,
    put_calls: Cell<usize>,
}

impl PersistentConflictStorage {
    fn new(key_prefix: &'static str) -> Self {
        Self {
            inner: InMemoryStorage::new(),
            key_prefix,
            put_calls: Cell::new(0),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Storage for PersistentConflictStorage {
    async fn get(&self, key: &str) -> StorageResult<Option<(Bytes, i64)>> {
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        expected: Option<i64>,
        new_position: i64,
    ) -> StorageResult<bool> {
        if key.starts_with(self.key_prefix) {
            self.put_calls.set(self.put_calls.get() + 1);
            return Err(StorageError::TransactionConflict(anyhow::anyhow!(
                "persistent transaction conflict"
            )));
        }
        self.inner.put(key, value, expected, new_position).await
    }
}

struct StatelessTestVoterFactory<S> {
    storage: Rc<S>,
}

impl<S: Storage + 'static> VoterFactory for StatelessTestVoterFactory<S> {
    fn validate_config(&self, config: Option<&VoterConfig>) -> anyhow::Result<VoterConfig> {
        config
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing voter config"))
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> anyhow::Result<Rc<dyn Applicator>> {
        Ok(Rc::new(StatelessVoterAdapter::new(
            ImmutableVoter::new(
                spec.id.clone(),
                spec.config.clone(),
                Rc::new(PlaceholderVoter),
            ),
            self.storage.clone(),
        )))
    }
}

#[derive(Clone, prost::Message)]
struct RandomVoterState {
    #[prost(uint64, tag = "1")]
    prng: u64,
    #[prost(int64, tag = "2")]
    last_position: i64,
}

const INITIAL_SEED: u64 = 88172645463325252;

#[derive(Clone)]
struct ControllablePolicyProvider {
    desired: Rc<RefCell<VersionedPolicyState>>,
}

impl ControllablePolicyProvider {
    fn new(desired: PolicyState, version: i64) -> Self {
        Self {
            desired: Rc::new(RefCell::new(VersionedPolicyState {
                state: desired,
                version,
            })),
        }
    }

    fn set_version(&self, version: i64) {
        self.desired.borrow_mut().version = version;
    }

    fn set_desired(&self, state: PolicyState, version: i64) {
        *self.desired.borrow_mut() = VersionedPolicyState { state, version };
    }
}

impl PolicyProvider for ControllablePolicyProvider {
    type Error = anyhow::Error;

    async fn read(&self, _bus_id: &str) -> anyhow::Result<VersionedPolicyState> {
        Ok(self.desired.borrow().clone())
    }
}

#[derive(Clone)]
struct DefaultPolicyProvider;

impl PolicyProvider for DefaultPolicyProvider {
    type Error = anyhow::Error;

    async fn read(&self, _bus_id: &str) -> anyhow::Result<VersionedPolicyState> {
        Ok(VersionedPolicyState {
            state: PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
            version: -1,
        })
    }
}

/// xorshift64 step — shared by `RandomVoter` and its test so the expected vote for
/// a given seed can be computed without duplicating the logic.
fn next_prng(x: u64) -> u64 {
    let mut x = x;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Config for `RandomVoter`: the initial PRNG seed.
#[derive(Clone, prost::Message)]
struct RandomVoterConfig {
    #[prost(uint64, tag = "1")]
    seed: u64,
}

struct RandomVoter {
    storage: Rc<InMemoryStorage>,
    voter_id: String,
    seed: u64,
}

impl RandomVoter {
    fn state_key(&self, bus_id: &str) -> String {
        format!("random_voter:{}:{}", self.voter_id, bus_id)
    }

    async fn load_state(&self, bus_id: &str) -> (RandomVoterState, Option<i64>) {
        match self
            .storage
            .get(&self.state_key(bus_id))
            .await
            .ok()
            .flatten()
        {
            Some((b, pos)) => (
                RandomVoterState::decode(b.as_ref()).unwrap_or_default(),
                Some(pos),
            ),
            None => (
                RandomVoterState {
                    prng: self.seed,
                    last_position: 0,
                },
                None,
            ),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Applicator for RandomVoter {
    async fn apply(&self, bus_id: &str, entry: &BusEntry) -> Result<Option<Payload>, ApplyError> {
        let position = entry.header.as_ref().map(|h| h.log_position).unwrap_or(0);
        let inner = match entry.payload.as_ref().and_then(|p| p.payload.as_ref()) {
            Some(p) => p,
            None => return Ok(None),
        };

        match inner {
            payload::Payload::Intention(_) => {
                let (mut state, stored_pos) = self.load_state(bus_id).await;
                if stored_pos.is_some() {
                    if position < state.last_position {
                        return Err(ApplyError::StalePosition {
                            requested: position,
                            last: state.last_position,
                        });
                    }
                    if position == state.last_position {
                        // Replay of the last entry: recompute from the stored
                        // (already-advanced) prng without advancing again.
                        return Ok(Some(boolean_vote(
                            position,
                            &self.voter_id,
                            state.prng & 1 == 0,
                        )));
                    }
                }
                let x = next_prng(state.prng);
                state.prng = x;
                state.last_position = position;
                let _ = self
                    .storage
                    .put(
                        &self.state_key(bus_id),
                        Bytes::from(state.encode_to_vec()),
                        stored_pos,
                        position,
                    )
                    .await;
                Ok(Some(boolean_vote(position, &self.voter_id, x & 1 == 0)))
            }
            _ => Ok(None),
        }
    }
}

struct TestVoterFactory {
    storage: Rc<InMemoryStorage>,
}

impl TestVoterFactory {
    fn new(storage: Rc<InMemoryStorage>) -> Self {
        Self { storage }
    }
}

fn placeholder_voter_config() -> VoterConfig {
    VoterConfig {
        config: Some(voter_config::Config::RuleBased(Default::default())),
    }
}

fn custom_placeholder_voter_config() -> VoterConfig {
    VoterConfig {
        config: Some(voter_config::Config::Custom(prost_types::Any {
            type_url: "agentbus/placeholder_voter".to_string(),
            value: Vec::new(),
        })),
    }
}

fn custom_voter_config(config: &VoterConfig) -> anyhow::Result<&prost_types::Any> {
    match config.config.as_ref() {
        Some(voter_config::Config::Custom(any)) => Ok(any),
        _ => anyhow::bail!("test factory only supports custom voter configs"),
    }
}

impl VoterFactory for TestVoterFactory {
    fn validate_config(&self, config: Option<&VoterConfig>) -> anyhow::Result<VoterConfig> {
        let config = config.ok_or_else(|| anyhow::anyhow!("missing voter config"))?;
        if matches!(
            config.config.as_ref(),
            Some(voter_config::Config::RuleBased(_))
        ) {
            return Ok(config.clone());
        }

        let custom = custom_voter_config(config)?;
        match custom.type_url.as_str() {
            "test/counting_voter" | "test/random_voter" | "agentbus/placeholder_voter" => {
                Ok(config.clone())
            }
            other => Err(anyhow::anyhow!("unknown test voter type: {other}")),
        }
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> anyhow::Result<Rc<dyn Applicator>> {
        if matches!(
            spec.config.config.as_ref(),
            Some(voter_config::Config::RuleBased(_))
        ) {
            return Ok(Rc::new(StatelessVoterAdapter::new(
                logact_commit_service_engine::ImmutableVoter::new(
                    spec.id.clone(),
                    spec.config.clone(),
                    Rc::new(PlaceholderVoter),
                ),
                self.storage.clone(),
            )));
        }

        let custom = custom_voter_config(&spec.config)?;
        match custom.type_url.as_str() {
            "test/counting_voter" => {
                CountingVoterFactory::new(self.storage.clone()).create_voter(spec)
            }
            "test/random_voter" => {
                let cfg = RandomVoterConfig::decode(custom.value.as_slice()).unwrap_or_default();
                Ok(Rc::new(RandomVoter {
                    storage: self.storage.clone(),
                    voter_id: spec.id.clone(),
                    // An unset (default 0) seed falls back to the fixed seed.
                    seed: if cfg.seed == 0 {
                        INITIAL_SEED
                    } else {
                        cfg.seed
                    },
                }))
            }
            "agentbus/placeholder_voter" => Ok(Rc::new(StatelessVoterAdapter::new(
                logact_commit_service_engine::ImmutableVoter::new(
                    spec.id.clone(),
                    spec.config.clone(),
                    Rc::new(PlaceholderVoter),
                ),
                self.storage.clone(),
            ))),
            other => Err(anyhow::anyhow!("unknown test voter type: {other}")),
        }
    }
}

struct ObservedTestVoterFactory<L, E> {
    inner: TestVoterFactory,
    observability: Observability<L, E>,
}

impl<L, E> VoterFactory for ObservedTestVoterFactory<L, E>
where
    L: agentbus_api::logger::AgentbusLogger,
    E: agentbus_api::Environment + 'static,
{
    fn validate_config(&self, config: Option<&VoterConfig>) -> anyhow::Result<VoterConfig> {
        self.inner.validate_config(config)
    }

    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> anyhow::Result<Rc<dyn Applicator>> {
        let voter = self.inner.create_voter(spec)?;
        Ok(Rc::new(ObservableApplicator::new(
            voter,
            "test-voter",
            self.observability.clone(),
        )))
    }
}

fn payload_type_name(p: &payload::Payload) -> &'static str {
    match p {
        payload::Payload::Intention(_) => "Intention",
        payload::Payload::Vote(_) => "Vote",
        payload::Payload::Commit(_) => "Commit",
        payload::Payload::Abort(_) => "Abort",
        payload::Payload::VoterPolicy(_) => "VoterPolicy",
        payload::Payload::Control(control) => match control.control.as_ref() {
            Some(control::Control::BaseEngineControl(base_engine_control)) => {
                match base_engine_control.control.as_ref() {
                    Some(base_engine_control::Control::AddVoter(_)) => "AddVoter",
                    Some(base_engine_control::Control::RemoveVoter(_)) => "RemoveVoter",
                    Some(base_engine_control::Control::PolicyBatch(_)) => "PolicyBatch",
                    None => "BaseEngineControl",
                }
            }
            _ => "Control",
        },
        payload::Payload::DeciderPolicy(_) => "DeciderPolicy",
        payload::Payload::AgentInput(_) => "AgentInput",
        _ => "Other",
    }
}

fn collect_payload_types(
    entries: &[agent_bus_proto_rust::agent_bus::BusEntry],
) -> Vec<&'static str> {
    entries
        .iter()
        .filter_map(|e| e.payload.as_ref().and_then(|p| p.payload.as_ref()))
        .flat_map(|p| match p {
            // A policy batch is transparent to the type view: flatten it into the
            // types of the policy changes it carries (decider policy, then adds,
            // then removes).
            payload::Payload::Control(control) => match control.control.as_ref() {
                Some(control::Control::BaseEngineControl(base)) => match base.control.as_ref() {
                    Some(base_engine_control::Control::PolicyBatch(batch)) => {
                        let mut names = Vec::new();
                        if batch.decider_policy.is_some() {
                            names.push("DeciderPolicy");
                        }
                        names.extend(batch.voter_ops.values().filter_map(|op| {
                            matches!(op.op.as_ref(), Some(voter_op::Op::Add(_)))
                                .then_some("AddVoter")
                        }));
                        names.extend(batch.voter_ops.values().filter_map(|op| {
                            matches!(op.op.as_ref(), Some(voter_op::Op::Remove(_)))
                                .then_some("RemoveVoter")
                        }));
                        names
                    }
                    _ => vec![payload_type_name(p)],
                },
                _ => vec![payload_type_name(p)],
            },
            _ => vec![payload_type_name(p)],
        })
        .collect()
}

fn string_intention(body: &str) -> Payload {
    Payload {
        payload: Some(payload::Payload::Intention(
            agent_bus_proto_rust::agent_bus::Intention {
                intention: Some(
                    agent_bus_proto_rust::agent_bus::intention::Intention::StringIntention(
                        body.to_string(),
                    ),
                ),
                ..Default::default()
            },
        )),
    }
}

fn constrained_string_intention(
    body: &str,
    constraint: intention::PolicyVersionConstraint,
) -> Payload {
    Payload {
        payload: Some(payload::Payload::Intention(Intention {
            intention: Some(intention::Intention::StringIntention(body.to_string())),
            policy_version_constraint: Some(constraint),
        })),
    }
}

async fn append_intention<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    intention: Payload,
) -> anyhow::Result<i64> {
    Ok(bus
        .append(AppendRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            payload: Some(intention),
        })
        .await?
        .log_position)
}

fn intention_entry(position: i64, body: &str) -> BusEntry {
    BusEntry {
        header: Some(Header {
            log_position: position,
            ..Default::default()
        }),
        payload: Some(string_intention(body)),
    }
}

fn vote_bool(out: Option<Payload>) -> Option<bool> {
    match out.and_then(|p| p.payload) {
        Some(payload::Payload::Vote(v)) => {
            v.abstract_vote
                .and_then(|vt| vt.vote_type)
                .and_then(|vt| match vt {
                    vote_type::VoteType::BooleanVote(b) => Some(b),
                    _ => None,
                })
        }
        _ => None,
    }
}

fn vote_voter_id(out: Option<Payload>) -> Option<String> {
    match out.and_then(|p| p.payload) {
        Some(payload::Payload::Vote(v)) => Some(v.voter_id),
        _ => None,
    }
}

async fn append_voter_policy<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    type_url: &str,
) -> anyhow::Result<()> {
    append_voter_policy_with_config(bus, agent_id, type_url, vec![]).await
}

async fn append_voter_policy_with_config<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    type_url: &str,
    value: Vec<u8>,
) -> anyhow::Result<()> {
    append_add_voter(bus, agent_id, type_url, value).await?;
    Ok(())
}

async fn append_add_voter<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    type_url: &str,
    value: Vec<u8>,
) -> anyhow::Result<String> {
    let voter_id = bus
        .poll(PollRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            start_log_position: 0,
            max_entries: i32::MAX,
            ..Default::default()
        })
        .await?
        .entries
        .len()
        .to_string();
    let (expected_current_version, new_version) = next_policy_batch_versions(bus, agent_id).await?;
    append_policy_batch(
        bus,
        agent_id,
        PolicyBatch {
            expected_current_version,
            new_version,
            voter_ops: [(
                voter_id.clone(),
                VoterOp {
                    op: Some(voter_op::Op::Add(AddVoterOp {
                        config: Some(VoterConfig {
                            config: Some(voter_config::Config::Custom(prost_types::Any {
                                type_url: type_url.to_string(),
                                value,
                            })),
                        }),
                    })),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        },
    )
    .await?;
    Ok(voter_id)
}

async fn append_remove_voter<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    voter_id: &str,
) -> anyhow::Result<()> {
    let (expected_current_version, new_version) = next_policy_batch_versions(bus, agent_id).await?;
    append_policy_batch(
        bus,
        agent_id,
        PolicyBatch {
            expected_current_version,
            new_version,
            voter_ops: [(
                voter_id.to_string(),
                VoterOp {
                    op: Some(voter_op::Op::Remove(RemoveVoterOp {})),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        },
    )
    .await
}

async fn next_policy_batch_versions<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
) -> anyhow::Result<(Option<i64>, i64)> {
    let entries = bus
        .poll(PollRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            start_log_position: 0,
            max_entries: i32::MAX,
            filter: None,
        })
        .await?
        .entries;
    let current = entries
        .iter()
        .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
            payload::Payload::Control(Control {
                control:
                    Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(batch)),
                    })),
            }) => Some(batch.new_version),
            _ => None,
        })
        .max();
    Ok((current, current.map_or(0, |version| version + 1)))
}
async fn append_policy_batch<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
    batch: PolicyBatch,
) -> anyhow::Result<()> {
    bus.append(AppendRequest {
        agent_bus_id: agent_id.to_string(),
        bus_id: Some(BusId {
            agent_bus_id: agent_id.to_string(),
        }),
        payload: Some(Payload {
            payload: Some(payload::Payload::Control(Control {
                control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                    control: Some(base_engine_control::Control::PolicyBatch(batch)),
                })),
            })),
        }),
    })
    .await?;
    Ok(())
}

async fn propose_intention<T, F, S, P, D, E>(
    engine: &BaseEngine<T, F, S, P, D, E>,
    agent_id: &str,
    body: &str,
) -> EngineResult<ProposalOutcome>
where
    T: agentbus_api::AgentBus + 'static,
    F: VoterFactory,
    S: Storage + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    D: DeciderFactory,
{
    engine
        .propose_intention(agent_id, string_intention(body))
        .await
}

async fn poll_all_types<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
) -> anyhow::Result<Vec<&'static str>> {
    let resp = bus
        .poll(PollRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            start_log_position: 0,
            max_entries: 100,
            filter: None,
        })
        .await?;
    Ok(collect_payload_types(&resp.entries))
}

/// Poll the bus and return the boolean verdict of each `Vote` entry, in order.
async fn poll_vote_bools<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
) -> anyhow::Result<Vec<bool>> {
    let resp = bus
        .poll(PollRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            start_log_position: 0,
            max_entries: 100,
            filter: None,
        })
        .await?;
    Ok(resp
        .entries
        .into_iter()
        .filter_map(|e| vote_bool(e.payload))
        .collect())
}

/// Poll the bus and return the voter ID of each `Vote` entry, in order.
async fn poll_vote_voter_ids<T: agentbus_api::AgentBus>(
    bus: &T,
    agent_id: &str,
) -> anyhow::Result<Vec<String>> {
    let resp = bus
        .poll(PollRequest {
            agent_bus_id: agent_id.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: agent_id.to_string(),
            }),
            start_log_position: 0,
            max_entries: 100,
            filter: None,
        })
        .await?;
    Ok(resp
        .entries
        .into_iter()
        .filter_map(|e| vote_voter_id(e.payload))
        .collect())
}

fn make_engine<T, E>(
    bus: T,
    storage: Rc<InMemoryStorage>,
    environment: Rc<E>,
) -> BaseEngine<T, TestVoterFactory, InMemoryStorage, DefaultPolicyProvider, DeciderFactoryImpl, E>
where
    T: agentbus_api::AgentBus + 'static,
    E: Environment + 'static,
{
    make_engine_with_policy_provider(bus, storage, DefaultPolicyProvider, environment)
}

fn make_engine_with_policy_provider<T, P, E>(
    bus: T,
    storage: Rc<InMemoryStorage>,
    policy_provider: P,
    environment: Rc<E>,
) -> BaseEngine<T, TestVoterFactory, InMemoryStorage, P, DeciderFactoryImpl, E>
where
    T: agentbus_api::AgentBus + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    E: Environment + 'static,
{
    let voter_factory = TestVoterFactory::new(storage.clone());
    let decider_factory = DeciderFactoryImpl::new(storage.clone());
    BaseEngine::new(
        bus,
        storage,
        voter_factory,
        policy_provider,
        decider_factory,
        environment,
    )
}

#[test]
fn engine_voter_and_decider_storage_conflicts_are_retried() {
    let simulator = agentbus_simulator::Simulator::new(generate_seed());
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let storage = Rc::new(RetryTestStorage::new());
        let voter_factory = StatelessTestVoterFactory {
            storage: storage.clone(),
        };
        let policy_validator = StatelessTestVoterFactory {
            storage: storage.clone(),
        };
        let engine = BaseEngine::new(
            bus.clone(),
            storage.clone(),
            voter_factory,
            StaticConfigPolicyProvider::new(
                DeciderPolicy::FirstBooleanWins,
                vec![VoterConfig::default()],
                move |policy| validate_voter_configs(&policy_validator, policy),
            ),
            DeciderFactoryImpl::new(storage.clone()),
            env_for_engine,
        );

        let outcome = propose_intention(&engine, "agent-1", "retry conflicts").await?;

        assert!(outcome.approved, "the retried vote should approve");
        assert!(
            !storage.engine_cas_pending.get(),
            "the engine CAS rejection should be injected"
        );
        assert!(
            !storage.engine_transaction_conflict_pending.get(),
            "the engine transaction conflict should be injected"
        );
        assert!(
            !storage.decider_transaction_conflict_pending.get(),
            "the decider transaction conflict should be injected"
        );
        assert!(
            !storage.voter_cas_pending.get(),
            "the voter CAS rejection should be injected"
        );
        assert_eq!(
            poll_all_types(&bus, "agent-1").await?,
            vec!["DeciderPolicy", "AddVoter", "Intention", "Vote", "Commit"],
            "all storage-concurrency retry paths should complete the intention"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn storage_concurrency_is_retried_only_by_its_owner() {
    for key_prefix in ["engine:state:", "decider:state:", "voter:"] {
        let simulator = agentbus_simulator::Simulator::new(generate_seed());
        let fixture = SimpleMemoryFixture::new(simulator);
        let env = fixture.get_env();
        let env_for_engine = env.clone();

        let handle = env.spawn(async move {
            let storage = Rc::new(PersistentConflictStorage::new(key_prefix));
            let voter_factory = StatelessTestVoterFactory {
                storage: storage.clone(),
            };
            let policy_validator = StatelessTestVoterFactory {
                storage: storage.clone(),
            };
            let engine = BaseEngine::new_with_config(
                fixture.create_impl(),
                storage.clone(),
                voter_factory,
                StaticConfigPolicyProvider::new(
                    DeciderPolicy::FirstBooleanWins,
                    vec![VoterConfig::default()],
                    move |policy| validate_voter_configs(&policy_validator, policy),
                ),
                DeciderFactoryImpl::new(storage.clone()),
                env_for_engine,
                BaseEngineConfig {
                    retry_config: RetryConfig::try_new(
                        1,
                        Duration::from_millis(10),
                        Duration::from_millis(100),
                    )
                    .expect("test retry configuration should be valid"),
                },
            );

            let error = match propose_intention(&engine, "agent-1", "exhaust retries").await {
                Ok(_) => panic!("persistent storage conflict should exhaust retries"),
                Err(error) => error,
            };

            let put_calls = storage.put_calls.get();
            assert_eq!(
                put_calls, 2,
                "the fixture should perform its one configured retry"
            );
            let EngineError::Playback(apply_error) = error else {
                panic!("the playback error should retain its ApplyError, got {error:?}");
            };
            let correctly_classified = match (key_prefix, &apply_error) {
                (
                    "engine:state:",
                    ApplyError::Concurrency(ConcurrencyError::Engine {
                        source: Some(StorageError::TransactionConflict(_)),
                        ..
                    }),
                )
                | (
                    "decider:state:",
                    ApplyError::Concurrency(ConcurrencyError::Decider {
                        source: Some(StorageError::TransactionConflict(_)),
                        ..
                    }),
                )
                | (
                    "voter:",
                    ApplyError::Concurrency(ConcurrencyError::Voter {
                        source: Some(StorageError::TransactionConflict(_)),
                        ..
                    }),
                ) => true,
                _ => false,
            };
            assert!(
                correctly_classified,
                "storage conflict should retain its owner: {apply_error:?}"
            );
            anyhow::Ok(())
        });

        env.run();
        futures::executor::block_on(handle)
            .expect("task should complete")
            .expect("test should succeed");
    }
}

#[test]
fn policy_provider_entries_are_resolved_before_first_intention() {
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            StaticConfigPolicyProvider::new(
                DeciderPolicy::FirstBooleanWins,
                vec![placeholder_voter_config()],
                |policy| {
                    validate_voter_configs(
                        &TestVoterFactory::new(Rc::new(InMemoryStorage::new())),
                        policy,
                    )
                },
            ),
            fixture.get_env(),
        );
        let agent = "agent-1";

        let outcome = propose_intention(&engine, agent, "after policy").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec!["DeciderPolicy", "AddVoter", "Intention", "Vote", "Commit"],
            "resolved policy entries should precede the first proposed intention"
        );
        let entries = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 100,
                ..Default::default()
            })
            .await?
            .entries;
        let constraint = entries.iter().find_map(|entry| {
            let payload::Payload::Intention(intention) =
                entry.payload.as_ref()?.payload.as_ref()?
            else {
                return None;
            };
            intention.policy_version_constraint.as_ref()
        });
        assert!(matches!(
            constraint,
            Some(
                agent_bus_proto_rust::agent_bus::intention::PolicyVersionConstraint::MinimumPolicyVersion(0)
            )
        ));

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_provider_rejects_reconfiguring_existing_voter() {
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let provider = ControllablePolicyProvider::new(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                voters: [("stable-voter".to_string(), placeholder_voter_config())]
                    .into_iter()
                    .collect(),
            },
            1,
        );
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            provider.clone(),
            fixture.get_env(),
        );
        let agent = "agent-1";

        assert!(
            propose_intention(&engine, agent, "install voter")
                .await?
                .approved
        );
        let entries_before_reconfiguration = poll_all_types(&bus, agent).await?;

        provider.set_desired(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                voters: [(
                    "stable-voter".to_string(),
                    custom_placeholder_voter_config(),
                )]
                .into_iter()
                .collect(),
            },
            2,
        );
        let error = match propose_intention(&engine, agent, "reject reconfiguration").await {
            Ok(_) => panic!("reconfiguring an existing voter should fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("voter 'stable-voter' is already installed with a different config"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            poll_all_types(&bus, agent).await?,
            entries_before_reconfiguration,
            "rejection must happen before policy or intention entries are appended"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn intention_policy_constraints_gate_dispatch_against_applied_version() {
    use agent_bus_proto_rust::agent_bus::intention::PolicyVersionConstraint;

    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let provider = ControllablePolicyProvider::new(
            PolicyState {
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                voters: [("voter".to_string(), placeholder_voter_config())]
                    .into_iter()
                    .collect(),
            },
            5,
        );
        let storage = Rc::new(InMemoryStorage::new());
        let engine = BaseEngine::new(
            fixture.create_impl(),
            storage.clone(),
            TestVoterFactory::new(storage.clone()),
            provider.clone(),
            DeciderFactoryImpl::new(storage),
            env_for_engine,
        );
        let agent = "agent-1";

        assert!(
            propose_intention(&engine, agent, "install version 5")
                .await?
                .approved
        );
        provider.set_version(6);
        assert!(
            propose_intention(&engine, agent, "install version 6")
                .await?
                .approved
        );
        provider.set_version(5);
        assert!(
            propose_intention(&engine, agent, "ignore stale version 5")
                .await?
                .approved
        );

        let exact_match = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "exact match",
                PolicyVersionConstraint::RequiredPolicyVersion(6),
            ),
        )
        .await?;
        let exact_mismatch = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "exact mismatch",
                PolicyVersionConstraint::RequiredPolicyVersion(5),
            ),
        )
        .await?;
        let minimum_satisfied = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "minimum satisfied",
                PolicyVersionConstraint::MinimumPolicyVersion(5),
            ),
        )
        .await?;
        let minimum_unmet = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "minimum unmet",
                PolicyVersionConstraint::MinimumPolicyVersion(7),
            ),
        )
        .await?;

        assert!(
            propose_intention(&engine, agent, "drive queued intentions")
                .await?
                .approved
        );

        let entries = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 100,
                ..Default::default()
            })
            .await?
            .entries;
        let policy_batches = entries
            .iter()
            .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                payload::Payload::Control(Control {
                    control:
                        Some(control::Control::BaseEngineControl(BaseEngineControl {
                            control: Some(base_engine_control::Control::PolicyBatch(batch)),
                        })),
                }) => Some(batch),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(policy_batches.len(), 2);
        let version_marker = policy_batches[1];
        assert_eq!(version_marker.expected_current_version, Some(5));
        assert_eq!(version_marker.new_version, 6);
        assert!(version_marker.decider_policy.is_none());
        assert!(version_marker.voter_ops.is_empty());

        let decisions_for = |intention_id| {
            entries
                .iter()
                .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                    payload::Payload::Commit(commit) if commit.intention_id == intention_id => {
                        Some("Commit")
                    }
                    payload::Payload::Abort(abort) if abort.intention_id == intention_id => {
                        Some("Abort")
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(decisions_for(exact_match), vec!["Commit"]);
        assert_eq!(decisions_for(exact_mismatch), vec!["Abort"]);
        assert_eq!(decisions_for(minimum_satisfied), vec!["Commit"]);
        assert_eq!(decisions_for(minimum_unmet), vec!["Abort"]);

        let voted_intentions = entries
            .iter()
            .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                payload::Payload::Vote(vote) => Some(vote.intention_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(voted_intentions.contains(&exact_match));
        assert!(voted_intentions.contains(&minimum_satisfied));
        assert!(!voted_intentions.contains(&exact_mismatch));
        assert!(!voted_intentions.contains(&minimum_unmet));

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn missing_policy_register_fails_before_appending() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let register = PolicyRegister::new(Rc::new(InMemoryStorage::new()), "missing-policy");
        let provider = SynchronousRegisterProvider::new(register, |policy| {
            validate_voter_configs(
                &TestVoterFactory::new(Rc::new(InMemoryStorage::new())),
                policy,
            )
        });
        let storage = Rc::new(InMemoryStorage::new());
        let engine = BaseEngine::new(
            fixture.create_impl(),
            storage.clone(),
            TestVoterFactory::new(storage.clone()),
            provider,
            DeciderFactoryImpl::new(storage),
            env_for_engine,
        );
        let agent = "agent-1";

        let error = match propose_intention(&engine, agent, "must not be appended").await {
            Ok(_) => panic!("a missing policy register should reject the proposal"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("is not initialized"),
            "unexpected error: {error:#}"
        );
        assert!(
            poll_all_types(&bus, agent).await?.is_empty(),
            "a rejected proposal must not append policy or intention entries"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn corrupt_persisted_decider_state_fails_playback() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let agent = "agent-1";
        let storage = Rc::new(InMemoryStorage::new());
        let state = PerBusEngineState {
            decider: Some(VersionedPolicy { policy: 99, id: 0 }),
            ..Default::default()
        };
        assert!(state.save(storage.as_ref(), agent, None, 0).await?);
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        let error = match engine.apply(agent, &intention_entry(1, "must fail")).await {
            Ok(_) => panic!("playback should fail on corrupt persisted decider state"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ApplyError::InvalidEngineState { ref message }
                if message.contains("unrecognized decider policy 99")
        ));

        let (_, stored_position) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert_eq!(
            stored_position,
            Some(0),
            "failed playback must not advance the engine cursor"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_batch_applies_decider_add_and_remove_atomically() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        let old_voter_id = append_add_voter(&bus, agent, "test/counting_voter", vec![]).await?;
        assert_eq!(old_voter_id, "0");

        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: Some(0),
                new_version: 1,
                decider_policy: Some(DeciderPolicy::FirstBooleanWins as i32),
                voter_ops: [
                    (
                        old_voter_id,
                        VoterOp {
                            op: Some(voter_op::Op::Remove(RemoveVoterOp {})),
                        },
                    ),
                    (
                        "batch-voter".to_string(),
                        VoterOp {
                            op: Some(voter_op::Op::Add(AddVoterOp {
                                config: Some(VoterConfig {
                                    config: Some(voter_config::Config::Custom(prost_types::Any {
                                        type_url: "test/counting_voter".to_string(),
                                        value: vec![],
                                    })),
                                }),
                            })),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            },
        )
        .await?;

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            DefaultPolicyProvider,
            fixture.get_env(),
        );
        let outcome = propose_intention(&engine, agent, "after batch").await?;
        assert!(outcome.approved, "the batch voter should approve");
        assert_eq!(poll_vote_voter_ids(&bus, agent).await?, vec!["batch-voter"]);
        assert_eq!(
            poll_all_types(&bus, agent).await?,
            vec![
                "AddVoter",
                "DeciderPolicy",
                "AddVoter",
                "RemoveVoter",
                "Intention",
                "Vote",
                "Commit",
            ]
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn propose_rejects_caller_supplied_policy_constraint_before_appending() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );
        let agent = "agent-1";
        let intention = constrained_string_intention(
            "caller constrained",
            intention::PolicyVersionConstraint::RequiredPolicyVersion(7),
        );

        let error = match engine.propose_intention(agent, intention).await {
            Ok(_) => panic!("caller-supplied policy constraints should be rejected"),
            Err(error) => error,
        };
        let EngineError::Internal(source) = error else {
            panic!("a caller-supplied constraint should be rejected before appending");
        };
        assert!(
            source
                .to_string()
                .contains("constraints are engine-managed"),
            "unexpected error: {source:#}"
        );
        assert!(
            poll_all_types(&bus, agent).await?.is_empty(),
            "rejection must happen before policy or intention entries are appended"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn intention_policy_constraints_gate_replay_before_dispatch() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 5,
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
        )
        .await?;

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        let exact_match = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "exact match",
                intention::PolicyVersionConstraint::RequiredPolicyVersion(5),
            ),
        )
        .await?;
        let exact_mismatch = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "exact mismatch",
                intention::PolicyVersionConstraint::RequiredPolicyVersion(6),
            ),
        )
        .await?;
        let minimum_satisfied = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "minimum satisfied",
                intention::PolicyVersionConstraint::MinimumPolicyVersion(4),
            ),
        )
        .await?;
        let minimum_unmet = append_intention(
            &bus,
            agent,
            constrained_string_intention(
                "minimum unmet",
                intention::PolicyVersionConstraint::MinimumPolicyVersion(6),
            ),
        )
        .await?;

        assert!(
            propose_intention(&engine, agent, "drive queued intentions")
                .await?
                .approved
        );

        let entries = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 100,
                filter: None,
            })
            .await?
            .entries;
        let decisions_for = |intention_id| {
            entries
                .iter()
                .filter_map(|entry| match entry.payload.as_ref()?.payload.as_ref()? {
                    payload::Payload::Commit(commit) if commit.intention_id == intention_id => {
                        Some("Commit")
                    }
                    payload::Payload::Abort(abort) if abort.intention_id == intention_id => {
                        Some("Abort")
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(decisions_for(exact_match), vec!["Commit"]);
        assert_eq!(decisions_for(exact_mismatch), vec!["Abort"]);
        assert_eq!(decisions_for(minimum_satisfied), vec!["Commit"]);
        assert_eq!(decisions_for(minimum_unmet), vec!["Abort"]);

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn invalid_policy_batch_is_atomic_and_wedges_playback() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 0,
                decider_policy: Some(DeciderPolicy::FirstBooleanWins as i32),
                voter_ops: [
                    (
                        "valid-add".to_string(),
                        VoterOp {
                            op: Some(voter_op::Op::Add(AddVoterOp {
                                config: Some(placeholder_voter_config()),
                            })),
                        },
                    ),
                    ("invalid-op".to_string(), VoterOp { op: None }),
                ]
                .into_iter()
                .collect(),
            },
        )
        .await?;

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            DefaultPolicyProvider,
            fixture.get_env(),
        );
        let malformed_entry = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
            })
            .await?
            .entries
            .into_iter()
            .next()
            .expect("the malformed batch was appended");
        let apply_error = match engine.apply(agent, &malformed_entry).await {
            Ok(_) => panic!("invalid PolicyBatch should fail direct playback"),
            Err(error) => error,
        };
        assert!(
            matches!(
                apply_error,
                ApplyError::MalformedPolicy {
                    kind: MalformedPolicyBatchKind::PolicyBatch,
                    ref message,
                } if message.contains("has no operation")
            ),
            "engine playback should return the typed malformed-entry error"
        );

        let error = match propose_intention(&engine, agent, "reports invalid batch").await {
            Ok(_) => panic!("invalid PolicyBatch should be reported"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("has no operation"),
            "unexpected error: {error:#}"
        );
        let retry_error = match propose_intention(&engine, agent, "after invalid batch").await {
            Ok(_) => panic!("malformed policy should wedge playback at the same entry"),
            Err(error) => error,
        };
        assert!(
            retry_error.to_string().contains("has no operation"),
            "retry should encounter the same malformed entry: {retry_error:#}"
        );
        assert!(
            poll_vote_voter_ids(&bus, agent).await?.is_empty(),
            "the rejected voter addition must not be applied"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn malformed_batch_add_voter_has_add_voter_kind() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                voter_ops: [(
                    "invalid-add".to_string(),
                    VoterOp {
                        op: Some(voter_op::Op::Add(AddVoterOp { config: None })),
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
        )
        .await?;

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            DefaultPolicyProvider,
            fixture.get_env(),
        );
        let malformed_entry = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
            })
            .await?
            .entries
            .into_iter()
            .next()
            .expect("the malformed batch was appended");
        let apply_error = engine
            .apply(agent, &malformed_entry)
            .await
            .expect_err("the malformed batch add must fail playback");
        assert!(
            matches!(
                apply_error,
                ApplyError::MalformedPolicy {
                    kind: MalformedPolicyBatchKind::AddVoter,
                    ref message,
                } if message.contains("missing voter config")
            ),
            "a malformed batch AddVoterOp should use the AddVoter kind"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn malformed_batch_remove_voter_has_remove_voter_kind() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                voter_ops: [(
                    "unknown-voter".to_string(),
                    VoterOp {
                        op: Some(voter_op::Op::Remove(RemoveVoterOp {})),
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
        )
        .await?;

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            DefaultPolicyProvider,
            fixture.get_env(),
        );
        let malformed_entry = bus
            .poll(PollRequest {
                agent_bus_id: agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
            })
            .await?
            .entries
            .into_iter()
            .next()
            .expect("the malformed batch was appended");
        let apply_error = engine
            .apply(agent, &malformed_entry)
            .await
            .expect_err("the malformed batch remove must fail playback");
        assert!(
            matches!(
                apply_error,
                ApplyError::MalformedPolicy {
                    kind: MalformedPolicyBatchKind::RemoveVoter,
                    ref message,
                } if message.contains("unknown voter 'unknown-voter'")
            ),
            "a malformed batch RemoveVoterOp should use the RemoveVoter kind"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn invalid_batch_decider_policy_is_rejected_without_mutating_state() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 0,
                decider_policy: Some(99),
                voter_ops: [(
                    "valid-add".to_string(),
                    VoterOp {
                        op: Some(voter_op::Op::Add(AddVoterOp {
                            config: Some(placeholder_voter_config()),
                        })),
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
        )
        .await?;

        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            StaticConfigPolicyProvider::new(DeciderPolicy::OnByDefault, vec![], |policy| {
                validate_voter_configs(
                    &TestVoterFactory::new(Rc::new(InMemoryStorage::new())),
                    policy,
                )
            }),
            fixture.get_env(),
        );
        let error = match propose_intention(&engine, agent, "reports invalid batch policy").await {
            Ok(_) => panic!("invalid batch decider policy should be reported"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("decider policy Some(99) is invalid"),
            "unexpected error: {error:#}"
        );

        let (state, _) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert!(
            state.decider.is_none(),
            "the invalid policy must not configure a decider"
        );
        assert!(
            state.voters.is_empty(),
            "the invalid batch must not install its voter"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_batch_rejects_an_unexpected_current_version() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 1,
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 2,
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        let outcome = propose_intention(&engine, agent, "after stale policy batch").await?;
        assert!(
            outcome.approved,
            "sync should continue using the applied policy"
        );
        let (state, _) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert_eq!(
            state.applied_policy_version,
            Some(1),
            "the stale batch must not advance the applied version"
        );
        assert_eq!(
            state.decider.map(|decider| decider.policy),
            Some(DeciderPolicy::OnByDefault as i32),
            "the stale batch must not replace the applied decider policy"
        );
        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_batch_applies_chained_version_transitions() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 1,
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: Some(1),
                new_version: 2,
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        propose_intention(&engine, agent, "process valid version transitions").await?;

        let (state, _) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert_eq!(
            state.applied_policy_version,
            Some(2),
            "the second compatible batch must advance the applied version"
        );
        assert_eq!(
            state.decider.map(|decider| decider.policy),
            Some(DeciderPolicy::OffByDefault as i32),
            "the second compatible batch must apply its decider policy"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_batch_rejects_version_regression() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 2,
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: Some(2),
                new_version: 1,
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        let outcome = propose_intention(&engine, agent, "after regressing policy batch").await?;
        assert!(
            outcome.approved,
            "sync should continue using the applied policy"
        );
        let (state, _) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert_eq!(
            state.applied_policy_version,
            Some(2),
            "the regressing batch must not change the applied version"
        );
        assert_eq!(
            state.decider.map(|decider| decider.policy),
            Some(DeciderPolicy::OnByDefault as i32),
            "the regressing batch must not replace the applied decider policy"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_batch_rejects_unchanged_version() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: None,
                new_version: 2,
                decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                expected_current_version: Some(2),
                new_version: 2,
                decider_policy: Some(DeciderPolicy::OffByDefault as i32),
                ..Default::default()
            },
        )
        .await?;
        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );

        let outcome = propose_intention(&engine, agent, "after unchanged policy version").await?;
        assert!(
            outcome.approved,
            "sync should continue using the applied policy"
        );
        let (state, _) = PerBusEngineState::load(storage.as_ref(), agent).await?;
        assert_eq!(
            state.applied_policy_version,
            Some(2),
            "the unchanged-version batch must not change the applied version"
        );
        assert_eq!(
            state.decider.map(|decider| decider.policy),
            Some(DeciderPolicy::OnByDefault as i32),
            "the unchanged-version batch must not replace the applied decider policy"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_provider_skips_all_policy_after_engine_state_has_voter_config() {
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            StaticConfigPolicyProvider::new(
                DeciderPolicy::FirstBooleanWins,
                vec![placeholder_voter_config()],
                |policy| {
                    validate_voter_configs(
                        &TestVoterFactory::new(Rc::new(InMemoryStorage::new())),
                        policy,
                    )
                },
            ),
            fixture.get_env(),
        );

        let outcome = propose_intention(&engine, agent, "first").await?;
        assert!(outcome.approved);
        let outcome = propose_intention(&engine, agent, "second").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "DeciderPolicy",
                "AddVoter",
                "Intention",
                "Vote",
                "Commit",
                "Intention",
                "Vote",
                "Commit",
            ],
            "non-empty engine state should suppress duplicate policy entries"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn policy_provider_reconciles_voters_after_existing_policy() {
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_voter_policy_with_config(
            &bus,
            agent,
            "test/counting_voter",
            CountingVoterConfig { modulus: 3 }.encode_to_vec(),
        )
        .await?;

        let storage = Rc::new(InMemoryStorage::new());
        let initial_engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            DefaultPolicyProvider,
            fixture.get_env(),
        );
        let outcome = propose_intention(&initial_engine, agent, "before reconfig").await?;
        assert!(outcome.approved);

        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage,
            ControllablePolicyProvider::new(
                PolicyState {
                    decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                    voters: [("provider-voter".to_string(), placeholder_voter_config())]
                        .into_iter()
                        .collect(),
                },
                1,
            ),
            fixture.get_env(),
        );

        let outcome = propose_intention(&engine, agent, "after reconfig").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert!(
            types.contains(&"RemoveVoter"),
            "provider policy should remove the existing voter"
        );
        assert_eq!(
            types
                .iter()
                .filter(|entry_type| **entry_type == "Vote")
                .count(),
            2,
            "only the configured voter should vote after reconfiguration"
        );
        assert!(
            poll_vote_voter_ids(&bus, agent)
                .await?
                .iter()
                .any(|voter_id| voter_id == "provider-voter"),
            "provider policy should add its configured voter"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn add_and_remove_voter_batches_update_active_voters() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );
        let agent = "agent-1";

        let voter_id = append_add_voter(
            &bus,
            agent,
            "test/counting_voter",
            CountingVoterConfig { modulus: 3 }.encode_to_vec(),
        )
        .await?;
        assert_eq!(
            voter_id, "0",
            "the voter should use its policy batch position as its stable ID"
        );

        let outcome = propose_intention(&engine, agent, "with added voter").await?;
        assert!(outcome.approved);

        append_remove_voter(&bus, agent, &voter_id).await?;
        let outcome = propose_intention(&engine, agent, "after removal").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "RemoveVoter",
                "Intention",
                "Commit",
            ],
            "RemoveVoter should stop the added voter from voting on later intentions"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn duplicate_add_voter_configs_create_independent_voters() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );
        let agent = "agent-1";
        let config = CountingVoterConfig { modulus: 3 }.encode_to_vec();

        let first_voter_id =
            append_add_voter(&bus, agent, "test/counting_voter", config.clone()).await?;
        let second_voter_id = append_add_voter(&bus, agent, "test/counting_voter", config).await?;
        assert_eq!(first_voter_id, "0");
        assert_eq!(second_voter_id, "1");

        let outcome = propose_intention(&engine, agent, "with two duplicate voters").await?;
        assert!(outcome.approved);

        append_remove_voter(&bus, agent, &first_voter_id).await?;
        let outcome = propose_intention(&engine, agent, "after removing first").await?;
        assert!(outcome.approved);

        append_remove_voter(&bus, agent, &second_voter_id).await?;
        let outcome = propose_intention(&engine, agent, "after removing second").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "AddVoter",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "Vote",
                "RemoveVoter",
                "Intention",
                "Commit",
                "Vote",
                "RemoveVoter",
                "Intention",
                "Commit",
            ],
            "duplicate AddVoter configs should create independent voters removable by ID"
        );

        let vote_voter_ids = poll_vote_voter_ids(&bus, agent).await?;
        assert_eq!(
            vote_voter_ids,
            vec![
                first_voter_id.clone(),
                second_voter_id.clone(),
                second_voter_id
            ],
            "both duplicate voters should vote initially, then only the second should vote after removing the first"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn standalone_policy_controls_are_rejected() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let register = PolicyRegister::new(Rc::new(InMemoryStorage::new()), "test-policy");
        register
            .set_policy(
                &PolicyState {
                    decider_policy: Some(DeciderPolicy::OnByDefault as i32),
                    ..Default::default()
                },
                None,
            )
            .await?;
        let provider = SynchronousRegisterProvider::new(register, |policy| {
            validate_voter_configs(
                &TestVoterFactory::new(Rc::new(InMemoryStorage::new())),
                policy,
            )
        });
        let storage = Rc::new(InMemoryStorage::new());
        let engine = make_engine_with_policy_provider(
            fixture.create_impl(),
            storage.clone(),
            provider,
            fixture.get_env(),
        );
        let add_agent = "add-agent";

        bus.append(AppendRequest {
            agent_bus_id: add_agent.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: add_agent.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Control(Control {
                    control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::AddVoter(AddVoter {
                            config: None,
                        })),
                    })),
                })),
            }),
        })
        .await?;

        let malformed_entry = bus
            .poll(PollRequest {
                agent_bus_id: add_agent.to_string(),
                bus_id: Some(BusId {
                    agent_bus_id: add_agent.to_string(),
                }),
                start_log_position: 0,
                max_entries: 1,
                filter: None,
            })
            .await?
            .entries
            .into_iter()
            .next()
            .expect("the malformed standalone add was appended");
        let apply_error = engine
            .apply(add_agent, &malformed_entry)
            .await
            .expect_err("the malformed standalone add must fail playback");
        assert!(
            matches!(
                apply_error,
                ApplyError::DeprecatedOperation {
                    operation: DeprecatedOperationKind::AddVoter,
                }
            ),
            "a standalone AddVoter should use the deprecated-operation kind"
        );

        let error = match propose_intention(&engine, add_agent, "reports standalone add").await {
            Ok(_) => panic!("standalone AddVoter should be reported"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("standalone AddVoter is disabled; policy must use PolicyBatch"),
            "unexpected error: {error:#}"
        );

        let retry_error =
            match propose_intention(&engine, add_agent, "retries standalone add").await {
                Ok(_) => panic!("standalone AddVoter should wedge playback"),
                Err(error) => error,
            };
        assert!(
            retry_error
                .to_string()
                .contains("standalone AddVoter is disabled; policy must use PolicyBatch"),
            "retry should encounter the same standalone AddVoter: {retry_error:#}"
        );

        let remove_agent = "remove-agent";
        bus.append(AppendRequest {
            agent_bus_id: remove_agent.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: remove_agent.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::Control(Control {
                    control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::RemoveVoter(RemoveVoter {
                            voter_id: "unused".to_string(),
                        })),
                    })),
                })),
            }),
        })
        .await?;
        let error =
            match propose_intention(&engine, remove_agent, "reports standalone remove").await {
                Ok(_) => panic!("standalone RemoveVoter should be reported once"),
                Err(error) => error,
            };
        assert!(
            error
                .to_string()
                .contains("standalone RemoveVoter is disabled; policy must use PolicyBatch"),
            "unexpected error: {error:#}"
        );
        let retry_error =
            match propose_intention(&engine, remove_agent, "retries standalone remove").await {
                Ok(_) => panic!("standalone RemoveVoter should wedge playback"),
                Err(error) => error,
            };
        assert!(
            retry_error
                .to_string()
                .contains("standalone RemoveVoter is disabled; policy must use PolicyBatch"),
            "retry should encounter the same standalone RemoveVoter: {retry_error:#}"
        );

        let decider_agent = "decider-agent";
        bus.append(AppendRequest {
            agent_bus_id: decider_agent.to_string(),
            bus_id: Some(BusId {
                agent_bus_id: decider_agent.to_string(),
            }),
            payload: Some(Payload {
                payload: Some(payload::Payload::DeciderPolicy(
                    DeciderPolicy::OffByDefault as i32,
                )),
            }),
        })
        .await?;
        let error =
            match propose_intention(&engine, decider_agent, "reports standalone decider").await {
                Ok(_) => panic!("standalone DeciderPolicy should be reported once"),
                Err(error) => error,
            };
        assert!(
            error
                .to_string()
                .contains("standalone SetDeciderPolicy is disabled; policy must use PolicyBatch"),
            "unexpected error: {error:#}"
        );
        let retry_error =
            match propose_intention(&engine, decider_agent, "retries standalone decider").await {
                Ok(_) => panic!("standalone DeciderPolicy should wedge playback"),
                Err(error) => error,
            };
        assert!(
            retry_error
                .to_string()
                .contains("standalone SetDeciderPolicy is disabled; policy must use PolicyBatch"),
            "retry should encounter the same standalone DeciderPolicy: {retry_error:#}"
        );
        let (state, _) = PerBusEngineState::load(storage.as_ref(), decider_agent).await?;
        assert!(
            state.decider.is_none(),
            "the standalone policy must not configure a decider"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn test_voter_lifecycle() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );
        let agent = "agent-1";

        // Phase 1: Empty log — decider auto-commits, no voters
        let outcome = propose_intention(&engine, agent, "before any policy").await?;
        assert!(outcome.approved, "ON_BY_DEFAULT should approve");

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec!["Intention", "Commit"],
            "the default decider approves without adding voters"
        );

        // Phase 2: First voter policy batch — one voter
        append_voter_policy(&bus, agent, "test/counting_voter").await?;

        let outcome = propose_intention(&engine, agent, "after first voter policy").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "Intention",
                "Commit",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote"
            ],
            "one voter produces one vote"
        );

        // Phase 3: Add a different voter kind in another policy batch — two voters.
        append_voter_policy(&bus, agent, "test/random_voter").await?;

        let outcome = propose_intention(&engine, agent, "after second voter policy").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "Intention",
                "Commit",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "Vote",
            ],
            "two voters of different kinds produce two votes"
        );

        // Phase 4: Add a stateless voter through a policy batch — three voters
        append_voter_policy(&bus, agent, "agentbus/placeholder_voter").await?;

        let outcome = propose_intention(&engine, agent, "after stateless voter").await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "Intention",
                "Commit",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "Vote",
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "Vote",
                "Vote",
            ],
            "stateless voter (via adapter) produces a vote alongside stateful voters"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn voter_config_is_applied_and_survives_reboot() {
    // A non-trivial voter config (the counting voter's modulus) must reach the
    // voter and persist across a reboot — proving the engine stores the full
    // config, not just the type URL.
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let storage = Rc::new(InMemoryStorage::new());
        let agent = "agent-1";

        // Counting voter with modulus 4 (default is 3): rejects every 4th intention.
        let config = CountingVoterConfig { modulus: 4 }.encode_to_vec();
        append_voter_policy_with_config(&bus, agent, "test/counting_voter", config).await?;

        let engine1 = make_engine(
            fixture.create_impl(),
            storage.clone(),
            env_for_engine.clone(),
        );
        for i in 1..=3 {
            propose_intention(&engine1, agent, &format!("intent-{i}")).await?;
        }

        // Reboot: fresh engine, same Storage and bus.
        let engine2 = make_engine(fixture.create_impl(), storage.clone(), env_for_engine);
        propose_intention(&engine2, agent, "intent-4").await?;

        // counts 1,2,3 -> % 4 != 0 -> approve; count 4 -> % 4 == 0 -> reject. With
        // the default modulus 3, count 4 -> % 3 == 1 -> approve, so a reject here
        // proves the modulus-4 config was preserved across the reboot.
        let votes = poll_vote_bools(&bus, agent).await?;
        assert_eq!(votes, vec![true, true, true, false]);

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn random_voter_uses_configured_seed() {
    // The configured seed reaches the voter and determines its first vote.
    let sim_seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(sim_seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );
        let agent = "agent-1";

        let voter_seed: u64 = 12345;
        let config = RandomVoterConfig { seed: voter_seed }.encode_to_vec();
        append_voter_policy_with_config(&bus, agent, "test/random_voter", config).await?;

        propose_intention(&engine, agent, "intent").await?;

        let votes = poll_vote_bools(&bus, agent).await?;
        assert_eq!(
            votes,
            vec![next_prng(voter_seed) & 1 == 0],
            "the first vote is determined by the configured seed"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn test_engine_survives_reboot() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let storage = Rc::new(InMemoryStorage::new());
        let agent = "agent-1";

        let engine1 = make_engine(
            fixture.create_impl(),
            storage.clone(),
            env_for_engine.clone(),
        );
        append_voter_policy(&bus, agent, "test/counting_voter").await?;

        let outcome = engine1
            .propose_intention(agent, string_intention("before reboot"))
            .await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(types, vec!["AddVoter", "Intention", "Commit", "Vote"]);

        // Reboot: new engine, same Storage, same bus
        let engine2 = make_engine(fixture.create_impl(), storage.clone(), env_for_engine);

        let outcome = engine2
            .propose_intention(agent, string_intention("after reboot"))
            .await?;
        assert!(outcome.approved);

        let types = poll_all_types(&bus, agent).await?;
        assert_eq!(
            types,
            vec![
                "AddVoter",
                "Intention",
                "Commit",
                "Vote",
                "Intention",
                "Commit",
                "Vote",
            ],
            "rebooted engine recovers voters from Storage — no duplicates, voter still active"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn test_1000_buses() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let engine = make_engine(
            fixture.create_impl(),
            Rc::new(InMemoryStorage::new()),
            env_for_engine,
        );

        for i in 0..1000 {
            let agent = format!("agent-{i}");

            if i % 3 == 0 {
                append_voter_policy(&bus, &agent, "test/counting_voter").await?;
            }

            let outcome = engine
                .propose_intention(&agent, string_intention(&format!("intent-{i}")))
                .await?;
            assert!(outcome.approved, "agent-{i} should be approved");
        }

        let types_with_voter = poll_all_types(&bus, "agent-0").await?;
        assert_eq!(
            types_with_voter,
            vec!["AddVoter", "Intention", "Commit", "Vote"],
            "agent-0 (with voter) should have vote"
        );

        let types_without_voter = poll_all_types(&bus, "agent-1").await?;
        assert_eq!(
            types_without_voter,
            vec!["Intention", "Commit"],
            "agent-1 (no voter) should have no vote"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn counting_voter_honors_duplication_contract() {
    // The three apply behaviors for a stateful applicator: normal forward
    // progress, replay of the last position, and rejection of a stale one.
    let voter = CountingVoter::new(Rc::new(InMemoryStorage::new()), "1".to_owned(), 3);
    let bus = "bus-1";

    // Forward: counts 1 and 2 (both n % 3 != 0 -> approved).
    let v_at_10 = vote_bool(
        futures::executor::block_on(voter.apply(bus, &intention_entry(10, "a"))).unwrap(),
    );
    let v_at_11 = vote_bool(
        futures::executor::block_on(voter.apply(bus, &intention_entry(11, "b"))).unwrap(),
    );
    assert_eq!(v_at_10, Some(true));
    assert_eq!(v_at_11, Some(true));

    // Replay of the last position returns the same vote without re-counting.
    let replay = vote_bool(
        futures::executor::block_on(voter.apply(bus, &intention_entry(11, "b"))).unwrap(),
    );
    assert_eq!(replay, v_at_11, "replay must return the last vote");

    // The next forward apply is count 3 (-> not approved), proving the replay
    // did not advance the counter.
    let v_at_12 = vote_bool(
        futures::executor::block_on(voter.apply(bus, &intention_entry(12, "c"))).unwrap(),
    );
    assert_eq!(
        v_at_12,
        Some(false),
        "counter advanced exactly once per forward apply"
    );

    // Stale re-application of an older position is rejected.
    let err =
        futures::executor::block_on(voter.apply(bus, &intention_entry(5, "old"))).unwrap_err();
    assert!(
        matches!(
            err,
            ApplyError::StalePosition {
                requested: 5,
                last: 12
            }
        ),
        "stale apply must be rejected, got {err:?}"
    );
}

#[test]
fn engine_and_applicator_state_use_separate_storage() {
    let seed = generate_seed();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let engine_storage = Rc::new(InMemoryStorage::new());
        let applicator_storage = Rc::new(InMemoryStorage::new());
        let provider = ControllablePolicyProvider::new(
            PolicyState {
                decider_policy: Some(DeciderPolicy::FirstBooleanWins as i32),
                voters: [("voter".to_string(), placeholder_voter_config())]
                    .into_iter()
                    .collect(),
            },
            0,
        );
        let engine = BaseEngine::new(
            fixture.create_impl(),
            engine_storage.clone(),
            TestVoterFactory::new(applicator_storage.clone()),
            provider,
            DeciderFactoryImpl::new(applicator_storage.clone()),
            env_for_engine,
        );
        let agent = "agent-1";

        assert!(
            propose_intention(&engine, agent, "split storage")
                .await?
                .approved
        );

        let engine_key = "engine:state:agent-1";
        let decider_key = "decider:state:agent-1:0";
        let voter_key = "voter:voter:agent-1";
        assert!(engine_storage.get(engine_key).await?.is_some());
        assert!(engine_storage.get(decider_key).await?.is_none());
        assert!(engine_storage.get(voter_key).await?.is_none());
        assert!(applicator_storage.get(engine_key).await?.is_none());
        assert!(applicator_storage.get(decider_key).await?.is_some());
        assert!(applicator_storage.get(voter_key).await?.is_some());

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");
}

#[test]
fn observed_applicators_emit_metrics_and_rows() {
    // Wires the engine with an observed decider and observed voter factory.
    // Driving one intention through should record apply metrics and log a row
    // per `apply`.
    let seed: u64 = rand::random();
    let simulator = agentbus_simulator::Simulator::new(seed);
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let metrics = Rc::new(InMemoryMetrics::new());
    let logger = InMemoryLogger::new();

    let metrics_for_engine: Rc<dyn agentbus_api::AgentBusMetrics> = metrics.clone();
    let logger_for_engine = Rc::new(logger.clone());
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let storage = Rc::new(InMemoryStorage::new());
        let observability = Observability {
            metrics: metrics_for_engine,
            logger: logger_for_engine,
            environment: env_for_engine.clone(),
        };
        let engine = BaseEngine::new(
            fixture.create_impl(),
            storage.clone(),
            ObservedTestVoterFactory {
                inner: TestVoterFactory::new(storage.clone()),
                observability: observability.clone(),
            },
            DefaultPolicyProvider,
            ObservedDeciderFactory::new(storage, observability.clone()),
            env_for_engine,
        )
        .with_playback_wrapper(|playback| {
            Rc::new(ObservableApplicator::new(
                playback,
                "base-engine",
                observability,
            ))
        });
        let agent = "agent-1";

        // Install the built-in placeholder voter, then propose an intention so
        // both the voter and the decider apply.
        append_voter_policy(&bus, agent, "agentbus/placeholder_voter").await?;
        let outcome = propose_intention(&engine, agent, "observed").await?;
        assert!(outcome.approved, "ON_BY_DEFAULT decider approves");

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");

    assert!(
        metrics.counter("apply.applicator.num_calls") >= 1,
        "wrapped applicators should have recorded apply throughput"
    );
    assert_eq!(
        metrics.counter("apply.applicator.num_errors"),
        0,
        "a healthy run records no apply errors"
    );

    let rows = logger.rows();
    assert!(
        !rows.is_empty(),
        "wrapped applicators should have logged rows"
    );
    assert!(
        rows.iter().all(|r| r.api_call_name == "apply"),
        "every logged row is an apply row"
    );
    assert!(
        rows.iter()
            .any(|r| r.additional_fields.iter().any(|(k, v)| {
                k == "payload_type"
                    && matches!(
                        v,
                        LogFieldValue::String(payload_type) if payload_type == "intention"
                    )
            })),
        "the proposed intention was applied and logged"
    );
    assert!(
        rows.iter()
            .any(|r| r.additional_fields.iter().any(|(k, v)| {
                k == "component"
                    && matches!(v, LogFieldValue::String(component) if component == "base-engine")
            })),
        "base-engine entry playback was observed"
    );
}

#[test]
fn observed_engine_entry_applicator_records_playback_errors() {
    let simulator = agentbus_simulator::Simulator::new(generate_seed());
    let fixture = SimpleMemoryFixture::new(simulator);
    let env = fixture.get_env();

    let metrics = Rc::new(InMemoryMetrics::new());
    let logger = InMemoryLogger::new();
    let metrics_for_engine: Rc<dyn agentbus_api::AgentBusMetrics> = metrics.clone();
    let logger_for_engine = Rc::new(logger.clone());
    let env_for_engine = env.clone();

    let handle = env.spawn(async move {
        let bus = fixture.create_impl();
        let agent = "agent-1";
        append_policy_batch(
            &bus,
            agent,
            PolicyBatch {
                voter_ops: [(
                    "unknown-voter".to_string(),
                    VoterOp {
                        op: Some(voter_op::Op::Remove(RemoveVoterOp {})),
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
        )
        .await?;

        let observability = Observability {
            metrics: metrics_for_engine,
            logger: logger_for_engine,
            environment: env_for_engine.clone(),
        };
        let storage = Rc::new(InMemoryStorage::new());
        let engine = BaseEngine::new(
            fixture.create_impl(),
            storage.clone(),
            TestVoterFactory::new(storage.clone()),
            DefaultPolicyProvider,
            DeciderFactoryImpl::new(storage),
            env_for_engine,
        )
        .with_playback_wrapper(|playback| {
            Rc::new(ObservableApplicator::new(
                playback,
                "base-engine",
                observability,
            ))
        });

        let error = match propose_intention(&engine, agent, "blocked by malformed policy").await {
            Ok(_) => panic!("malformed policy playback should fail the proposal"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("unknown voter 'unknown-voter'"),
            "playback error should retain its diagnostic: {error:#}"
        );

        anyhow::Ok(())
    });

    env.run();
    futures::executor::block_on(handle)
        .expect("task should complete")
        .expect("test should succeed");

    assert_eq!(metrics.counter("apply.applicator.num_calls"), 1);
    assert_eq!(metrics.counter("apply.applicator.num_errors"), 1);

    let rows = logger.rows();
    let row = rows
        .iter()
        .find(|row| {
            row.additional_fields.iter().any(|(key, value)| {
                key == "component"
                    && matches!(value, LogFieldValue::String(component) if component == "base-engine")
            })
        })
        .expect("base-engine playback should emit a structured row");
    assert_eq!(row.error_code.as_deref(), Some("MalformedPolicy"));
    assert!(row.additional_fields.iter().any(|(key, value)| {
        key == "malformed_policy_kind"
            && matches!(value, LogFieldValue::String(kind) if kind == "RemoveVoter")
    }));
}
