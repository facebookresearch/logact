/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::Abort;
use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::CheckTailRequest;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::SelectivePollType;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::helpers::get_payload_type;
use agentbus_api::helpers::read_range;
use agentbus_api::retry;
use anyhow::Context;
use anyhow::Result;
use logact_commit_service_api::Policy;
use logact_commit_service_api::PolicyProvider;

use crate::Applicator;
use crate::ApplyError;
use crate::ConcurrencyError;
use crate::EngineStateLoadError;
use crate::FirstBooleanWinsApplicator;
use crate::OffByDefaultApplicator;
use crate::OnByDefaultApplicator;
use crate::PerBusEngineState;
use crate::StateMachineSpec;
use crate::Storage;
use crate::StorageError;
use crate::applicator::StorageWriteResultExt;
use crate::policy::apply_policy_batch;
use crate::policy::apply_policy_entry;
use crate::policy::installed_policy;
use crate::policy::plan;
use crate::policy::policy_batch_payload;
use crate::policy::policy_constraint_failure;
use crate::storage::InMemoryStorage;
use crate::storage_concurrency_retry::ConcurrencyRetryPolicy;

pub trait VoterFactory {
    /// Validate a voter config without constructing a voter or touching storage.
    fn validate_config(&self, config: Option<&VoterConfig>) -> Result<VoterConfig>;

    /// Build a voter from its stable ID and full typed config.
    fn create_voter(
        &self,
        spec: &StateMachineSpec<String, VoterConfig>,
    ) -> Result<Rc<dyn Applicator>>;
}

/// Validate every voter config in `policy` against the consumer's voter factory.
pub fn validate_voter_configs(voter_factory: &impl VoterFactory, policy: &Policy) -> Result<()> {
    for (voter_id, config) in &policy.voters {
        voter_factory
            .validate_config(Some(config))
            .with_context(|| format!("voter '{voter_id}' has invalid config"))?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct ApplicatorBinding {
    pub types: Vec<i32>,
    pub applicator: Rc<dyn Applicator>,
}

impl ApplicatorBinding {
    fn matches(&self, payload: &Payload) -> bool {
        match get_payload_type(payload) {
            Some(t) => self.types.contains(&t),
            None => false,
        }
    }
}

/// Builds a decider from its policy and state-machine ID.
pub trait DeciderFactory {
    /// `spec.id` is `None` only for an unconfigured bus.
    fn create_decider(
        &self,
        spec: StateMachineSpec<Option<i64>, DeciderPolicy>,
    ) -> Rc<dyn Applicator>;
}

/// Builds concrete decider applicators.
pub struct DeciderFactoryImpl<S = InMemoryStorage> {
    storage: Rc<S>,
}

impl<S> Clone for DeciderFactoryImpl<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<S> DeciderFactoryImpl<S> {
    pub fn new(storage: Rc<S>) -> Self {
        Self { storage }
    }
}

impl Default for DeciderFactoryImpl {
    fn default() -> Self {
        Self::new(Rc::new(InMemoryStorage::new()))
    }
}

impl<S: Storage + 'static> DeciderFactory for DeciderFactoryImpl<S> {
    fn create_decider(
        &self,
        spec: StateMachineSpec<Option<i64>, DeciderPolicy>,
    ) -> Rc<dyn Applicator> {
        match spec.config {
            DeciderPolicy::OnByDefault => Rc::new(OnByDefaultApplicator::new()),
            DeciderPolicy::OffByDefault => Rc::new(OffByDefaultApplicator::new()),
            DeciderPolicy::FirstBooleanWins => Rc::new(FirstBooleanWinsApplicator::new(
                self.storage.clone(),
                spec.id,
            )),
        }
    }
}

pub struct ProposalOutcome {
    pub approved: bool,
    pub reason: String,
    /// Bus log position where the intention was appended — a logical timestamp
    /// giving its serialization order on the agent's log. This is the intention's
    /// own slot, not the later `Commit`/`Abort`, so an intention orders ahead of
    /// any policy change that lands while it is being decided.
    pub log_position: i64,
}

/// Error returned by [`BaseEngine`] operations.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The bus history is inconsistent with durable state and cannot be replayed safely.
    #[error(transparent)]
    BusDataLoss(anyhow::Error),

    /// A direct engine storage operation failed.
    #[error(transparent)]
    Storage(StorageError),

    /// Persisted engine state could not be interpreted.
    #[error(transparent)]
    InvalidEngineState(anyhow::Error),

    /// Applying a bus entry through playback failed.
    #[error(transparent)]
    Playback(ApplyError),

    /// A direct AgentBus operation failed.
    #[error(transparent)]
    Bus(AgentBusError),

    /// Reading the desired policy failed.
    #[error(transparent)]
    PolicyProvider(anyhow::Error),

    /// The operation failed for another internal reason.
    #[error(transparent)]
    Internal(anyhow::Error),
}

/// Result returned by [`BaseEngine`] operations.
pub type EngineResult<T> = std::result::Result<T, EngineError>;

impl From<AgentBusError> for EngineError {
    fn from(error: AgentBusError) -> Self {
        match error {
            error @ AgentBusError::PositionNotFound(_) => Self::BusDataLoss(error.into()),
            error => Self::Bus(error),
        }
    }
}

impl From<EngineStateLoadError> for EngineError {
    fn from(error: EngineStateLoadError) -> Self {
        match error {
            EngineStateLoadError::Storage(error) => Self::Storage(error),
            error @ EngineStateLoadError::Decode(_) => {
                Self::InvalidEngineState(anyhow::Error::new(error))
            }
        }
    }
}

/// Extract the outcome for `proposed_position` from a payload, if it is the
/// matching `Commit`/`Abort`. The reported position is the intention's own append
/// slot (`proposed_position`), not the deciding entry's.
pub fn outcome_for(payload: &Payload, proposed_position: i64) -> Option<ProposalOutcome> {
    match &payload.payload {
        Some(payload::Payload::Commit(c)) if c.intention_id == proposed_position => {
            Some(ProposalOutcome {
                approved: true,
                reason: c.reason.clone(),
                log_position: proposed_position,
            })
        }
        Some(payload::Payload::Abort(a)) if a.intention_id == proposed_position => {
            Some(ProposalOutcome {
                approved: false,
                reason: a.reason.clone(),
                log_position: proposed_position,
            })
        }
        _ => None,
    }
}

/// The decision the engine forces when the log drains without one for the
/// intention, so `propose` always returns an outcome.
///
/// Safe under first-decision-wins: readers return the *first* `Commit`/`Abort`
/// for the intention, and this abort is appended only after the whole log was
/// read with no decision found — so it lands ahead of anything a later vote could
/// produce and every reader agrees on it; a later decision is a redundant no-op.
/// If a real decision instead raced in just ahead of the abort, it takes the
/// earlier position and wins, so the abort never overrides an earlier decision.
/// (No assumption about inline vs. async voters is needed.)
pub fn forced_abort(intention_id: i64) -> Payload {
    abort_payload(intention_id, "no decision before the log was drained")
}

/// An `Abort` for `intention_id` with `reason`.
pub fn abort_payload(intention_id: i64, reason: &str) -> Payload {
    Payload {
        payload: Some(payload::Payload::Abort(Abort {
            intention_id,
            reason: reason.to_string(),
        })),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BaseEngineConfig {
    pub retry_config: RetryConfig,
}

const DEFAULT_STORAGE_CONCURRENCY_MAX_RETRIES: usize = 7;

impl Default for BaseEngineConfig {
    fn default() -> Self {
        Self {
            retry_config: RetryConfig::try_new(
                DEFAULT_STORAGE_CONCURRENCY_MAX_RETRIES,
                Duration::from_millis(10),
                Duration::from_millis(100),
            )
            .expect("default retry configuration should be valid"),
        }
    }
}

#[derive(Clone)]
struct EngineSnapshot {
    state: PerBusEngineState,
    position: Option<i64>,
}

impl EngineSnapshot {
    async fn load(
        storage: &impl Storage,
        bus_id: &str,
    ) -> std::result::Result<Self, EngineStateLoadError> {
        let (state, position) = PerBusEngineState::load(storage, bus_id).await?;
        Ok(Self { state, position })
    }

    /// Compare only with the snapshot loaded before this fresh append. Reloading
    /// here could mistake a concurrent drive's progress for a reused position.
    fn check_append_position(&self, bus_id: &str, appended_position: i64) -> EngineResult<()> {
        if let Some(saved_position) = self.position
            && appended_position <= saved_position
        {
            return Err(EngineError::BusDataLoss(anyhow::anyhow!(
                "bus '{bus_id}' reused a checkpointed log position"
            )));
        }
        Ok(())
    }
}

/// Mutable playback state retained for one engine proposal.
pub struct EngineContext {
    snapshot: EngineSnapshot,
    highest_append: Cell<i64>,
}

struct EngineRuntime<T, F, S, D, E> {
    bus: T,
    voter_factory: F,
    engine_storage: Rc<S>,
    decider_factory: D,
    environment: Rc<E>,
    config: BaseEngineConfig,
}

pub struct BaseEngine<T, F, S, P, D, E> {
    runtime: Rc<EngineRuntime<T, F, S, D, E>>,
    playback: Rc<dyn Applicator<EngineContext>>,
    policy_provider: P,
}

impl<T, F, S, P, D, E> Clone for BaseEngine<T, F, S, P, D, E>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            playback: self.playback.clone(),
            policy_provider: self.policy_provider.clone(),
        }
    }
}

const DECIDER_TYPES: &[i32] = &[
    SelectivePollType::Intention as i32,
    SelectivePollType::Vote as i32,
];
const VOTER_TYPES: &[i32] = &[SelectivePollType::Intention as i32];
const ENGINE_TYPES: &[i32] = &[
    SelectivePollType::Control as i32,
    SelectivePollType::DeciderPolicy as i32,
];

fn bind_voter_applicator(voter: Rc<dyn Applicator>) -> ApplicatorBinding {
    ApplicatorBinding {
        types: VOTER_TYPES.to_vec(),
        applicator: voter,
    }
}

const POLL_BATCH_SIZE: i32 = 64;

/// Whether an entry can change any state owned by the base engine or one of its
/// applicators. Other payloads are still inspected by `sync_and_apply` for the
/// proposal's outcome, but do not need an engine-state read and CAS merely to
/// advance past them.
fn requires_application(entry: &BusEntry) -> bool {
    let Some(payload_type) = entry.payload.as_ref().and_then(get_payload_type) else {
        return false;
    };
    DECIDER_TYPES.contains(&payload_type)
        || VOTER_TYPES.contains(&payload_type)
        || ENGINE_TYPES.contains(&payload_type)
}

/// Apply an entry to one voter or decider, retrying storage races and tolerating
/// a stale re-delivery. Re-entering `apply` reloads the applicator's state, so it
/// either replays the persisted result, reports that the entry is stale, or
/// attempts the transition again.
///
/// The error carries the applicator's last-applied position, so in principle the
/// engine could fast-forward past entries that applicator has already consumed.
/// We don't: the engine cursor is shared across all applicators, each at its own
/// position, so a single cursor bump can't honor them individually. Left as a
/// future optimization.
async fn apply_tolerating_stale<E: Environment>(
    applicator: &dyn Applicator,
    bus_id: &str,
    entry: &BusEntry,
    environment: &E,
    retry_config: RetryConfig,
) -> std::result::Result<Option<Payload>, ApplyError> {
    // human: retry on concurrency errors to ensure forward progress. We cannot
    // wait on a competing drive and cannot assume it will finish.
    let mut retry_policy = ConcurrencyRetryPolicy::default();
    retry(
        environment,
        retry_config,
        || async {
            match applicator.apply(bus_id, entry, &mut ()).await {
                Err(ApplyError::StalePosition { .. }) => Ok(None),
                result => result,
            }
        },
        |error| match error {
            ApplyError::Concurrency(
                error @ (ConcurrencyError::Voter { .. } | ConcurrencyError::Decider { .. }),
            ) => retry_policy.decide(error),
            _ => RetryDecision::Stop,
        },
    )
    .await
    .map_err(RetryFailure::into_last_error)
}

impl<T, F, S, D, E> EngineRuntime<T, F, S, D, E>
where
    T: AgentBus,
    F: VoterFactory,
    S: Storage + 'static,
    D: DeciderFactory,
{
    /// Rebuild the decider from engine state, using the default policy if
    /// unconfigured.
    fn build_decider(
        &self,
        state: &PerBusEngineState,
    ) -> std::result::Result<ApplicatorBinding, ApplyError> {
        Ok(ApplicatorBinding {
            types: DECIDER_TYPES.to_vec(),
            applicator: self
                .decider_factory
                .create_decider(load_decider_spec_from_state(state)?),
        })
    }

    /// Build the installed voters for a bus from its persisted voter configs.
    /// Voters are cheap, storage-backed wrappers, so we rebuild them on demand
    /// rather than caching them.
    fn build_voters(&self, state: &PerBusEngineState) -> Result<Vec<ApplicatorBinding>> {
        state_voter_configs(state)
            .into_iter()
            .map(|(voter_id, config)| {
                let spec = StateMachineSpec::new(voter_id, config);
                Ok(bind_voter_applicator(
                    self.voter_factory.create_voter(&spec)?,
                ))
            })
            .collect()
    }
}

impl<T, F, S, P, D, E> BaseEngine<T, F, S, P, D, E>
where
    T: AgentBus + 'static,
    F: VoterFactory + 'static,
    S: Storage + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    D: DeciderFactory + 'static,
    E: Environment + 'static,
{
    pub fn new(
        bus: T,
        storage: Rc<S>,
        voter_factory: F,
        policy_provider: P,
        decider_factory: D,
        environment: Rc<E>,
    ) -> Self {
        Self::new_with_config(
            bus,
            storage,
            voter_factory,
            policy_provider,
            decider_factory,
            environment,
            BaseEngineConfig::default(),
        )
    }

    pub fn new_with_config(
        bus: T,
        storage: Rc<S>,
        voter_factory: F,
        policy_provider: P,
        decider_factory: D,
        environment: Rc<E>,
        config: BaseEngineConfig,
    ) -> Self {
        let runtime = Rc::new(EngineRuntime {
            bus,
            voter_factory,
            engine_storage: storage,
            decider_factory,
            environment,
            config,
        });
        Self {
            playback: runtime.clone(),
            runtime,
            policy_provider,
        }
    }

    /// Decorate the applicator used for entry playback.
    pub fn with_playback_wrapper<W>(mut self, wrap: W) -> Self
    where
        W: FnOnce(Rc<dyn Applicator<EngineContext>>) -> Rc<dyn Applicator<EngineContext>>,
    {
        self.playback = wrap(self.playback);
        self
    }
}

impl<T, F, S, P, D, E> BaseEngine<T, F, S, P, D, E>
where
    T: AgentBus,
    F: VoterFactory,
    S: Storage + 'static,
    P: PolicyProvider,
    P::Error: Into<anyhow::Error>,
    D: DeciderFactory,
    E: Environment,
{
    /// Append the initial policy and optimistically checkpoint a fresh bus.
    pub async fn create_bus(&self, bus_id: &str) -> EngineResult<()> {
        let desired = self
            .policy_provider
            .read(bus_id)
            .await
            .map_err(|error| EngineError::PolicyProvider(error.into()))?;
        let batch = plan(None, None, &desired)
            .map_err(EngineError::Internal)?
            .ok_or_else(|| {
                EngineError::Internal(anyhow::anyhow!(
                    "CreateBus initial policy planning produced no batch"
                ))
            })?;
        let position = self
            .runtime
            .bus
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_owned(),
                }),
                payload: Some(policy_batch_payload(batch.clone())),
            })
            .await
            .map_err(EngineError::from)?
            .log_position;
        if position == 0 {
            let mut state = PerBusEngineState::default();
            apply_policy_batch(&mut state, &batch, position, &|config| {
                self.runtime.voter_factory.validate_config(config)
            })
            .map_err(EngineError::Playback)?;
            match state
                .save(&self.runtime.engine_storage, bus_id, None, 0)
                .await
            {
                Ok(true) => {}
                Ok(false) => tracing::info!(
                    bus_id,
                    position,
                    "CreateBus initial checkpoint was rejected by a conditional write; deferring replay to commit"
                ),
                Err(error) => tracing::info!(
                    bus_id,
                    position,
                    error = %error,
                    "CreateBus initial checkpoint failed; deferring replay to commit"
                ),
            }
        } else {
            tracing::info!(
                bus_id,
                position,
                "CreateBus policy append returned a nonzero position; deferring replay to commit"
            );
        }
        Ok(())
    }

    /// Propose an intention: append it, drive the log until it has a decision, and
    /// return the outcome. The engine forces an abort if nothing decides it, so
    /// this always yields an outcome.
    pub async fn propose_intention(
        &self,
        bus_id: &str,
        mut intention: Payload,
    ) -> EngineResult<ProposalOutcome> {
        let Some(payload::Payload::Intention(proposed_intention)) = intention.payload.as_ref()
        else {
            return Err(EngineError::Internal(anyhow::anyhow!(
                "proposed payload is not an intention"
            )));
        };
        if proposed_intention.policy_version_constraint.is_some() {
            return Err(EngineError::Internal(anyhow::anyhow!(
                "proposed intention already has a policy version constraint; constraints are engine-managed"
            )));
        }

        let engine_snapshot = EngineSnapshot::load(&self.runtime.engine_storage, bus_id)
            .await
            .map_err(EngineError::from)?;
        let (policy_version, policy_position) = self
            .append_resolved_policy(bus_id, &engine_snapshot.state)
            .await?;
        let Some(payload::Payload::Intention(proposed_intention)) = intention.payload.as_mut()
        else {
            unreachable!("the proposed payload was validated as an intention");
        };
        proposed_intention.policy_version_constraint = Some(
            intention::PolicyVersionConstraint::MinimumPolicyVersion(policy_version),
        );
        let position = self
            .runtime
            .bus
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_owned(),
                }),
                payload: Some(intention),
            })
            .await
            .map_err(EngineError::from)?
            .log_position;

        let first_append_position = policy_position.unwrap_or(position).min(position);
        engine_snapshot.check_append_position(bus_id, first_append_position)?;
        let mut context = EngineContext {
            snapshot: engine_snapshot,
            highest_append: Cell::new(position),
        };
        self.sync_and_apply(bus_id, position, &mut context).await
    }

    /// Return the desired policy version and the log position of any policy append.
    async fn append_resolved_policy(
        &self,
        bus_id: &str,
        state: &PerBusEngineState,
    ) -> EngineResult<(i64, Option<i64>)> {
        let desired = self
            .policy_provider
            .read(bus_id)
            .await
            .map_err(|error| EngineError::PolicyProvider(error.into()))?;
        let version = desired.version;
        let current = installed_policy(state).map_err(EngineError::InvalidEngineState)?;
        let Some(batch) = plan(Some(&current), state.applied_policy_version, &desired)
            .map_err(EngineError::Internal)?
        else {
            return Ok((version, None));
        };
        let position = self
            .runtime
            .bus
            .append(AppendRequest {
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_owned(),
                }),
                payload: Some(policy_batch_payload(batch)),
            })
            .await
            .map_err(EngineError::from)?
            .log_position;
        Ok((version, Some(position)))
    }

    /// Drive the log forward until the proposed intention has a decision, then
    /// return it.
    ///
    /// Reading starts after the checkpoint loaded before the proposal's append.
    /// The append-position check guarantees the proposal's appends follow this checkpoint.
    /// A concurrent drive may advance durable state after our snapshot was loaded;
    /// those re-deliveries are tolerated as stale while we look for the decision.
    ///
    /// If the log drains with no decision for the intention, the engine forces an
    /// `Abort`, so this always returns an outcome. Safe under first-decision-wins:
    /// the forced abort is appended ahead of any decision a later vote could
    /// produce, so it wins consistently (see `forced_abort`).
    async fn sync_and_apply(
        &self,
        bus_id: &str,
        proposed_position: i64,
        context: &mut EngineContext,
    ) -> EngineResult<ProposalOutcome> {
        let bus_id_owned = bus_id.to_string();
        let cursor = context.snapshot.position.map_or(0, |position| position + 1);
        let mut pos = cursor;
        let mut known_position = context.highest_append.get();

        loop {
            if pos > known_position {
                let tail = self
                    .runtime
                    .bus
                    .check_tail(CheckTailRequest {
                        bus_id: Some(BusId {
                            agent_bus_id: bus_id_owned.clone(),
                        }),
                    })
                    .await
                    .map_err(EngineError::from)?
                    .tail_position;

                if pos >= tail {
                    let response = self
                        .runtime
                        .bus
                        .append(AppendRequest {
                            bus_id: Some(BusId {
                                agent_bus_id: bus_id_owned.clone(),
                            }),
                            payload: Some(forced_abort(proposed_position)),
                        })
                        .await
                        .map_err(EngineError::from)?;
                    known_position = known_position.max(response.log_position);
                    continue;
                }
                known_position = tail - 1;
            }

            let (entries, next_cursor) = read_range(
                &self.runtime.bus,
                bus_id,
                pos,
                known_position + 1,
                POLL_BATCH_SIZE,
                None,
            )
            .await
            .map_err(EngineError::from)?;

            for entry in &entries {
                entry
                    .header
                    .as_ref()
                    .ok_or(ApplyError::MissingHeader)
                    .map_err(EngineError::Playback)?;

                // Commit/Abort entries never affect an engine state machine, but
                // one may be the outcome that completes this proposal.
                if let Some(outcome) = entry
                    .payload
                    .as_ref()
                    .and_then(|p| outcome_for(p, proposed_position))
                {
                    return Ok(outcome);
                }

                if requires_application(entry) {
                    self.apply(bus_id, entry, context)
                        .await
                        .map_err(EngineError::Playback)?;
                    known_position = known_position.max(context.highest_append.get());
                }
            }
            pos = next_cursor;
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<T, F, S, D, E> Applicator<EngineContext> for EngineRuntime<T, F, S, D, E>
where
    T: AgentBus,
    F: VoterFactory,
    S: Storage + 'static,
    D: DeciderFactory,
    E: Environment,
{
    /// Drive one entry: dispatch it to every matching applicator, append whatever
    /// they produce, and only then commit the engine cursor as the final write.
    /// An entry already behind the cursor is skipped, so re-reading applied
    /// entries while searching for an outcome neither re-drives them nor moves the
    /// cursor backward.
    async fn apply(
        &self,
        bus_id: &str,
        entry: &BusEntry,
        context: &mut EngineContext,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        let mut initial_snapshot = Some(context.snapshot.clone());
        let highest_append = &context.highest_append;
        // human: retry on concurrency errors to ensure forward progress. We
        // cannot wait on a competing drive and cannot assume it will finish.
        let mut retry_policy = ConcurrencyRetryPolicy::default();
        let updated_snapshot = retry(
            self.environment.as_ref(),
            self.config.retry_config,
            || {
                let initial_snapshot = initial_snapshot.take();
                async move {
                    let snapshot = match initial_snapshot {
                        Some(snapshot) => snapshot,
                        None => EngineSnapshot::load(&self.engine_storage, bus_id).await?,
                    };
                    self.apply_entry_once(bus_id, entry, snapshot, highest_append)
                        .await
                }
            },
            |error| match error {
                ApplyError::Concurrency(error @ ConcurrencyError::Engine { .. }) => {
                    retry_policy.decide(error)
                }
                _ => RetryDecision::Stop,
            },
        )
        .await
        .map_err(RetryFailure::into_last_error)?;
        context.snapshot = updated_snapshot;
        Ok(None)
    }
}

#[async_trait::async_trait(?Send)]
impl<T, F, S, P, D, E> Applicator<EngineContext> for BaseEngine<T, F, S, P, D, E> {
    async fn apply(
        &self,
        bus_id: &str,
        entry: &BusEntry,
        context: &mut EngineContext,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        self.playback.apply(bus_id, entry, context).await
    }
}

impl<T, F, S, D, E> EngineRuntime<T, F, S, D, E>
where
    T: AgentBus,
    F: VoterFactory,
    S: Storage + 'static,
    D: DeciderFactory,
    E: Environment,
{
    async fn apply_entry_once(
        &self,
        bus_id: &str,
        entry: &BusEntry,
        engine_snapshot: EngineSnapshot,
        highest_append: &Cell<i64>,
    ) -> std::result::Result<EngineSnapshot, ApplyError> {
        let entry_position = entry
            .header
            .as_ref()
            .map(|h| h.log_position)
            .ok_or(ApplyError::MissingHeader)?;

        let EngineSnapshot {
            mut state,
            position: state_position,
        } = engine_snapshot;
        if entry_position < state_position.map_or(0, |position| position + 1) {
            // Already applied (e.g. re-read while searching for the outcome). The
            // cursor is the slot position + 1.
            return Ok(EngineSnapshot {
                state,
                position: state_position,
            });
        }

        // 1. Dispatch the entry to every matching applicator (each persists its own
        // state). An applicator already past this entry returns `StalePosition`,
        // which we swallow and skip (see `apply_tolerating_stale`). A `PolicyBatch`
        // matches no applicator here — its decider policy and voters are applied to
        // engine state in step 3 — so it produces nothing.
        let mut produced = Vec::new();
        if let Some(payload) = entry.payload.as_ref()
            && let Some(reason) = policy_constraint_failure(payload, state.applied_policy_version)
        {
            produced.push(abort_payload(entry_position, &reason));
        } else if let Some(payload) = entry.payload.as_ref() {
            let decider = self.build_decider(&state)?;
            if decider.matches(payload) {
                if let Some(p) = apply_tolerating_stale(
                    decider.applicator.as_ref(),
                    bus_id,
                    entry,
                    self.environment.as_ref(),
                    self.config.retry_config,
                )
                .await?
                {
                    produced.push(p);
                }
            }
            for voter in self.build_voters(&state)? {
                if voter.matches(payload) {
                    if let Some(p) = apply_tolerating_stale(
                        voter.applicator.as_ref(),
                        bus_id,
                        entry,
                        self.environment.as_ref(),
                        self.config.retry_config,
                    )
                    .await?
                    {
                        produced.push(p);
                    }
                }
            }
        }

        // 2. Append what the applicators produced, before committing the cursor.
        for payload in produced {
            let response = self
                .bus
                .append(AppendRequest {
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.to_owned(),
                    }),
                    payload: Some(payload),
                })
                .await
                .map_err(ApplyError::Bus)?;
            // Retain confirmed writes even if the following cursor CAS needs a retry.
            highest_append.set(highest_append.get().max(response.log_position));
        }

        // 3. Advance the engine state last — recording decider policy and
        // installing/removing voters — as a single CAS at `entry_position`. The
        // slot position is the cursor (next position to apply is `entry_position +
        // 1`), so it isn't stored separately. A `PolicyBatch` applies its whole
        // policy (decider policy + voters) here, atomically under that one CAS.
        // The fallible conversions below deliberately return before the CAS. A
        // malformed policy or control entry therefore leaves the cursor unchanged,
        // making every subsequent drive stop and retry the same entry.
        if let Some(payload) = entry.payload.as_ref() {
            apply_policy_entry(&mut state, payload, entry_position, &|config| {
                self.voter_factory.validate_config(config)
            })?;
        }
        // This final CAS commits policy changes and the cursor together. A crash
        // before the CAS causes the entry to be re-driven; applicators replay their
        // stored result, and policy-state changes are reapplied idempotently.
        state
            .save(&self.engine_storage, bus_id, state_position, entry_position)
            .await
            .into_engine_apply_result(bus_id, entry_position)?;
        Ok(EngineSnapshot {
            state,
            position: Some(entry_position),
        })
    }
}

/// Resolve the decider configuration and identity.
pub fn load_decider_spec_from_state(
    state: &PerBusEngineState,
) -> std::result::Result<StateMachineSpec<Option<i64>, DeciderPolicy>, ApplyError> {
    match &state.decider {
        Some(config) => {
            let policy = DeciderPolicy::try_from(config.policy).map_err(|_| {
                ApplyError::InvalidEngineState {
                    message: format!("unrecognized decider policy {}", config.policy),
                }
            })?;
            Ok(StateMachineSpec::new(Some(config.id), policy))
        }
        None => Ok(StateMachineSpec::new(None, DeciderPolicy::OnByDefault)),
    }
}

/// Installed voter configs in dispatch order.
pub fn state_voter_configs(state: &PerBusEngineState) -> Vec<(String, VoterConfig)> {
    let mut configs: Vec<_> = state
        .voters
        .iter()
        .map(|(voter_id, config)| (voter_id.clone(), config.clone()))
        .collect();
    // Voter ids may be caller-assigned strings (not just numeric log positions),
    // so order them lexicographically for a deterministic, total order.
    configs.sort_by(|(a, _), (b, _)| a.cmp(b));
    configs
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use agentbus_simulator::Simulator;
    use futures::executor::block_on;

    use super::*;
    use crate::StorageError;
    use crate::VersionedDecider;

    struct ScriptedApplicator {
        calls: Cell<usize>,
        results: RefCell<VecDeque<std::result::Result<Option<Payload>, ApplyError>>>,
    }

    impl ScriptedApplicator {
        fn new(
            results: impl IntoIterator<Item = std::result::Result<Option<Payload>, ApplyError>>,
        ) -> Rc<Self> {
            Rc::new(Self {
                calls: Cell::new(0),
                results: RefCell::new(results.into_iter().collect()),
            })
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Applicator for ScriptedApplicator {
        async fn apply(
            &self,
            _bus_id: &str,
            _entry: &BusEntry,
            _context: &mut (),
        ) -> std::result::Result<Option<Payload>, ApplyError> {
            self.calls.set(self.calls.get() + 1);
            self.results
                .borrow_mut()
                .pop_front()
                .expect("test should provide one result per apply call")
        }
    }

    fn voter_transaction_conflict(
        attempt: usize,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        Err(ApplyError::Concurrency(ConcurrencyError::Voter {
            bus_id: "bus".to_string(),
            position: 7,
            source: Some(StorageError::TransactionConflict(anyhow::anyhow!(
                "conflict {attempt}"
            ))),
        }))
    }

    fn apply_with_test_retries(
        applicator: Rc<ScriptedApplicator>,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        let environment = Rc::new(Simulator::new(0));
        let task_environment = environment.clone();
        let handle = environment.spawn(async move {
            let entry = BusEntry::default();
            apply_tolerating_stale(
                applicator.as_ref(),
                "bus",
                &entry,
                task_environment.as_ref(),
                BaseEngineConfig::default().retry_config,
            )
            .await
        });
        environment.run();
        block_on(handle).expect("retry task should complete")
    }

    #[test]
    fn voter_and_decider_cas_rejections_are_retried() {
        for conflict in [
            ConcurrencyError::Voter {
                bus_id: "bus".to_string(),
                position: 7,
                source: None,
            },
            ConcurrencyError::Decider {
                bus_id: "bus".to_string(),
                position: 7,
                source: None,
            },
        ] {
            let expected = Payload::default();
            let applicator = ScriptedApplicator::new([
                Err(ApplyError::Concurrency(conflict)),
                Ok(Some(expected.clone())),
            ]);
            let result =
                apply_with_test_retries(applicator.clone()).expect("CAS retry should succeed");

            assert_eq!(result, Some(expected));
            assert_eq!(applicator.calls.get(), 2);
        }
    }

    #[test]
    fn transaction_conflicts_are_retried() {
        let applicator = ScriptedApplicator::new([voter_transaction_conflict(0), Ok(None)]);
        let result = apply_with_test_retries(applicator.clone())
            .expect("transaction conflict retry should succeed");

        assert_eq!(result, None);
        assert_eq!(applicator.calls.get(), 2);
    }

    #[test]
    fn final_transaction_conflict_escapes_after_retry_limit() {
        let applicator = ScriptedApplicator::new(
            (0..=DEFAULT_STORAGE_CONCURRENCY_MAX_RETRIES).map(voter_transaction_conflict),
        );
        let error = apply_with_test_retries(applicator.clone())
            .expect_err("the final conflict should escape after the retry limit is exhausted");

        assert!(matches!(
            error,
            ApplyError::Concurrency(ConcurrencyError::Voter {
                source: Some(StorageError::TransactionConflict(_)),
                ..
            })
        ));
        assert_eq!(
            applicator.calls.get(),
            DEFAULT_STORAGE_CONCURRENCY_MAX_RETRIES + 1,
            "the initial apply and every configured retry should run"
        );
    }

    #[test]
    fn stale_result_after_a_conflict_is_tolerated() {
        let applicator = ScriptedApplicator::new([
            Err(ApplyError::Concurrency(ConcurrencyError::Voter {
                bus_id: "bus".to_string(),
                position: 7,
                source: None,
            })),
            Err(ApplyError::StalePosition {
                requested: 7,
                last: 8,
            }),
        ]);
        let result = apply_with_test_retries(applicator.clone())
            .expect("stale state after a conflict should be tolerated");

        assert_eq!(result, None);
        assert_eq!(applicator.calls.get(), 2);
    }

    #[test]
    fn unrelated_errors_are_not_retried() {
        let applicator = ScriptedApplicator::new([
            Err(ApplyError::Concurrency(ConcurrencyError::Engine {
                bus_id: "bus".to_string(),
                position: 7,
                source: None,
            })),
            Ok(None),
        ]);
        let error = apply_with_test_retries(applicator.clone())
            .expect_err("engine CAS rejection should escape");

        assert!(matches!(
            error,
            ApplyError::Concurrency(ConcurrencyError::Engine { .. })
        ));
        assert_eq!(applicator.calls.get(), 1);

        let applicator = ScriptedApplicator::new([
            Err(ApplyError::Storage(StorageError::TransactionConflict(
                anyhow::anyhow!("unclassified conflict"),
            ))),
            Ok(None),
        ]);
        let error = apply_with_test_retries(applicator.clone())
            .expect_err("unclassified storage conflict should escape");

        assert!(matches!(
            error,
            ApplyError::Storage(StorageError::TransactionConflict(_))
        ));
        assert_eq!(applicator.calls.get(), 1);
    }

    #[test]
    fn engine_storage_cas_rejection_is_typed() {
        let error = Ok::<bool, StorageError>(false)
            .into_engine_apply_result("bus-1", 7)
            .expect_err("storage CAS should be rejected");

        assert_eq!(
            error.to_string(),
            "engine storage conflict for bus 'bus-1' at position 7"
        );
        assert!(
            matches!(
                error,
                ApplyError::Concurrency(ConcurrencyError::Engine {
                    ref bus_id,
                    position: 7,
                    source: None,
                }) if bus_id == "bus-1"
            ),
            "CAS rejection should retain its context, got {error:?}"
        );
    }

    #[test]
    fn unconfigured_bus_uses_default_policy() {
        let state = PerBusEngineState::default();

        let spec = load_decider_spec_from_state(&state)
            .expect("an unconfigured bus should use the default policy");
        assert_eq!(spec.config, DeciderPolicy::OnByDefault);
        assert_eq!(spec.id, None);
    }

    #[test]
    fn configured_bus_uses_its_policy_id() {
        let state = PerBusEngineState {
            decider: Some(VersionedDecider {
                policy: DeciderPolicy::FirstBooleanWins as i32,
                id: 17,
            }),
            ..Default::default()
        };

        let spec = load_decider_spec_from_state(&state)
            .expect("a recognized configured policy should load");
        assert_eq!(spec.config, DeciderPolicy::FirstBooleanWins);
        assert_eq!(spec.id, Some(17));
    }

    #[test]
    fn unrecognized_persisted_policy_is_an_error() {
        let state = PerBusEngineState {
            decider: Some(VersionedDecider { policy: 99, id: 17 }),
            ..Default::default()
        };

        let error = match load_decider_spec_from_state(&state) {
            Ok(_) => panic!("an unrecognized persisted policy should fail playback"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ApplyError::InvalidEngineState { ref message }
                if message.contains("unrecognized decider policy 99")
        ));
    }
}
