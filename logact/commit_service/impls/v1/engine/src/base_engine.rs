/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::Abort;
use agent_bus_proto_rust::agent_bus::AppendRequest;
use agent_bus_proto_rust::agent_bus::BaseEngineControl;
use agent_bus_proto_rust::agent_bus::BusEntry;
use agent_bus_proto_rust::agent_bus::BusId;
use agent_bus_proto_rust::agent_bus::Control;
use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agent_bus_proto_rust::agent_bus::Payload;
use agent_bus_proto_rust::agent_bus::PolicyBatch;
use agent_bus_proto_rust::agent_bus::PollRequest;
use agent_bus_proto_rust::agent_bus::SelectivePollType;
use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::base_engine_control;
use agent_bus_proto_rust::agent_bus::control;
use agent_bus_proto_rust::agent_bus::intention;
use agent_bus_proto_rust::agent_bus::payload;
use agent_bus_proto_rust::agent_bus::voter_op;
use agentbus_api::AgentBus;
use agentbus_api::AgentBusError;
use agentbus_api::Environment;
use agentbus_api::RetryConfig;
use agentbus_api::RetryDecision;
use agentbus_api::RetryFailure;
use agentbus_api::helpers::get_payload_type;
use agentbus_api::retry;
use anyhow::Context;
use anyhow::Result;
use logact_commit_service_api::PolicyProvider;
use logact_commit_service_api::PolicyState;

use crate::Applicator;
use crate::ApplyError;
use crate::ConcurrencyError;
use crate::DeprecatedOperationKind;
use crate::EngineStateLoadError;
use crate::FirstBooleanWinsApplicator;
use crate::MalformedPolicyBatchKind;
use crate::OffByDefaultApplicator;
use crate::OnByDefaultApplicator;
use crate::PerBusEngineState;
use crate::StateMachineSpec;
use crate::Storage;
use crate::StorageError;
use crate::VersionedPolicy;
use crate::applicator::StorageWriteResultExt;
use crate::policy::plan;
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
pub fn validate_voter_configs(
    voter_factory: &impl VoterFactory,
    policy: &PolicyState,
) -> Result<()> {
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
fn outcome_for(payload: &Payload, proposed_position: i64) -> Option<ProposalOutcome> {
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
fn forced_abort(intention_id: i64) -> Payload {
    abort(intention_id, "no decision before the log was drained")
}

fn abort(intention_id: i64, reason: &str) -> Payload {
    Payload {
        payload: Some(payload::Payload::Abort(Abort {
            intention_id,
            reason: reason.to_string(),
        })),
    }
}

fn policy_constraint_failure(payload: &Payload, applied_version: Option<i64>) -> Option<String> {
    let payload::Payload::Intention(intention) = payload.payload.as_ref()? else {
        return None;
    };
    match intention.policy_version_constraint.as_ref()? {
        intention::PolicyVersionConstraint::RequiredPolicyVersion(required)
            if applied_version != Some(*required) =>
        {
            Some(format!(
                "required policy version {required}, but applied version is {applied_version:?}"
            ))
        }
        intention::PolicyVersionConstraint::MinimumPolicyVersion(minimum)
            if applied_version.is_none_or(|applied| applied < *minimum) =>
        {
            Some(format!(
                "minimum policy version {minimum}, but applied version is {applied_version:?}"
            ))
        }
        _ => None,
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

struct EngineRuntime<T, F, S, D, E> {
    bus: T,
    voter_factory: F,
    engine_storage: Rc<S>,
    decider_factory: D,
    environment: Rc<E>,
    config: BaseEngineConfig,
}

struct EngineEntryApplicator<T, F, S, D, E> {
    runtime: Rc<EngineRuntime<T, F, S, D, E>>,
}

pub struct BaseEngine<T, F, S, P, D, E> {
    runtime: Rc<EngineRuntime<T, F, S, D, E>>,
    playback: Rc<dyn Applicator>,
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
            match applicator.apply(bus_id, entry).await {
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

fn malformed_policy(kind: MalformedPolicyBatchKind, error: anyhow::Error) -> ApplyError {
    ApplyError::MalformedPolicy {
        kind,
        message: format!("{error:#}"),
    }
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

    fn apply_policy_batch(
        &self,
        state: &mut PerBusEngineState,
        batch: &PolicyBatch,
        entry_position: i64,
    ) -> std::result::Result<(), ApplyError> {
        let voter_ops = batch
            .voter_ops
            .iter()
            .map(|(voter_id, op)| {
                let op = match op.op.as_ref() {
                    Some(voter_op::Op::Add(add)) => {
                        let config = self
                            .voter_factory
                            .validate_config(add.config.as_ref())
                            .with_context(|| {
                                format!("PolicyBatch voter '{voter_id}' has invalid config")
                            })
                            .map_err(|error| {
                                malformed_policy(MalformedPolicyBatchKind::AddVoter, error)
                            })?;
                        ValidatedVoterOp::Add(config)
                    }
                    Some(voter_op::Op::Remove(_)) => {
                        if !state.voters.contains_key(voter_id) {
                            return Err(malformed_policy(
                                MalformedPolicyBatchKind::RemoveVoter,
                                anyhow::anyhow!(
                                    "PolicyBatch cannot remove unknown voter '{voter_id}'"
                                ),
                            ));
                        }
                        ValidatedVoterOp::Remove
                    }
                    None => {
                        return Err(malformed_policy(
                            MalformedPolicyBatchKind::PolicyBatch,
                            anyhow::anyhow!("PolicyBatch voter '{voter_id}' has no operation"),
                        ));
                    }
                };
                Ok((voter_id, op))
            })
            .collect::<std::result::Result<Vec<_>, ApplyError>>()?;

        set_decider_policy_in_engine_state(state, batch.decider_policy, entry_position)
            .with_context(|| {
                format!(
                    "PolicyBatch decider policy {:?} is invalid",
                    batch.decider_policy
                )
            })
            .map_err(|error| malformed_policy(MalformedPolicyBatchKind::PolicyBatch, error))?;
        state.applied_policy_version = Some(batch.new_version);
        for (voter_id, op) in voter_ops {
            match op {
                ValidatedVoterOp::Add(config) => {
                    upsert_voter_config(state, voter_id.clone(), config);
                }
                ValidatedVoterOp::Remove => remove_voter_config(state, voter_id),
            }
        }
        Ok(())
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
        let playback = Rc::new(EngineEntryApplicator {
            runtime: runtime.clone(),
        });
        Self {
            runtime,
            playback,
            policy_provider,
        }
    }

    /// Decorate the applicator used for entry playback.
    pub fn with_playback_wrapper<W>(mut self, wrap: W) -> Self
    where
        W: FnOnce(Rc<dyn Applicator>) -> Rc<dyn Applicator>,
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
{
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

        let (state, state_pos) = PerBusEngineState::load(&self.runtime.engine_storage, bus_id)
            .await
            .map_err(EngineError::from)?;
        let policy_version = self.append_resolved_policy(bus_id, &state).await?;
        if let Some(policy_version) = policy_version {
            let Some(payload::Payload::Intention(proposed_intention)) = intention.payload.as_mut()
            else {
                unreachable!("the proposed payload was validated as an intention");
            };
            proposed_intention.policy_version_constraint = Some(
                intention::PolicyVersionConstraint::MinimumPolicyVersion(policy_version),
            );
        }
        let position = self
            .runtime
            .bus
            .append(AppendRequest {
                agent_bus_id: bus_id.to_owned(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_owned(),
                }),
                payload: Some(intention),
            })
            .await
            .map_err(EngineError::Bus)?
            .log_position;

        self.sync_and_apply(bus_id, position, state_pos.map_or(0, |p| p + 1))
            .await
    }

    async fn append_resolved_policy(
        &self,
        bus_id: &str,
        state: &PerBusEngineState,
    ) -> EngineResult<Option<i64>> {
        let desired = self
            .policy_provider
            .read(bus_id)
            .await
            .map_err(|error| EngineError::PolicyProvider(error.into()))?;
        let version = desired.version;
        let current = PolicyState {
            decider_policy: Some(
                state
                    .decider
                    .as_ref()
                    .map(|decider| DeciderPolicy::try_from(decider.policy))
                    .transpose()
                    .context("engine state has an invalid decider policy")
                    .map_err(EngineError::InvalidEngineState)?
                    .unwrap_or(DeciderPolicy::OnByDefault) as i32,
            ),
            voters: state
                .voters
                .iter()
                .map(|(voter_id, config)| (voter_id.clone(), config.clone()))
                .collect(),
        };
        let Some(batch) = plan(&current, state.applied_policy_version, &desired)
            .map_err(EngineError::Internal)?
        else {
            return Ok(Some(version));
        };
        self.runtime
            .bus
            .append(AppendRequest {
                agent_bus_id: bus_id.to_owned(),
                bus_id: Some(BusId {
                    agent_bus_id: bus_id.to_owned(),
                }),
                payload: Some(policy_batch_payload(batch)),
            })
            .await
            .map_err(EngineError::Bus)?;

        Ok(Some(version))
    }

    /// Drive the log forward until the proposed intention has a decision, then
    /// return it.
    ///
    /// Reading starts at `min(proposed_position, cursor)`, which unifies two
    /// cases: when the engine is behind we catch up from the cursor and drive
    /// every state-machine input through to the decision; when an earlier drive
    /// already advanced the cursor past the intention we re-read from the intention
    /// to observe a decision it produced (those re-deliveries are tolerated as
    /// stale).
    ///
    /// If the log drains with no decision for the intention, the engine forces an
    /// `Abort`, so this always returns an outcome. Safe under first-decision-wins:
    /// the forced abort is appended ahead of any decision a later vote could
    /// produce, so it wins consistently (see `forced_abort`).
    async fn sync_and_apply(
        &self,
        bus_id: &str,
        proposed_position: i64,
        cursor: i64,
    ) -> EngineResult<ProposalOutcome> {
        let bus_id_owned = bus_id.to_string();
        let mut pos = proposed_position.min(cursor);

        loop {
            // `poll` is self-bounding — an empty result means we've reached the
            // tail (no `check_tail` needed). Read a bounded batch to amortize bus
            // latency over replay rather than paying it once per historical entry.
            let resp = self
                .runtime
                .bus
                .poll(PollRequest {
                    agent_bus_id: bus_id_owned.clone(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id_owned.clone(),
                    }),
                    start_log_position: pos,
                    max_entries: POLL_BATCH_SIZE,
                    filter: None,
                })
                .await
                .map_err(EngineError::Bus)?;

            if resp.entries.is_empty() {
                // Drained the log with no decision — force an abort, then loop to
                // read it back as the outcome.
                self.runtime
                    .bus
                    .append(AppendRequest {
                        agent_bus_id: bus_id_owned.clone(),
                        bus_id: Some(BusId {
                            agent_bus_id: bus_id_owned.clone(),
                        }),
                        payload: Some(forced_abort(proposed_position)),
                    })
                    .await
                    .map_err(EngineError::Bus)?;
                continue;
            }

            for entry in &resp.entries {
                let entry_position = entry
                    .header
                    .as_ref()
                    .map(|header| header.log_position)
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
                    self.playback
                        .apply(bus_id, entry)
                        .await
                        .map_err(EngineError::Playback)?;
                }
                pos = entry_position + 1;
            }
        }
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
    /// Drive one entry: dispatch it to every matching applicator, append whatever
    /// they produce, and only then commit the engine cursor as the final write.
    /// An entry already behind the cursor is skipped, so re-reading applied
    /// entries while searching for an outcome neither re-drives them nor moves the
    /// cursor backward.
    async fn apply_entry(
        &self,
        bus_id: &str,
        entry: &BusEntry,
    ) -> std::result::Result<(), ApplyError> {
        // human: retry on concurrency errors to ensure forward progress. We
        // cannot wait on a competing drive and cannot assume it will finish.
        let mut retry_policy = ConcurrencyRetryPolicy::default();
        retry(
            self.environment.as_ref(),
            self.config.retry_config,
            || self.apply_entry_once(bus_id, entry),
            |error| match error {
                ApplyError::Concurrency(error @ ConcurrencyError::Engine { .. }) => {
                    retry_policy.decide(error)
                }
                _ => RetryDecision::Stop,
            },
        )
        .await
        .map_err(RetryFailure::into_last_error)
    }

    async fn apply_entry_once(
        &self,
        bus_id: &str,
        entry: &BusEntry,
    ) -> std::result::Result<(), ApplyError> {
        let entry_position = entry
            .header
            .as_ref()
            .map(|h| h.log_position)
            .ok_or(ApplyError::MissingHeader)?;

        let (mut state, state_position) =
            PerBusEngineState::load(&self.engine_storage, bus_id).await?;
        if entry_position < state_position.map_or(0, |position| position + 1) {
            // Already applied (e.g. re-read while searching for the outcome). The
            // cursor is the slot position + 1.
            return Ok(());
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
            produced.push(abort(entry_position, &reason));
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
            self.bus
                .append(AppendRequest {
                    agent_bus_id: bus_id.to_owned(),
                    bus_id: Some(BusId {
                        agent_bus_id: bus_id.to_owned(),
                    }),
                    payload: Some(payload),
                })
                .await
                .map_err(ApplyError::Bus)?;
        }

        // 3. Advance the engine state last — recording decider policy and
        // installing/removing voters — as a single CAS at `entry_position`. The
        // slot position is the cursor (next position to apply is `entry_position +
        // 1`), so it isn't stored separately. A `PolicyBatch` applies its whole
        // policy (decider policy + voters) here, atomically under that one CAS.
        // The fallible conversions below deliberately return before the CAS. A
        // malformed policy or control entry therefore leaves the cursor unchanged,
        // making every subsequent drive stop and retry the same entry.
        match base_engine_control_of(entry) {
            Some(base_engine_control::Control::PolicyBatch(batch)) => {
                if should_apply_policy_batch(&state, batch) {
                    self.apply_policy_batch(&mut state, batch, entry_position)?;
                }
            }
            Some(base_engine_control::Control::AddVoter(_)) => {
                return Err(ApplyError::DeprecatedOperation {
                    operation: DeprecatedOperationKind::AddVoter,
                });
            }
            Some(base_engine_control::Control::RemoveVoter(_)) => {
                return Err(ApplyError::DeprecatedOperation {
                    operation: DeprecatedOperationKind::RemoveVoter,
                });
            }
            None if matches!(
                entry.payload.as_ref().and_then(|p| p.payload.as_ref()),
                Some(payload::Payload::DeciderPolicy(_))
            ) =>
            {
                return Err(ApplyError::DeprecatedOperation {
                    operation: DeprecatedOperationKind::SetDeciderPolicy,
                });
            }
            None => {}
        }
        // This final CAS commits policy changes and the cursor together. A crash
        // before the CAS causes the entry to be re-driven; applicators replay their
        // stored result, and policy-state changes are reapplied idempotently.
        state
            .save(&self.engine_storage, bus_id, state_position, entry_position)
            .await
            .into_engine_apply_result(bus_id, entry_position)?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl<T, F, S, D, E> Applicator for EngineEntryApplicator<T, F, S, D, E>
where
    T: AgentBus,
    F: VoterFactory,
    S: Storage + 'static,
    D: DeciderFactory,
    E: Environment + 'static,
{
    async fn apply(
        &self,
        bus_id: &str,
        entry: &BusEntry,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        self.runtime.apply_entry(bus_id, entry).await?;
        Ok(None)
    }
}

#[async_trait::async_trait(?Send)]
impl<T, F, S, P, D, E> Applicator for BaseEngine<T, F, S, P, D, E> {
    async fn apply(
        &self,
        bus_id: &str,
        entry: &BusEntry,
    ) -> std::result::Result<Option<Payload>, ApplyError> {
        self.playback.apply(bus_id, entry).await
    }
}

fn should_apply_policy_batch(state: &PerBusEngineState, batch: &PolicyBatch) -> bool {
    state.applied_policy_version == batch.expected_current_version
        && batch
            .expected_current_version
            .is_none_or(|current| batch.new_version > current)
}

/// Resolve the decider configuration and identity.
fn load_decider_spec_from_state(
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

fn state_voter_configs(state: &PerBusEngineState) -> Vec<(String, VoterConfig)> {
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

fn upsert_voter_config(state: &mut PerBusEngineState, voter_id: String, config: VoterConfig) {
    state.voters.insert(voter_id, config);
}

fn remove_voter_config(state: &mut PerBusEngineState, voter_id: &str) {
    state.voters.remove(voter_id);
}

enum ValidatedVoterOp {
    Add(VoterConfig),
    Remove,
}

/// Record `policy` with this entry's log position as its ID.
fn set_decider_policy_in_engine_state(
    state: &mut PerBusEngineState,
    policy: Option<i32>,
    position: i64,
) -> Result<()> {
    if let Some(policy) = policy {
        let policy = DeciderPolicy::try_from(policy).context("invalid decider policy")?;
        state.decider = Some(VersionedPolicy {
            policy: policy as i32,
            id: position,
        });
    }
    Ok(())
}

/// The `BaseEngineControl` directive carried by `entry`, or `None` for other
/// payloads such as a standalone `DeciderPolicy`.
fn base_engine_control_of(entry: &BusEntry) -> Option<&base_engine_control::Control> {
    match entry.payload.as_ref()?.payload.as_ref()? {
        payload::Payload::Control(control) => {
            let control::Control::BaseEngineControl(base) = control.control.as_ref()?;
            base.control.as_ref()
        }
        _ => None,
    }
}

/// Wrap a `PolicyBatch` in the `Control` payload the engine appends to provision
/// policy as a single atomic log entry.
fn policy_batch_payload(batch: PolicyBatch) -> Payload {
    Payload {
        payload: Some(payload::Payload::Control(Control {
            control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                control: Some(base_engine_control::Control::PolicyBatch(batch)),
            })),
        })),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use agent_bus_proto_rust::agent_bus::AddVoterOp;
    use agent_bus_proto_rust::agent_bus::BaseEngineControl;
    use agent_bus_proto_rust::agent_bus::Control;
    use agent_bus_proto_rust::agent_bus::Intention;
    use agent_bus_proto_rust::agent_bus::PolicyBatch;
    use agent_bus_proto_rust::agent_bus::VoterOp;
    use agentbus_simulator::Simulator;
    use futures::executor::block_on;

    use super::*;
    use crate::StorageError;

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
            decider: Some(VersionedPolicy {
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
    fn repeated_policy_entry_changes_id() {
        let mut state = PerBusEngineState {
            decider: Some(VersionedPolicy {
                policy: DeciderPolicy::FirstBooleanWins as i32,
                id: 10,
            }),
            ..Default::default()
        };

        set_decider_policy_in_engine_state(
            &mut state,
            Some(DeciderPolicy::FirstBooleanWins as i32),
            20,
        )
        .expect("recognized policy should be stored");

        assert_eq!(
            state.decider.expect("policy should remain configured").id,
            20
        );
    }

    #[test]
    fn unrecognized_persisted_policy_is_an_error() {
        let state = PerBusEngineState {
            decider: Some(VersionedPolicy { policy: 99, id: 17 }),
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

    #[test]
    fn unrecognized_policy_entry_is_rejected_without_mutation() {
        let mut state = PerBusEngineState {
            decider: Some(VersionedPolicy {
                policy: DeciderPolicy::FirstBooleanWins as i32,
                id: 10,
            }),
            ..Default::default()
        };
        let original_decider = state.decider.clone();

        let error = set_decider_policy_in_engine_state(&mut state, Some(99), 17)
            .expect_err("unknown policy should be rejected");

        assert!(error.to_string().contains("invalid decider policy"));
        assert_eq!(state.decider, original_decider);
    }

    #[test]
    fn base_engine_control_of_reads_a_batch() {
        let mut voter_ops = HashMap::new();
        voter_ops.insert(
            "v".to_string(),
            VoterOp {
                op: Some(voter_op::Op::Add(AddVoterOp {
                    config: Some(VoterConfig::default()),
                })),
            },
        );
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(payload::Payload::Control(Control {
                    control: Some(control::Control::BaseEngineControl(BaseEngineControl {
                        control: Some(base_engine_control::Control::PolicyBatch(PolicyBatch {
                            decider_policy: None,
                            voter_ops,
                            ..Default::default()
                        })),
                    })),
                })),
            }),
        };
        assert!(matches!(
            base_engine_control_of(&entry),
            Some(base_engine_control::Control::PolicyBatch(batch)) if batch.voter_ops.len() == 1
        ));
    }

    #[test]
    fn base_engine_control_of_ignores_non_control() {
        let entry = BusEntry {
            header: None,
            payload: Some(Payload {
                payload: Some(payload::Payload::DeciderPolicy(0)),
            }),
        };
        assert!(base_engine_control_of(&entry).is_none());
    }

    fn constrained_intention(constraint: intention::PolicyVersionConstraint) -> Payload {
        Payload {
            payload: Some(payload::Payload::Intention(Intention {
                intention: Some(intention::Intention::StringIntention("test".to_string())),
                policy_version_constraint: Some(constraint),
            })),
        }
    }

    #[test]
    fn intention_policy_version_constraints_are_enforced() {
        let exact =
            constrained_intention(intention::PolicyVersionConstraint::RequiredPolicyVersion(5));
        assert!(policy_constraint_failure(&exact, Some(5)).is_none());
        assert!(policy_constraint_failure(&exact, Some(6)).is_some());
        assert!(policy_constraint_failure(&exact, None).is_some());

        let minimum =
            constrained_intention(intention::PolicyVersionConstraint::MinimumPolicyVersion(5));
        assert!(policy_constraint_failure(&minimum, Some(5)).is_none());
        assert!(policy_constraint_failure(&minimum, Some(6)).is_none());
        assert!(policy_constraint_failure(&minimum, Some(4)).is_some());
        assert!(policy_constraint_failure(&minimum, None).is_some());
    }
}
