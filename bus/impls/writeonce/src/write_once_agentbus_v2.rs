/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! WriteOnceAgentBusV2: An AgentBus backed by a write-once address space.
//!
//! The WriteOnceSpace handles routing via space_id internally. Multiple WriteOnceAgentBusV2
//! instances can share the same WriteOnceSpace, enabling testing of contention and retry-on-conflict.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::*;
use agentbus_api::AgentBusError;
use agentbus_api::BusResult;
use agentbus_api::TailError;
use agentbus_api::WriteOnceError;
use agentbus_api::WriteOnceResult;
use agentbus_api::WriteOnceSpace;
use agentbus_api::environment::Clock;
use agentbus_api::environment::Environment;
use agentbus_api::payload_matches_filter;
use agentbus_api::validate_bus_id;
use bytes::Bytes;
use lru::LruCache;
use prost::Message as ProstMessage;
use tracing::debug;
use tracing::error;

use crate::sequencer::Claim as _;
use crate::sequencer::Sequencer;
use crate::simple_sequencer::SimpleSequencer;

const MAX_POLL_ENTRIES: usize = 64;
// An append gives up after this many failed rewrites of its entry.
const MAX_REWRITES: usize = 3;
const DEFAULT_BLOCKING_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const SPACE_PREFIX: &str = "bus/";
const DEFAULT_NEXT_POSITIONS_CACHE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(100_000).expect("default cache capacity is nonzero");

fn prefixed_space_id(bus_id: &str) -> String {
    format!("{}{}", SPACE_PREFIX, bus_id)
}

fn tail_error_to_bus(error: TailError) -> AgentBusError {
    AgentBusError::Unavailable(anyhow::Error::new(error).context("Failed to find tail"))
}

fn read_error_to_bus(error: WriteOnceError) -> AgentBusError {
    match error {
        error @ WriteOnceError::Timeout(_) => {
            AgentBusError::Timeout(anyhow::Error::new(error).context("WriteOnceSpace read failed"))
        }
        error @ WriteOnceError::BackendUnavailable(_) => AgentBusError::Unavailable(
            anyhow::Error::new(error).context("WriteOnceSpace read failed"),
        ),
        error @ (WriteOnceError::InternalError(_)
        | WriteOnceError::AddressAlreadyExists(_)
        | WriteOnceError::AmbiguousWrite(_)) => {
            AgentBusError::Internal(anyhow::Error::new(error).context("WriteOnceSpace read failed"))
        }
    }
}

/// Shared state across all clones of a WriteOnceAgentBusV2
struct WriteOnceAgentBusState<W: WriteOnceSpace, E: Environment, Q: Sequencer<E>> {
    space: W,
    environment: Rc<E>,
    // Evicting a sequencer that is still in use lets a later append claim its positions.
    sequencers: LruCache<String, Rc<Q>>,
    blocking_poll_interval: std::time::Duration,
}

/// WriteOnceAgentBusV2 - AgentBus backed by a write-once address space.
///
/// This is a lightweight handle that can be cloned. All state is shared
/// via Rc<RefCell<...>>.
///
/// Generic over:
/// - `W: WriteOnceSpace + Clone` - the write-once storage backend (cloneable for async ops)
/// - `E: Environment` - the environment for clock access
/// - `Q: Sequencer` - assigns append positions for each bus
pub struct WriteOnceAgentBusV2<
    W: WriteOnceSpace + Clone,
    E: Environment,
    Q: Sequencer<E> = SimpleSequencer,
> {
    state: Rc<RefCell<WriteOnceAgentBusState<W, E, Q>>>,
}

impl<W: WriteOnceSpace + Clone, E: Environment, Q: Sequencer<E>> Clone
    for WriteOnceAgentBusV2<W, E, Q>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<W: WriteOnceSpace + Clone + 'static, E: Environment, Q: Sequencer<E>>
    WriteOnceAgentBusV2<W, E, Q>
{
    /// Create a new WriteOnceAgentBusV2 with the given WriteOnceSpace and Environment.
    ///
    /// `blocking_poll_interval` controls how often the backing store is polled
    /// inside `blocking_poll`. Defaults to 50ms if `None`.
    pub fn new(
        space: W,
        environment: Rc<E>,
        blocking_poll_interval: Option<std::time::Duration>,
    ) -> Self {
        Self::new_with_cache_capacity(
            space,
            environment,
            blocking_poll_interval,
            DEFAULT_NEXT_POSITIONS_CACHE_CAPACITY,
        )
    }

    /// Construct a bus with a bounded LRU cache of per-bus sequencers.
    /// A missing sequencer is rebuilt from storage before appending.
    pub fn new_with_cache_capacity(
        space: W,
        environment: Rc<E>,
        blocking_poll_interval: Option<std::time::Duration>,
        next_positions_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            state: Rc::new(RefCell::new(WriteOnceAgentBusState {
                space,
                environment,
                sequencers: LruCache::new(next_positions_capacity),
                blocking_poll_interval: blocking_poll_interval
                    .unwrap_or(DEFAULT_BLOCKING_POLL_INTERVAL),
            })),
        }
    }
}

impl<W: WriteOnceSpace + Clone + 'static, E: Environment + 'static, Q: Sequencer<E>>
    agentbus_api::AgentBus for WriteOnceAgentBusV2<W, E, Q>
{
    async fn append(&self, request: AppendRequest) -> BusResult<AppendResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        let bus_id = bus_id.agent_bus_id.clone();

        let payload = request.payload.ok_or_else(|| {
            AgentBusError::InvalidArgument(anyhow::anyhow!("Missing or unknown payload in request"))
        })?;

        let sequencer = self.sequencer(&bus_id).await?;

        // Get the space and environment, releasing the borrow before await
        let (space, environment) = {
            let state = self.state.borrow();
            (state.space.clone(), state.environment.clone())
        };

        let rt_timestamp_ms = environment.with_clock(|c| c.unsafe_wall_time().as_millis() as i64);

        // Try to write at position; if slot is taken, advance and retry.
        // This handles:
        // 1. Crash recovery: on restart, we discover the actual tail
        // 2. Contention: when multiple instances share the same WriteOnceSpace,
        //    they race to claim slots and retry on conflict
        loop {
            let claim = sequencer
                .claim(None)
                .await
                .expect("a claim without a bound claims a position");
            let position = claim.position();
            let entry = serialize_entry(&payload, position as i64, rt_timestamp_ms);
            let error = match self.write_claimed(&bus_id, claim, entry).await {
                (Ok(()), _) => {
                    // Polls stop at the first unwritten position.
                    if let Err(error) = self.fill_unwritten(&sequencer, &bus_id, position).await {
                        let error = AgentBusError::Timeout(
                            anyhow::Error::new(error)
                                .context("positions before the appended entry are unwritten"),
                        );
                        error!(
                            agent_bus_id = bus_id,
                            position = position,
                            error = %format!("{error:#}"),
                            "Positions before the appended entry are unwritten"
                        );
                        return Err(error);
                    }
                    debug!(
                        agent_bus_id = bus_id,
                        position = position,
                        rt_timestamp_ms = rt_timestamp_ms,
                        "Intention added to AgentBus"
                    );

                    return Ok(AppendResponse {
                        log_position: position as i64,
                    });
                }
                // The position holds either this entry, from an earlier write that
                // landed late, or another instance's.
                (Err(WriteOnceError::AddressAlreadyExists(address)), true) => {
                    AgentBusError::Timeout(
                        anyhow::Error::new(WriteOnceError::AmbiguousWrite(address))
                            .context("WriteOnceSpace write outcome unknown"),
                    )
                }
                (Err(WriteOnceError::AddressAlreadyExists(_)), false) => {
                    let new_position = space
                        .tail(&prefixed_space_id(&bus_id), Q::TAIL_WINDOW_SIZE)
                        .await
                        .map_err(tail_error_to_bus)?;
                    debug!(
                        "WriteOnceAgentBus: position {} taken, retrying at {}",
                        position, new_position
                    );
                    sequencer.advance(new_position);
                    continue;
                }
                (Err(error @ WriteOnceError::Timeout(_)), _) => AgentBusError::Timeout(
                    anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                ),
                // The entry may or may not be at `position`; the append's outcome is unknown.
                (Err(error @ WriteOnceError::AmbiguousWrite(_)), _) => AgentBusError::Timeout(
                    anyhow::Error::new(error).context("WriteOnceSpace write outcome unknown"),
                ),
                (Err(error @ WriteOnceError::BackendUnavailable(_)), _) => {
                    AgentBusError::Unavailable(
                        anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                    )
                }
                (Err(error @ WriteOnceError::InternalError(_)), _) => AgentBusError::Internal(
                    anyhow::Error::new(error).context("WriteOnceSpace write failed"),
                ),
            };

            error!(
                agent_bus_id = bus_id,
                error = %format!("{error:#}"),
                "WriteOnceSpace write failed"
            );
            return Err(error);
        }
    }

    async fn poll(&self, request: PollRequest) -> BusResult<PollResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        let bus_id = bus_id.agent_bus_id.clone();
        let max_entries = (request.max_entries as usize).min(MAX_POLL_ENTRIES);
        let start_position = request.start_log_position.max(0) as u64;

        let payload_types = request.filter.as_ref().map(|f| f.payload_types.clone());

        // If filter is set to Some(vec![]), return no entries
        if let Some(ref filter) = payload_types {
            if filter.is_empty() {
                return Ok(PollResponse {
                    entries: vec![],
                    complete: true,
                });
            }
        }

        // Use tail() to find the first unwritten position
        let space = { self.state.borrow().space.clone() };
        let end_position = space
            .tail(&prefixed_space_id(&bus_id), Q::TAIL_WINDOW_SIZE)
            .await
            .map_err(tail_error_to_bus)?;

        if end_position == 0 {
            return Ok(PollResponse {
                entries: vec![],
                complete: true,
            });
        }

        let mut entries = Vec::new();
        let mut complete = true;
        let mut pos = start_position;

        while pos < end_position {
            let serialized = space
                .read(&prefixed_space_id(&bus_id), pos)
                .await
                .map_err(read_error_to_bus)?
                .ok_or_else(|| {
                    AgentBusError::PositionNotFound(anyhow::anyhow!("position {pos} not found"))
                })?;

            if let Some(stored_entry) = deserialize_entry(&serialized) {
                // Verify the stored log_position matches the slot we read from
                if let Some(ref header) = stored_entry.header {
                    if header.log_position != pos as i64 {
                        error!(
                            agent_bus_id = bus_id,
                            expected_position = pos,
                            stored_position = header.log_position,
                            "Stored log_position does not match read position"
                        );
                    }
                }

                if let Some(ref payload) = stored_entry.payload {
                    // Apply filter if present
                    if !payload_matches_filter(payload, &payload_types) {
                        pos += 1;
                        continue;
                    }
                } else {
                    pos += 1;
                    continue;
                }

                if entries.len() >= max_entries {
                    complete = false;
                    break;
                }

                entries.push(stored_entry);
            }
            pos += 1;
        }

        Ok(PollResponse { entries, complete })
    }

    async fn read_next(&self, request: ReadNextRequest) -> BusResult<ReadNextResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        let bus_id = bus_id.agent_bus_id.as_str();
        if request.max_entries <= 0 {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "max_entries must be > 0"
            )));
        }
        if request.start_log_position < 0 {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "start_log_position must be >= 0"
            )));
        }
        if request.end_log_position < request.start_log_position {
            return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                "end_log_position {} is before start_log_position {}",
                request.end_log_position,
                request.start_log_position
            )));
        }
        if let Some(ref f) = request.filter {
            if f.payload_types.is_empty() {
                return Err(AgentBusError::InvalidArgument(anyhow::anyhow!(
                    "filter.payload_types must not be empty; omit filter entirely for no filtering"
                )));
            }
        }

        let payload_types = request.filter.as_ref().map(|f| f.payload_types.clone());

        let (entries, next_start) = self
            .read_filtered_entries(
                bus_id,
                request.start_log_position as u64,
                request.max_entries as usize,
                &payload_types,
                request.end_log_position as u64,
            )
            .await?;
        Ok(ReadNextResponse {
            entries,
            next_start_position: next_start as i64,
        })
    }

    async fn check_tail(&self, request: CheckTailRequest) -> BusResult<CheckTailResponse> {
        let bus_id = validate_bus_id(request.bus_id.as_ref())?;
        let bus_id = bus_id.agent_bus_id.as_str();
        let space = { self.state.borrow().space.clone() };
        let tail_position = space
            .tail(&prefixed_space_id(bus_id), Q::TAIL_WINDOW_SIZE)
            .await
            .map_err(tail_error_to_bus)?;
        let sequencer = self.state.borrow_mut().sequencers.peek(bus_id).cloned();
        if let Some(sequencer) = sequencer {
            sequencer.advance(tail_position);
        }
        Ok(CheckTailResponse {
            tail_position: tail_position as i64,
        })
    }

    async fn blocking_poll(&self, request: BlockingPollRequest) -> BusResult<BlockingPollResponse> {
        let environment = { self.state.borrow().environment.clone() };
        let interval = { self.state.borrow().blocking_poll_interval };
        agentbus_api::helpers::blocking_poll_default(
            self,
            &*environment,
            &request,
            Some(interval),
            None,
        )
        .await
    }
}

impl<W: WriteOnceSpace + Clone + 'static, E: Environment + 'static, Q: Sequencer<E>>
    WriteOnceAgentBusV2<W, E, Q>
{
    async fn sequencer(&self, bus_id: &str) -> BusResult<Rc<Q>> {
        let (cached, space) = {
            let mut state = self.state.borrow_mut();
            (state.sequencers.get(bus_id).cloned(), state.space.clone())
        };
        if let Some(sequencer) = cached {
            return Ok(sequencer);
        }
        let tail = space
            .tail(&prefixed_space_id(bus_id), Q::TAIL_WINDOW_SIZE)
            .await
            .map_err(tail_error_to_bus)?;
        let environment = self.state.borrow().environment.clone();
        Ok(self
            .state
            .borrow_mut()
            .sequencers
            .get_or_insert(bus_id.to_string(), || Rc::new(Q::new(tail, environment)))
            .clone())
    }

    /// Writes `entry` at the claimed position, and while its writes fail, since a
    /// failed write may still land, writes it again up to `MAX_REWRITES` times.
    /// Returns the last write's result, and whether it was a rewrite.
    async fn write_claimed(
        &self,
        bus_id: &str,
        claim: Q::Claim<'_>,
        entry: Bytes,
    ) -> (WriteOnceResult<()>, bool) {
        let mut space = self.state.borrow().space.clone();
        let space_id = prefixed_space_id(bus_id);
        let mut rewrites = 0;
        let result = loop {
            let result = space
                .write(&space_id, claim.position(), entry.clone())
                .await;
            if written(&result) || rewrites == MAX_REWRITES {
                break result;
            }
            rewrites += 1;
        };
        claim.set(written(&result));
        (result, rewrites > 0)
    }

    /// Waits until every position before `end` is written, filling with junk each
    /// one its claim left unwritten, and gives up when a junk write still fails
    /// after its rewrites.
    async fn fill_unwritten(&self, sequencer: &Q, bus_id: &str, end: u64) -> WriteOnceResult<()> {
        let environment = self.state.borrow().environment.clone();
        let rt_timestamp_ms = environment.with_clock(|c| c.unsafe_wall_time().as_millis() as i64);
        while let Some(claim) = sequencer.claim(Some(end)).await {
            let junk = serialize_junk(claim.position() as i64, rt_timestamp_ms);
            // Any entry fills the position, whoever wrote it.
            let (result, _) = self.write_claimed(bus_id, claim, junk).await;
            if !written(&result) {
                return result;
            }
        }
        Ok(())
    }

    /// Read filtered entries from [start_position, end_position) in the write-once space.
    /// Returns (entries, next_start_position) where next_start_position is the position
    /// after the last returned entry, or end_position if the scan completed without
    /// hitting max_entries.
    async fn read_filtered_entries(
        &self,
        agent_bus_id: &str,
        start_position: u64,
        max_entries: usize,
        payload_types: &Option<Vec<i32>>,
        end_position: u64,
    ) -> BusResult<(Vec<BusEntry>, u64)> {
        let max_entries = max_entries.min(MAX_POLL_ENTRIES);

        if start_position > end_position {
            return Err(AgentBusError::Internal(anyhow::anyhow!(
                "start_position {} is beyond end_position {}",
                start_position,
                end_position
            )));
        }
        if start_position == end_position {
            return Ok((vec![], end_position));
        }

        let space = { self.state.borrow().space.clone() };
        let mut entries = Vec::new();
        let mut pos = start_position;

        while pos < end_position {
            let serialized = space
                .read(&prefixed_space_id(agent_bus_id), pos)
                .await
                .map_err(read_error_to_bus)?
                .ok_or_else(|| {
                    AgentBusError::PositionNotFound(anyhow::anyhow!("position {pos} not found"))
                })?;

            if let Some(stored_entry) = deserialize_entry(&serialized) {
                if let Some(ref header) = stored_entry.header {
                    if header.log_position != pos as i64 {
                        error!(
                            agent_bus_id = agent_bus_id,
                            expected_position = pos,
                            stored_position = header.log_position,
                            "Stored log_position does not match read position"
                        );
                    }
                }

                if let Some(ref payload) = stored_entry.payload {
                    if !payload_matches_filter(payload, payload_types) {
                        pos += 1;
                        continue;
                    }
                } else {
                    pos += 1;
                    continue;
                }

                entries.push(stored_entry);
                if entries.len() >= max_entries {
                    return Ok((entries, pos + 1));
                }
            }
            pos += 1;
        }

        Ok((entries, end_position))
    }
}

/// Whether the address is known to be written after a write with this result.
/// After a failed write it is left empty, whether or not the write may still
/// land, so that the next write to it finds out.
fn written(result: &WriteOnceResult<()>) -> bool {
    match result {
        Ok(()) | Err(WriteOnceError::AddressAlreadyExists(_)) => true,
        Err(
            WriteOnceError::AmbiguousWrite(_)
            | WriteOnceError::Timeout(_)
            | WriteOnceError::BackendUnavailable(_)
            | WriteOnceError::InternalError(_),
        ) => false,
    }
}

fn serialize_entry(payload: &Payload, log_position: i64, rt_timestamp_ms: i64) -> Bytes {
    let entry = BusEntry {
        header: Some(Header {
            log_position,
            rt_timestamp_ms,
        }),
        payload: Some(payload.clone()),
    };
    Bytes::from(ProstMessage::encode_to_vec(&entry))
}

/// Junk, which readers skip, filling a position whose append failed in place of
/// its entry.
fn serialize_junk(log_position: i64, rt_timestamp_ms: i64) -> Bytes {
    let entry = BusEntry {
        header: Some(Header {
            log_position,
            rt_timestamp_ms,
        }),
        payload: Some(Payload {
            payload: Some(payload::Payload::Junk(Junk {})),
        }),
    };
    Bytes::from(ProstMessage::encode_to_vec(&entry))
}

fn deserialize_entry(bytes: &Bytes) -> Option<BusEntry> {
    <BusEntry as ProstMessage>::decode(&bytes[..]).ok()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashSet;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use agentbus_api::AgentBus;
    use agentbus_api::RealEnvironment;
    use agentbus_api::TailResult;
    use agentbus_api::TailableSpace;
    use agentbus_api::WriteOnceResult;

    use super::*;
    use crate::InMemoryWriteOnceSpace;
    use crate::windowed_sequencer::WindowedSequencer;

    /// Fails a write with `AmbiguousWrite` while another write to the same
    /// address is in flight, as ZippyDB reports a transaction conflict.
    #[derive(Clone, Default)]
    struct OverlapConflictSpace {
        inner: InMemoryWriteOnceSpace,
        in_flight: Rc<RefCell<HashSet<u64>>>,
    }

    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    impl TailableSpace for OverlapConflictSpace {
        async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
            self.inner.tail(space_id, window_size).await
        }
    }

    impl WriteOnceSpace for OverlapConflictSpace {
        async fn write(
            &mut self,
            space_id: &str,
            address: u64,
            value: Bytes,
        ) -> WriteOnceResult<()> {
            if !self.in_flight.borrow_mut().insert(address) {
                return Err(WriteOnceError::AmbiguousWrite(address));
            }
            YieldOnce(false).await;
            let result = self.inner.write(space_id, address, value).await;
            self.in_flight.borrow_mut().remove(&address);
            result
        }

        async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
            self.inner.read(space_id, address).await
        }
    }

    // The sequencer's loop is a local task, which needs a `LocalSet`.
    #[tokio::test]
    async fn concurrent_appends_to_one_bus_take_distinct_positions() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let bus: WriteOnceAgentBusV2<_, _, WindowedSequencer> = WriteOnceAgentBusV2::new(
                    OverlapConflictSpace::default(),
                    Rc::new(RealEnvironment::new()),
                    None,
                );
                let append = || {
                    bus.append(AppendRequest {
                        bus_id: Some(BusId {
                            agent_bus_id: "bus".to_owned(),
                        }),
                        payload: Some(Payload::default()),
                    })
                };
                let (first, second) = futures::join!(append(), append());
                let mut positions = [
                    first.expect("first append").log_position,
                    second.expect("second append").log_position,
                ];
                positions.sort();
                assert_eq!(positions, [0, 1]);
            })
            .await;
    }

    /// Fails the first `failures` writes to address 0 with `BackendUnavailable`.
    #[derive(Clone)]
    struct UnavailableAtZeroSpace {
        inner: InMemoryWriteOnceSpace,
        failures: Rc<Cell<usize>>,
    }

    impl TailableSpace for UnavailableAtZeroSpace {
        async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
            self.inner.tail(space_id, window_size).await
        }
    }

    impl WriteOnceSpace for UnavailableAtZeroSpace {
        async fn write(
            &mut self,
            space_id: &str,
            address: u64,
            value: Bytes,
        ) -> WriteOnceResult<()> {
            YieldOnce(false).await;
            if address == 0 && self.failures.get() > 0 {
                self.failures.set(self.failures.get() - 1);
                return Err(WriteOnceError::BackendUnavailable(anyhow::anyhow!(
                    "unavailable"
                )));
            }
            self.inner.write(space_id, address, value).await
        }

        async fn read(&self, space_id: &str, address: u64) -> WriteOnceResult<Option<Bytes>> {
            self.inner.read(space_id, address).await
        }
    }

    #[tokio::test]
    async fn failed_append_below_a_written_one_is_filled_with_junk() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let bus: WriteOnceAgentBusV2<_, _, WindowedSequencer> = WriteOnceAgentBusV2::new(
                    UnavailableAtZeroSpace {
                        inner: InMemoryWriteOnceSpace::default(),
                        failures: Rc::new(Cell::new(MAX_REWRITES + 1)),
                    },
                    Rc::new(RealEnvironment::new()),
                    None,
                );
                let bus_id = || {
                    Some(BusId {
                        agent_bus_id: "bus".to_owned(),
                    })
                };
                let append = || {
                    bus.append(AppendRequest {
                        bus_id: bus_id(),
                        payload: Some(Payload::default()),
                    })
                };
                let (first, second) = futures::join!(append(), append());
                assert!(
                    matches!(first, Err(AgentBusError::Unavailable(_))),
                    "every write of the first append fails: {first:?}"
                );
                assert_eq!(second.expect("second append").log_position, 1);

                let poll = bus
                    .poll(PollRequest {
                        bus_id: bus_id(),
                        start_log_position: 0,
                        max_entries: 10,
                        filter: None,
                    })
                    .await
                    .expect("poll");
                let positions: Vec<_> = poll
                    .entries
                    .iter()
                    .map(|entry| entry.header.as_ref().expect("header").log_position)
                    .collect();
                assert_eq!(
                    positions,
                    [1],
                    "junk fills position 0 in place of the failed entry"
                );
            })
            .await;
    }
}
