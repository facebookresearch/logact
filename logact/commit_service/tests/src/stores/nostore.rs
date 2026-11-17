/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::VoterConfig;
use agent_bus_proto_rust::agent_bus::payload;
use agentbus_api::AgentBus;
use logact_commit_service_api::CommitResult;
use logact_commit_service_replay::Checkpoint;
use logact_commit_service_replay::CheckpointStore;
use logact_commit_service_replay::ReplayState;
use logact_commit_service_replay::store_support::LogPlayer;
use logact_commit_service_replay::store_support::apply_error;
use logact_commit_service_replay::store_support::decided_intention;
use logact_commit_service_replay::store_support::entry_position;

/// Validates a voter config from a logged `PolicyBatch`, as V1's policy playback
/// does.
pub type VoterConfigValidator = Rc<dyn Fn(Option<&VoterConfig>) -> anyhow::Result<VoterConfig>>;

/// Checkpoints without storage: every load materializes the bus from position 0,
/// and saving does nothing.
#[derive(Clone)]
pub struct NoStoreCheckpoints<T> {
    bus: T,
    validate_config: VoterConfigValidator,
}

impl<T> NoStoreCheckpoints<T> {
    pub fn new(bus: T, validate_config: VoterConfigValidator) -> Self {
        Self {
            bus,
            validate_config,
        }
    }
}

impl<T: AgentBus> CheckpointStore for NoStoreCheckpoints<T> {
    /// The bus materialized up to its first intention with no logged decision.
    /// Everything before it is settled; the drive replays the rest and appends
    /// whatever outputs it calls for.
    async fn load(&self, bus_id: &str) -> CommitResult<Option<Checkpoint>> {
        let mut player = LogPlayer::new(&self.bus, bus_id, 0);
        let mut entries = Vec::new();
        loop {
            let batch = player.next_batch().await?;
            if batch.is_empty() {
                break;
            }
            entries.extend(batch);
        }
        // Pass 1: the intentions with a logged decision.
        let decided: HashSet<i64> = entries
            .iter()
            .filter_map(|entry| entry.payload.as_ref()?.payload.as_ref())
            .filter_map(decided_intention)
            .collect();
        // Pass 2: materialize up to the first intention without one.
        let mut state = ReplayState::default();
        let mut position = None;
        for entry in &entries {
            let entry_position = entry_position(entry)?;
            if let Some(payload) = entry.payload.as_ref() {
                if matches!(payload.payload, Some(payload::Payload::Intention(_)))
                    && !decided.contains(&entry_position)
                {
                    break;
                }
                state
                    .materialize(entry, &*self.validate_config)
                    .map_err(apply_error)?;
            }
            position = Some(entry_position);
        }
        Ok(position.map(|position| Checkpoint { state, position }))
    }

    async fn save(&self, _bus_id: &str, _checkpoint: &Checkpoint) -> CommitResult<()> {
        Ok(())
    }
}
