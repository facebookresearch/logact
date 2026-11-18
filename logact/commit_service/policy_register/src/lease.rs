/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A [`PolicyRegister`] with lease-cached reads that drains readers before
//! publishing a policy, using the [`agentbus_lease`] protocol.

use std::rc::Rc;
use std::time::Duration;

use agent_bus_proto_rust::agent_bus::LeasingMetadata;
use agentbus_api::Environment;
use agentbus_lease::CasCell;
pub use agentbus_lease::LeaseConfig as PolicyLeaseConfig;
use agentbus_lease::LeaseMetadata;
use agentbus_lease::Leased;
use agentbus_lease::LeasedCell;
use agentbus_lease::StoredValue;
use logact_commit_service_api::Policy;
use logact_commit_service_api::PolicyState;
use logact_commit_service_api::VersionedPolicyState;

use crate::PolicyRegister;
use crate::PolicyRegisterError;
use crate::PolicyRegisterResult;

/// Timing bounds for commit-service policy registers. Every reader and every
/// writer of a register must use the same bounds.
pub const DEFAULT_POLICY_LEASE_CONFIG: PolicyLeaseConfig = PolicyLeaseConfig {
    lease_duration: Duration::from_secs(5 * 60),
    drift_allowance: Duration::from_secs(30),
    recovery_grace: Duration::from_secs(10),
};

/// A [`PolicyRegister`] that adds the [`agentbus_lease`] protocol to another
/// register.
///
/// `read_validated` serves stable policies from a lease cached until a
/// deadline; clones share one cache. Writes drain leases on the old
/// policy before publishing. Lease metadata is stored in `PolicyState::leasing`
/// and removed from returned states. A value without lease metadata was
/// written without the protocol and is read without caching, so readers can
/// adopt the protocol while other writers remain; the first write through the
/// protocol enables caching. After that, every writer of the inner register
/// must use this protocol with the same timing bounds.
///
/// Versions are the inner register's versions of published policies.
/// Reservations and recoveries rewrite the inner register without changing the
/// version reported to callers.
pub struct LeaseBasedPolicyRegister<R: PolicyRegister, E> {
    leased: Rc<LeasedCell<RegisterCell<R>, E>>,
    key: String,
}

impl<R: PolicyRegister, E> Clone for LeaseBasedPolicyRegister<R, E> {
    fn clone(&self) -> Self {
        Self {
            leased: self.leased.clone(),
            key: self.key.clone(),
        }
    }
}

impl<R: PolicyRegister, E: Environment> LeaseBasedPolicyRegister<R, E> {
    /// Wrap `register` using the lease timing bounds agreed by all participants.
    pub fn new(register: R, environment: Rc<E>, config: PolicyLeaseConfig) -> Self {
        Self {
            key: register.key().to_string(),
            leased: Rc::new(LeasedCell::new(
                RegisterCell { register },
                environment,
                config,
            )),
        }
    }
}

impl<R: PolicyRegister, E: Environment> PolicyRegister for LeaseBasedPolicyRegister<R, E> {
    fn key(&self) -> &str {
        &self.key
    }

    async fn read_validated(&self) -> PolicyRegisterResult<VersionedPolicyState> {
        let Leased { payload, version } =
            self.leased
                .read()
                .await?
                .ok_or_else(|| PolicyRegisterError::Uninitialized {
                    key: self.key.clone(),
                })?;
        Ok(VersionedPolicyState {
            state: payload.into(),
            version,
        })
    }

    /// Conditionally publish a policy after draining leases on the old one.
    /// `None` initializes an absent register without a drain. A writer that
    /// finds its expected version reserved waits for the reservation to be
    /// published or abandoned. Any `leasing` in `state` is ignored.
    async fn set_policy(
        &self,
        state: &PolicyState,
        expected_version: Option<i64>,
    ) -> PolicyRegisterResult<i64> {
        let payload = Policy::try_from(state)?;
        self.leased
            .write(payload, expected_version)
            .await?
            .ok_or_else(|| PolicyRegisterError::ConcurrentUpdate {
                key: self.key.clone(),
            })
    }
}

/// A [`CasCell`] view of a policy register. The payload is the stored policy
/// as a [`Policy`], and its metadata is stored in `PolicyState::leasing` with
/// big-endian visible versions.
struct RegisterCell<R> {
    register: R,
}

impl<R: PolicyRegister> CasCell for RegisterCell<R> {
    type Payload = Policy;
    type Version = i64;
    type Error = PolicyRegisterError;

    async fn read(&self) -> PolicyRegisterResult<Option<StoredValue<Policy, i64>>> {
        let current = match self.register.read_validated().await {
            Ok(current) => current,
            Err(PolicyRegisterError::Uninitialized { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some(StoredValue {
            payload: Policy::try_from(&current.state)?,
            leasing: current.state.leasing.map(decode_leasing).transpose()?,
            version: current.version,
        }))
    }

    async fn write(
        &self,
        payload: Policy,
        leasing: LeaseMetadata<i64>,
        expected: Option<i64>,
    ) -> PolicyRegisterResult<Option<i64>> {
        let state = PolicyState {
            leasing: Some(LeasingMetadata {
                revoked: leasing.revoked,
                visible_version: leasing
                    .visible_version
                    .map(|version| version.to_be_bytes().to_vec()),
                lease_duration_ms: u64::try_from(leasing.lease_duration.as_millis())
                    .unwrap_or(u64::MAX),
            }),
            ..payload.into()
        };
        match self.register.set_policy(&state, expected).await {
            Ok(version) => Ok(Some(version)),
            Err(PolicyRegisterError::ConcurrentUpdate { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn decode_leasing(leasing: LeasingMetadata) -> PolicyRegisterResult<LeaseMetadata<i64>> {
    let visible_version = leasing
        .visible_version
        .map(|bytes| {
            Ok::<_, PolicyRegisterError>(i64::from_be_bytes(bytes.try_into().map_err(|_| {
                PolicyRegisterError::MalformedPolicy {
                    message: "invalid lease visible version".to_string(),
                }
            })?))
        })
        .transpose()?;
    Ok(LeaseMetadata {
        revoked: leasing.revoked,
        visible_version,
        lease_duration: Duration::from_millis(leasing.lease_duration_ms),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leasing_metadata_round_trips_and_rejects_invalid_versions() {
        let leasing = LeasingMetadata {
            revoked: true,
            visible_version: Some(3i64.to_be_bytes().to_vec()),
            lease_duration_ms: 1000,
        };
        let decoded = decode_leasing(leasing).expect("valid metadata");
        assert_eq!(decoded.visible_version, Some(3));
        assert_eq!(decoded.lease_duration, Duration::from_millis(1000));
        assert!(matches!(
            decode_leasing(LeasingMetadata {
                visible_version: Some(vec![0xff]),
                ..Default::default()
            }),
            Err(PolicyRegisterError::MalformedPolicy { .. })
        ));
    }
}
