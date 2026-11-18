/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic `PolicyRegister` conformance scenarios.
//!
//! Versions are compared only for equality and order, since implementations may
//! assign them differently.

#[conformance_macros::scenarios(policy_register_test_scenarios_list)]
mod defs {
    use std::cell::Cell;
    use std::pin::pin;
    use std::time::Duration;

    use agent_bus_proto_rust::agent_bus::DeciderPolicy;
    use agent_bus_proto_rust::agent_bus::VoterConfig;
    use agent_bus_proto_rust::agent_bus::voter_config;
    use agentbus_api::Environment;
    use anyhow::Result;
    use futures::future::Either;
    use futures::future::select;
    use logact_commit_service_api::PolicyState;
    use logact_commit_service_api::VersionedPolicyState;
    use logact_commit_service_policy_register::PolicyRegister;
    use logact_commit_service_policy_register::PolicyRegisterError;
    use prost_types::Any;

    use crate::fixtures::LeasedFixture;
    use crate::fixtures::PolicyRegisterTestFixture;

    fn policy(decider_policy: DeciderPolicy) -> PolicyState {
        PolicyState {
            decider_policy: Some(decider_policy as i32),
            ..Default::default()
        }
    }

    fn voter(type_url: &str) -> VoterConfig {
        VoterConfig {
            config: Some(voter_config::Config::Custom(Any {
                type_url: type_url.to_string(),
                value: Vec::new(),
            })),
        }
    }

    fn is_concurrent_update<T>(result: &Result<T, PolicyRegisterError>) -> bool {
        matches!(result, Err(PolicyRegisterError::ConcurrentUpdate { .. }))
    }

    #[scenario]
    pub async fn run_test_read_uninitialized<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        assert!(matches!(
            register.read_validated().await,
            Err(PolicyRegisterError::Uninitialized { .. })
        ));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_initialize_and_read<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let state = policy(DeciderPolicy::OnByDefault);
        let version = register.set_policy(&state, None).await?;

        let current = register.read_validated().await?;
        assert_eq!(current, VersionedPolicyState { state, version });
        Ok(())
    }

    #[scenario]
    pub async fn run_test_initialize_twice_is_rejected<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let state = policy(DeciderPolicy::OnByDefault);
        let version = register.set_policy(&state, None).await?;

        let result = register
            .set_policy(&policy(DeciderPolicy::OffByDefault), None)
            .await;
        assert!(is_concurrent_update(&result), "got {result:?}");
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_update_of_uninitialized_is_rejected<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let result = register
            .set_policy(&policy(DeciderPolicy::OnByDefault), Some(0))
            .await;
        assert!(is_concurrent_update(&result), "got {result:?}");
        assert!(matches!(
            register.read_validated().await,
            Err(PolicyRegisterError::Uninitialized { .. })
        ));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_update_advances_version<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let initial = register
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;
        let state = policy(DeciderPolicy::OffByDefault);
        let version = register.set_policy(&state, Some(initial)).await?;

        assert!(version > initial);
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_unchanged_policy_advances_version<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let state = policy(DeciderPolicy::OnByDefault);
        let initial = register.set_policy(&state, None).await?;
        let version = register.set_policy(&state, Some(initial)).await?;

        assert!(version > initial);
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_stale_update_is_rejected<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let initial = register
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;
        let state = policy(DeciderPolicy::OffByDefault);
        let version = register.set_policy(&state, Some(initial)).await?;

        let result = register
            .set_policy(&policy(DeciderPolicy::FirstBooleanWins), Some(initial))
            .await;
        assert!(is_concurrent_update(&result), "got {result:?}");
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }

    #[scenario]
    pub async fn run_test_invalid_policy_is_not_written<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let result = register.set_policy(&PolicyState::default(), None).await;
        assert!(
            matches!(result, Err(PolicyRegisterError::MalformedPolicy { .. })),
            "got {result:?}"
        );
        assert!(matches!(
            register.read_validated().await,
            Err(PolicyRegisterError::Uninitialized { .. })
        ));
        Ok(())
    }

    #[scenario]
    pub async fn run_test_update_preserves_voters<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let register = fixture.create_impl();
        let mut state = PolicyState {
            decider_policy: Some(DeciderPolicy::OnByDefault as i32),
            ..Default::default()
        };
        state.voters.insert("voter-a".to_string(), voter("test/a"));
        state.voters.insert("voter-b".to_string(), voter("test/b"));
        let initial = register.set_policy(&state, None).await?;

        let mut current = register.read_validated().await?;
        current.state.decider_policy = Some(DeciderPolicy::OffByDefault as i32);
        register.set_policy(&current.state, Some(initial)).await?;

        assert_eq!(register.read_validated().await?.state.voters, state.voters);
        Ok(())
    }

    #[scenario]
    pub async fn run_test_one_of_concurrent_writers_wins<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let first = fixture.create_impl();
        let second = fixture.create_impl();
        let initial = first
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;
        let first_state = policy(DeciderPolicy::OffByDefault);
        let second_state = policy(DeciderPolicy::FirstBooleanWins);

        let (first_result, second_result) = futures::join!(
            first.set_policy(&first_state, Some(initial)),
            second.set_policy(&second_state, Some(initial)),
        );

        let expected = match (first_result, second_result) {
            (Ok(version), loser) if is_concurrent_update(&loser) => VersionedPolicyState {
                state: first_state,
                version,
            },
            (loser, Ok(version)) if is_concurrent_update(&loser) => VersionedPolicyState {
                state: second_state,
                version,
            },
            results => anyhow::bail!("expected exactly one winner, got {results:?}"),
        };
        assert_eq!(first.read_validated().await?, expected);
        assert_eq!(second.read_validated().await?, expected);
        Ok(())
    }

    /// Reads concurrent with an update return the old or new policy with
    /// nondecreasing versions, and never the old policy after the new one.
    #[scenario(sim_only)]
    pub async fn run_test_reads_during_update_are_atomic<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let writer = fixture.create_impl();
        let reader = fixture.create_impl();
        let env = fixture.get_env();
        let old_state = policy(DeciderPolicy::OnByDefault);
        let old = VersionedPolicyState {
            version: writer.set_policy(&old_state, None).await?,
            state: old_state,
        };
        let new_state = policy(DeciderPolicy::OffByDefault);
        let written = Cell::new(false);

        let read_loop = async {
            let mut observed_new = false;
            let mut last_version = old.version;
            while !written.get() {
                let current = reader.read_validated().await?;
                assert!(current.version >= last_version);
                last_version = current.version;
                if current.state == new_state {
                    assert!(current.version > old.version);
                    observed_new = true;
                } else {
                    assert!(!observed_new, "read the old policy after the new one");
                    assert_eq!(current.state, old.state);
                }
                env.sleep(Duration::from_millis(1)).await;
            }
            anyhow::Ok(())
        };
        let write = async {
            let version = writer.set_policy(&new_state, Some(old.version)).await;
            written.set(true);
            version
        };
        let (read_result, version) = futures::join!(read_loop, write);
        read_result?;

        assert_eq!(
            reader.read_validated().await?,
            VersionedPolicyState {
                state: new_state,
                version: version?,
            }
        );
        Ok(())
    }

    /// A writer that stops mid-update, as if it crashed, must not prevent later
    /// writers from making progress.
    #[scenario(sim_only)]
    pub async fn run_test_crashed_writer_does_not_block_writers<F: PolicyRegisterTestFixture>(
        fixture: &F,
    ) -> Result<()> {
        let crashed = fixture.create_impl();
        let survivor = fixture.create_impl();
        let env = fixture.get_env();
        let initial = crashed
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;

        let abandoned = policy(DeciderPolicy::OffByDefault);
        let write = crashed.set_policy(&abandoned, Some(initial));
        let _ = select(pin!(write), pin!(env.sleep(Duration::from_millis(1)))).await;

        let state = policy(DeciderPolicy::FirstBooleanWins);
        let update = async {
            let current = survivor.read_validated().await?;
            anyhow::Ok(survivor.set_policy(&state, Some(current.version)).await?)
        };
        let version = match select(pin!(update), pin!(env.sleep(Duration::from_secs(1)))).await {
            Either::Left((version, _)) => version?,
            Either::Right(_) => anyhow::bail!("writer blocked by a crashed writer"),
        };
        assert_eq!(
            survivor.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }

    fn is_reserved(current: &VersionedPolicyState) -> bool {
        current
            .state
            .leasing
            .as_ref()
            .is_some_and(|leasing| leasing.revoked)
    }

    /// Reads roll back a crashed writer's reservation without changing
    /// the reported version, so the policy becomes cacheable again.
    #[scenario_for(
        logact_commit_service_policy_register_tests::fixtures::LeaseBasedPolicyRegisterFixture<
            logact_commit_service_policy_register_tests::fixtures::InMemoryPolicyRegisterFixture,
        >,
        suffix = lease_in_memory,
        sim_only
    )]
    pub async fn run_test_reader_rolls_back_crashed_writer<F>(fixture: &F) -> Result<()>
    where
        F: LeasedFixture<Impl: PolicyRegister>,
    {
        let crashed = fixture.create_impl();
        let reader = fixture.create_impl();
        let inner = fixture.create_inner();
        let env = fixture.get_env();
        let state = policy(DeciderPolicy::OnByDefault);
        let expected = VersionedPolicyState {
            version: crashed.set_policy(&state, None).await?,
            state,
        };
        let abandoned = policy(DeciderPolicy::OffByDefault);
        let write = crashed.set_policy(&abandoned, Some(expected.version));
        let _ = select(pin!(write), pin!(env.sleep(Duration::from_millis(1)))).await;
        let reserved = inner.read_validated().await?;
        assert!(is_reserved(&reserved));
        assert!(reserved.version > expected.version);
        for _ in 0..100 {
            assert_eq!(reader.read_validated().await?, expected);
            let stored = inner.read_validated().await?;
            if !is_reserved(&stored) {
                assert!(stored.version > reserved.version);
                assert_eq!(reader.read_validated().await?, expected);
                let state = policy(DeciderPolicy::FirstBooleanWins);
                let version = reader.set_policy(&state, Some(expected.version)).await?;
                assert!(version > stored.version);
                assert_eq!(
                    reader.read_validated().await?,
                    VersionedPolicyState { state, version }
                );
                return Ok(());
            }
            env.sleep(Duration::from_millis(10)).await;
        }
        anyhow::bail!("reads did not roll back the crashed writer's reservation")
    }

    /// A register written without the lease protocol is never cached, so readers
    /// can adopt the protocol while older writers remain. The first write
    /// through the protocol enables caching.
    #[scenario_for(
        logact_commit_service_policy_register_tests::fixtures::LeaseBasedPolicyRegisterFixture<
            logact_commit_service_policy_register_tests::fixtures::InMemoryPolicyRegisterFixture,
        >,
        suffix = lease_in_memory,
        sim_only
    )]
    pub async fn run_test_unleased_register_is_not_cached<F>(fixture: &F) -> Result<()>
    where
        F: LeasedFixture<Impl: PolicyRegister>,
    {
        let inner = fixture.create_inner();
        let reader = fixture.create_impl();
        let first = inner
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;
        reader.read_validated().await?;
        let second = inner
            .set_policy(&policy(DeciderPolicy::OffByDefault), Some(first))
            .await?;
        let current = reader.read_validated().await?;
        assert_eq!(
            current.state.decider_policy,
            Some(DeciderPolicy::OffByDefault as i32),
            "an unleased policy should not be cached"
        );
        assert_eq!(current.version, second);

        let published = fixture
            .create_impl()
            .set_policy(&policy(DeciderPolicy::FirstBooleanWins), Some(second))
            .await?;
        let current = reader.read_validated().await?;
        assert_eq!(
            current.state.decider_policy,
            Some(DeciderPolicy::FirstBooleanWins as i32)
        );
        assert_eq!(current.version, published);
        // A write that bypasses the protocol is hidden while the lease lasts.
        inner
            .set_policy(&policy(DeciderPolicy::OnByDefault), Some(published))
            .await?;
        assert_eq!(
            reader.read_validated().await?.version,
            published,
            "a protocol write should enable caching"
        );
        Ok(())
    }

    /// The lease protocol adopts a register written without it, continuing from
    /// its existing version.
    #[scenario_for(
        logact_commit_service_policy_register_tests::fixtures::LeaseBasedPolicyRegisterFixture<
            logact_commit_service_policy_register_tests::fixtures::InMemoryPolicyRegisterFixture,
        >,
        suffix = lease_in_memory,
        sim_only
    )]
    #[scenario_for(
        logact_commit_service_policy_register_tests::fixtures::LeaseBasedPolicyRegisterFixture<
            logact_commit_service_policy_register_tests::fixtures::SqlitePolicyRegisterFixture,
        >,
        suffix = lease_sqlite_file,
        int_only
    )]
    pub async fn run_test_adopts_existing_register<F>(fixture: &F) -> Result<()>
    where
        F: LeasedFixture<Impl: PolicyRegister>,
    {
        let inner = fixture.create_inner();
        let register = fixture.create_impl();
        let initial = inner
            .set_policy(&policy(DeciderPolicy::OnByDefault), None)
            .await?;
        let state = policy(DeciderPolicy::OffByDefault);
        let existing = inner.set_policy(&state, Some(initial)).await?;
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState {
                state,
                version: existing,
            }
        );

        let state = policy(DeciderPolicy::FirstBooleanWins);
        let version = register.set_policy(&state, Some(existing)).await?;
        assert!(version > existing);
        assert_eq!(version, inner.read_validated().await?.version);
        assert_eq!(
            register.read_validated().await?,
            VersionedPolicyState { state, version }
        );
        Ok(())
    }
}

pub use defs::*;
