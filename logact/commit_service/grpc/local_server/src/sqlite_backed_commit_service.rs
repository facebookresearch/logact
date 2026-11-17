/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::rc::Rc;

use agent_bus_proto_rust::agent_bus::DeciderPolicy;
use agentbus_api::AgentBusMetrics;
use agentbus_api::AgentbusLogger;
use agentbus_api::NoopLogger;
use agentbus_api::NoopMetrics;
use agentbus_api::RealEnvironment;
use agentbus_core::ChanneledAgentBus;
use agentbus_core::server_lib::ParsedBackend;
use agentbus_factory::ChanneledAgentBusFactory as _;
use agentbus_factory::CoreAgentBusFactory;
use anyhow::Context as _;
use anyhow::Result;
use logact_commit_service_core::ChanneledCommitService;
use logact_commit_service_engine::BaseEngine;
use logact_commit_service_engine::DeciderFactoryImpl;
use logact_commit_service_engine::Observability;
use logact_commit_service_engine::PolicyState;
use logact_commit_service_engine::validate_voter_configs;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_policy_register::SynchronousRegisterProvider;
use logact_commit_service_policy_register::bootstrap_policy_register_if_absent;
use logact_commit_service_policy_register::default_policy_register_bootstrap_retry_config;
use logact_commit_service_sqlite_storage::SqliteStorage;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::LlmVoterFactory;
use logact_commit_service_v1::RuleBasedVoterFactory;
use logact_private_fs::ensure_private_parent_directory;

pub(crate) const POLICY_REGISTER_KEY: &str = "policy";

/// Create AgentBus and CommitService over one SQLite database on `environment`.
pub(crate) async fn create_sqlite_backed_commit_service(
    path: impl AsRef<Path>,
    environment: Rc<RealEnvironment>,
) -> Result<ChanneledCommitService<ChanneledAgentBus>> {
    let path = path.as_ref();
    ensure_private_parent_directory(path).await?;
    initialize_policy_register(path, &environment).await?;
    build_sqlite_backed_commit_service(path, environment)
}

async fn initialize_policy_register(path: &Path, environment: &RealEnvironment) -> Result<()> {
    let storage =
        Rc::new(SqliteStorage::open(path).context("failed to initialize policy storage")?);
    let register = StoragePolicyRegister::new(storage, POLICY_REGISTER_KEY);
    bootstrap_policy_register_if_absent(
        &register,
        &PolicyState {
            decider_policy: Some(DeciderPolicy::OnByDefault as i32),
            ..Default::default()
        },
        environment,
        default_policy_register_bootstrap_retry_config(),
    )
    .await
    .context("failed to initialize policy register")?;
    Ok(())
}

fn build_sqlite_backed_commit_service(
    path: &Path,
    environment: Rc<RealEnvironment>,
) -> Result<ChanneledCommitService<ChanneledAgentBus>> {
    let path_string = path
        .to_str()
        .context("SQLite path must be valid UTF-8")?
        .to_owned();

    // The verified 0700 parent protects the database and every SQLite
    // journal/WAL sidecar from the moment each file is created.
    let storage =
        SqliteStorage::open(path).context("failed to initialize CommitService storage")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .context("failed to restrict LogAct database permissions")?;
    let bus = CoreAgentBusFactory::new(
        None,
        Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
        Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
    )
    .build_on(environment.clone(), ParsedBackend::Sqlite(path_string))
    .context("failed to initialize AgentBus storage")?;

    Ok(ChanneledCommitService::new_on_environment(
        environment.clone(),
        bus,
        move |bus| {
            let storage = Rc::new(storage);
            let observability = Observability {
                metrics: Rc::new(NoopMetrics),
                logger: Rc::new(NoopLogger),
                environment: environment.clone(),
            };
            let voter_factory = DelegatingVoterFactory::new(
                LlmVoterFactory::default(),
                RuleBasedVoterFactory,
                observability,
            );
            let decider_factory = DeciderFactoryImpl::new(storage.clone());
            let policy_validator = voter_factory.clone();
            let policy_provider = SynchronousRegisterProvider::new(
                StoragePolicyRegister::new(storage.clone(), POLICY_REGISTER_KEY),
                move |policy| validate_voter_configs(&policy_validator, policy),
            );

            CommitServiceV1::new(bus, move |engine_bus| {
                BaseEngine::new(
                    engine_bus,
                    storage,
                    voter_factory,
                    policy_provider,
                    decider_factory,
                    environment,
                )
            })
        },
    ))
}
