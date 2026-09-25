/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

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
use logact_commit_service_sqlite_storage::SqliteStorage;
use logact_commit_service_v1::CommitServiceV1;
use logact_commit_service_v1::DelegatingVoterFactory;
use logact_commit_service_v1::LlmVoterFactory;
use logact_commit_service_v1::RuleBasedVoterFactory;
use logact_commit_service_v1::StaticConfigPolicyProvider;
use logact_private_fs::ensure_private_parent_directory;

/// Create an in-process AgentBus and CommitService over one SQLite database.
pub(crate) async fn create_sqlite_backed_commit_service(
    path: impl AsRef<Path>,
) -> Result<ChanneledCommitService<ChanneledAgentBus>> {
    let path = path.as_ref().to_path_buf();
    ensure_private_parent_directory(&path).await?;
    tokio::task::spawn_blocking(move || create_sqlite_backed_commit_service_blocking(path))
        .await
        .context("failed to join LogAct database initialization")?
}

fn create_sqlite_backed_commit_service_blocking(
    path: PathBuf,
) -> Result<ChanneledCommitService<ChanneledAgentBus>> {
    let path_string = path
        .to_str()
        .context("SQLite path must be valid UTF-8")?
        .to_owned();

    // The verified 0700 parent protects the database and every SQLite
    // journal/WAL sidecar from the moment each file is created.
    let storage =
        SqliteStorage::open(&path).context("failed to initialize CommitService storage")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .context("failed to restrict LogAct database permissions")?;
    let bus = CoreAgentBusFactory::new(
        None,
        Box::new(|| Ok(Rc::new(NoopMetrics) as Rc<dyn AgentBusMetrics>)),
        Box::new(|| Ok(Rc::new(NoopLogger) as Rc<dyn AgentbusLogger>)),
    )
    .build(ParsedBackend::Sqlite(path_string))
    .context("failed to initialize AgentBus storage")?;

    ChanneledCommitService::try_new_in_process(bus, move |bus| {
        let environment = Rc::new(RealEnvironment::new());
        let storage = Rc::new(storage);
        let observability = Observability {
            metrics: Rc::new(NoopMetrics),
            logger: Rc::new(NoopLogger),
            environment: environment.clone(),
        };
        let voter_factory = DelegatingVoterFactory::new(
            LlmVoterFactory::new(storage.clone(), None, None, None),
            RuleBasedVoterFactory::new(storage.clone()),
            observability,
        );
        let decider_factory = DeciderFactoryImpl::new(storage.clone());

        CommitServiceV1::new(bus, move |engine_bus| {
            BaseEngine::new(
                engine_bus,
                storage,
                voter_factory,
                StaticConfigPolicyProvider::default(),
                decider_factory,
                environment,
            )
        })
    })
    .context("failed to start CommitService worker")
}
