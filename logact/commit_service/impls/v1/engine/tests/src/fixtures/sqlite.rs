/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_api::environment::RealEnvironment;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use logact_commit_service_sqlite_storage::SqliteStorage;
use tempfile::TempDir;

pub struct SqliteFileStorageFixture {
    storage: Rc<SqliteStorage>,
    env: Rc<RealEnvironment>,
    _dir: TempDir,
}

impl ConformanceFixture for SqliteFileStorageFixture {
    type Env = RealEnvironment;
    type Impl = Rc<SqliteStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.storage.clone()
    }
}

impl IntegrationFixture for SqliteFileStorageFixture {
    async fn new_async(_fb: FacebookInit) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let storage = Rc::new(SqliteStorage::open(dir.path().join("commit-service.db"))?);
        Ok(Self {
            storage,
            env: Rc::new(RealEnvironment::new()),
            _dir: dir,
        })
    }
}
