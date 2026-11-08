/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::rc::Rc;

use agentbus_api::environment::RealEnvironment;
use anyhow::Result;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use logact_commit_service_policy_register::StoragePolicyRegister;
use logact_commit_service_sqlite_storage::SqliteStorage;
use tempfile::TempDir;

use crate::fixtures::ConformanceFixture;

pub struct SqlitePolicyRegisterFixture {
    env: Rc<RealEnvironment>,
    register: StoragePolicyRegister<SqliteStorage>,
    _dir: TempDir,
}

impl ConformanceFixture for SqlitePolicyRegisterFixture {
    type Env = RealEnvironment;
    type Impl = StoragePolicyRegister<SqliteStorage>;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.register.clone()
    }
}

impl IntegrationFixture for SqlitePolicyRegisterFixture {
    async fn new_async(_fb: FacebookInit) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let storage = Rc::new(SqliteStorage::open(dir.path().join("policy-register.db"))?);
        Ok(Self {
            env: Rc::new(RealEnvironment::new()),
            register: StoragePolicyRegister::new(storage, "test-policy"),
            _dir: dir,
        })
    }
}
