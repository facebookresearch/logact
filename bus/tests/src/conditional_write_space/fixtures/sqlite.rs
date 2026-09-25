/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Sqlite-backed test fixture for ConditionalWriteSpace

use std::rc::Rc;

use agentbus_api::environment::RealEnvironment;
use agentbus_sqlite::SqliteConditionalWriteSpace;
use agentbus_sqlite::SqliteDb;
use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use tempfile::TempDir;

pub struct SqliteConditionalWriteSpaceFixture {
    space: SqliteConditionalWriteSpace,
    env: Rc<RealEnvironment>,
    _dir: TempDir,
}

impl ConformanceFixture for SqliteConditionalWriteSpaceFixture {
    type Env = RealEnvironment;
    type Impl = SqliteConditionalWriteSpace;

    fn get_env(&self) -> Rc<Self::Env> {
        self.env.clone()
    }

    fn create_impl(&self) -> Self::Impl {
        self.space.clone()
    }
}

impl IntegrationFixture for SqliteConditionalWriteSpaceFixture {
    async fn new_async(_fb: FacebookInit) -> Result<Self> {
        let dir = TempDir::new()?;
        let path = dir.path().join("agentbus.db");
        let db = SqliteDb::open(&path)?;
        Ok(Self {
            space: SqliteConditionalWriteSpace::new(&db)?,
            env: Rc::new(RealEnvironment::new()),
            _dir: dir,
        })
    }
}
