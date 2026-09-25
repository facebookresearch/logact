/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common test fixtures for TailableSpace implementations

use std::rc::Rc;

use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailableSpace;
use agentbus_api::WriteOnceSpace;
use anyhow::Result;
use bytes::Bytes;
use conformance::ConformanceFixture;
use fbinit::FacebookInit;

use crate::conditional_write_space::fixtures::ConditionalWriteSpaceTestFixture;
use crate::fixtures::IntegrationFixture;
use crate::fixtures::SimulatorFixture;
use crate::simulator::Simulator;
use crate::write_once_space::fixtures::WriteOnceSpaceTestFixture;

/// The `TailableSpace`-specific view of a fixture: a `ConformanceFixture` whose
/// `Impl` is a `TailableSpace`, plus a `write_at` helper so scenarios can write at
/// arbitrary addresses without coupling to a specific write API.
pub trait TailableSpaceTestFixture: ConformanceFixture<Impl: TailableSpace + 'static> {
    /// Write an arbitrary value at the given address.
    ///
    /// For write-once spaces this writes a fresh value (ignoring AddressAlreadyExists).
    /// For conditional-write spaces this reads the current version and overwrites.
    fn write_at(
        &self,
        space_id: &str,
        address: u64,
    ) -> impl std::future::Future<Output = Result<()>>;
}

/// Adapts a `WriteOnceSpaceTestFixture` into a `TailableSpaceTestFixture`.
pub struct WosTailableFixture<F: WriteOnceSpaceTestFixture> {
    inner: F,
}

impl<F: WriteOnceSpaceTestFixture> ConformanceFixture for WosTailableFixture<F> {
    type Env = F::Env;
    type Impl = F::Impl;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.inner.create_impl()
    }
}

impl<F: WriteOnceSpaceTestFixture> TailableSpaceTestFixture for WosTailableFixture<F> {
    async fn write_at(&self, space_id: &str, address: u64) -> Result<()> {
        let mut w = self.inner.create_impl();
        // Ignore AddressAlreadyExists — the tail test may call write_at on the
        // same address twice (overwrite scenario).
        let _ = w
            .write(space_id, address, Bytes::from(format!("{}", address)))
            .await;
        Ok(())
    }
}

impl<F: WriteOnceSpaceTestFixture + SimulatorFixture> SimulatorFixture for WosTailableFixture<F> {
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}

impl<F: WriteOnceSpaceTestFixture + IntegrationFixture> IntegrationFixture
    for WosTailableFixture<F>
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
        })
    }
}

/// Adapts a `ConditionalWriteSpaceTestFixture` into a `TailableSpaceTestFixture`.
pub struct CwsTailableFixture<F: ConditionalWriteSpaceTestFixture> {
    inner: F,
}

impl<F: ConditionalWriteSpaceTestFixture> ConformanceFixture for CwsTailableFixture<F> {
    type Env = F::Env;
    type Impl = F::Impl;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.inner.create_impl()
    }
}

impl<F: ConditionalWriteSpaceTestFixture> TailableSpaceTestFixture for CwsTailableFixture<F> {
    async fn write_at(&self, space_id: &str, address: u64) -> Result<()> {
        let mut w = self.inner.create_impl();
        let current = w.read(space_id, address).await.unwrap_or(None);
        let expected_version = current.map(|v| v.version);
        w.write(
            space_id,
            address,
            expected_version,
            Bytes::from(format!("{}", address)),
        )
        .await?;
        Ok(())
    }
}

impl<F: ConditionalWriteSpaceTestFixture + SimulatorFixture> SimulatorFixture
    for CwsTailableFixture<F>
{
    fn new(simulator: Simulator) -> Self {
        Self {
            inner: F::new(simulator),
        }
    }
}

impl<F: ConditionalWriteSpaceTestFixture + IntegrationFixture> IntegrationFixture
    for CwsTailableFixture<F>
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
        })
    }
}
