/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Test adapters that run CommitService scenarios with legacy and typed IDs.

use std::marker::PhantomData;
use std::rc::Rc;

use anyhow::Result;
use conformance::ConformanceFixture;
use conformance::IntegrationFixture;
use fbinit::FacebookInit;
use logact_commit_service_api::CommitSvc;

pub struct EncodedBusId {
    pub legacy_bus_id: String,
    pub retain_typed_bus_id: bool,
}

pub trait BusIdEncoding {
    fn encode(legacy_bus_id: &str, typed_bus_id: Option<&str>) -> EncodedBusId;
}

pub struct LegacyBusIdEncoding;

impl BusIdEncoding for LegacyBusIdEncoding {
    fn encode(_legacy_bus_id: &str, typed_bus_id: Option<&str>) -> EncodedBusId {
        EncodedBusId {
            legacy_bus_id: typed_bus_id
                .expect("CommitService scenarios must populate the typed bus ID")
                .to_owned(),
            retain_typed_bus_id: false,
        }
    }
}

pub struct TypedBusIdEncoding;

impl BusIdEncoding for TypedBusIdEncoding {
    fn encode(_legacy_bus_id: &str, typed_bus_id: Option<&str>) -> EncodedBusId {
        assert!(
            typed_bus_id.is_some(),
            "CommitService scenarios must populate the typed bus ID"
        );
        EncodedBusId {
            legacy_bus_id: "ignored-legacy-commit-intention".to_string(),
            retain_typed_bus_id: true,
        }
    }
}

pub trait BusIdEncodingFixtureFactory<M>: ConformanceFixture
where
    M: BusIdEncoding,
{
    type EncodedImpl: CommitSvc;

    fn create_encoded_impl(&self) -> Self::EncodedImpl;
}

pub struct BusIdEncodingFixture<F, M> {
    inner: F,
    mode: PhantomData<M>,
}

impl<F, M> ConformanceFixture for BusIdEncodingFixture<F, M>
where
    F: BusIdEncodingFixtureFactory<M>,
    M: BusIdEncoding,
{
    type Env = F::Env;
    type Impl = F::EncodedImpl;

    fn get_env(&self) -> Rc<Self::Env> {
        self.inner.get_env()
    }

    fn create_impl(&self) -> Self::Impl {
        self.inner.create_encoded_impl()
    }
}

impl<F, M> IntegrationFixture for BusIdEncodingFixture<F, M>
where
    F: BusIdEncodingFixtureFactory<M> + IntegrationFixture,
    M: BusIdEncoding,
{
    async fn new_async(fb: FacebookInit) -> Result<Self> {
        Ok(Self {
            inner: F::new_async(fb).await?,
            mode: PhantomData,
        })
    }
}
