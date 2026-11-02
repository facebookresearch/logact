/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[conformance_macros::scenarios(gated_counter_gated_counter_list)]
mod defs {
    use agentbus_simulator::Simulator;
    use anyhow::Result;
    use conformance::ConformanceFixture;
    use linearizability::object::LinearizabilityImplementation;
    use linearizability::object::LinearizableObject;
    use linearizability::object::run_linearizability_test;

    use crate::model::GatedCounterSpec;
    use crate::model::GatedCounterWorkload;

    #[scenario(sim_only)]
    pub async fn run_gated_counter_linearizability<F>(fixture: &F) -> Result<()>
    where
        F: ConformanceFixture<Env = Simulator>,
        F::Impl: LinearizabilityImplementation,
        <F::Impl as LinearizabilityImplementation>::Object:
            LinearizableObject<Spec = GatedCounterSpec>,
    {
        run_linearizability_test(GatedCounterWorkload::new(3, 8, 8), fixture.create_impl()).await?;
        Ok(())
    }
}
pub use defs::*;
