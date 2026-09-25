/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared, abstraction-agnostic core of the conformance test framework.
//!
//! Every annotation-driven conformance suite (Storage, AgentBus, CommitSvc, …)
//! reuses everything here, so it is written once:
//!
//! - **The fixture traits** ([`ConformanceFixture`], [`SimulatorFixture`],
//!   [`IntegrationFixture`]) — the contract a fixture implements. They are
//!   surface-agnostic: `Impl` is unconstrained here; each suite narrows it (e.g.
//!   `where Impl: Storage`) in its own scenarios.
//! - **The per-test codegen** ([`sim_test!`] / [`integration_test!`]) — the exact
//!   `#[test]` / `#[fbinit::test]` body that constructs a fixture, runs one
//!   scenario, and asserts success.
//! - **The driver generator** ([`define_driver!`]) — one invocation per suite
//!   emits that suite's whole driver: the kind × environment policy table, the
//!   per-fixture fan-out, and the suite entry points. The kind × environment
//!   policy (which scenario kinds run in which environments) is defined here, in
//!   `define_driver!`, so it is the single source of truth across all suites.
//!
//! A suite therefore supplies only *data*, never cross-product logic: its suite
//! name, the fixtures under test (`variants.rs`), and the scenarios (`scenarios/`).

use std::rc::Rc;

use agentbus_api::environment::Environment;
use agentbus_simulator::Simulator;
use anyhow::Result;
use fbinit::FacebookInit;

/// A fixture that constructs one concrete implementation in some environment.
///
/// `Impl` is intentionally unconstrained: this trait is shared across every
/// conformance suite, and each suite constrains `Impl` to its own surface (e.g.
/// `Storage`) where its scenarios need it.
pub trait ConformanceFixture: Sized {
    type Env: Environment + 'static;
    type Impl;

    fn get_env(&self) -> Rc<Self::Env>;
    fn create_impl(&self) -> Self::Impl;
}

/// A fixture that can be built from a deterministic simulator.
pub trait SimulatorFixture: Sized {
    fn new(simulator: Simulator) -> Self;
}

/// A fixture that talks to a real backend and is built asynchronously under
/// `fbinit`.
///
/// Native `impl Future` (not `#[async_trait]`) so backends whose construction
/// holds `!Send` state across an await (e.g. `Rc`-based environments) are allowed
/// and unboxed — matching the AgentBus suites, which share this trait.
///
/// Bound is `Sized` (not `ConformanceFixture`) so a suite can implement this on a
/// fixture before that fixture gains its `ConformanceFixture` impl (the AgentBus
/// suites are migrated to `ConformanceFixture` one stacked diff at a time). The
/// scenario signatures separately require `ConformanceFixture`, so nothing is lost.
pub trait IntegrationFixture: Sized {
    fn new_async(fb: FacebookInit) -> impl std::future::Future<Output = Result<Self>>;
}

/// Emit one deterministic simulator `#[test]`.
///
/// `$test_fn` is the fully-qualified scenario function (the caller supplies it
/// already qualified, so this macro needs no knowledge of where scenarios live);
/// `$test_name` names the generated test, `$suffix` disambiguates it per fixture,
/// and `$fix` is the concrete fixture type to run against.
///
/// An optional trailing `$seed` (a `u64` literal) pins the simulator to a fixed
/// seed instead of a fresh random one — for a scenario that needs a specific
/// ordering to reproduce (e.g. a fixture engineered to trip a checker). Omit it
/// and every run picks a new random seed.
#[macro_export]
macro_rules! sim_test {
    ($test_fn:path, $test_name:ident, $suffix:ident, $fix:ty) => {
        $crate::sim_test!($test_fn, $test_name, $suffix, $fix, {
            rand::random::<u64>()
        });
    };
    ($test_fn:path, $test_name:ident, $suffix:ident, $fix:ty, $seed:tt) => {
        paste::paste! {
            #[test]
            fn [<$test_name _ $suffix>]() {
                use $crate::ConformanceFixture as _;
                use $crate::SimulatorFixture as _;
                let seed: u64 = $seed;
                eprintln!("Simulator seed: {seed}");
                let simulator = agentbus_simulator::Simulator::new(seed);
                let fixture = <$fix as $crate::SimulatorFixture>::new(simulator);
                let env = fixture.get_env();
                let handle = env.spawn(async move { $test_fn(&fixture).await });
                env.run();
                futures::executor::block_on(handle)
                    .expect("scenario task should complete")
                    .expect("scenario should succeed");
            }
        }
    };
}

/// Emit one integration `#[fbinit::test]` against a real backend.
#[macro_export]
macro_rules! integration_test {
    ($test_fn:path, $test_name:ident, $suffix:ident, $fix:ty) => {
        paste::paste! {
            #[fbinit::test]
            async fn [<$test_name _ $suffix>](fb: fbinit::FacebookInit) -> anyhow::Result<()> {
                use $crate::IntegrationFixture as _;
                let fixture = <$fix as $crate::IntegrationFixture>::new_async(fb).await?;
                $test_fn(&fixture).await
            }
        }
    };
}

/// Emit one determinism `#[test]`: run `$test_fn` `$cardinality` times, each on a
/// fresh fixture built from the *same* seed, and assert every run produces identical
/// output. This catches an implementation that reaches outside the `Environment`
/// (real clock/RNG/threads), which the single-run "assert `Ok`" contract can't.
///
/// Unlike [`sim_test!`], `$test_fn` returns a comparable value (e.g. the committed
/// log) rather than `Result<()>` — the runs' return values are what's compared. The
/// optional trailing `$seed` pins a fixed seed (default: a fresh random one, held
/// fixed across the runs). Runs are driven sequentially, each self-contained (build
/// fixture, spawn, `env.run()`, `block_on`), so there is no nesting of simulators.
#[macro_export]
macro_rules! sim_determinism_test {
    ($test_fn:path, $test_name:ident, $suffix:ident, $fix:ty, $cardinality:tt) => {
        $crate::sim_determinism_test!($test_fn, $test_name, $suffix, $fix, $cardinality, {
            rand::random::<u64>()
        });
    };
    ($test_fn:path, $test_name:ident, $suffix:ident, $fix:ty, $cardinality:tt, $seed:tt) => {
        paste::paste! {
            #[test]
            fn [<$test_name _ $suffix>]() {
                use $crate::ConformanceFixture as _;
                use $crate::SimulatorFixture as _;
                let seed: u64 = $seed;
                eprintln!("Simulator seed: {seed}");
                let run = || {
                    let fixture = <$fix as $crate::SimulatorFixture>::new(
                        agentbus_simulator::Simulator::new(seed),
                    );
                    let env = fixture.get_env();
                    let handle = env.spawn(async move { $test_fn(&fixture).await });
                    env.run();
                    futures::executor::block_on(handle).expect("determinism task should complete")
                };
                let expected = run();
                for _ in 1..$cardinality {
                    assert_eq!(
                        run(),
                        expected,
                        "determinism: the same seed must produce identical output across all {} runs",
                        $cardinality,
                    );
                }
            }
        }
    };
}

/// Generate a complete per-suite driver — the kind × environment table, the
/// per-fixture fan-out, and the suite entry points — from a single invocation.
///
/// Every conformance suite's driver is the same cross-product logic; only the
/// names differ. So that logic lives here once, and each suite supplies just its
/// scenario files and fixtures. Invoke this once per suite — in the test crate
/// root (`lib.rs`) for a one-suite-per-crate layout, or in the suite's module file
/// for a module-of-shared-crate suite (see the `module` variant below) — passing a
/// literal `$` as the first argument — that is the standard trick that lets the
/// *generated* macros refer to their own `$crate` and metavariables (we splice it
/// back in as `$d`):
///
/// ```ignore
/// conformance::define_driver! {
///     $,
///     root = logact_commit_service_engine_tests,  // this test crate, by name
///     scenario_mods = [test_scenarios, fault_injection_scenarios],  // files under `scenarios/`
///     fixtures = storage_fixtures,          // registry macro (variants.rs)
///     suite = storage,                      // name prefix for every generated macro
/// }
/// ```
///
/// `scenario_mods` lists the suite's scenario *files* (modules under `scenarios/`,
/// each an `#[scenarios(<suite>_<mod>_list)]` module). The driver generates the
/// `scenarios` module tree (`pub mod <mod>; pub use <mod>::*;` for each, flattening
/// every scenario to `root::scenarios::<fn>`) and the `<suite>_scenarios!` combiner
/// that folds the per-file `<suite>_<mod>_list!` lists — so a suite writes no
/// `scenarios.rs` at all. Adding a scenario *file* is just another list entry.
///
/// `suite` is a single name prefix from which the driver derives every macro it
/// generates — the two suite entry points the test targets call
/// (`storage_sim_suite!` / `storage_integration_suite!`), the `storage_scenarios!`
/// combiner, and the internal fan-out steps. They are `#[macro_export]`
/// (crate-global), so each suite needs a unique prefix; `paste` does the
/// name-stitching so the caller passes one name.
///
/// `root` is the test crate named explicitly (rather than relying on `$crate`):
/// the generated macros are invoked from a *separate* test-binary crate, where a
/// `$crate` spliced in here would point at `conformance`, not the test crate. With
/// an explicit `root`, every generated path is absolute and resolves anywhere.
/// `root` is taken as an ident (not a path) so the generated code can append
/// `::scenarios` to it — a `:path` metavariable is opaque and can't be extended.
///
/// The environment-inclusion policy is the single source of truth (`all` is what
/// an unannotated `#[scenario]` opts into):
///
/// | envs \ env | sim   | integration |
/// |------------|-------|-------------|
/// | all        | run   | run         |
/// | sim_only   | run   | skip        |
/// | int_only   | skip  | run         |
///
/// The crate-root form resolves scenarios at `root::scenarios::<fn>`. A suite that
/// lives as a module of a shared test crate uses the second form, adding
/// `module = <mod>` so scenarios resolve at `root::<mod>::scenarios::<fn>`.
/// Both forms generate simulator and integration suite entry points. Downstream
/// crates may also run environment-specific fixtures directly through the generated
/// integration emitter.
#[macro_export]
macro_rules! define_driver {
    // Crate-root suite (`lib.rs`, one suite per crate): scenarios resolve at
    // `root::scenarios`, and the integration *suite* entry point is generated (the
    // test crate owns its own integration fixtures). Forwards to the shared `@impl`
    // body below.
    (
        $d:tt,
        root = $root:ident,
        scenario_mods = [$($mod:ident),+ $(,)?],
        fixtures = $fixtures:ident,
        suite = $suite:ident $(,)?
    ) => {
        $crate::define_driver! {
            @impl $d,
            root = $root,
            module = [],
            scenario_mods = [$($mod),+],
            fixtures = $fixtures,
            suite = $suite,
        }
    };

    // Module-of-shared-crate suite (the AgentBus abstraction suites live as modules
    // of one crate, not one crate each): scenarios resolve at
    // `root::<module>::scenarios`. Forwards to the same shared `@impl` body.
    (
        $d:tt,
        root = $root:ident,
        module = $module:ident,
        scenario_mods = [$($mod:ident),+ $(,)?],
        fixtures = $fixtures:ident,
        suite = $suite:ident $(,)?
    ) => {
        $crate::define_driver! {
            @impl $d,
            root = $root,
            module = [$module],
            scenario_mods = [$($mod),+],
            fixtures = $fixtures,
            suite = $suite,
        }
    };

    // Shared body for both public forms. They differ only in where scenarios resolve
    // (`root $(::<module>)? ::scenarios`).
    //
    // `module` arrives as `[]` or `[<ident>]` (an optional-ident capture) rather than a
    // `:path`, because the generated code appends `::scenarios` to it and a `:path`
    // metavariable is opaque and can't be extended.
    (
        @impl $d:tt,
        root = $root:ident,
        module = [$($module:ident)?],
        scenario_mods = [$($mod:ident),+ $(,)?],
        fixtures = $fixtures:ident,
        suite = $suite:ident $(,)?
    ) => {
        // Scenario module tree: declare each scenario file and flatten its public
        // items up to `scenarios::*`, so the generated codegen reaches every scenario
        // at `root $(::<module>)? ::scenarios::<fn>` no matter which file defines it.
        // This replaces a hand-written `scenarios.rs`; adding a scenario *file* is just
        // another `scenario_mods` entry.
        pub mod scenarios {
            $(
                pub mod $mod;
                pub use $mod::*;
            )+
        }

        // The generated macros are `#[macro_export]` (crate-global), so each suite
        // needs unique names. `paste` derives all of them from one `suite` ident
        // (e.g. `suite = storage` -> `storage_emit_sim`, `storage_sim_suite`, …),
        // so the caller passes one name instead of several.
        paste::paste! {
        // Scenario combiner: fold each file's `#[scenarios]`-generated
        // `<suite>_<mod>_list!` into the single callback the fan-out drives.
        // Generated here so a suite never hand-writes the combiner.
        #[macro_export]
        macro_rules! [<$suite _scenarios>] {
            ($d cb:path, $d suffix:ident, $d fix:ty) => {
                $(
                    $root::[<$suite _ $mod _list>]!($d cb, $d suffix, $d fix);
                )+
            };
        }

        // Environment-inclusion table — simulator side. The optional trailing
        // `$seed` (from a seeded `sim_only` `#[scenario]` / `#[scenario_for]`)
        // forwards a fixed seed to `sim_test!`; without it every run picks a fresh
        // random seed. `seed` requires `sim_only`, so only that arm takes one.
        #[macro_export]
        macro_rules! [<$suite _emit_sim>] {
            (all, $d name:ident, $d suffix:ident, $d fix:ty) => {
                conformance::sim_test!($root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix);
            };
            (sim_only, $d name:ident, $d suffix:ident, $d fix:ty) => {
                conformance::sim_test!($root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix);
            };
            (int_only, $d name:ident, $d suffix:ident, $d fix:ty) => {};
            (sim_only, $d name:ident, $d suffix:ident, $d fix:ty, $d seed:tt) => {
                conformance::sim_test!($root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix, $d seed);
            };
        }

        // Environment-inclusion table — integration side. Generated for both forms;
        // the crate-root form also wraps it in an integration *suite* (below), while a
        // module suite's sibling fb/oss crates call this directly with their backend
        // fixture. `seed` requires `sim_only`, so a seeded scenario never runs in
        // integration; the lone seeded arm just absorbs the `sim_only` pin's emit
        // (which is a no-op in integration anyway).
        #[macro_export]
        macro_rules! [<$suite _emit_integration>] {
            (all, $d name:ident, $d suffix:ident, $d fix:ty) => {
                conformance::integration_test!(
                    $root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix
                );
            };
            (int_only, $d name:ident, $d suffix:ident, $d fix:ty) => {
                conformance::integration_test!(
                    $root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix
                );
            };
            (sim_only, $d name:ident, $d suffix:ident, $d fix:ty) => {};
            (sim_only, $d name:ident, $d suffix:ident, $d fix:ty, $d seed:tt) => {};
        }

        // Per-fixture fan-out: run every scenario (and every determinism scenario),
        // filtered by environment tag.
        #[macro_export]
        macro_rules! [<$suite _sim_fixture>] {
            ([$d fix:ty, $d suffix:ident, sim]) => {
                $root::[<$suite _scenarios>]!($root::[<$suite _emit_sim>], $d suffix, $d fix);
                $root::[<$suite _determinism_scenarios>]!($root::[<$suite _emit_sim_determinism>], $d suffix, $d fix);
            };
            ([$d fix:ty, $d suffix:ident, integration]) => {};
        }

        // Pinned-scenario combiner: fold each file's `#[scenarios]`-generated
        // `<suite>_<mod>_list_pinned!` and emit each pinned scenario once, through
        // the same environment table `$emit` (so `sim_only` / `int_only` still
        // filter). Pinned scenarios carry their own fixture and suffix, so unlike
        // the fan-out this does not loop over the registry.
        #[macro_export]
        macro_rules! [<$suite _pinned_scenarios>] {
            ($d emit:path) => {
                $(
                    $root::[<$suite _ $mod _list_pinned>]!($d emit);
                )+
            };
        }

        // Determinism combiner + emit: fold each file's `#[scenarios]`-generated
        // `<suite>_<mod>_list_determinism!` and emit each via `sim_determinism_test!`
        // (run twice at one seed, compare). Fanned over the sim registry like the
        // ordinary scenarios (see `<suite>_sim_fixture!`). Always generated (empty
        // when a suite has no determinism scenarios).
        #[macro_export]
        macro_rules! [<$suite _determinism_scenarios>] {
            ($d emit:path, $d suffix:ident, $d fix:ty) => {
                $(
                    $root::[<$suite _ $mod _list_determinism>]!($d emit, $d suffix, $d fix);
                )+
            };
        }
        #[macro_export]
        macro_rules! [<$suite _emit_sim_determinism>] {
            ($d name:ident, $d suffix:ident, $d fix:ty, $d cardinality:tt) => {
                conformance::sim_determinism_test!(
                    $root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix, $d cardinality
                );
            };
            ($d name:ident, $d suffix:ident, $d fix:ty, $d cardinality:tt, $d seed:tt) => {
                conformance::sim_determinism_test!(
                    $root $(:: $module)? ::scenarios::$d name, $d name, $d suffix, $d fix, $d cardinality, $d seed
                );
            };
        }

        // Sim suite entry point — the entire body of the sim test target: the registry
        // fan-out, then the pinned scenarios.
        #[macro_export]
        macro_rules! [<$suite _sim_suite>] {
            () => {
                $root::$fixtures!($root::[<$suite _sim_fixture>]);
                $root::[<$suite _pinned_scenarios>]!($root::[<$suite _emit_sim>]);
            };
        }

        // Per-fixture fan-out over the integration registry entries.
        #[macro_export]
        macro_rules! [<$suite _integration_fixture>] {
            ([$d fix:ty, $d suffix:ident, integration]) => {
                $root::[<$suite _scenarios>]!($root::[<$suite _emit_integration>], $d suffix, $d fix);
            };
            ([$d fix:ty, $d suffix:ident, sim]) => {};
        }
        #[macro_export]
        macro_rules! [<$suite _integration_suite>] {
            () => {
                $root::$fixtures!($root::[<$suite _integration_fixture>]);
                $root::[<$suite _pinned_scenarios>]!($root::[<$suite _emit_integration>]);
            };
        }
        }
    };
}
