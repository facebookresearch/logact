/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runs a gated-counter scenario on one of the suite's fixtures, exactly as its
//! simtest does, with the scenario's annotation and the feature flags overridden
//! from the command line. Benchmark scenarios print their own timings.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use agentbus_api::feature_flags::FeatureFlags;
use agentbus_simulator::Simulator;
use agentbus_tests::fixtures::simtest::FaultInjectingFixture;
use agentbus_tests::fixtures::simtest::SimpleMemoryFixture;
use anyhow::Context as _;
use anyhow::Result;
use clap::Parser;
use conformance::ConformanceFixture;
use conformance::LatencyConfig;
use conformance::LatencyDistribution;
use conformance::SimulatorFixture;
use conformance::TestConfig;
use conformance::run_sim_scenario;
use conformance::try_new_fixture;
use logact_commit_service_tests::fixtures::ChanneledCommitServiceFixture;
use logact_commit_service_tests::fixtures::CommitServiceV1Fixture;
use logact_commit_service_tests::fixtures::CommitServiceV1LatencyFixture;
use logact_commit_service_tests::fixtures::InMemCommitServiceFixture;
use logact_commit_service_tests::fixtures::ObservableCommitServiceFixture;
use logact_commit_service_tests::fixtures::RandomRoutingCommitServiceFixture;

/// Run a gated-counter scenario on a fixture.
/// Options override the scenario's annotation.
#[derive(Parser)]
struct Args {
    /// Fixture variant, as in the simtest names (see `--list`).
    #[arg(long, default_value = "commit_service_production_shape")]
    fixture: String,
    /// Scenario function (see `--list`).
    #[arg(long, default_value = "run_benchmark_independent_buses_concurrent")]
    scenario: String,
    /// First simulator seed; each further iteration uses the next one.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 1)]
    iterations: u64,
    /// Maximum injected latency per hop.
    #[arg(long)]
    latency_ms: Option<u64>,
    /// Fraction of hops lost.
    #[arg(long)]
    loss: Option<f64>,
    /// Draw latencies from a Zipf distribution with this exponent.
    #[arg(long)]
    zipf: Option<f64>,
    /// Retry the bottom-most APIs.
    #[arg(long)]
    retry: bool,
    /// Probability that a routed request goes to its bus's home service.
    #[arg(long)]
    sticky: Option<f64>,
    #[arg(long)]
    max_iterations: Option<usize>,
    #[arg(long)]
    ttl_ms: Option<u64>,
    /// Run the replay commit service instead of V1.
    #[arg(long)]
    replay: bool,
    /// Save replay checkpoints in the background.
    #[arg(long)]
    background_checkpoints: bool,
    /// Don't serialize commit intentions per bus.
    #[arg(long)]
    no_buffering: bool,
    /// List the fixtures and scenarios.
    #[arg(long)]
    list: bool,
}

fn with_overrides(mut config: TestConfig, args: &Args) -> Result<TestConfig> {
    if args.latency_ms.is_some() || args.loss.is_some() || args.zipf.is_some() {
        let mut latency = config.latency.unwrap_or(LatencyConfig {
            max_latency: Duration::ZERO,
            loss_fraction: 0.0,
            distribution: LatencyDistribution::Uniform,
        });
        if let Some(ms) = args.latency_ms {
            latency.max_latency = Duration::from_millis(ms);
        }
        if let Some(loss) = args.loss {
            anyhow::ensure!((0.0..=1.0).contains(&loss), "--loss must be in [0, 1]");
            latency.loss_fraction = loss;
        }
        if let Some(exponent) = args.zipf {
            anyhow::ensure!(exponent >= 0.0, "--zipf must be >= 0");
            latency.distribution = LatencyDistribution::Zipf { exponent };
        }
        config.latency = Some(latency);
    }
    config.retry |= args.retry;
    if let Some(sticky) = args.sticky {
        anyhow::ensure!((0.0..=1.0).contains(&sticky), "--sticky must be in [0, 1]");
        config.sticky = Some(sticky);
    }
    if let Some(max_iterations) = args.max_iterations {
        config.max_iterations = Some(max_iterations);
    }
    if let Some(ms) = args.ttl_ms {
        config.ttl = Some(Duration::from_millis(ms));
    }
    Ok(config)
}

/// Runs `scenario` once per iteration.
fn run_scenario<F, S>(args: &Args, config: &TestConfig, scenario: impl Fn() -> S) -> Result<()>
where
    F: SimulatorFixture + ConformanceFixture<Env = Simulator> + 'static,
    S: AsyncFnOnce(&F) -> Result<()> + 'static,
{
    let flags = FeatureFlags {
        enable_replay_commit_service: args.replay,
        enable_background_replay_checkpoints: args.background_checkpoints,
        enable_commit_buffering: !args.no_buffering,
        ..FeatureFlags::default()
    };
    eprintln!("{flags:?}");
    for iteration in 0..args.iterations {
        let seed = args.seed.wrapping_add(iteration);
        eprintln!("Run {}/{}, seed: {seed}", iteration + 1, args.iterations);
        run_sim_scenario(
            seed,
            |simulator| {
                simulator.set_feature_flags(flags);
                try_new_fixture::<F>(simulator, config)
            },
            scenario(),
        )
        .with_context(|| format!("failed at seed {seed}"))?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut available = BTreeSet::new();
    let mut registry = BTreeSet::new();
    let mut ran = false;

    macro_rules! scenario {
        (int_only, $name:ident, $suffix:ident, $fix:ty $(, $($rest:tt)*)?) => {};
        ($envs:ident, $name:ident, $suffix:ident, $fix:ty, config = $config:expr $(, $seed:tt)?) => {
            scenario!(@run $name, $suffix, $fix, $config);
        };
        ($envs:ident, $name:ident, $suffix:ident, $fix:ty $(, $seed:tt)?) => {
            scenario!(@run $name, $suffix, $fix, TestConfig::default());
        };
        (@run $name:ident, $suffix:ident, $fix:ty, $config:expr) => {
            available.insert((stringify!($suffix), stringify!($name)));
            if !args.list
                && !ran
                && args.fixture == stringify!($suffix)
                && args.scenario == stringify!($name)
            {
                let config = with_overrides($config, &args)?;
                eprintln!("{} on {}: {config:?}", stringify!($name), stringify!($suffix));
                run_scenario::<$fix, _>(
                    &args,
                    &config,
                    || async |fixture: &$fix| gated_counter_suite::scenarios::$name(fixture).await,
                )?;
                ran = true;
            }
        };
    }
    macro_rules! fixture {
        ([$fix:ty, $suffix:ident, sim]) => {
            registry.insert(stringify!($suffix));
            gated_counter_suite::gated_counter_scenarios!(scenario, $suffix, $fix);
        };
        ([$fix:ty, $suffix:ident, integration]) => {};
    }
    gated_counter_suite::gated_counter_fixtures!(fixture);
    gated_counter_suite::gated_counter_pinned_scenarios!(scenario);

    if args.list {
        let fixtures: BTreeSet<_> = available.iter().map(|(fixture, _)| *fixture).collect();
        let mut scenarios: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (fixture, scenario) in &available {
            scenarios.entry(*scenario).or_default().push(*fixture);
        }
        println!("Fixtures:");
        fixtures.iter().for_each(|fixture| println!("  {fixture}"));
        println!("Scenarios:");
        for (scenario, on) in scenarios {
            if on.len() == registry.len() {
                println!("  {scenario}");
            } else {
                println!("  {scenario}: {}", on.join(", "));
            }
        }
        return Ok(());
    }
    anyhow::ensure!(
        ran,
        "no scenario {} on fixture {}; see --list",
        args.scenario,
        args.fixture
    );
    Ok(())
}
