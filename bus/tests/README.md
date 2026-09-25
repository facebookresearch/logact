# AgentBus Test Suite

Conformance tests for the AgentBus family of abstractions, run against the
deterministic simulator and integration backends such as SQLite and DynamoDB.

Every suite is annotation-driven: you write generic scenario functions over a
fixture trait, list the fixtures to run them against, and the shared
`conformance::define_driver!` macro generates the whole driver (the per-test
codegen, the scenario combiner, and the per-fixture fan-out). Adding coverage means
writing a function or adding a registry line — no macro plumbing.

## The framework

The reusable core lives in a separate crate, `bus/testing/conformance` (+ the
`conformance_macros` proc-macro crate):

- **Fixture traits** — `ConformanceFixture` (`Env` + `Impl`, `get_env` /
  `create_impl`), `SimulatorFixture` (`new(Simulator)`), `IntegrationFixture`
  (`new_async(fb)`). Each suite defines a thin `XxxTestFixture: ConformanceFixture<Impl: Xxx>`
  blanket subtrait.
- **`#[conformance_macros::scenarios(<suite>_<mod>_list)]`** — placed on an inline
  `mod`; turns every `#[scenario]`-tagged `async fn(&F) -> Result<()>` into a test
  fanned over the whole fixture registry. `#[scenario(sim_only)]` /
  `#[scenario(int_only)]` restrict the environment. `#[scenario_for(<Fixture>, suffix
  = <id> [, sim_only | int_only] [, seed = <n>])]` instead pins a scenario to one
  named fixture (for a test that only makes sense against a fixture with a particular
  capability), optionally at a fixed simulator seed.
- **`define_driver! { .. }`** — one invocation per suite generates the `scenarios`
  module tree, the `<suite>_scenarios!` combiner, the sim/integration emit tables,
  and the suite entry points from `scenario_mods` + a fixture registry.

## Suites

This crate hosts the **AgentBus** suite at the crate root, plus three
abstraction suites as modules — each its own `define_driver!` (module arm):

| Suite | Module root | Fixtures |
|-------|-------------|----------|
| AgentBus | `lib.rs` (full arm) | `variants.rs` (`agentbus_fixtures!`) |
| ConditionalWriteSpace | `conditional_write_space.rs` | `conditional_write_space/variants.rs` |
| WriteOnceSpace | `write_once_space.rs` | `write_once_space/variants.rs` |
| TailableSpace | `tailable_space.rs` | `tailable_space/variants.rs` |

Each suite keeps its scenarios in `<suite>/scenarios/` and its fixtures in
`<suite>/fixtures.rs`. AgentBus, being the crate-root suite, keeps these at the top
level (`scenarios/`, `fixtures.rs`, `variants.rs`).

## Test binaries

- `src/sim_tests.rs` — simulator tests for the whole family (calls each suite's
  `*_sim_suite!()`), plus the portable AgentBus integration fixtures via
  `agentbus_integration_suite!()`.
- `tests/oss/src/integration_tests.rs` — DynamoDB integration.

Additional integration binaries can drive a suite by calling its generated
`<suite>_emit_integration` with a backend fixture.

## Adding coverage

**A scenario:** add an `#[scenario]` (optionally `sim_only` / `int_only`) function
to the relevant `scenarios/<file>.rs` (an `#[scenarios(..)]` module). It is fanned
over every fixture automatically; use `#[scenario_for(<Fixture>, ..)]` to pin it to
one fixture instead.

**A new scenario file:** create `scenarios/<name>.rs` as an
`#[conformance_macros::scenarios(<suite>_<name>_list)]` module and add `<name>` to
the suite's `scenario_mods = [..]`.

**A fixture:** add one line to the suite's `variants.rs` registry,
`[FixtureType, suffix, sim|integration]`. Sim suffixes are suite-prefixed
(`cws_` / `wos_` / `tail_` / `bus_`) to keep names unique in the shared
`sim_tests` binary.

## Non-conformance tests

Almost every AgentBus test is a scenario:

- **Failure injection** is expressed with `#[scenario_for]` pinning
  `FaultFixture<Base, Mix>` fixtures (`scenarios/fault_injection.rs`). Faults live in
  the fixture — not a runtime wrapper — because space-level faults are injected below
  the bus, where a scenario body can't reach; the shared assertion logic is in
  `fault_injection_helpers.rs`.
- The **buggy-linearizability** check is a seeded `#[scenario_for]`
  (`run_buggy_poll_is_caught` in `scenarios/lin_test`): it asserts the checker
  *catches* a deliberately-buggy fixture at a fixed seed.

The one test that can't be a scenario is the **multi-node determinism guard**
(`run_test_multiple_nodes_deterministic` in `scenarios/multi_node.rs`): it builds two
fixtures from one seed and compares the runs byte-for-byte, guarding against an
implementation that reaches outside the `Environment`. Because it needs two fixtures
at one seed — which the one-fixture scenario contract can't express — it stays
hand-written and is emitted per fixture from `sim_tests.rs`. (The multi-node
*consistency* check is a regular `sim_only` scenario.)
