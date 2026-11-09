/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proc-macros for the annotation-driven conformance test framework.
//!
//! A test author writes generic scenario functions and tags each with how it runs:
//!
//! ```ignore
//! #[conformance_macros::scenarios(storage_scenarios)]
//! mod defs {
//!     #[scenario]                 // runs against the WHOLE fixture registry, sim AND integration
//!     pub async fn run_test_x<F: StorageTestFixture>(f: &F) -> anyhow::Result<()> { ... }
//!
//!     #[scenario(sim_only)]       // whole registry, simulator only
//!     pub async fn run_test_y<F: StorageTestFixture>(f: &F) -> anyhow::Result<()> { ... }
//!
//!     // pinned to ONE fixture; runs only against it. Repeat for several variants.
//!     #[scenario_for(MyFixture<SimpleMemory>, suffix = simple_memory, sim_only)]
//!     pub async fn run_test_z<F: SpecialFixture>(f: &F) -> anyhow::Result<()> { ... }
//! }
//! ```
//!
//! `#[scenario]` fans out over every fixture in the suite's registry. `#[scenario_for]`
//! pins a scenario to a single named fixture — for scenarios that only make sense
//! against fixtures with a particular capability — without hand-writing a
//! `conformance::sim_test!` call. `sim_only` / `int_only` restrict the environment
//! (`all` by default) for both. A pinned scenario may also add `seed = <int>` to run
//! the simulator at a fixed seed (for a case that needs a specific ordering to
//! reproduce); the default is a fresh random seed each run. A fixed simulator seed is
//! meaningless against a real backend, so `seed` requires `sim_only`.
//!
//! `#[scenario(sim_only, ttl_ms = 100)]` builds the fixture with a
//! `conformance::TestConfig` whose writes expire after the given positive number of
//! milliseconds. A fixture that does not support TTL rejects it; omitting `ttl_ms`
//! preserves the fixture's normal construction. Like `seed`, `ttl_ms` requires
//! `sim_only`.
//!
//! `#[scenario(sim_only, latency_ms = 10, loss = 0.01)]` builds the fixture with a
//! `conformance::LatencyConfig`: every request and response through a bottom-most
//! API is delayed uniformly in `[0, latency_ms]`, or lost with probability `loss`:
//! a lost request never reaches the backend, and a lost response never reaches the
//! caller. A caller whose response is later than `conformance::LatencyConfig::TIMEOUT`
//! gets a timeout error, so a scenario with `loss > 0` must tolerate errors. Either key enables injection;
//! the other defaults to 0. Both require `sim_only`. The latency configures the
//! simulator, which a scenario can pause and resume with
//! `fixture.get_env().latency_injector().disable()` / `.enable()`.
//!
//! `zipf = <float>` (with `latency_ms`) instead draws each hop's latency from a
//! Zipf distribution with that exponent: `k` milliseconds for `k` in
//! `[1, latency_ms]`, with probability proportional to `1 / k^zipf`, i.e. mostly
//! fast hops with a heavy tail.
//!
//! `#[scenario(sim_only, retry)]` wraps the bottom-most APIs a fixture builds in
//! their retrying wrappers (e.g. `RetryingWriteOnceSpace`), with the fixed policy
//! of `conformance::TestConfig::retry_config`. It requires `sim_only`.
//!
//! `#[scenario(sim_only, cardinality = N)]` (N > 1) marks a *determinism* scenario:
//! the driver runs it N times on fresh fixtures built from one held-fixed seed
//! (optionally pinned via `seed = <int>`) and asserts identical output, via
//! `conformance::sim_determinism_test!` — catching an implementation that reaches
//! outside the `Environment`. Such a scenario returns a comparable value (e.g. the
//! committed log) instead of `Result<()>`.
//!
//! The `#[scenarios(NAME)]` attribute scans the module and generates three callback
//! macros: `NAME!` (the fanned scenarios), `NAME_pinned!` (the pinned ones), and
//! `NAME_determinism!` (the `cardinality > 1` scenarios). `define_driver!` folds all
//! three — so adding a test is just writing an annotated function, no macro edits.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::format_ident;
use quote::quote;
use syn::Ident;
use syn::Item;
use syn::ItemMod;
use syn::LitFloat;
use syn::LitInt;
use syn::Meta;
use syn::Token;
use syn::Type;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;

/// Parse a positive millisecond TTL literal.
fn parse_ttl(input: ParseStream) -> syn::Result<LitInt> {
    let ttl: LitInt = input.parse()?;
    if ttl.base10_parse::<u64>()? == 0 {
        return Err(syn::Error::new_spanned(ttl, "`ttl_ms` must be positive"));
    }
    Ok(ttl)
}

/// Parse a non-negative Zipf exponent literal.
fn parse_zipf(input: ParseStream) -> syn::Result<LitFloat> {
    let exponent: LitFloat = input.parse()?;
    if exponent.base10_parse::<f64>()? < 0.0 {
        return Err(syn::Error::new_spanned(exponent, "`zipf` must be >= 0"));
    }
    Ok(exponent)
}

/// Parse a loss fraction literal in `[0, 1]`.
fn parse_loss(input: ParseStream) -> syn::Result<LitFloat> {
    let loss: LitFloat = input.parse()?;
    if !(0.0..=1.0).contains(&loss.base10_parse::<f64>()?) {
        return Err(syn::Error::new_spanned(loss, "`loss` must be in [0, 1]"));
    }
    Ok(loss)
}

/// The `TestConfig` keys shared by `#[scenario]` and `#[scenario_for]`.
#[derive(Default)]
struct ConfigArgs {
    ttl_ms: Option<LitInt>,
    latency_ms: Option<LitInt>,
    loss: Option<LitFloat>,
    zipf: Option<LitFloat>,
    retry: bool,
}

impl ConfigArgs {
    /// Parse the value of `key` if it is a config key. Returns whether it was.
    fn parse_key(&mut self, key: &Ident, input: ParseStream) -> syn::Result<bool> {
        if key == "ttl_ms" {
            input.parse::<Token![=]>()?;
            self.ttl_ms = Some(parse_ttl(input)?);
        } else if key == "latency_ms" {
            input.parse::<Token![=]>()?;
            let latency_ms: LitInt = input.parse()?;
            latency_ms.base10_parse::<u64>()?;
            self.latency_ms = Some(latency_ms);
        } else if key == "loss" {
            input.parse::<Token![=]>()?;
            self.loss = Some(parse_loss(input)?);
        } else if key == "zipf" {
            input.parse::<Token![=]>()?;
            self.zipf = Some(parse_zipf(input)?);
        } else if key == "retry" {
            self.retry = true;
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn validate(&self, input: ParseStream, envs: &Ident) -> syn::Result<()> {
        if self.ttl_ms.is_some() && envs != "sim_only" {
            return Err(input.error("`ttl_ms` requires `sim_only`"));
        }
        if (self.latency_ms.is_some() || self.loss.is_some()) && envs != "sim_only" {
            return Err(input.error("`latency_ms` and `loss` require `sim_only`"));
        }
        if self.zipf.is_some() && self.latency_ms.is_none() {
            return Err(input.error("`zipf` requires `latency_ms`"));
        }
        if self.retry && envs != "sim_only" {
            return Err(input.error("`retry` requires `sim_only`"));
        }
        Ok(())
    }

    /// The `TestConfig` these keys select, or `None` for the default config.
    fn test_config(&self) -> Option<proc_macro2::TokenStream> {
        if self.ttl_ms.is_none() && self.latency_ms.is_none() && self.loss.is_none() && !self.retry
        {
            return None;
        }
        let some = |value| quote! { ::core::option::Option::Some(#value) };
        let none = quote! { ::core::option::Option::None };
        let ttl = match &self.ttl_ms {
            Some(ttl) => some(quote! { ::core::time::Duration::from_millis(#ttl) }),
            None => none.clone(),
        };
        let latency = match (&self.latency_ms, &self.loss) {
            (None, None) => none,
            (latency_ms, loss) => {
                let latency_ms = latency_ms
                    .as_ref()
                    .map_or(quote! { 0 }, |ms| quote! { #ms });
                let loss = loss
                    .as_ref()
                    .map_or(quote! { 0.0 }, |loss| quote! { #loss });
                let distribution = match &self.zipf {
                    Some(exponent) => {
                        quote! { ::conformance::LatencyDistribution::Zipf { exponent: #exponent } }
                    }
                    None => quote! { ::conformance::LatencyDistribution::Uniform },
                };
                some(quote! {
                    ::conformance::LatencyConfig {
                        max_latency: ::core::time::Duration::from_millis(#latency_ms),
                        loss_fraction: #loss,
                        distribution: #distribution,
                    }
                })
            }
        };
        let retry = self.retry;
        Some(quote! { ::conformance::TestConfig { ttl: #ttl, latency: #latency, retry: #retry } })
    }
}

/// Parsed `#[scenario_for(<FixtureType>, suffix = <ident> [, sim_only | int_only] [, seed = <int>])]`.
struct ScenarioForArgs {
    fixture: Type,
    suffix: Ident,
    envs: Ident,
    seed: Option<LitInt>,
    config: ConfigArgs,
}

impl Parse for ScenarioForArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // The fixture type comes first (positionally); any commas inside its
        // generics are consumed by `Type` parsing, so the next top-level comma
        // separates it from the named args.
        let fixture: Type = input.parse()?;
        let mut suffix: Option<Ident> = None;
        let mut envs = Ident::new("all", Span::call_site());
        let mut seed: Option<LitInt> = None;
        let mut config = ConfigArgs::default();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if key == "suffix" {
                input.parse::<Token![=]>()?;
                suffix = Some(input.parse()?);
            } else if key == "seed" {
                input.parse::<Token![=]>()?;
                seed = Some(input.parse()?);
            } else if key == "sim_only" || key == "int_only" {
                envs = key;
            } else if !config.parse_key(&key, input)? {
                return Err(syn::Error::new_spanned(
                    key,
                    "expected `suffix = <ident>`, `seed = <int>`, `ttl_ms = <int>`, `latency_ms = <int>`, `loss = <float>`, `zipf = <float>`, `retry`, `sim_only`, or `int_only`",
                ));
            }
        }
        let suffix =
            suffix.ok_or_else(|| input.error("`#[scenario_for]` requires `suffix = <ident>`"))?;
        config.validate(input, &envs)?;
        if seed.is_some() && envs != "sim_only" {
            return Err(input.error(
                "`seed` requires `sim_only` (a fixed simulator seed is meaningless against a real backend)",
            ));
        }
        Ok(Self {
            fixture,
            suffix,
            envs,
            seed,
            config,
        })
    }
}

/// Parsed `#[scenario([sim_only | int_only] [, cardinality = <int>] [, seed = <int>])]`.
///
/// `cardinality` defaults to 1 — a normal single-run scenario asserting `Ok`.
/// `cardinality > 1` makes it a *determinism* scenario: it is run that many times on
/// fresh fixtures built from one held-fixed seed and the results are compared (via
/// `sim_determinism_test!`), so such a scenario returns a comparable value rather
/// than `Result<()>` and must be `sim_only`.
///
/// `seed` is orthogonal to `cardinality`: it pins the simulator to a fixed seed
/// instead of a fresh random one — for a case that needs a specific ordering to
/// reproduce — whether the scenario runs once (`cardinality = 1`) or several times
/// (the runs share the one fixed seed). A fixed simulator seed is meaningless against
/// a real backend, so `seed` requires `sim_only`; the default is a fresh random seed.
struct ScenarioArgs {
    envs: Ident,
    cardinality: usize,
    seed: Option<LitInt>,
    config: ConfigArgs,
}

impl Parse for ScenarioArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut envs = Ident::new("all", Span::call_site());
        let mut cardinality: usize = 1;
        let mut seed: Option<LitInt> = None;
        let mut config = ConfigArgs::default();
        let mut first = true;
        while !input.is_empty() {
            if !first {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
            }
            first = false;
            let key: Ident = input.parse()?;
            if key == "sim_only" || key == "int_only" {
                envs = key;
            } else if key == "cardinality" {
                input.parse::<Token![=]>()?;
                cardinality = input.parse::<LitInt>()?.base10_parse()?;
            } else if key == "seed" {
                input.parse::<Token![=]>()?;
                seed = Some(input.parse()?);
            } else if !config.parse_key(&key, input)? {
                return Err(syn::Error::new_spanned(
                    key,
                    "expected `sim_only`, `int_only`, `cardinality = <int>`, `seed = <int>`, `ttl_ms = <int>`, `latency_ms = <int>`, `loss = <float>`, `zipf = <float>`, or `retry`",
                ));
            }
        }
        if cardinality == 0 {
            return Err(input.error("`cardinality` must be >= 1"));
        }
        if cardinality > 1 && envs != "sim_only" {
            return Err(
                input.error("`cardinality > 1` (a determinism scenario) must be `sim_only`")
            );
        }
        config.validate(input, &envs)?;
        if seed.is_some() && envs != "sim_only" {
            return Err(input.error(
                "`seed` requires `sim_only` (a fixed simulator seed is meaningless against a real backend)",
            ));
        }
        Ok(Self {
            envs,
            cardinality,
            seed,
            config,
        })
    }
}

/// Parse a `#[scenario]` / `#[scenario(..)]` attribute. Bare `#[scenario]` is a
/// `Meta::Path` (all defaults); the list form carries the args. Any other shape is a
/// hard error.
fn parse_scenario_args(attr: &syn::Attribute) -> syn::Result<ScenarioArgs> {
    match &attr.meta {
        Meta::Path(_) => Ok(ScenarioArgs {
            envs: Ident::new("all", Span::call_site()),
            cardinality: 1,
            seed: None,
            config: ConfigArgs::default(),
        }),
        Meta::List(_) => attr.parse_args::<ScenarioArgs>(),
        other => Err(syn::Error::new_spanned(
            other,
            "expected `#[scenario]` or `#[scenario(sim_only | int_only [, cardinality = N] [, seed = N])]`",
        )),
    }
}

#[proc_macro_attribute]
pub fn scenarios(attr: TokenStream, item: TokenStream) -> TokenStream {
    let macro_name = parse_macro_input!(attr as Ident);
    let pinned_macro_name = format_ident!("{}_pinned", macro_name);
    let determinism_macro_name = format_ident!("{}_determinism", macro_name);
    let mut module = parse_macro_input!(item as ItemMod);

    let content = match module.content.as_mut() {
        Some((_, items)) => items,
        None => {
            return syn::Error::new_spanned(
                &module,
                "#[scenarios] must be applied to an inline module with a body",
            )
            .to_compile_error()
            .into();
        }
    };

    // `#[scenario]` fns fan out over the registry: (fn_ident, envs, optional fixed seed).
    let mut fanned: Vec<(Ident, ScenarioArgs)> = Vec::new();
    // `#[scenario_for]` fns are pinned to one fixture: (fn_ident, envs, fixture, suffix, seed).
    let mut pinned: Vec<(Ident, ScenarioForArgs)> = Vec::new();
    // `#[scenario(.., cardinality > 1)]` fns fan over the registry but run `cardinality`
    // times at one held-fixed seed and compare (via `sim_determinism_test!`):
    // (fn_ident, cardinality, optional fixed seed).
    let mut determinism: Vec<(Ident, ScenarioArgs)> = Vec::new();

    for item in content.iter_mut() {
        if let Item::Fn(func) = item {
            let name = func.sig.ident.clone();
            let mut kept = Vec::new();
            // Strip `#[scenario]` / `#[scenario_for]` so the fn compiles normally;
            // a fn may carry several (e.g. one `#[scenario_for]` per variant).
            for attr in std::mem::take(&mut func.attrs) {
                if attr.path().is_ident("scenario") {
                    match parse_scenario_args(&attr) {
                        Ok(args) if args.cardinality <= 1 => fanned.push((name.clone(), args)),
                        Ok(args) => determinism.push((name.clone(), args)),
                        Err(err) => return err.to_compile_error().into(),
                    }
                } else if attr.path().is_ident("scenario_for") {
                    match attr.parse_args::<ScenarioForArgs>() {
                        Ok(args) => pinned.push((name.clone(), args)),
                        Err(err) => return err.to_compile_error().into(),
                    }
                } else {
                    kept.push(attr);
                }
            }
            func.attrs = kept;
        }
    }

    let options = |seed: &Option<LitInt>, config: &ConfigArgs| {
        let config = config
            .test_config()
            .map(|config| quote! { , config = #config });
        let seed = seed.as_ref().map(|value| quote! { , #value });
        quote! { #config #seed }
    };
    let fanned_arms = fanned.iter().map(|(name, args)| {
        let envs = &args.envs;
        let options = options(&args.seed, &args.config);
        quote! { $cb!(#envs, #name, $suffix, $fix #options); }
    });
    let pinned_arms = pinned.iter().map(|(name, args)| {
        let envs = &args.envs;
        let fixture = &args.fixture;
        let suffix = &args.suffix;
        let options = options(&args.seed, &args.config);
        quote! { $emit!(#envs, #name, #suffix, #fixture #options); }
    });
    let determinism_arms = determinism.iter().map(|(name, args)| {
        let cardinality = args.cardinality;
        let options = options(&args.seed, &args.config);
        quote! { $emit!(#name, $suffix, $fix, #cardinality #options); }
    });

    let expanded = quote! {
        #module

        #[macro_export]
        macro_rules! #macro_name {
            ($cb:path, $suffix:ident, $fix:ty) => {
                #(#fanned_arms)*
            };
        }

        // Pinned scenarios: each carries its own fixture and suffix, so the driver
        // emits them once via the environment-inclusion table rather than fanning
        // them over the registry. Always generated (empty when there are no pins).
        #[macro_export]
        macro_rules! #pinned_macro_name {
            ($emit:path) => {
                #(#pinned_arms)*
            };
        }

        // Determinism scenarios: fanned over the registry like the fanned scenarios,
        // but emitted via `sim_determinism_test!` (run twice at one seed, compare) —
        // so each returns a comparable value rather than `Result<()>`. Always
        // generated (empty when there are none).
        #[macro_export]
        macro_rules! #determinism_macro_name {
            ($emit:path, $suffix:ident, $fix:ty) => {
                #(#determinism_arms)*
            };
        }
    };

    expanded.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scenario_ttls() {
        let args = syn::parse_str::<ScenarioArgs>("sim_only, ttl_ms = 100").unwrap();
        assert_eq!(
            args.config.ttl_ms.unwrap().base10_parse::<u64>().unwrap(),
            100
        );
        let args =
            syn::parse_str::<ScenarioArgs>("sim_only, ttl_ms = 100, seed = 1, cardinality = 2")
                .unwrap();
        assert_eq!(args.cardinality, 2);
        assert!(args.seed.is_some());
        let args = syn::parse_str::<ScenarioForArgs>(
            "Fixture<Inner>, suffix = test, sim_only, ttl_ms = 5",
        )
        .unwrap();
        assert!(args.config.ttl_ms.is_some());
    }

    #[test]
    fn rejects_invalid_scenario_ttls() {
        for input in [
            "ttl_ms = 100",
            "int_only, ttl_ms = 100",
            "sim_only, ttl_ms = 0",
            "sim_only, ttl_ms = -1",
            "sim_only, ttl_ms = 1.5",
            "sim_only, ttl_ms = \"100\"",
        ] {
            assert!(
                syn::parse_str::<ScenarioArgs>(input).is_err(),
                "should reject {input}"
            );
        }
        assert!(syn::parse_str::<ScenarioForArgs>("Fixture, suffix = test, ttl_ms = 100").is_err());
    }

    #[test]
    fn parses_scenario_latencies() {
        let args =
            syn::parse_str::<ScenarioArgs>("sim_only, latency_ms = 10, loss = 0.01").unwrap();
        assert_eq!(
            args.config
                .latency_ms
                .unwrap()
                .base10_parse::<u64>()
                .unwrap(),
            10
        );
        assert_eq!(
            args.config.loss.unwrap().base10_parse::<f64>().unwrap(),
            0.01
        );
        assert!(syn::parse_str::<ScenarioArgs>("sim_only, latency_ms = 0").is_ok());
        let args =
            syn::parse_str::<ScenarioArgs>("sim_only, latency_ms = 100, zipf = 1.5").unwrap();
        assert_eq!(
            args.config.zipf.unwrap().base10_parse::<f64>().unwrap(),
            1.5
        );
        assert!(syn::parse_str::<ScenarioArgs>("sim_only, loss = 1.0").is_ok());
        assert!(
            syn::parse_str::<ScenarioForArgs>("Fixture, suffix = test, sim_only, latency_ms = 5")
                .is_ok()
        );
    }

    #[test]
    fn rejects_invalid_scenario_latencies() {
        for input in [
            "latency_ms = 10",
            "loss = 0.1",
            "int_only, latency_ms = 10",
            "sim_only, latency_ms = -1",
            "sim_only, latency_ms = 1.5",
            "sim_only, loss = 1",
            "sim_only, loss = 1.5",
            "sim_only, loss = -0.5",
            "sim_only, zipf = 1.5",
            "sim_only, latency_ms = 10, zipf = -1.0",
            "sim_only, latency_ms = 10, zipf = 1",
        ] {
            assert!(
                syn::parse_str::<ScenarioArgs>(input).is_err(),
                "should reject {input}"
            );
        }
        assert!(syn::parse_str::<ScenarioForArgs>("Fixture, suffix = test, loss = 0.1").is_err());
    }

    #[test]
    fn parses_scenario_retry() {
        let args = syn::parse_str::<ScenarioArgs>("sim_only, latency_ms = 20, retry").unwrap();
        assert!(args.config.retry);
        assert!(args.config.test_config().is_some());
        assert!(syn::parse_str::<ScenarioArgs>("retry").is_err());
        assert!(syn::parse_str::<ScenarioArgs>("int_only, retry").is_err());
        assert!(
            syn::parse_str::<ScenarioForArgs>("Fixture, suffix = test, sim_only, retry").is_ok()
        );
    }
}
