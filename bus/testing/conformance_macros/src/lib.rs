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
use syn::LitInt;
use syn::Meta;
use syn::Token;
use syn::Type;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;

/// Parsed `#[scenario_for(<FixtureType>, suffix = <ident> [, sim_only | int_only] [, seed = <int>])]`.
struct ScenarioForArgs {
    fixture: Type,
    suffix: Ident,
    envs: Ident,
    seed: Option<LitInt>,
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
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "expected `suffix = <ident>`, `seed = <int>`, `sim_only`, or `int_only`",
                ));
            }
        }
        let suffix =
            suffix.ok_or_else(|| input.error("`#[scenario_for]` requires `suffix = <ident>`"))?;
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
}

impl Parse for ScenarioArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut envs = Ident::new("all", Span::call_site());
        let mut cardinality: usize = 1;
        let mut seed: Option<LitInt> = None;
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
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "expected `sim_only`, `int_only`, `cardinality = <int>`, or `seed = <int>`",
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
        if seed.is_some() && envs != "sim_only" {
            return Err(input.error(
                "`seed` requires `sim_only` (a fixed simulator seed is meaningless against a real backend)",
            ));
        }
        Ok(Self {
            envs,
            cardinality,
            seed,
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
    let mut fanned: Vec<(Ident, Ident, Option<LitInt>)> = Vec::new();
    // `#[scenario_for]` fns are pinned to one fixture: (fn_ident, envs, fixture, suffix, seed).
    let mut pinned: Vec<(Ident, Ident, Type, Ident, Option<LitInt>)> = Vec::new();
    // `#[scenario(.., cardinality > 1)]` fns fan over the registry but run `cardinality`
    // times at one held-fixed seed and compare (via `sim_determinism_test!`):
    // (fn_ident, cardinality, optional fixed seed).
    let mut determinism: Vec<(Ident, usize, Option<LitInt>)> = Vec::new();

    for item in content.iter_mut() {
        if let Item::Fn(func) = item {
            let name = func.sig.ident.clone();
            let mut kept = Vec::new();
            // Strip `#[scenario]` / `#[scenario_for]` so the fn compiles normally;
            // a fn may carry several (e.g. one `#[scenario_for]` per variant).
            for attr in std::mem::take(&mut func.attrs) {
                if attr.path().is_ident("scenario") {
                    match parse_scenario_args(&attr) {
                        Ok(args) if args.cardinality <= 1 => {
                            fanned.push((name.clone(), args.envs, args.seed))
                        }
                        Ok(args) => determinism.push((name.clone(), args.cardinality, args.seed)),
                        Err(err) => return err.to_compile_error().into(),
                    }
                } else if attr.path().is_ident("scenario_for") {
                    match attr.parse_args::<ScenarioForArgs>() {
                        Ok(args) => pinned.push((
                            name.clone(),
                            args.envs,
                            args.fixture,
                            args.suffix,
                            args.seed,
                        )),
                        Err(err) => return err.to_compile_error().into(),
                    }
                } else {
                    kept.push(attr);
                }
            }
            func.attrs = kept;
        }
    }

    let fanned_arms = fanned.iter().map(|(name, envs, seed)| match seed {
        // A fixed seed rides along as a trailing token so the driver's emit table can
        // forward it to `sim_test!` (and ignore it for integration).
        Some(seed) => quote! { $cb!(#envs, #name, $suffix, $fix, #seed); },
        None => quote! { $cb!(#envs, #name, $suffix, $fix); },
    });
    let pinned_arms = pinned
        .iter()
        .map(|(name, envs, fixture, suffix, seed)| match seed {
            // A pinned fixed seed rides along as a trailing token so the driver's
            // emit table can forward it to `sim_test!` (and ignore it for integration).
            Some(seed) => quote! { $emit!(#envs, #name, #suffix, #fixture, #seed); },
            None => quote! { $emit!(#envs, #name, #suffix, #fixture); },
        });
    let determinism_arms = determinism.iter().map(|(name, cardinality, seed)| {
        let cardinality = *cardinality;
        match seed {
            // Cardinality (and optional fixed seed) ride along so the driver's emit
            // forwards them to `sim_determinism_test!`.
            Some(seed) => quote! { $emit!(#name, $suffix, $fix, #cardinality, #seed); },
            None => quote! { $emit!(#name, $suffix, $fix, #cardinality); },
        }
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
