/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() -> std::io::Result<()> {
    let manifest_dir = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .expect("Cargo should set CARGO_MANIFEST_DIR for build scripts"),
    );
    let out_dir = std::path::PathBuf::from(
        std::env::var("OUT_DIR").expect("Cargo should set OUT_DIR for build scripts"),
    );
    let policy_proto = manifest_dir.join("logact_commit_service_policy.proto");
    let agent_bus_proto = manifest_dir.join("../../../bus/proto/agent_bus.proto");

    println!("cargo:rerun-if-changed={}", policy_proto.display());

    setup_protoc_env();

    let imported_protos = [
        (
            agent_bus_proto,
            out_dir.join("agentbus_proto/agent_bus.proto"),
        ),
        (
            manifest_dir.join("../../../bus/voters/llm/proto/llm_voter.proto"),
            out_dir.join("llm_voter_proto/llm_voter.proto"),
        ),
        (
            manifest_dir.join("../../../bus/voters/rule-based/proto/rule_based_voter.proto"),
            out_dir.join("rule_based_voter_proto/rule_based_voter.proto"),
        ),
    ];
    for (source, destination) in imported_protos {
        println!("cargo:rerun-if-changed={}", source.display());
        std::fs::create_dir_all(destination.parent().expect("staged proto has a parent"))?;
        std::fs::copy(source, destination)?;
    }

    let mut config = prost_build::Config::new();
    config.enable_type_names();
    config.extern_path(".agent_bus", "::agent_bus_proto_rust::agent_bus");
    config.compile_protos(&[policy_proto], &[manifest_dir, out_dir])?;
    Ok(())
}

fn setup_protoc_env() {
    let protoc_bin = protoc_bin_vendored::protoc_bin_path()
        .expect("vendored protoc should provide a compiler binary");
    let protoc_include = protoc_bin_vendored::include_path()
        .expect("vendored protoc should provide an include directory")
        .canonicalize()
        .expect("vendored protoc include directory should be canonicalizable");
    // SAFETY: Cargo runs this build script as a single-threaded process, so no
    // other thread can concurrently read or mutate the process environment.
    unsafe { std::env::set_var("PROTOC", protoc_bin) };
    // SAFETY: Cargo runs this build script as a single-threaded process, so no
    // other thread can concurrently read or mutate the process environment.
    unsafe { std::env::set_var("PROTOC_INCLUDE", protoc_include) };
}
