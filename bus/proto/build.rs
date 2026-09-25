/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() -> std::io::Result<()> {
    let protos = ["agent_bus.proto", "appserver.proto"];
    for p in &protos {
        println!("cargo:rerun-if-changed={}", p);
    }

    setup_protoc_env();

    // appserver.proto imports `agentbus_proto/agent_bus.proto`. Stage all proto
    // files under that canonical import path before invoking protoc.
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let staged = out_dir.join("agentbus_proto");
    std::fs::create_dir_all(&staged)?;
    for p in &protos {
        std::fs::copy(manifest_dir.join(p), staged.join(p))?;
    }

    // Destination paths must match the imports in agent_bus.proto.
    let imported_protos = [
        (
            manifest_dir.join("../voters/llm/proto/llm_voter.proto"),
            out_dir.join("llm_voter_proto/llm_voter.proto"),
        ),
        (
            manifest_dir.join("../voters/rule-based/proto/rule_based_voter.proto"),
            out_dir.join("rule_based_voter_proto/rule_based_voter.proto"),
        ),
    ];
    for (source, destination) in imported_protos {
        println!("cargo:rerun-if-changed={}", source.display());
        std::fs::create_dir_all(destination.parent().expect("staged proto has a parent"))?;
        std::fs::copy(source, destination)?;
    }

    let compile_protos: Vec<std::path::PathBuf> = protos.iter().map(|p| staged.join(p)).collect();

    let mut prost_config = prost_build::Config::new();
    prost_config.enable_type_names();
    tonic_prost_build::configure()
        .extern_path(".llm_voter", "::llm_voter_proto_rust::llm_voter")
        .extern_path(
            ".rule_based_voter",
            "::rule_based_voter_proto_rust::rule_based_voter",
        )
        .compile_with_config(prost_config, &compile_protos, &[out_dir])
}

/// Setup process level env vars for tonic to find protoc etc
fn setup_protoc_env() {
    let protoc_bin = protoc_bin_vendored::protoc_bin_path().unwrap();
    unsafe {
        std::env::set_var("PROTOC", protoc_bin);
    }

    let protoc_inc = protoc_bin_vendored::include_path().unwrap();
    let protoc_inc = protoc_inc.canonicalize().unwrap(); // protoc wants canonicalized paths
    unsafe {
        std::env::set_var("PROTOC_INCLUDE", protoc_inc);
    }
}
