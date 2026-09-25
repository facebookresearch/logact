/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=rule_based_voter.proto");

    let protoc_bin = protoc_bin_vendored::protoc_bin_path().unwrap();
    unsafe {
        std::env::set_var("PROTOC", protoc_bin);
    }
    let protoc_inc = protoc_bin_vendored::include_path().unwrap();
    let protoc_inc = protoc_inc.canonicalize().unwrap();
    unsafe {
        std::env::set_var("PROTOC_INCLUDE", protoc_inc);
    }

    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());

    let mut prost_config = prost_build::Config::new();
    prost_config.enable_type_names();
    prost_config.compile_protos(
        &[manifest_dir.join("rule_based_voter.proto")],
        &[manifest_dir],
    )
}
