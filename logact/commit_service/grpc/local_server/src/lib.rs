/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! SQLite-backed LogAct composition served over a local Unix socket.

mod private_fs;
mod server;
mod socket;
mod sqlite_backed_commit_service;

pub use private_fs::ensure_private_parent_directory;
pub use server::run_server;

#[cfg(test)]
mod tests;
