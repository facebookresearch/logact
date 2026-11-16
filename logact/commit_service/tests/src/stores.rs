/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Replay checkpoint stores that only tests use. Production uses the replay
//! crate's `StorageCheckpoints`.

mod nostore;

pub use nostore::NoStoreCheckpoints;
pub use nostore::VoterConfigValidator;
