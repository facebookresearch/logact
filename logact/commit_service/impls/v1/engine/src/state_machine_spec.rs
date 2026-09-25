/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Configuration and stable identity for one state-machine instance.
pub struct StateMachineSpec<Id, Config> {
    pub id: Id,
    pub config: Config,
}

impl<Id, Config> StateMachineSpec<Id, Config> {
    pub fn new(id: Id, config: Config) -> Self {
        Self { id, config }
    }
}
