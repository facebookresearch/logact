/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Helpers for packing and unpacking `prost_types::Any`.

use prost::Message;
use prost::Name;

/// Pack a typed protobuf message into `Any`.
pub fn pack_any<M: Message + Name>(msg: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: M::type_url(),
        value: msg.encode_to_vec(),
    }
}

/// Decode `Any` into `M` if the type URL matches.
pub fn unpack_any<M: Message + Default + Name>(any: &prost_types::Any) -> Option<M> {
    if any.type_url != M::type_url() {
        return None;
    }
    M::decode(any.value.as_slice()).ok()
}
