#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

socket="${LOGACT_OSS_SOCKET:-$HOME/.logact-oss/logact.sock}"
exec logact-oss-hook --socket "$socket" muse-hook "$1"
