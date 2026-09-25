#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

launchd_require_macos() {
  if [ "$(uname -s)" != "Darwin" ]; then
    echo "LogAct currently supports macOS only" >&2
    exit 1
  fi
}

launchd_label() {
  printf '%s\n' "com.facebookresearch.logact.oss"
}

launchd_agent_path() {
  printf '%s/Library/LaunchAgents/%s.plist\n' "$1" "$(launchd_label)"
}

launchd_service() {
  printf 'gui/%s/%s\n' "$(id -u)" "$(launchd_label)"
}

launchd_plist_value() {
  /usr/bin/plutil -extract "$2" raw -o - "$1" 2>/dev/null
}

launchd_is_owned_agent() {
  candidate=$1
  [ -f "$candidate" ] &&
    [ "$(launchd_plist_value "$candidate" Label)" = "$(launchd_label)" ]
}

launchd_render_agent() (
  template=$1
  output=$2
  server=$3
  state_dir=$4

  cp "$template" "$output"
  [ "$(launchd_plist_value "$output" ProgramArguments.0)" = "LOGACT_SERVER_PATH" ]
  [ "$(launchd_plist_value "$output" ProgramArguments.2)" = "LOGACT_SOCKET_PATH" ]
  [ "$(launchd_plist_value "$output" ProgramArguments.4)" = "LOGACT_SQLITE_PATH" ]
  # `plutil -replace` inserts rather than overwrites array elements on macOS.
  /usr/bin/plutil -remove ProgramArguments.4 "$output"
  /usr/bin/plutil -remove ProgramArguments.2 "$output"
  /usr/bin/plutil -remove ProgramArguments.0 "$output"
  /usr/bin/plutil -insert ProgramArguments.0 -string "$server" "$output"
  /usr/bin/plutil -insert ProgramArguments.2 \
    -string "$state_dir/logact.sock" "$output"
  /usr/bin/plutil -insert ProgramArguments.4 \
    -string "$state_dir/logact.sqlite" "$output"
  /usr/bin/plutil -replace StandardOutPath \
    -string "$state_dir/logact.log" "$output"
  /usr/bin/plutil -replace StandardErrorPath \
    -string "$state_dir/logact.log" "$output"
  chmod 644 "$output"
)

launchd_run() {
  "${LOGACT_LAUNCHCTL:-/bin/launchctl}" "$@"
}

launchd_is_loaded() {
  if [ "${LOGACT_TEST_SKIP_LAUNCHCTL:-0}" = 1 ]; then
    return 1
  fi
  launchd_run print "$1" >/dev/null 2>&1
}

launchd_bootstrap() {
  if [ "${LOGACT_TEST_SKIP_LAUNCHCTL:-0}" = 1 ]; then
    return
  fi
  launchd_run bootstrap "gui/$(id -u)" "$1"
}

launchd_restart() {
  if [ "${LOGACT_TEST_SKIP_LAUNCHCTL:-0}" = 1 ]; then
    return
  fi
  launchd_run kickstart -k "$1"
}

launchd_unload() {
  if [ "${LOGACT_TEST_SKIP_LAUNCHCTL:-0}" = 1 ]; then
    return
  fi
  launchd_run bootout "$1"
}
