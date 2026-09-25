#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -eu

ownership_marker=".logact-install-owner"
package_identity="facebookresearch/logact"

path_exists() {
  [ -e "$1" ] || [ -L "$1" ]
}

is_owned_package_directory() {
  package_dir=$1
  [ -d "$package_dir" ] &&
    [ ! -L "$package_dir" ] &&
    [ -f "$package_dir/$ownership_marker" ] &&
    [ "$(cat "$package_dir/$ownership_marker")" = "$package_identity" ]
}

remove_owned_entrypoint() {
  entrypoint=$1
  expected_target=$2

  if [ -L "$entrypoint" ] && [ "$(readlink "$entrypoint")" = "$expected_target" ]; then
    rm -f "$entrypoint"
  elif [ -e "$entrypoint" ] || [ -L "$entrypoint" ]; then
    echo "warning: retaining unowned path: $entrypoint" >&2
  fi
}

try_client_command() {
  client=$1
  shift

  if command -v "$client" >/dev/null 2>&1; then
    "$client" "$@" >/dev/null 2>&1 ||
      echo "warning: could not unregister LogAct OSS from $client" >&2
  fi
}

unregister_plugins() {
  try_client_command claude plugin uninstall logact-oss@logact-oss
  try_client_command claude plugin marketplace remove logact-oss
  try_client_command codex plugin remove logact-oss@logact-oss
  try_client_command codex plugin marketplace remove logact-oss
  try_client_command muse plugins remove logact-oss
  try_client_command muse plugins marketplace remove logact-oss
}
if [ "$#" -ne 0 ]; then
  echo "usage: $0" >&2
  exit 2
fi

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$script_dir/launchd.sh"
launchd_require_macos

prefix="$HOME/.local"
for package_dir in "$prefix/libexec/logact-oss" "$prefix/share/logact-oss"; do
  if path_exists "$package_dir" &&
    ! is_owned_package_directory "$package_dir"; then
    echo "refusing to remove unowned package directory: $package_dir" >&2
    exit 1
  fi
done
launch_agent="$(launchd_agent_path "$HOME")"
launch_service="$(launchd_service)"

launch_agent_owned=0
if path_exists "$launch_agent"; then
  if ! launchd_is_owned_agent "$launch_agent"; then
    echo "refusing to remove unowned LaunchAgent: $launch_agent" >&2
    exit 1
  fi
  launch_agent_owned=1
fi

if launchd_is_loaded "$launch_service"; then
  if [ "$launch_agent_owned" -eq 0 ]; then
    echo "cannot verify ownership of loaded LaunchAgent $launch_service" >&2
    exit 1
  fi
  if ! launchd_unload "$launch_service" && \
      launchd_is_loaded "$launch_service"; then
    echo "failed to unload $launch_service; no files were removed" >&2
    exit 1
  fi
fi
unregister_plugins
if [ "$launch_agent_owned" -eq 1 ]; then
  rm -f "$launch_agent"
fi
for binary in logact-oss-agentbus logact-oss-hook logact-oss-server; do
  remove_owned_entrypoint \
    "$prefix/bin/$binary" \
    "../libexec/logact-oss/$binary"
done
rm -rf "$prefix/libexec/logact-oss"
rm -rf "$prefix/share/logact-oss"

cat <<EOF
Removed LogAct OSS and its LaunchAgent. Data under $HOME/.logact-oss was retained.
Available agent clients were asked to remove their plugin registrations.
Restart any running clients to unload cached hooks.
EOF
