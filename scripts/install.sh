#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -eu

bundle_root=""
prefix=""
bin_dir=""
libexec_parent=""
libexec_dir=""
share_parent=""
share_dir=""
state_dir=""
launch_agent=""
launch_service=""
binaries=""
libexec_staging=""
share_staging=""
launch_agent_staging=""
backup_dir=""
rollback_needed=0
cleanup_status=""
ownership_marker=".logact-install-owner"
package_identity="facebookresearch/logact"
service_was_loaded=0
bootstrap_attempted=0
launch_agent_created=0
launch_agent_needs_install=0

path_exists() {
  [ -e "$1" ] || [ -L "$1" ]
}

cleanup_temporary_files() (
  for cleanup_path in "$@"; do
    if [ -n "$cleanup_path" ]; then
      rm -rf "$cleanup_path"
    fi
  done
)

stage_payload() (
  stage_bundle_root=$1
  stage_libexec_dir=$2
  stage_share_dir=$3

  for binary in $binaries; do
    install -m 0755 \
      "$stage_bundle_root/bin/$binary" \
      "$stage_libexec_dir/$binary"
  done
  cp -R "$stage_bundle_root/share/logact-oss/." "$stage_share_dir/"
  install -m 0755 "$stage_bundle_root/uninstall.sh" "$stage_share_dir/uninstall.sh"
  install -m 0644 "$stage_bundle_root/launchd.sh" "$stage_share_dir/launchd.sh"
  printf '%s\n' "$package_identity" > "$stage_libexec_dir/$ownership_marker"
  printf '%s\n' "$package_identity" > "$stage_share_dir/$ownership_marker"
)

entrypoint_target() {
  printf '%s\n' "../libexec/logact-oss/$1"
}

is_owned_entrypoint() {
  entrypoint=$1
  binary=$2
  [ -L "$entrypoint" ] &&
    [ "$(readlink "$entrypoint")" = "$(entrypoint_target "$binary")" ]
}

is_owned_package_directory() {
  package_dir=$1
  [ -d "$package_dir" ] &&
    [ ! -L "$package_dir" ] &&
    [ -f "$package_dir/$ownership_marker" ] &&
    [ "$(cat "$package_dir/$ownership_marker")" = "$package_identity" ]
}

validate_existing_installation() {
  for binary in $binaries; do
    entrypoint="$bin_dir/$binary"
    if path_exists "$entrypoint" &&
      ! is_owned_entrypoint "$entrypoint" "$binary"; then
      echo "refusing to replace unowned path: $entrypoint" >&2
      exit 1
    fi
  done
  for package_dir in "$libexec_dir" "$share_dir"; do
    if path_exists "$package_dir" &&
      ! is_owned_package_directory "$package_dir"; then
      echo "refusing to replace unowned package directory: $package_dir" >&2
      exit 1
    fi
  done
  if path_exists "$launch_agent" &&
    ! launchd_is_owned_agent "$launch_agent"; then
    echo "refusing to replace unowned or unreadable LaunchAgent: $launch_agent" >&2
    exit 1
  fi
}

backup_existing_installation() (
  source_bin_dir=$1
  source_libexec_dir=$2
  source_share_dir=$3
  destination_dir=$4

  mkdir -p "$destination_dir/bin" "$destination_dir/libexec" "$destination_dir/share"
  for binary in $binaries; do
    if path_exists "$source_bin_dir/$binary"; then
      cp -pP "$source_bin_dir/$binary" "$destination_dir/bin/$binary"
    fi
  done
  if path_exists "$source_libexec_dir"; then
    cp -RP "$source_libexec_dir" "$destination_dir/libexec/logact-oss"
  fi
  if path_exists "$source_share_dir"; then
    cp -RP "$source_share_dir" "$destination_dir/share/logact-oss"
  fi
)

activate_staged_installation() (
  source_libexec_dir=$1
  source_share_dir=$2
  destination_bin_dir=$3
  destination_libexec_dir=$4
  destination_share_dir=$5

  rm -rf "$destination_libexec_dir"
  mv "$source_libexec_dir" "$destination_libexec_dir"
  rm -rf "$destination_share_dir"
  mv "$source_share_dir" "$destination_share_dir"
  for binary in $binaries; do
    entrypoint="$destination_bin_dir/$binary"
    if path_exists "$entrypoint"; then
      if ! is_owned_entrypoint "$entrypoint" "$binary"; then
        echo "refusing to replace unowned path: $entrypoint" >&2
        return 1
      fi
    else
      ln -s "$(entrypoint_target "$binary")" "$entrypoint"
    fi
  done
)

rollback_installation() (
  destination_bin_dir=$1
  destination_libexec_dir=$2
  destination_share_dir=$3
  source_dir=$4
  current_launch_agent=$5
  current_launch_service=$6

  for binary in $binaries; do
    rm -f "$destination_bin_dir/$binary"
    if path_exists "$source_dir/bin/$binary"; then
      mv "$source_dir/bin/$binary" "$destination_bin_dir/$binary"
    fi
  done
  rm -rf "$destination_libexec_dir"
  if path_exists "$source_dir/libexec/logact-oss"; then
    mv "$source_dir/libexec/logact-oss" "$destination_libexec_dir"
  fi
  rm -rf "$destination_share_dir"
  if path_exists "$source_dir/share/logact-oss"; then
    mv "$source_dir/share/logact-oss" "$destination_share_dir"
  fi
  if [ "$bootstrap_attempted" -eq 1 ]; then
    launchd_unload "$current_launch_service" >/dev/null 2>&1
  fi
  if [ "$launch_agent_created" -eq 1 ]; then
    rm -f "$current_launch_agent"
  fi

  if [ "$service_was_loaded" -eq 1 ]; then
    if ! launchd_restart "$current_launch_service"; then
      echo "also failed to restart LaunchAgent $current_launch_service" >&2
    fi
  fi
)

cleanup() {
  cleanup_status=$?
  set +e
  trap - EXIT HUP INT QUIT PIPE TERM
  if [ "$rollback_needed" -eq 1 ]; then
    rollback_installation \
      "$bin_dir" \
      "$libexec_dir" \
      "$share_dir" \
      "$backup_dir" \
      "$launch_agent" \
      "$launch_service"
  fi
  cleanup_temporary_files \
    "$libexec_staging" \
    "$share_staging" \
    "$launch_agent_staging" \
    "$backup_dir"
  exit "$cleanup_status"
}

print_install_summary() {
  cat <<EOF
Installed LogAct OSS.

Installed files:
  binaries: $bin_dir
  plugins: $share_dir
  LaunchAgent: $launch_agent

Local state:
  database: $state_dir/logact.sqlite
  socket: $state_dir/logact.sock
  log: $state_dir/logact.log

Ensure this directory is on PATH before starting an agent client:
  $bin_dir

Register the plugin from this marketplace:
  claude plugin marketplace add "$share_dir"
  claude plugin install logact-oss@logact-oss

  codex plugin marketplace add "$share_dir"
  codex plugin add logact-oss@logact-oss

  muse plugins marketplace add logact-oss "$share_dir"
  muse plugins install logact-oss@logact-oss

The LogAct LaunchAgent starts the local server and restarts it after failures.
EOF
}

main() {
  if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
    exit 2
  fi

  bundle_root="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
  # The current release installs a per-user launchd service.
  # shellcheck disable=SC1091
  . "$bundle_root/launchd.sh"
  launchd_require_macos
  prefix="$HOME/.local"
  # Executables live in libexec; bin contains only user-facing symlinks.
  # Portable plugin data and the uninstaller live in share/logact-oss.
  bin_dir="$prefix/bin"
  libexec_parent="$prefix/libexec"
  libexec_dir="$libexec_parent/logact-oss"
  share_parent="$prefix/share"
  share_dir="$prefix/share/logact-oss"
  state_dir="$HOME/.logact-oss"
  launch_agent="$(launchd_agent_path "$HOME")"
  launch_service="$(launchd_service)"
  binaries="logact-oss-agentbus logact-oss-hook logact-oss-server"

  mkdir -p \
    "$bin_dir" \
    "$libexec_parent" \
    "$share_parent" \
    "$state_dir" \
    "$(dirname -- "$launch_agent")"
  # The socket and SQLite sidecars require a user-private parent directory.
  chmod 700 "$state_dir"
  validate_existing_installation
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
  trap cleanup EXIT
  libexec_staging="$(mktemp -d "$libexec_parent/.logact-oss-install.XXXXXX")"
  share_staging="$(mktemp -d "$share_parent/.logact-oss-install.XXXXXX")"

  stage_payload "$bundle_root" "$libexec_staging" "$share_staging"
  launch_agent_staging="$(mktemp "$prefix/.logact-oss-launch-agent.XXXXXX")"
  launchd_render_agent \
    "$bundle_root/com.facebookresearch.logact.oss.plist.in" \
    "$launch_agent_staging" \
    "$bin_dir/logact-oss-server" \
    "$state_dir"
  if path_exists "$launch_agent"; then
    if ! cmp -s "$launch_agent_staging" "$launch_agent"; then
      echo "installed LaunchAgent configuration differs; uninstall it before reinstalling" >&2
      exit 1
    fi
  else
    launch_agent_needs_install=1
  fi

  if launchd_is_loaded "$launch_service"; then
    service_was_loaded=1
  fi
  if [ "$launch_agent_needs_install" -eq 1 ] && \
      [ "$service_was_loaded" -eq 1 ]; then
    echo "LaunchAgent $launch_service is loaded without its plist; unload it before installing" >&2
    exit 1
  fi

  backup_dir="$(mktemp -d "$prefix/.logact-oss-backup.XXXXXX")"
  backup_existing_installation "$bin_dir" "$libexec_dir" "$share_dir" "$backup_dir"

  rollback_needed=1
  activate_staged_installation \
    "$libexec_staging" \
    "$share_staging" \
    "$bin_dir" \
    "$libexec_dir" \
    "$share_dir"

  if [ "$launch_agent_needs_install" -eq 1 ]; then
    launch_agent_created=1
    mv "$launch_agent_staging" "$launch_agent"
    launch_agent_staging=""
  fi

  if [ "$service_was_loaded" -eq 1 ]; then
    launchd_restart "$launch_service"
  else
    bootstrap_attempted=1
    launchd_bootstrap "$launch_agent"
  fi

  rollback_needed=0
  cleanup_temporary_files \
    "$libexec_staging" \
    "$share_staging" \
    "$launch_agent_staging" \
    "$backup_dir"
  libexec_staging=""
  share_staging=""
  launch_agent_staging=""
  backup_dir=""
  trap - EXIT HUP INT QUIT PIPE TERM

  print_install_summary
}

main "$@"
