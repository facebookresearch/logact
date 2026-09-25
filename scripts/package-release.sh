#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -eu

work_dir=""
cleanup_status=""

cleanup() {
  cleanup_status=$?
  set +e
  trap - EXIT HUP INT QUIT PIPE TERM
  if [ -n "$work_dir" ]; then
    rm -rf "$work_dir"
  fi
  exit "$cleanup_status"
}

usage() {
  echo "usage: $0 VERSION" >&2
}

validate_version() {
  case "$1" in
    ""|[!A-Za-z0-9]*|*[!A-Za-z0-9._-]*)
      echo "version must start with a letter or digit and contain only letters, digits, dots, underscores, and hyphens" >&2
      exit 2
      ;;
  esac
}

determine_platform() {
  if [ -n "${LOGACT_PACKAGE_PLATFORM:-}" ]; then
    platform="$LOGACT_PACKAGE_PLATFORM"
  else
    case "$(uname -s)" in
      Darwin) platform="macos" ;;
      *)
        echo "LogAct packages currently support macOS only" >&2
        exit 1
        ;;
    esac
  fi

  if [ "$platform" != "macos" ]; then
    echo "LogAct packages currently support macOS only" >&2
    exit 1
  fi
  printf '%s\n' "$platform"
}

determine_architecture() {
  if [ -n "${LOGACT_PACKAGE_ARCH:-}" ]; then
    printf '%s\n' "$LOGACT_PACKAGE_ARCH"
    return
  fi

  case "$(uname -m)" in
    arm64|aarch64) printf '%s\n' "arm64" ;;
    x86_64|amd64) printf '%s\n' "x86_64" ;;
    *)
      echo "unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

copy_binaries() (
  source_dir="$1"
  destination_dir="$2"

  install_binary() {
    source_name=$1
    destination_name=$2
    source_path="$source_dir/$source_name"
    if [ ! -x "$source_path" ]; then
      echo "missing executable: $source_path" >&2
      exit 1
    fi
    install -m 0755 "$source_path" "$destination_dir/$destination_name"
  }

  install_binary agentbus_cli logact-oss-agentbus
  install_binary logact_commit_cli logact-oss-hook
  install_binary logact_local_server logact-oss-server
)

copy_release_files() (
  source_root="$1"
  source_script_dir="$2"
  destination_dir="$3"

  cp -R "$source_root/.claude-plugin" "$destination_dir/share/logact-oss/"
  cp -R "$source_root/plugins" "$destination_dir/share/logact-oss/"
  install -m 0755 "$source_script_dir/install.sh" "$destination_dir/install.sh"
  install -m 0755 "$source_script_dir/uninstall.sh" "$destination_dir/uninstall.sh"
  install -m 0644 "$source_script_dir/launchd.sh" "$destination_dir/launchd.sh"
  install -m 0644 \
    "$source_script_dir/com.facebookresearch.logact.oss.plist.in" \
    "$destination_dir/com.facebookresearch.logact.oss.plist.in"
  cp "$source_root/INSTALL.md" "$source_root/LICENSE" "$source_root/README.md" \
    "$destination_dir/"
)

write_checksum() (
  checksum_output_dir="$1"
  checksum_archive="$2"
  checksum_archive_name="$(basename -- "$checksum_archive")"

  if command -v shasum >/dev/null 2>&1; then
    (cd "$checksum_output_dir" && shasum -a 256 "$checksum_archive_name" > "$checksum_archive_name.sha256")
  elif command -v sha256sum >/dev/null 2>&1; then
    (cd "$checksum_output_dir" && sha256sum "$checksum_archive_name" > "$checksum_archive_name.sha256")
  else
    echo "neither shasum nor sha256sum is available" >&2
    exit 1
  fi
)

main() {
  if [ "$#" -ne 1 ]; then
    usage
    exit 2
  fi

  version="$1"
  validate_version "$version"

  script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
  repo_root="$(CDPATH='' cd -- "$script_dir/.." && pwd)"
  binary_dir="${LOGACT_BINARY_DIR:-${CARGO_TARGET_DIR:-$repo_root/target}/release}"
  output_dir="${LOGACT_DIST_DIR:-$repo_root/dist}"
  platform="$(determine_platform)"
  arch="$(determine_architecture)"

  package_name="logact-$version-$platform-$arch"
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
  trap cleanup EXIT
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/logact-package.XXXXXX")"
  package_dir="$work_dir/$package_name"

  mkdir -p "$package_dir/bin" "$package_dir/share/logact-oss" "$output_dir"
  copy_binaries "$binary_dir" "$package_dir/bin"
  copy_release_files "$repo_root" "$script_dir" "$package_dir"
  printf '%s\n' "$version" > "$package_dir/VERSION"

  archive="$output_dir/$package_name.tar.gz"
  tar -C "$work_dir" -czf "$archive" "$package_name"
  write_checksum "$output_dir" "$archive"

  echo "Created $archive"
  echo "Created $archive.sha256"
}

main "$@"
