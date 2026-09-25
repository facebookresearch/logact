#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -eu

work_dir=""
cleanup_status=""
version="latest"
platform=""
arch=""
repository=""
release_base_url=""
tag=""
archive_name=""
checksum_name=""
bundle_dir=""

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
  echo "usage: $0 [--version VERSION]" >&2
}

parse_arguments() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --version)
        shift
        if [ "$#" -eq 0 ]; then
          usage
          exit 2
        fi
        version="$1"
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        usage
        exit 2
        ;;
    esac
    shift
  done
}

is_rosetta_translated() {
  [ "$(/usr/sbin/sysctl -in sysctl.proc_translated 2>/dev/null)" = "1" ]
}

detect_platform() {
  case "$(uname -s)" in
    Darwin) platform="macos" ;;
    *)
      echo "LogAct release installation currently supports macOS only" >&2
      exit 1
      ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch="arm64" ;;
    x86_64)
      # uname reports the translated process architecture under Rosetta.
      if is_rosetta_translated; then
        arch="arm64"
      else
        echo "LogAct releases currently support Apple Silicon only" >&2
        exit 1
      fi
      ;;
    *)
      echo "LogAct releases currently support Apple Silicon only" >&2
      exit 1
      ;;
  esac
}

validate_version() {
  case "$version" in
    ""|*[!A-Za-z0-9._-]*)
      echo "invalid LogAct release version: $version" >&2
      exit 2
      ;;
  esac
}

resolve_release() {
  repository="${LOGACT_GITHUB_REPOSITORY:-facebookresearch/logact}"
  release_base_url="${LOGACT_RELEASE_BASE_URL:-}"
  if [ "$version" = "latest" ]; then
    if [ -n "$release_base_url" ]; then
      echo "--version is required with LOGACT_RELEASE_BASE_URL" >&2
      exit 2
    fi
    latest_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' \
      "https://github.com/$repository/releases/latest")"
    latest_url="${latest_url%/}"
    tag="${latest_url##*/}"
    case "$tag" in
      v*) version="${tag#v}" ;;
      *)
        echo "could not determine the latest LogAct release" >&2
        exit 1
        ;;
    esac
  else
    case "$version" in
      v*)
        tag="$version"
        version="${version#v}"
        ;;
      *) tag="v$version" ;;
    esac
  fi

  validate_version
  if [ -z "$release_base_url" ]; then
    release_base_url="https://github.com/$repository/releases/download/$tag"
  fi
  archive_name="logact-$version-$platform-$arch.tar.gz"
  checksum_name="$archive_name.sha256"
}

download_release() {
  curl -fsSL "$release_base_url/$archive_name" -o "$work_dir/$archive_name"
  curl -fsSL "$release_base_url/$checksum_name" -o "$work_dir/$checksum_name"
}

verify_checksum() {
  if command -v shasum >/dev/null 2>&1; then
    (cd "$work_dir" && shasum -a 256 -c "$checksum_name")
  elif command -v sha256sum >/dev/null 2>&1; then
    (cd "$work_dir" && sha256sum -c "$checksum_name")
  else
    echo "neither shasum nor sha256sum is available" >&2
    exit 1
  fi
}

extract_bundle() {
  extract_dir="$work_dir/extracted"
  mkdir "$extract_dir"
  tar -xzf "$work_dir/$archive_name" -C "$extract_dir"
  set -- "$extract_dir"/logact-*
  if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
    echo "release archive must contain exactly one LogAct bundle" >&2
    exit 1
  fi
  bundle_dir="$1"
}

main() {
  parse_arguments "$@"
  detect_platform
  resolve_release

  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
  trap cleanup EXIT
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/logact-install.XXXXXX")"

  download_release
  verify_checksum
  extract_bundle
  "$bundle_dir/install.sh"
}

main "$@"
