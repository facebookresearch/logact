#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

verification_dir=""
cleanup_status=""
declare -a release_assets=()

usage() {
  echo "usage: $0 TAG ASSET_DIRECTORY" >&2
}

fail() {
  echo "$*" >&2
  exit 1
}

cleanup() {
  cleanup_status=$?
  set +e
  trap - EXIT HUP INT QUIT PIPE TERM
  if [[ -n "$verification_dir" ]]; then
    rm -rf "$verification_dir"
  fi
  exit "$cleanup_status"
}

collect_assets() {
  local -a archives=()
  local archive

  shopt -s nullglob
  archives=("$asset_dir"/*.tar.gz)
  shopt -u nullglob
  [[ "${#archives[@]}" -gt 0 ]] ||
    fail "no packaged archives found in $asset_dir"

  release_assets=()
  for archive in "${archives[@]}"; do
    [[ -f "$archive.sha256" ]] || fail "missing checksum $archive.sha256"
    release_assets+=("$archive" "$archive.sha256")
  done
}

expected_prerelease() {
  case "$tag" in
    *-*) printf '%s\n' true ;;
    *) printf '%s\n' false ;;
  esac
}

verify_release_metadata() {
  local release_json="$1"
  local actual_prerelease

  actual_prerelease="$(jq -r '.isPrerelease' <<<"$release_json")"
  [[ "$actual_prerelease" == "$prerelease" ]] ||
    fail "release prerelease state does not match tag $tag"
}

is_expected_asset() {
  local asset
  local name="$1"

  for asset in "${release_assets[@]}"; do
    if [[ "$(basename -- "$asset")" == "$name" ]]; then
      return 0
    fi
  done
  return 1
}

remove_unexpected_draft_assets() {
  local asset_name
  local asset_names="$verification_dir/draft-assets"

  gh release view "$tag" \
    --repo "$repo" \
    --json assets \
    --jq '.assets[].name' \
    > "$asset_names"
  while IFS= read -r asset_name; do
    if ! is_expected_asset "$asset_name"; then
      gh release delete-asset "$tag" "$asset_name" \
        --repo "$repo" \
        --yes
    fi
  done < "$asset_names"
}

verify_release_assets() {
  local actual_names="$verification_dir/actual-assets"
  local expected_names="$verification_dir/expected-assets"
  local local_asset
  local remote_asset

  : > "$expected_names"
  for local_asset in "${release_assets[@]}"; do
    basename -- "$local_asset" >> "$expected_names"
  done
  LC_ALL=C sort -o "$expected_names" "$expected_names"

  gh release view "$tag" \
    --repo "$repo" \
    --json assets \
    --jq '.assets[].name' \
    | LC_ALL=C sort > "$actual_names"

  if ! cmp -s "$expected_names" "$actual_names"; then
    echo "release assets do not match the packaged assets:" >&2
    diff -u "$expected_names" "$actual_names" >&2 || true
    fail "published release has unexpected assets for tag $tag"
  fi

  rm -rf "$verification_dir/download"
  mkdir -p "$verification_dir/download"
  gh release download "$tag" \
    --repo "$repo" \
    --dir "$verification_dir/download"

  for local_asset in "${release_assets[@]}"; do
    remote_asset="$verification_dir/download/$(basename -- "$local_asset")"
    cmp -s "$local_asset" "$remote_asset" ||
      fail "published asset differs from packaged asset: $(basename -- "$local_asset")"
  done

  if command -v shasum >/dev/null 2>&1; then
    (cd "$verification_dir/download" && shasum -a 256 -c ./*.sha256)
  else
    (cd "$verification_dir/download" && sha256sum -c ./*.sha256)
  fi
}

read_release() {
  local owner="${repo%%/*}"
  local name="${repo#*/}"
  local release_json

  # GraphQL variables are interpreted by GitHub after `gh` binds them.
  # shellcheck disable=SC2016
  release_json="$(gh api graphql \
    -F owner="$owner" \
    -F name="$name" \
    -F tag="$tag" \
    -f query='query($owner: String!, $name: String!, $tag: String!) {
      repository(owner: $owner, name: $name) {
        release(tagName: $tag) {
          isDraft
          isPrerelease
        }
      }
    }' \
    --jq '.data.repository.release')" || return 2

  [[ "$release_json" != null ]] || return 1
  printf '%s\n' "$release_json"
}

create_draft_release() {
  local release_flags=(--draft)

  if [[ "$prerelease" == true ]]; then
    release_flags+=(--prerelease)
  fi

  gh release create "$tag" \
    --repo "$repo" \
    --verify-tag \
    --generate-notes \
    --title "LogAct $tag" \
    "${release_flags[@]}"
}

main() {
  if [[ "$#" -ne 2 ]]; then
    usage
    exit 2
  fi

  tag="$1"
  asset_dir="$2"
  repo="${GH_REPO:?GH_REPO must identify the GitHub repository}"
  prerelease="$(expected_prerelease)"
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
  trap cleanup EXIT
  verification_dir="$(mktemp -d)"

  collect_assets

  local read_status
  local release_json
  if release_json="$(read_release)"; then
    verify_release_metadata "$release_json"
    if [[ "$(jq -r '.isDraft' <<<"$release_json")" == false ]]; then
      verify_release_assets
      echo "Release $tag is already published with matching assets."
      return
    fi
  else
    read_status=$?
    [[ "$read_status" -eq 1 ]] || return "$read_status"
    create_draft_release
  fi

  remove_unexpected_draft_assets
  gh release upload "$tag" \
    --repo "$repo" \
    --clobber \
    "${release_assets[@]}"
  verify_release_assets
  gh release edit "$tag" --repo "$repo" --draft=false
  echo "Published release $tag."
}

main "$@"
