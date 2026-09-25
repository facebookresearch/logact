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
  echo "usage: $0 DIST_DIR" >&2
}

assert_installed_payload() {
  install_prefix=$1

  for binary in logact-oss-agentbus logact-oss-hook logact-oss-server; do
    test -L "$install_prefix/bin/$binary"
    test "$(readlink "$install_prefix/bin/$binary")" = \
      "../libexec/logact-oss/$binary"
    test -x "$install_prefix/libexec/logact-oss/$binary"
  done
  test "$(cat "$install_prefix/libexec/logact-oss/.logact-install-owner")" = \
    "facebookresearch/logact"
  test "$(cat "$install_prefix/share/logact-oss/.logact-install-owner")" = \
    "facebookresearch/logact"
  test -f "$install_prefix/share/logact-oss/.claude-plugin/marketplace.json"
  test -f "$install_prefix/share/logact-oss/plugins/logact-oss/.claude-plugin/plugin.json"
  test -f "$install_prefix/share/logact-oss/plugins/logact-oss/.codex-plugin/plugin.json"
  test -f "$install_prefix/share/logact-oss/plugins/logact-oss/.muse-plugin/plugin.json"
  test -f "$install_prefix/share/logact-oss/launchd.sh"
}

assert_launch_agent() {
  launch_agent=$1
  install_prefix=$2
  home=$3

  /usr/bin/plutil -lint "$launch_agent"
  python3 - "$launch_agent" "$install_prefix" "$home" <<'PY'
import plistlib
import sys

launch_agent, prefix, home = sys.argv[1:]
with open(launch_agent, "rb") as source:
    plist = plistlib.load(source)

expected_arguments = [
    f"{prefix}/bin/logact-oss-server",
    "--socket",
    f"{home}/.logact-oss/logact.sock",
    "--sqlite-path",
    f"{home}/.logact-oss/logact.sqlite",
]
assert plist["ProgramArguments"] == expected_arguments, plist["ProgramArguments"]
expected_log = f"{home}/.logact-oss/logact.log"
assert plist["StandardOutPath"] == expected_log, plist["StandardOutPath"]
assert plist["StandardErrorPath"] == expected_log, plist["StandardErrorPath"]
PY
}

create_test_launchctl() {
  destination_dir=$1

  mkdir "$destination_dir"
  cat > "$destination_dir/launchctl" <<'SH'
#!/bin/sh
state_dir="$(dirname -- "$0")"
case "$1" in
  print) test -e "$state_dir/loaded" ;;
  bootstrap)
    : > "$state_dir/loaded"
    if [ -e "$state_dir/fail-bootstrap" ]; then
      rm "$state_dir/fail-bootstrap"
      exit 1
    fi
    : > "$state_dir/bootstrap-called"
    ;;
  kickstart)
    if [ -e "$state_dir/fail-kickstart" ]; then
      rm "$state_dir/fail-kickstart"
      exit 1
    fi
    : > "$state_dir/kickstart-called"
    ;;
  bootout)
    rm -f "$state_dir/loaded"
    : > "$state_dir/bootout-called"
    ;;
  *) exit 2 ;;
esac
SH
  chmod 755 "$destination_dir/launchctl"
}

create_fake_agent_clients() {
  destination_dir=$1

  mkdir "$destination_dir"
  cat > "$destination_dir/client" <<'SH'
#!/bin/sh
printf '%s %s\n' "$(basename "$0")" "$*" >> "$LOGACT_TEST_PLUGIN_LOG"
SH
  chmod 755 "$destination_dir/client"
  for client in claude codex muse; do
    ln -s client "$destination_dir/$client"
  done
}

assert_plugin_unregistration() {
  log_file=$1
  expected_file=$2

  cat > "$expected_file" <<'EOF'
claude plugin uninstall logact-oss@logact-oss
claude plugin marketplace remove logact-oss
codex plugin remove logact-oss@logact-oss
codex plugin marketplace remove logact-oss
muse plugins remove logact-oss
muse plugins marketplace remove logact-oss
EOF
  cmp -s "$expected_file" "$log_file"
}

assert_rollback() {
  install_prefix=$1
  checksum_file=$2

  test -L "$install_prefix/bin/logact-oss-agentbus"
  test "$(readlink "$install_prefix/bin/logact-oss-agentbus")" = \
    "../libexec/logact-oss/logact-oss-agentbus"
  shasum -a 256 -c "$checksum_file"
}

assert_uninstalled() {
  install_prefix=$1
  home=$2
  launch_agent=$3

  for binary in logact-oss-agentbus logact-oss-hook logact-oss-server; do
    test ! -e "$install_prefix/bin/$binary"
    test ! -L "$install_prefix/bin/$binary"
  done
  test ! -e "$install_prefix/libexec/logact-oss"
  test ! -e "$install_prefix/share/logact-oss"
  test -d "$home/.logact-oss"
  test ! -e "$launch_agent"
  test ! -L "$launch_agent"
}

assert_unowned_directory_rejected() (
  home=$1
  bundle=$2
  package_dir=$3

  mkdir -p "$package_dir"
  printf '%s\n' untouched > "$package_dir/foreign-file"
  if HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly replaced an unowned package directory" >&2
    exit 1
  fi
  test "$(cat "$package_dir/foreign-file")" = untouched
  rm -rf "$package_dir"
)

main() {
  if [ "$#" -ne 1 ]; then
    usage
    exit 2
  fi

  dist_dir=$1
  (cd "$dist_dir" && shasum -a 256 -c ./*.sha256)
  set -- "$dist_dir"/*.tar.gz
  test "$#" -eq 1

  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 131' QUIT
  trap 'exit 141' PIPE
  trap 'exit 143' TERM
  trap cleanup EXIT
  work_dir="$(mktemp -d)"
  home="$work_dir/home"
  mkdir -p "$home"
  tar -xzf "$1" -C "$work_dir"
  set -- "$work_dir"/logact-*
  test "$#" -eq 1

  bundle=$1
  install_prefix="$home/.local"
  launch_agent="$home/Library/LaunchAgents/com.facebookresearch.logact.oss.plist"
  launchctl_dir="$work_dir/fake-bin"
  create_test_launchctl "$launchctl_dir"
  launchctl_bin="$launchctl_dir/launchctl"

  assert_unowned_directory_rejected \
    "$home" "$bundle" "$install_prefix/libexec/logact-oss"
  assert_unowned_directory_rejected \
    "$home" "$bundle" "$install_prefix/share/logact-oss"

  mkdir -p "$install_prefix/bin"
  cp "$bundle/bin/logact-oss-agentbus" "$install_prefix/bin/logact-oss-agentbus"
  if LOGACT_TEST_SKIP_LAUNCHCTL=1 HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly replaced an unowned executable" >&2
    exit 1
  fi
  cmp \
    "$bundle/bin/logact-oss-agentbus" \
    "$install_prefix/bin/logact-oss-agentbus"
  rm "$install_prefix/bin/logact-oss-agentbus"

  : > "$launchctl_dir/loaded"
  if LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly reused a loaded job without a plist" >&2
    exit 1
  fi
  rm "$launchctl_dir/loaded"

  : > "$launchctl_dir/fail-bootstrap"
  if LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly succeeded after a failed bootstrap" >&2
    exit 1
  fi
  test -f "$launchctl_dir/bootout-called"
  test ! -e "$launchctl_dir/loaded"
  assert_uninstalled "$install_prefix" "$home" "$launch_agent"
  rm "$launchctl_dir/bootout-called"

  LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"
  test -f "$launchctl_dir/bootstrap-called"
  assert_installed_payload "$install_prefix"
  assert_launch_agent "$launch_agent" "$install_prefix" "$home"

  LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"
  test -f "$launchctl_dir/kickstart-called"
  rm "$launchctl_dir/kickstart-called"

  rm "$launchctl_dir/loaded"
  : > "$launchctl_dir/fail-bootstrap"
  if LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly succeeded after rebootstrap failed" >&2
    exit 1
  fi
  test -f "$launchctl_dir/bootout-called"
  test ! -e "$launchctl_dir/loaded"
  assert_installed_payload "$install_prefix"
  rm "$launchctl_dir/bootout-called"
  LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"

  cp "$launch_agent" "$work_dir/launch-agent.plist"
  /usr/bin/plutil -replace StandardOutPath \
    -string "$home/.logact-oss/original.log" \
    "$launch_agent"
  if LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly accepted a changed LaunchAgent" >&2
    exit 1
  fi
  cp "$work_dir/launch-agent.plist" "$launch_agent"

  shasum -a 256 \
    "$install_prefix/bin/logact-oss-agentbus" \
    "$install_prefix/bin/logact-oss-hook" \
    "$install_prefix/bin/logact-oss-server" \
    "$launch_agent" \
    > "$work_dir/installed.sha256"

  printf '#!/bin/sh\nexit 0\n' > "$bundle/bin/logact-oss-agentbus"
  chmod 755 "$bundle/bin/logact-oss-agentbus"
  : > "$launchctl_dir/fail-kickstart"
  if LOGACT_LAUNCHCTL="$launchctl_bin" HOME="$home" "$bundle/install.sh"; then
    echo "installer unexpectedly succeeded with a failing launchctl" >&2
    exit 1
  fi
  assert_rollback "$install_prefix" "$work_dir/installed.sha256"

  printf '%s\n' unowned > \
    "$install_prefix/share/logact-oss/.logact-install-owner"
  if LOGACT_TEST_SKIP_LAUNCHCTL=1 HOME="$home" \
    "$install_prefix/share/logact-oss/uninstall.sh"; then
    echo "uninstaller unexpectedly removed an unowned package directory" >&2
    exit 1
  fi
  test -d "$install_prefix/libexec/logact-oss"
  test -d "$install_prefix/share/logact-oss"
  printf '%s\n' facebookresearch/logact > \
    "$install_prefix/share/logact-oss/.logact-install-owner"

  /usr/bin/plutil -replace Label \
    -string com.example.unowned \
    "$launch_agent"
  if LOGACT_TEST_SKIP_LAUNCHCTL=1 HOME="$home" \
    "$install_prefix/share/logact-oss/uninstall.sh"; then
    echo "uninstaller unexpectedly removed an unowned LaunchAgent" >&2
    exit 1
  fi
  test -f "$launch_agent"
  /usr/bin/plutil -replace Label \
    -string com.facebookresearch.logact.oss \
    "$launch_agent"
  create_fake_agent_clients "$work_dir/fake-agent-clients"
  LOGACT_LAUNCHCTL="$launchctl_bin" \
    LOGACT_TEST_PLUGIN_LOG="$work_dir/plugin-unregister.log" \
    PATH="$work_dir/fake-agent-clients:$PATH" \
    HOME="$home" \
    "$install_prefix/share/logact-oss/uninstall.sh"
  assert_plugin_unregistration \
    "$work_dir/plugin-unregister.log" \
    "$work_dir/expected-plugin-unregister.log"
  test -f "$launchctl_dir/bootout-called"
  test ! -e "$launchctl_dir/loaded"
  assert_uninstalled "$install_prefix" "$home" "$launch_agent"
}

main "$@"
