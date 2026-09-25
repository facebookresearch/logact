# Install LogAct on macOS

## Published releases

Install the latest Apple Silicon release:

```bash
curl -fsSL \
  https://raw.githubusercontent.com/facebookresearch/logact/main/scripts/install-release.sh \
  | sh
```

Pass installer options after `sh -s --`. For example, to pin a version:

```bash
curl -fsSL \
  https://raw.githubusercontent.com/facebookresearch/logact/main/scripts/install-release.sh \
  | sh -s -- --version 0.1.0
```

The downloader verifies the release archive against its published SHA-256
checksum before running the bundled installer.

## Preview workflow artifacts

The `macOS package and release` GitHub Actions workflow builds an Apple Silicon
archive for local evaluation. It contains:

- `logact-oss-hook`
- `logact-oss-server`
- `logact-oss-agentbus`
- the Claude Code, Codex, and Muse Code plugin bundle

Download the `logact-macos` artifact from a workflow run. With the GitHub CLI:

```bash
mkdir -p /tmp/logact-preview
gh run download RUN_ID \
  --repo facebookresearch/logact \
  --name logact-macos \
  --dir /tmp/logact-preview
```

Verify, extract, and install it without administrator privileges:

```bash
cd /tmp/logact-preview
shasum -a 256 -c ./*.sha256
tar -xzf ./logact-*.tar.gz
cd ./logact-*/
./install.sh
```

LogAct installs under `$HOME/.local`. Add its binary directory to the
environment inherited by the agent clients:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

The installer prints the commands for registering the plugin with each
supported client. It also registers a per-user macOS LaunchAgent that starts
`logact-oss-server` and restarts it after failures. The service keeps its
SQLite state in `$HOME/.logact-oss/logact.sqlite` and writes diagnostics to
`$HOME/.logact-oss/logact.log`.

Install a newer preview by downloading it and running its `install.sh` again;
the installer restarts the LaunchAgent with the new binary. To unregister the
plugin from available clients, unload the LaunchAgent, and remove the installed
files while retaining the SQLite data:

```bash
"$HOME/.local/share/logact-oss/uninstall.sh"
```

These preview binaries are not Apple-notarized. If macOS quarantines an
archive downloaded through a browser, remove quarantine from the extracted
directory before running the installer:

```bash
xattr -dr com.apple.quarantine ./logact-*/
```
