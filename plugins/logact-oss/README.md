# LogAct OSS plugin

This plugin records Claude Code, Codex, and Muse Code lifecycle events in a
local LogAct service. `PreToolUse` events pass through the CommitService policy
engine before the client executes the tool.

## Prerequisites

Build or install these binaries and ensure they are on `PATH`:

- `logact-oss-server`, which owns the local CommitService and SQLite state
- `logact-oss-hook`, which handles hook events
- `logact-oss-agentbus`, which supports the bundled AgentBus inspection skill

The plugin defaults to `$HOME/.logact-oss` and connects to a running
`logact-oss-server` over `$HOME/.logact-oss/logact.sock`. The server writes
diagnostics to `$HOME/.logact-oss/logact.log`.

Set `LOGACT_OSS_SOCKET` and `LOGACT_OSS_SQLITE_PATH` before starting the agent
client to override those paths. Some clients sanitize hook environments, so
the default paths are the most portable choice for this initial version.

## Installation

After cloning the repository, add it as a local marketplace and install the
plugin for Claude Code:

```bash
claude plugin marketplace add /path/to/logact
claude plugin install logact-oss@logact-oss
```

For Codex:

```bash
codex plugin marketplace add /path/to/logact
codex plugin add logact-oss@logact-oss
```

For Muse Code:

```bash
muse plugins marketplace add logact-oss /path/to/logact
muse plugins install logact-oss@logact-oss
```

Hook failures return a diagnostic and a non-blocking failure status to the
client. The current local policy allows intentions by default.
