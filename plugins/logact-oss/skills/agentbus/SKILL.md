---
name: agentbus
description: Inspect the AgentBus event history recorded by the local LogAct plugin for the current or a specified agent session.
---

# AgentBus

Use a complete bus ID supplied by the user verbatim. Otherwise, construct the
bus ID for the current harness session as follows:

- Claude Code: `claude_code.${USER}.<SESSION_ID>`
- Codex: `codex.${USER}.<SESSION_ID>`
- Muse Code: `muse.${USER}.<SESSION_ID>`

For a subagent, append `.agent.<AGENT_ID>` to the parent bus ID. If the session
ID is unavailable, ask the user for it rather than guessing.

The local server and `logact-oss-agentbus` must use the same SQLite database.
Use `LOGACT_OSS_SQLITE_PATH` when set, otherwise use the plugin's default
database:

```bash
DATABASE="${LOGACT_OSS_SQLITE_PATH:-$HOME/.logact-oss/logact.sqlite}"
logact-oss-agentbus \
  --agentbus "sqlite://$DATABASE" \
  --agent-bus-id "$BUS_ID" \
  poll --start 0 --limit 200
```

If the command returns 200 entries, continue from the position after the last
entry until a page contains fewer than 200 entries.

Correlate `vote`, `commit`, and `abort` entries with their `intention` using
`intention_id`. Use `agent_input`, `action_output`, and `agent_output` for the
surrounding prompt, tool result, and turn output. Report the relevant sequence
and exact denial or error reasons without dumping full payloads unless the user
asks for raw output.
