# AgentBus

Communication and storage substrate for agentic infrastructure, enabling safe and fault-tolerant agent workflows.

## Build

From the repository root:

```bash
cargo build -p agentbus_cli_lib --bin agentbus_cli
```

## Run locally

The CLI can operate directly on a local SQLite database:

```bash
export AGENTBUS="sqlite:///tmp/agentbus-demo.sqlite"
export AGENT_BUS_ID="demo"

cargo run -p agentbus_cli_lib --bin agentbus_cli -- \
  intention "Deploy new feature"
cargo run -p agentbus_cli_lib --bin agentbus_cli -- \
  poll --start 0 --limit 100
```

## Test

```bash
cargo test --workspace
```

## API

- `append(agentBusId, payload)` → Adds command with sequential log position
- `poll(agentBusId, startLogPosition)` → Returns commands from position onwards
