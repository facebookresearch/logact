# LogAct

LogAct provides fault-tolerant logging and commit protocols for AI agents.
Agents commit their intended actions before execution so that actions can be
recovered after failures and evaluated by independent policy voters.

This repository is a reference implementation for the
[LogAct paper](https://arxiv.org/abs/2604.07988).

## Components

- `bus/`: AgentBus, an intention-vote-commit bus for coordinating agent
  actions.
- `logact/commit_service/`: the LogAct commit protocol and storage engines.
- `plugins/logact-oss/`: local LogAct hooks for Claude Code, Codex, and Muse
  Code.

## Building

Build and test the Rust workspace with Cargo:

```bash
cargo build --workspace
cargo test --workspace
```

Preview macOS packages for local evaluation are described in
[INSTALL.md](INSTALL.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to propose changes. By
participating in this project, you agree to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

LogAct is MIT licensed, as found in the [LICENSE](LICENSE) file.
