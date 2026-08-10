# agent-sync

`agent-sync` safely synchronizes coding-agent sessions and memories between a
local machine and an SSH peer. It ships as one local Rust binary; the peer does
not need agent-sync installed.

Built-in adapters:

- **Codex**: rollouts, archives, history/index, catalog timestamps, and memory.
- **Claude Code**: sessions, subagents, tool results, and project memory.

The default mode is a read-only preview. No credentials, settings, plugins,
caches, lock files, or SQLite databases are copied between machines.

## Usage

```console
agent-sync sync codex mini
agent-sync sync claude mini --only sessions
agent-sync sync codex mini --only memory --apply
agent-sync sync claude mini --apply --yes
agent-sync doctor codex mini
agent-sync adapters
```

`--apply` asks for the exact word `apply` in a terminal. Non-interactive use
requires both `--apply --yes`. Content divergence is never resolved by `--yes`.

The default resource set is `sessions` plus `memory`; select one with
`--only sessions` or `--only memory`. Add `--format json` for machine-readable
plans.

## Configuration

Configuration is optional. The default path is
`~/.config/agent-sync/config.toml`; override it with `--config` or
`AGENT_SYNC_CONFIG`.

```toml
version = 1

[peers.mini]
host = "mini"

[agents.codex]
local_root = "~/.codex"

[agents.claude]
local_root = "~/.claude"

[peers.mini.roots]
codex = ".codex"
claude = ".claude"
```

Precedence is CLI, configuration, then adapter defaults.

## Safety model

- Remote reads use an explicit rsync allowlist.
- JSONL identity, ordering, timestamps, and append relationships are validated.
- Same-path divergence blocks apply instead of guessing a winner.
- Active writers are detected before writes; Codex coordination locks are held
  through the file transaction.
- Both sides are backed up before mutation and verified against the staged
  SHA-256 manifest after transfer.
- Session mtimes come from the last event rather than transfer time.

The binary uses the existing SSH configuration and does not weaken host-key
checking. Runtime dependencies are `ssh`, `rsync`, `tar`, and `python3` on both
machines; Claude writer detection also needs `lsof`, and Codex catalog repair
needs a compatible `codex app-server`.

## Development

```console
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the adapter boundary and invariants.

## 中文快速开始

默认命令只生成计划，不写文件：

```console
agent-sync sync codex mini
agent-sync sync claude mini --only memory
```

正式写入使用 `--apply`；脚本环境必须同时使用 `--apply --yes`。任何 session
分叉、损坏数据或未解决的 memory 冲突都会阻止写入。

## License

MIT
