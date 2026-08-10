# agent-sync

`agent-sync` safely synchronizes coding-agent sessions and memories between a
local machine and an SSH peer. It ships as one local Rust binary and
automatically bootstraps a private, versioned copy of itself on a compatible
peer.

Built-in adapters:

- **Codex**: rollouts, archives, history/index, catalog timestamps, and memory.
- **Claude Code**: sessions, subagents, tool results, and project memory.

The default mode is a read-only preview. No credentials, settings, plugins,
caches, lock files, or SQLite databases are copied between machines.

## Installation

Install the latest release from [crates.io](https://crates.io/crates/agent-sync):

```console
cargo install agent-sync --locked
```

This requires Rust 1.85 or newer. Prebuilt binaries for macOS and Linux on
arm64 and x86_64 are also available from
[GitHub Releases](https://github.com/lidongpeng36/agent-sync/releases/latest),
with a SHA-256 checksum beside every archive.

To build the current source instead:

```console
git clone https://github.com/lidongpeng36/agent-sync.git
cd agent-sync
cargo install --path . --locked
```

`ssh` and `rsync` must be available locally. Claude writer detection also uses
`lsof` on both peers, while Codex catalog repair uses `codex app-server` on both
peers.

## Usage

```console
agent-sync sync codex mini
agent-sync sync claude mini --only sessions
agent-sync sync claude mini -s ask
agent-sync s claude -f diff > claude-sync.diff
agent-sync sync codex mini --only memory --apply
agent-sync sync claude mini --apply --yes
agent-sync doctor codex mini
agent-sync adapters
```

`--apply` asks `Apply these changes? [Y/n]` in a terminal; Enter accepts the
default `yes`. Non-interactive use requires both `--apply --yes`; `--yes`
confirms the staged plan but does not change its conflict strategy.

The default resource set is `sessions` plus `memory`; select one with
`--only sessions` or `--only memory`. Add `--format json` for machine-readable
plans. Safe unions, linear advances, Markdown block merges, and deterministic
session forks are always automatic. Irreconcilable Claude conflicts use the
configured `ask` strategy by default; override it per invocation with `-s ask`,
`-s local`, or `-s remote` (the long form is `--conflict-strategy`). The legacy
value `merge` remains accepted as an alias for `ask`.

The `sync`, `doctor`, and `adapters` commands have the aliases `s`, `d`, and
`a`. Common options also have short forms: `-o` (`--only`), `-a` (`--apply`),
`-y` (`--yes`), `-f` (`--format`), and `-s` (`--conflict-strategy`). Run a
subcommand with `--help` for the complete list.

The normal human preview uses shortened `project/session` names and a table with
`LOCAL`, `REMOTE`, and `RESULT` columns. Side symbols describe the operation
needed to reach the staged result: `=` unchanged, `+` create, `↻` update
content, `~` metadata only, and `−` remove. Result symbols describe where the
staged content came from: `L` local, `R` remote, `M` merged, `=` identical,
`✦` generated, and `?` requiring a choice. In particular, `↻` is an overwrite
operation on that side, not a merge.

When automatic merge cannot safely resolve a conflict, the preview reports
`action required` and prints commands to inspect the full diff, choose each
conflict interactively, or apply local/remote policy to all conflicts. JSON
output retains full paths, explicit action names, and SHA-256 values for tools.
`--format diff` emits complete, untruncated unified content diffs from both
`local` and `remote` to the staged result; metadata-only changes are emitted as
comments. Diff output can contain complete session and memory content, so treat
it as potentially sensitive.

## Configuration

Configuration is optional. The default path is
`~/.config/agent-sync/config.toml`; override it with `--config` or
`AGENT_SYNC_CONFIG`.

```toml
version = 1
# Optional: lets commands omit the positional peer.
default_peer = "mini"
# Optional global default; defaults to "ask" when omitted.
conflict_strategy = "ask"

[peers.mini]
host = "mini"

[agents.codex]
local_root = "~/.codex"

[agents.claude]
local_root = "~/.claude"
# Optional agent-specific override:
# conflict_strategy = "local"

[peers.mini.roots]
codex = ".codex"
claude = ".claude"
```

Conflict-strategy precedence is CLI, agent-specific configuration, global
configuration, then the built-in `ask` default. Other options use CLI,
configuration, then adapter defaults.
When `default_peer` is omitted, `<PEER>` remains required. With it configured,
`agent-sync sync claude` and `agent-sync doctor claude` use that peer; an
explicit positional peer always overrides the default.

The recommended argument order is `agent-sync <command> <agent> [peer]
[options]`. Options may appear before, between, or after positional arguments,
but the documented order is easier to read and copy.

For Claude, `ask` is the default conflict strategy. All three strategies first
take the safe union, choose the longer strict session prefix, merge independent
Markdown heading blocks, and preserve a true session divergence under a
deterministic fork UUID. The strategy is consulted only for conflicting memory
blocks or index descriptions: `local` and `remote` select that side for the
conflicting blocks, while `ask` offers local, remote, `$EDITOR`, or quit during
an interactive apply. Editor changes are staged and validated before the final
`[Y/n]` confirmation.

The editor choice opens the conflicted memory file and a `MEMORY-entry.md`
companion in a private temporary directory, using `$VISUAL`, then `$EDITOR`,
then `vi`. Conflicting regions use `<<<<<<< LOCAL`, `=======`, and
`>>>>>>> REMOTE <peer>` markers. All markers must be removed; the memory must
retain non-empty `name` and `description` frontmatter, and the index entry must
link to the memory file exactly once. Real local and remote files remain
unchanged until the resolved plan is shown and `[Y/n]` is confirmed.

## Safety model

- Remote reads use an explicit rsync allowlist.
- JSONL identity, ordering, timestamps, and append relationships are validated.
- Safe same-path changes are merged automatically. Ambiguous memory blocks
  follow the explicit conflict strategy, while Claude session forks are always
  preserved as separate UUIDs.
- Active writers are detected before writes; Codex coordination locks are held
  through the file transaction.
- Both sides are backed up before mutation and verified against the staged
  SHA-256 manifest after transfer.
- Session mtimes come from the last event rather than transfer time.
- Remote filesystem, lock, backup, mtime, and SQLite operations use a versioned
  typed protocol implemented by the same Rust binary; no Python source is sent
  to the peer.

The binary uses the existing SSH configuration and does not weaken host-key
checking. It invokes OpenSSH so aliases, ProxyJump, ControlMaster, ssh-agent,
and `known_hosts` continue to work. The helper is checksum-verified and stored
with private permissions below
`~/.cache/agent-sync/remotes/<version>/agent-sync`; bootstrapping currently
requires the peer to have the same OS and CPU architecture as the local binary.

Runtime dependencies are `ssh` and `rsync` locally. Claude writer detection
also needs `lsof` on both machines, and Codex catalog repair needs a compatible
`codex app-server` on both machines. Backup creation, timestamp handling,
locking, and SQLite maintenance are implemented in Rust and do not require
`tar` or `python3`.

## Development

```console
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the adapter boundary and invariants.

## 中文快速开始

使用 Cargo 安装：

```console
cargo install agent-sync --locked
```

也可以从
[GitHub Releases](https://github.com/lidongpeng36/agent-sync/releases/latest)
下载适用于 macOS/Linux、arm64/x86_64 的预编译包及 SHA-256 校验文件。

默认命令只生成计划，不写文件：

```console
agent-sync sync codex mini
agent-sync sync claude mini --only memory
```

可以在配置顶层设置 `default_peer = "mini"`，之后简写为
`agent-sync s claude`。命令行显式 peer 优先于配置。普通预览会列出逐文件
动作；`agent-sync s claude -f diff` 输出不截断的完整 unified diff。diff
可能包含完整 session 和 memory 内容，应按敏感数据处理。

正式写入使用 `--apply`，交互确认是 `Apply these changes? [Y/n]`，直接回车
表示确认；脚本环境必须同时使用 `--apply --yes`。Claude 默认使用 `ask`：
线性 session 取更长版本，真实分叉始终保留为两个 UUID，独立的 Markdown
标题块自动合并；仅无法无损合并的 memory block 或索引描述需要选择
`local`、`remote` 或通过 `$EDITOR` 编辑。旧值 `merge` 兼容映射为 `ask`。

## License

MIT
