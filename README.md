# agent-sync

`agent-sync` safely synchronizes coding-agent sessions and memories between a
local machine and an SSH peer. It ships as one local Rust binary and
automatically bootstraps a private, versioned copy of itself on a compatible
peer.

Built-in adapters:

- **Codex**: rollouts, archives, history/index, catalog timestamps, and memory.
- **Claude Code**: sessions, subagents, tool results, and project memory.
- **OpenCode**: portable session exports, linear advances, and deterministic
  forks. Credentials and the rest of `opencode.db` are never transferred.

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

`ssh` and `rsync` must be available locally. Claude and OpenCode writer
detection also use `lsof` on both peers. Codex catalog repair uses
`codex app-server`, and OpenCode synchronization uses the `opencode` CLI, on
both peers.

## Usage

```console
agent-sync sync codex mini
agent-sync sync claude mini --only sessions
agent-sync sync opencode mini
agent-sync sync claude mini -s ask
agent-sync s claude -f diff > claude-sync.diff
agent-sync sync codex mini --only memory --apply
agent-sync sync claude mini --apply --yes
agent-sync sync codex mini --bwlimit 16384
agent-sync doctor codex mini
cd ~/.codex && agent-sync sync mini       # infers codex
agent-sync export codex codex-backup.tar.gz
agent-sync import codex codex-backup.tar.gz
agent-sync import codex codex-backup.tar.gz --apply --yes
agent-sync adapters
```

`sync` and `doctor` may omit the agent while the current directory is the
configured local root (or any directory below it). For example, inside
`~/.codex`, `agent-sync sync mini` means `agent-sync sync codex mini`; with a
configured `default_peer`, `agent-sync sync` is sufficient. Explicit agent
names remain required outside a configured agent directory.

`--apply` asks `Apply these changes? [Y/n]` in a terminal; Enter accepts the
default `yes`. Non-interactive use requires both `--apply --yes`; `--yes`
confirms the staged plan but does not change its conflict strategy.

Codex and Claude first exchange SHA-256 manifests, then rsync only remote
objects whose content is unavailable locally. Identical cold-start trees do not
require a full remote download. Successful applies store small per-peer
checkpoints on both endpoints; unchanged size/mtime entries reuse their prior
hash on later scans. Transfers are compressed. On a shared or latency-sensitive
link, `--bwlimit <KiB/s>` reserves bandwidth for interactive SSH; the same
ceiling can be stored as `bandwidth_limit_kbps` under a peer.
OpenCode scans inexpensive SQLite revisions first. After a successful apply,
per-peer checkpoints let unchanged sessions reuse their canonical semantic
hashes without running `opencode export`; only new or revised sessions are
batch-exported. A missing checkpoint safely performs a full hash scan once.
Manifest reports label selected payload sizes as `uncompressed bytes`; this is
the logical source size before rsync compression. `rsync delta` reports actual
protocol bytes sent/received plus literal and locally matched data. Differing
same-path files are seeded from the local copy and transferred with checksum
verification, allowing rsync to send only changed blocks.

The default resource set is `sessions` plus `memory`; select one with
`--only sessions` or `--only memory`. Add `--format json` for machine-readable
plans. Safe unions, linear advances, Markdown block merges, and deterministic
session forks are always automatic. Irreconcilable conflicts use the configured
`ask` strategy by default; override it per invocation with `-s ask`, `-s local`,
or `-s remote` (the long form is `--conflict-strategy`). The legacy value
`merge` remains accepted as an alias for `ask`. In interactive `ask` applies,
Codex rollout/memory conflicts and Claude memory conflicts offer local, remote,
or `$EDITOR`. A conflict strategy does not override active-writer safety rules.

For Codex, sessions with an active writer are reported as warnings and excluded
from that run; other rollouts and memory continue to synchronize. While any
Codex session is active, `history.jsonl`, `session_index.jsonl`, catalog scans,
and state timestamp repair are deferred so the excluded session cannot be
modified indirectly. A newly active session discovered after preview makes the
plan stale and requires a rerun so it can be excluded safely.

The `sync`, `doctor`, and `adapters` commands have the aliases `s`, `d`, and
`a`. Common options also have short forms: `-o` (`--only`), `-a` (`--apply`),
`-y` (`--yes`), `-f` (`--format`), and `-s` (`--conflict-strategy`). Run a
subcommand with `--help` for the complete list.

### Portable local archives

`agent-sync export <AGENT> <FILE>` creates one portable gzip archive. Inside a
configured agent directory, omit the agent with `agent-sync export <FILE>`.
Use `--only sessions` or `--only memory` to narrow the archive and `--force`
to atomically replace an existing output file.

`agent-sync import <AGENT> <FILE>` fully validates and previews an archive but
does not write. Apply with `--apply`; non-interactive use also needs `--yes`.
An import is additive and never deletes local data. A differing existing item
is a conflict and is not overwritten unless `--apply --force` is explicit.
The target is backed up before writing and all imported content is read back
and verified afterward.

The archive manifest records its format version, agent, resource scope,
creation time, file sizes, and SHA-256 digests. Validation rejects a mismatched
agent, corrupt/truncated payload, duplicate or unlisted files, unsafe paths,
links, unsupported agent paths, and oversized expansion. Credentials and
private metadata remain excluded. OpenCode archives contain official portable
session exports, never `opencode.db`; source mtimes are preserved for formats
that use them as synchronization metadata.

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
# Successful syncs retain only this many backup sets on each endpoint.
backup_retention = 1

[peers.mini]
host = "mini"
# Optional rsync ceiling in KiB/s; useful on a shared or latency-sensitive link.
bandwidth_limit_kbps = 16384

[agents.codex]
local_root = "~/.codex"

[agents.claude]
local_root = "~/.claude"
# Optional agent-specific override:
# conflict_strategy = "local"
# backup_retention = 2

[agents.opencode]
local_root = "~/.local/share/opencode"

[peers.mini.roots]
codex = ".codex"
claude = ".claude"
opencode = ".local/share/opencode"
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

OpenCode currently synchronizes sessions only. It reads and writes sessions
through the official `opencode export` and `opencode import` commands instead
of copying the database. Linear histories take the longer version; true
divergences are retained under a deterministic fork ID so multi-machine syncs
converge. Conflict strategy does not apply to OpenCode sessions.

## Safety model

- Remote reads use an explicit rsync allowlist.
- JSONL identity, ordering, timestamps, and append relationships are validated.
- Safe same-path changes are merged automatically. Ambiguous memory blocks
  follow the explicit conflict strategy, while Claude session forks are always
  preserved as separate UUIDs.
- Active writers are detected before writes; active Codex sessions are excluded,
  and Codex coordination locks are held through the remaining file transaction.
- Expensive remote scans and applies use the same per-agent kernel lock. Apply
  locks are acquired on both endpoints in stable node-ID order, preventing
  concurrent mini writers and cross-direction deadlocks. Plans are never trusted
  after their manifest generation changes.
- Both sides are backed up before mutation and verified against the staged
  SHA-256 manifest after transfer.
- After both transaction journals are cleared, old generated backups are
  pruned on each endpoint. The current backup set is always protected and the
  retention defaults to one; unknown or manually named files are left alone.
- Apply builds separate sparse payloads for each endpoint, so unchanged files
  are not reinstalled or listed in the write transfer.
- A durable transaction journal records prepared, local-applied,
  remote-applied, and verified phases together with source generations and both
  backup paths. Unfinished partial transactions block later writes; verified
  journals left by a final cleanup failure are cleared automatically.
- Repeated safety checks exchange manifests rather than downloading readback
  snapshots. Per-peer checkpoints are optional optimization state; missing or
  invalid state falls back to hashing and manifest exchange, not full transfer.
- OpenCode backups remain on their originating machine, and account,
  credential, permission, and authentication tables are never transferred.
- Portable local archives are checksummed, path-confined, versioned, and
  validated before preview or import; import is preview-only by default.
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

Runtime dependencies are `ssh` and `rsync` locally. Claude and OpenCode writer
detection also need `lsof` on both machines. Codex catalog repair needs a
compatible `codex app-server`, and OpenCode synchronization needs a compatible
`opencode` CLI, on both machines. Backup creation, timestamp handling, locking,
and SQLite maintenance are implemented in Rust and do not require `tar` or
`python3`.

### Cache and interrupted-transaction recovery

Per-peer checkpoints below the agent-sync cache contain only reusable hashes
and revision metadata. They are never a source of truth: a missing, malformed,
stale, identity-mismatched, or checksum-invalid checkpoint is treated as a
cache miss and causes a fresh inventory scan.

Transaction journals are different. If a process stops after either endpoint
has been modified, the next apply refuses to write and reports the transaction
phase plus both backup paths. Keep those backups and inspect both endpoints
before recovery; do not remove the journal merely to bypass the guard. A
verified transaction whose final journal cleanup was interrupted is cleared
automatically.

## Development

```console
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
git diff --check
```

See [AGENTS.md](AGENTS.md) for repository working rules and required validation,
and [ARCHITECTURE.md](ARCHITECTURE.md) for the adapter boundary and invariants.

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
agent-sync sync opencode mini
```

可以在配置顶层设置 `default_peer = "mini"`，之后简写为
`agent-sync s claude`。命令行显式 peer 优先于配置。普通预览会列出逐文件
动作；`agent-sync s claude -f diff` 输出不截断的完整 unified diff。diff
可能包含完整 session 和 memory 内容，应按敏感数据处理。

当前目录位于对应配置根目录或其子目录时，可以省略 agent。例如在
`~/.codex` 中执行 `agent-sync sync mini`；同时配置了 `default_peer` 时可直接
执行 `agent-sync sync`。本地单文件迁移使用：

```console
agent-sync export codex codex-backup.tar.gz
agent-sync import codex codex-backup.tar.gz              # 仅校验和预览
agent-sync import codex codex-backup.tar.gz --apply --yes
```

归档包含版本化 manifest、逐文件大小与 SHA-256；导入拒绝损坏内容、危险路径、
agent 不匹配及未显式允许的覆盖。遇到已有内容不同，必须额外指定
`--apply --force`。导入前自动备份，导入后重新校验；OpenCode 归档只包含官方
session export，不包含数据库或凭据。

正式写入使用 `--apply`，交互确认是 `Apply these changes? [Y/n]`，直接回车
表示确认；脚本环境必须同时使用 `--apply --yes`。Claude 默认使用 `ask`：
线性 session 取更长版本，真实分叉始终保留为两个 UUID，独立的 Markdown
标题块自动合并；仅无法无损合并的 memory block 或索引描述需要选择
`local`、`remote` 或通过 `$EDITOR` 编辑。旧值 `merge` 兼容映射为 `ask`。
OpenCode 当前只同步 session，通过官方 `export`/`import` 接口工作，不复制
包含凭据的数据库；线性历史取较长版本，真实分叉使用确定性 ID 分别保留。

同步使用两端 manifest、增量 rsync 和 per-peer checkpoint，稳定状态下不会重复
下载或重新计算未变化内容。共享 SSH 链路可以使用 `--bwlimit <KiB/s>` 限速。
Codex 正在写入的 session 会被警告并跳过，其他内容继续同步；Claude 和
OpenCode 在检测到 writer 时拒绝 apply。损坏的 checkpoint 会自动退化为重新
扫描；未完成的事务 journal 则会阻止继续写入，并输出两端备份位置供恢复。

## License

MIT
