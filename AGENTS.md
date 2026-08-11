# AGENTS.md

This file guides coding agents working in this repository. Preserve the safety
properties below even when a smaller implementation would appear simpler.

## Product contract

`agent-sync` synchronizes coding-agent sessions and memory between a local
machine and an SSH peer. Preview is read-only by default. Apply is a staged,
two-endpoint transaction with writer checks, stale-plan detection, backups, and
post-write verification. Credentials, settings, plugins, caches, and complete
OpenCode databases are outside the synchronization boundary.

Prefer a safe refusal with actionable evidence over a partial or ambiguous
write. Optimization state must never become correctness state.

## Repository map

- `src/main.rs`: CLI parsing, target resolution, confirmation, command flow.
- `src/config.rs`: configuration, peer settings, and current-directory agent
  inference.
- `src/core.rs`: inventories, plans, shared conflict interaction, staging,
  validation helpers, and human/JSON/diff output.
- `src/transport.rs`: SSH helper bootstrap, typed requests, rsync, endpoint
  locks, and transaction-journal coordination.
- `src/remote.rs`: allowlisted remote protocol implementation. It must not
  become an arbitrary remote shell API.
- `src/state.rs`: node identity, per-peer checkpoints, locks, and durable
  transaction journals.
- `src/archive.rs`: validated, single-file local export/import archives.
- `src/adapters/`: agent-specific discovery, validation, merge, writer,
  backup, apply, and repair behavior.
- `tests/cli.rs`: CLI and remote-protocol integration tests.
- `tests/fixtures/`: small, non-sensitive format and end-to-end fixtures.

Read `ARCHITECTURE.md` before changing cross-module behavior.

## Required invariants

### Planning and interaction

- `prepare` must not mutate agent data. It produces a complete result manifest,
  the staged content needed for changes, per-side actions, notes, and blockers.
- Conflict strategy defaults to `ask`. Local/remote/editor prompting and editor
  lifecycle belong in shared core code; adapters only supply format-specific
  validation and staged metadata updates.
- A preview must remain useful when an active Codex session is excluded. Do not
  let its aggregate history, index, catalog, or timestamps change indirectly.
- Claude and OpenCode database/file writers block apply. Never reinterpret a
  conflict choice as permission to override writer safety.

### Apply and recovery

- Acquire both endpoint locks in stable node-ID order, then recompute enough
  state to reject a stale preview.
- Back up both endpoints before the first mutation.
- Persist matching transaction journals before writes and advance phases only
  after each durable step: `prepared`, `local_applied`, `remote_applied`, then
  `verified`.
- Verify the final full content manifest on both endpoints before checkpointing
  or clearing journals.
- An unfinished journal is a recovery gate. Do not silently clear it, retry
  over it, or claim success. A verified journal may be cleaned up.
- Sparse payloads may contain only planned changes. If a new plan can produce
  removals, implement explicit, validated deletion semantics before enabling
  it; the current sparse payload intentionally rejects removal.

### Checkpoints and manifests

- Checkpoints are disposable hash caches. Missing or invalid checkpoints must
  fall back to a fresh scan; they must not block synchronization.
- Reuse a hash only when the adapter's complete cheap revision matches. Final
  correctness still comes from staged hashes and post-apply verification.
- Keep logical payload size distinct from measured rsync protocol bytes.
- Preserve delta transfer checksum verification even when size and mtime are
  equal.

### Agent boundaries

- Codex and Claude use allowlisted files and validate JSONL identity, ordering,
  event times, and append relationships.
- OpenCode sessions must cross the boundary through official `opencode export`
  and `opencode import`. Never copy `opencode.db`, authentication rows, account
  data, permissions, or credentials.
- Deterministic forks must remain stable across machine order and repeated
  syncs. A converged rerun should be idempotent.
- Archive import is additive by default, path-confined, checksummed, backed up,
  and read back after apply. Overwrite requires the existing explicit force
  path.

### Remote protocol

- All remote operations use typed serde requests with validated paths and
  identifiers. Do not interpolate user-controlled values into shell snippets.
- Bump `PROTOCOL_VERSION` when a request or response changes incompatibly, and
  update `tests/cli.rs` in the same change.
- The helper is the same checksum-verified binary. Keep helper upload atomic
  and private, and preserve the user's OpenSSH host-key and routing policy.

## Development workflow

Before editing, inspect `git status` and preserve unrelated user changes. Keep
changes narrow and use existing helpers instead of duplicating safety logic in
each adapter.

Run the full local gate before handing off a change:

```console
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
git diff --check
```

Add focused regression tests for every bug. Adapter changes should cover the
relevant validation, merge or fork behavior, writer handling, idempotency, and
final verification. Protocol changes require a CLI/helper negotiation test.
Checkpoint changes require cache-hit and corrupt/missing-cache fallback tests.

## Live-host testing

Live SSH preview is appropriate only when the named peer is in scope. Start
with a release-build read-only preview and record whether work came from hash
reuse, selected payload, or actual wire bytes. Prefer isolated temporary roots
for apply tests.

Do not apply to real agent homes, stop writers, delete transaction journals,
restore backups, push commits, create tags, or publish releases without the
user's explicit authorization. Never print archive/session content, credentials,
or sensitive full diffs merely to prove a test passed.

After an authorized real apply, verify both endpoints independently and run a
second preview to prove convergence and checkpoint reuse. Report any backups or
other durable artifacts that the test created.

## Scope of commits and releases

Use focused commits that state the behavioral outcome. Do not mix formatting or
unrelated refactors with a safety fix. Keep `Cargo.toml` version, release tag,
README installation claims, and remote protocol compatibility aligned. Commit,
push, tag, and publish only when explicitly requested.
