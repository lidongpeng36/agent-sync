# Changelog

## 0.9.0

- Automatically defer Claude active session bundles and affected project memory/shared indexes, using live process registries and actual lsof access fields. Preserve writer rechecks, selected backups, stale-plan checks, and recovery gates.
- Align Claude memory with verified paired baselines, conservative three-way merges, localized editing, and optional evidence-backed project consistency review. Index entries and preambles participate in merging.
- Mark Claude journals verified only after final content and timestamp verification; publish memory baselines only from matching verified transactions.
- Helper protocol 8 adds Claude activity/project exclusions and agent-scoped baseline operations. Existing Codex baseline files remain compatible.

## 0.8.1

### Codex migration guidance

- Detect legacy/paginated history mismatches, including both missing and explicit legacy markers, and report affected endpoint counts and synchronized roots.
- Stop before interactive content choices when migration is required. Explicit local/remote strategies and `--yes` cannot bypass the migration gate.
- Explain official `codex migrate-rollouts` inspection and migration, including scoped `--thread` batches and checking both endpoints for whole-root migration. Migration is never run automatically by sync.

### Session timestamp convergence

- Reapply only planned session JSONL timestamps after transport and catalog repair, guarded by content checksums.
- Verify timestamps alongside the full content manifest before marking a transaction verified, preventing a successful apply from leaving metadata-only changes for the next preview.
- Preserve active-session exclusions, two-endpoint backups, transaction recovery gates, and helper protocol 7 compatibility.

### Validation

- Passed 106 unit tests and 11 CLI integration tests, formatting, and Clippy.
- In isolated copies of 281 conflicting sessions, official Codex migration produced 280 equivalent histories and one strict append. The subsequent sync converged with matching content and timestamps, zero changes on a second preview, and full checkpoint reuse.

## 0.8.0

### Memory merging

- Keep dependency-free builtin three-way and independent Markdown-section merging as the default.
- Add optional Codex and OpenCode resolvers, including model/executable selection, plus OpenAI Chat Completions and Anthropic Messages-compatible API backends with configurable URL, model and credentials.
- Partition initial Codex aggregate merges by explicit source thread IDs; preserve independent entries and submit only overlapping groups to the model. Verified one-sided edits bypass the model.
- Review linked raw memories, rollout summaries, catalog groups and the overview after merging. Factual corrections require bounded, exact edits and verbatim evidence from immutable original snapshots; generated summaries cannot certify themselves.
- Persist the review policy only in matching, checksummed, verified baselines. Older baselines remain usable for merges but require initial review; changed evidence invalidates review reuse.
- Bound backend time/output, withhold sensitive diagnostics, preserve explicit manual choices, and retain blockers for invalid or unresolved proposals.

### Session and transaction fixes

- Compare complete Codex JSON records independent of serialization order/whitespace, including the recognized empty `disabled_plugin_ids` default. Preserve unknown fields, identity, ordering, timestamps and full source bytes.
- Repair Codex catalogs with root-scoped official `thread/read` calls after backups and before final verification.
- Use checksums for local installation and remote pushes, including equal-size/equal-mtime changes. Preserve staged mtimes and converge with protocol-29 rsync.
- Remove the Codex executable requirement for Codex memory-only operations and previews; session apply still checks the runtime needed for catalog repair.

### Compatibility

- Helper protocol: 7. SSH peers use the matching checksum-verified helper binary.
- Rust 1.88 or newer is required; prebuilt macOS/Linux binaries remain available for arm64 and x86_64.
- Default backend remains `builtin`. Semantic providers are opt-in and can consume model usage during preview. No real agent-home migration or synchronization is performed by upgrading.
- Legacy-to-paginated session conversion remains the responsibility of Codex's migration tooling; this release does not discard legacy events to infer equivalence.
