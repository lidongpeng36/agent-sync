# Architecture

The binary has four layers:

1. `SshTransport` invokes OpenSSH, bootstraps the matching remote helper, and
   performs the remaining allowlisted rsync snapshots and staged transfers
   while honoring the user's SSH configuration.
2. The hidden `__remote` endpoint accepts a versioned serde protocol over
   stdin/stdout. It implements remote filesystem metadata, writer locks,
   backups, mtime normalization, and SQLite maintenance in Rust. The protocol
   deliberately has no arbitrary-command request.
3. The core owns manifests, path validation, private temporary storage, CLI
   output, confirmation, the shared local/remote/editor conflict interaction,
   and common safety invariants.
4. The local archive layer produces a versioned, checksummed single-file
   portable snapshot, validates it into a path-confined temporary tree, and
   plans additive imports before any mutation.
5. Built-in `AgentAdapter` implementations own agent formats, merge policy,
   writer protection, backup selection, and post-apply repair.

The helper handshake carries a protocol version and executable SHA-256. A
missing or stale same-platform helper is atomically uploaded to a private,
versioned cache before use. Business parameters are serialized in protocol
messages rather than interpolated into remote shell programs. OpenSSH remains
the authentication and transport boundary so existing host aliases, jump
hosts, agents, and host-key policy remain authoritative.

An adapter implements `doctor`, `prepare`, conflict mapping and validation, and
`apply`. The local/remote/editor prompt, editor selection, private edit files,
conflict markers, and marker validation are shared core behavior; adapters only
validate agent-specific edited formats and update their staged metadata.
`prepare` must be read-only and produce a complete staged tree, blockers, and a
file-granularity plan. Human and JSON output render that plan; diff output
compares each side with the same staged tree. `apply` must reject a stale plan,
back up before mutation, install the stage, and verify a fresh remote readback.
Codex active-session exclusion also defers its aggregate history/index files and
catalog/state repair, preventing an excluded live rollout from being changed
through derived metadata while unrelated resources continue to synchronize.

The trait is an internal Rust extension point, not a stable dynamic-plugin ABI.
Adding another agent means adding a module with fixtures that prove validation,
merge, writer, backup, and idempotency behavior.

Current-directory agent inference compares the canonical current directory
with configured local roots and chooses the deepest containing root. No match
or an ambiguous match requires an explicit agent. Archive import separately
checks that the selected/inferred agent equals the manifest agent.

Portable archives contain `manifest.json` plus regular files below `payload/`.
The manifest declares the schema version, agent, resource selection, size, and
SHA-256 of every payload file. Extraction rejects unsafe paths, links,
duplicates, undeclared content, and expansion limits before adapter validation.
Codex and Claude archives use their normal path allowlists and validators;
OpenCode archives use official session exports and semantic session hashes.

The current transfer backend remains rsync. The next transport step is a
manifest/file-stream RPC with append-aware JSONL transfer; it can replace
rsync without changing the adapter contract or the remote safety operations.
Rsync payloads are compressed and may be rate-limited per invocation or peer.
All readback phases update the same persistent remote snapshot, so their safety
fingerprints cost a file-list exchange plus actual deltas rather than another
full download into an empty directory.
