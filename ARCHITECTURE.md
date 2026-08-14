# Architecture

The binary has five layers:

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
`prepare` must be read-only and produce a complete result manifest, the staged
content needed for changes, blockers, and a file-granularity plan. Human and
JSON output render that plan; diff output compares each changed side with the
same staged result. `apply` must reject a stale plan, back up before mutation,
install the stage, and verify a fresh remote readback.
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

Codex and Claude use a typed remote inventory RPC before transfer. The
coordinator materializes a temporary remote view from matching local objects
and selectively rsyncs only hashes that are unavailable locally. Stale-plan and
final verification compare inventory generations and content manifests, so no
persistent full-content remote snapshot is required. Successful applies write
small per-peer checkpoints on both endpoints; exact size/mtime matches reuse
previous hashes while missing or invalid state safely falls back to hashing.

OpenCode uses the same checkpoint envelope with a session revision composed of
the maximum session/message/part update time and their row counts. It exports
only revisions whose canonical semantic hash cannot be reused, and omits equal
sessions from temporary snapshots and the apply stage.

Remote scans and applies share a stable per-agent kernel lock. Apply acquires
both endpoint locks in node-ID order, then revalidates the prepared generation.
This serializes mini I/O and writes across multiple peers without holding locks
during interactive conflict editing. A later transport step can add
append-aware JSONL range transfer and automatic recovery of interrupted
transactions while retaining the same manifest and locking protocol.

Selective pulls seed same-path differences from the local file and invoke
rsync with `--checksum`, compression, and statistics. This preserves content
verification when size/mtime collide while reusing unchanged blocks. Apply
derives endpoint-specific sparse payloads from the full result plan. Before the
first write, both endpoints persist the same transaction ID, source
generations, result hash, phase, and backup locations; each phase transition is
durable, and later writes refuse to proceed over an unfinished partial commit.
After verification, checkpointing, and successful journal cleanup, each
endpoint independently prunes generated backup sets to the configured bounded
retention. The just-created set is protected explicitly, and cleanup failure is
reported without changing a completed transaction into a failed one.
