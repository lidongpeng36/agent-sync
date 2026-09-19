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

Codex Markdown memory uses a separate durable, paired content baseline in the
OS local data directory. Each checksummed snapshot contains only allowlisted
UTF-8 Markdown text and is scoped to sorted node/root endpoints and resource
selection. Both copies must match before they can inform planning; missing or
invalid snapshots allow only independent-section bootstrap or configured
semantic review; unresolved shared sections remain conflicts. Baseline
identity and contents are revalidated under endpoint locks before apply.
Conservative line-based diff3 combines disjoint edits using the existing
`similar` diff engine; overlapping edits and ambiguous insertion order require
shared conflict interaction. This replaces the former longer-block and summary
concatenation heuristics. Session merge behavior is unchanged.
For unresolved Codex memory, the shared editor preserves equal lines once and
marks each differing region separately, keeping unchanged Markdown headings and
sections as context. This is presentation only: without a trusted baseline,
ambiguous shared text still requires a choice unless a configured semantic backend
resolves it, and local/remote choices remain whole-file choices.

After full two-endpoint verification and durable `verified` journals, each
endpoint independently checks baseline text against its installed memory and
atomically publishes the same snapshot with file and directory fsync. A partial
publication cannot be used because the snapshots differ. Baseline writes require
the matching verified Codex journal; ordinary checkpoint loss does not create
trust. The typed protocol carries baseline reads and verified publication, never
arbitrary file writes. Active-excluded paths are omitted and whole-file removal
remains unsupported. Preview baseline RPC content sizes are reported separately
from rsync transfer statistics.

Codex session relationship checks hash canonical JSON records, normalizing only
an absent versus empty disabled_plugin_ids in thread_settings_applied events.
All other fields remain significant. Equivalent inputs choose a deterministic
original byte stream; strict extensions retain the full extending stream.
Legacy/paginated conversions remain the responsibility of official Codex
migration tooling. Source inventories and final manifests remain byte-exact.

Codex catalog repair is a typed, root-scoped helper operation (protocol 7).
Explicit thread/read calls repair missing rows that list scans can omit. It runs
after both endpoint backups and journal publication, while endpoint locks remain
held, and before final manifest verification and the verified journal phase.

Sparse payload copies preserve staged mtimes. Codex stages portable whole-second
rollout mtimes and stable aggregate-file mtimes to converge with protocol 29
rsync. Local installation and remote pushes always use checksums so equal sizes
and mtimes cannot hide selected content changes.

Memory resolution is configured globally or per adapter in memory_resolver.
Builtin operation needs no agent or network: paired baseline diff3 first, or
independent unchanged Markdown sections without a baseline. Optional Codex and
OpenCode processes and OpenAI/Anthropic HTTP backends receive only bounded
memory text and return fingerprint-bound JSON proposals. The coordinator owns
staging and checks markers, references, response completeness and limits. Model
judgments are not substituted for writer checks, source-generation revalidation,
byte hashes, transaction phases or paired baseline publication. Backend errors
remain explicit blockers. Claude validates content/index bundles through the
same validator used for editor choices. Preview may contact an explicitly
configured backend, but never applies its result to either source tree.

Codex memory-only operations do not probe for the Codex executable. Session apply
checks runtime availability on both endpoints before backup or mutation. API
credentials stay on the coordinator and are never sent to the SSH helper, printed
in diagnostics, or placed in process arguments. Agent subprocesses have bounded
lifetimes and private temporary files; OpenCode sessions use isolated storage.

Semantic bootstrap of recognized Codex aggregates is source-scoped: MEMORY.md
Task Groups form connected components only through thread IDs in rollout-summary
citations; raw_memories.md uses explicit Thread headers. Source-only entries are
retained, shared entries are reviewed, and model output must preserve the section
forms and source identities. Unknown structures fall back to bounded whole-file
review. This avoids asking a model to regenerate unrelated memories. Trusted
one-sided baseline edits use deterministic advancement without a model call.
The merge is followed by a separate cross-file consistency review when a semantic
backend is configured.

Cross-file consistency (memory_consistency) groups staged units through declared
source IDs and includes the overview as context. Original allowlisted Markdown
is copied and hash-checked against each endpoint inventory before any backend
runs. Those immutable snapshots supply evidence; model-generated staged text
cannot act as evidence. Only known candidate units may be edited, using unique,
bounded replacements and exact quotes from independent original raw/leaf units.
All responses are validated before corrected files are materialized. Contradictions,
invalid evidence, backend failures and corrections to explicitly chosen files
remain blockers, never partial approvals. No session logs or arbitrary referenced
paths are opened for review.

The optional baseline review_policy field is part of its checksum. Its absence
retains the version-1 digest, preserving old merge bases while requiring initial
review. Matching reviewed bases can skip unchanged candidate/evidence groups;
original source-file versions are included in request fingerprints and cache
eligibility, and prior active exclusions force review when those sources become
eligible. Both endpoint copies are independently read back before publication;
only a verified journal permits saving the marker. Policy changes require a new
review version. Protocol 7 ensures helper agreement on these semantics.
