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
   output, confirmation, and common safety invariants.
4. Built-in `AgentAdapter` implementations own agent formats, merge policy,
   writer protection, backup selection, and post-apply repair.

The helper handshake carries a protocol version and executable SHA-256. A
missing or stale same-platform helper is atomically uploaded to a private,
versioned cache before use. Business parameters are serialized in protocol
messages rather than interpolated into remote shell programs. OpenSSH remains
the authentication and transport boundary so existing host aliases, jump
hosts, agents, and host-key policy remain authoritative.

An adapter implements `doctor`, `prepare`, optional interactive resolution,
and `apply`. `prepare` must be read-only and produce a complete staged tree,
blockers, and a file-granularity plan. Human and JSON output render that plan;
diff output compares each side with the same staged tree. `apply` must reject a
stale plan, back up before mutation, install the stage, and verify a fresh
remote readback.

The trait is an internal Rust extension point, not a stable dynamic-plugin ABI.
Adding another agent means adding a module with fixtures that prove validation,
merge, writer, backup, and idempotency behavior.

The current transfer backend remains rsync. The next transport step is a
manifest/file-stream RPC with append-aware JSONL transfer; it can replace
rsync without changing the adapter contract or the remote safety operations.
