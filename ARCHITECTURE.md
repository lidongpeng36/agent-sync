# Architecture

The binary has three layers:

1. `SshTransport` performs allowlisted snapshots and staged transfers while
   honoring the user's SSH configuration.
2. The core owns manifests, path validation, private temporary storage, CLI
   output, confirmation, and common safety invariants.
3. Built-in `AgentAdapter` implementations own agent formats, merge policy,
   writer protection, backup selection, and post-apply repair.

An adapter implements `doctor`, `prepare`, optional interactive resolution,
and `apply`. `prepare` must be read-only and produce a complete staged tree plus
blockers. `apply` must reject a stale plan, back up before mutation, install the
stage, and verify a fresh remote readback.

The trait is an internal Rust extension point, not a stable dynamic-plugin ABI.
Adding another agent means adding a module with fixtures that prove validation,
merge, writer, backup, and idempotency behavior.
