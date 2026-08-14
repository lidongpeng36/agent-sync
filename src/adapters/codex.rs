use super::{Adapter, Prepared};
use crate::core::{
    Blocker, ConflictStrategy, EditDocument, InteractiveChoice, PayloadSide, PlanReport,
    ResourceSelection, SyncOptions, build_sparse_payload, bytes_sha256, choose_interactively,
    complete_remote_view, edit_conflict_documents, file_lock_is_held, inventory, inventory_cached,
    inventory_transfer_paths, manifest, planned_file_changes, print_planned_diff, private_dir,
    seed_remote_deltas, stamp,
};
use crate::remote::{BackupKind, Request as RemoteRequest, StateTimes, create_backup};
use crate::transport::{RemoteGuard, SshTransport};
use anyhow::{Context, Result, bail};
use chrono::DateTime;
use fs2::FileExt;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, ChildStdout, Command, Stdio};
use tempfile::TempDir;
use walkdir::WalkDir;

pub struct CodexAdapter;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Times {
    created_at_ms: i64,
    updated_at_ms: i64,
    recency_at_ms: i64,
}

#[derive(Clone)]
struct Session {
    relative: PathBuf,
    lines: Vec<Vec<u8>>,
    times: Times,
}

#[derive(Clone)]
struct CodexConflict {
    resource: &'static str,
    key: String,
    local_relative: PathBuf,
    remote_relative: PathBuf,
    local_bytes: Vec<u8>,
    remote_bytes: Vec<u8>,
}

pub struct CodexPrepared {
    pub report: PlanReport,
    temp: TempDir,
    stage: PathBuf,
    remote_view: PathBuf,
    local_fingerprint: String,
    remote_fingerprint: String,
    resources: ResourceSelection,
    metadata: BTreeMap<String, Times>,
    active: BTreeSet<String>,
    conflicts: Vec<CodexConflict>,
    local_root: PathBuf,
    state_root: PathBuf,
    local_node_id: String,
    remote_node_id: String,
}

pub(super) fn print_diff(prepared: &CodexPrepared, local: &Path) -> Result<()> {
    println!(
        "# agent-sync: agent=codex peer={} status={}",
        prepared.report.peer,
        if prepared.report.blockers.is_empty() {
            "ready"
        } else {
            "action-required"
        }
    );
    for blocker in &prepared.report.blockers {
        println!(
            "# ACTION REQUIRED [{}] {}: {}",
            blocker.resource, blocker.path, blocker.reason
        );
    }
    print_planned_diff(local, &prepared.remote_view, &prepared.stage, |path| {
        excluded(path, prepared.resources) || active_excluded_path(path, &prepared.active)
    })
}

impl Adapter for CodexAdapter {
    fn doctor(&self, local: &Path, remote: &str, transport: &SshTransport) -> Result<()> {
        if !local.exists() {
            bail!("Codex root does not exist: {}", local.display());
        }
        for command in [&transport.ssh, &transport.rsync, "codex"] {
            if !SshTransport::command_exists(command) {
                bail!("required local command not found: {command}");
            }
        }
        let _: Value = transport.remote_request(&RemoteRequest::Doctor {
            root: remote.to_owned(),
            agent: "codex".to_owned(),
        })?;
        Ok(())
    }

    fn prepare(
        &self,
        local: &Path,
        remote_root: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<Prepared> {
        let temp = tempfile::Builder::new()
            .prefix("agent-sync-codex-")
            .tempdir()?;
        let scan_guard = transport.remote_guard(&RemoteRequest::HoldSyncLock {
            agent: "codex".to_owned(),
            resources: options.resources,
        })?;
        let remote = temp.path().join("remote-view");
        private_dir(&remote)?;
        let stage = temp.path().join("stage");
        private_dir(&stage)?;
        let active = active_writer_ids(local, remote_root, transport)?;
        let exclude = |p: &Path| excluded(p, options.resources) || active_excluded_path(p, &active);
        let state_root = crate::state::state_root(options)?;
        let local_node_id = crate::state::node_id(&state_root)?;
        let remote_node_id = transport.remote_node_id()?;
        let previous =
            crate::state::load(&state_root, "codex", &transport.host, options.resources)?;
        let (local_inventory, reused) = inventory_cached(
            local,
            exclude,
            previous.as_ref().map(|value| &value.inventory),
        )?;
        let remote_inventory: crate::core::Inventory =
            transport.remote_request(&RemoteRequest::Inventory {
                root: remote_root.to_owned(),
                agent: "codex".to_owned(),
                resources: options.resources,
                excluded_ids: active.iter().cloned().collect(),
                peer_id: local_node_id.clone(),
            })?;
        let transfer = inventory_transfer_paths(&local_inventory, &remote_inventory);
        let seeded = seed_remote_deltas(
            local,
            &local_inventory,
            &remote,
            &remote_inventory,
            &transfer,
        )?;
        let transfer_stats = transport.pull_files(remote_root, &remote, &transfer)?;
        complete_remote_view(local, &local_inventory, &remote, &remote_inventory)?;
        drop(scan_guard);
        let (mut report, metadata, conflicts) = build_stage(
            local,
            &remote,
            &stage,
            options.resources,
            &active,
            &transport.host,
            options.conflict_strategy,
        )?;
        let transferred_bytes = remote_inventory
            .entries
            .iter()
            .filter(|entry| transfer.contains(&entry.path))
            .map(|entry| entry.size)
            .sum::<u64>();
        report.notes.push(format!(
            "manifest: hashes reused local={reused}/{}, remote={}/{}; objects fetched={}/{} uncompressed bytes",
            local_inventory.entries.len(),
            remote_inventory.reused_entries,
            remote_inventory.entries.len(),
            transfer.len(),
            transferred_bytes
        ));
        report.notes.push(format!(
            "rsync delta: bases={seeded}; wire sent/received={}/{} bytes; literal/matched={}/{} bytes",
            transfer_stats.wire_sent.map_or_else(|| "unknown".into(), |value| value.to_string()),
            transfer_stats.wire_received.map_or_else(|| "unknown".into(), |value| value.to_string()),
            transfer_stats.literal_data.map_or_else(|| "unknown".into(), |value| value.to_string()),
            transfer_stats.matched_data.map_or_else(|| "unknown".into(), |value| value.to_string()),
        ));
        report.files = planned_file_changes(local, &remote, &stage, |path| {
            excluded(path, options.resources) || active_excluded_path(path, &active)
        })?;
        for conflict in &conflicts {
            let local_path = conflict.local_relative.to_string_lossy();
            let remote_path = conflict.remote_relative.to_string_lossy();
            for file in &mut report.files {
                if file.path == local_path || file.path == remote_path {
                    file.resolution = "unresolved".into();
                }
            }
        }
        let remote_fingerprint = remote_inventory.generation;
        Ok(Prepared::Codex(CodexPrepared {
            report,
            temp,
            stage,
            remote_view: remote,
            local_fingerprint: local_inventory.generation,
            remote_fingerprint,
            resources: options.resources,
            metadata,
            active,
            conflicts,
            local_root: local.to_path_buf(),
            state_root,
            local_node_id,
            remote_node_id,
        }))
    }

    fn resolve_interactive(&self, prepared: &mut Prepared, tty: bool) -> Result<()> {
        let Prepared::Codex(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        if value.conflicts.is_empty() || !tty {
            return Ok(());
        }
        let mut edited = BTreeSet::new();
        for conflict in value.conflicts.clone() {
            let choice = choose_interactively(
                &format!("Codex {} conflict [{}]", conflict.resource, conflict.key),
                &conflict.local_relative.display().to_string(),
                &conflict.remote_relative.display().to_string(),
                || edit_codex_conflict(&conflict, &value.temp.path().join("edit")),
            )?;
            let (relative, bytes, was_edited) = match choice {
                InteractiveChoice::Local => (
                    conflict.local_relative.clone(),
                    conflict.local_bytes.clone(),
                    false,
                ),
                InteractiveChoice::Remote => (
                    conflict.remote_relative.clone(),
                    conflict.remote_bytes.clone(),
                    false,
                ),
                InteractiveChoice::Edited(bytes) => (conflict.local_relative.clone(), bytes, true),
            };
            stage_codex_choice(value, &conflict, &relative, &bytes)?;
            if was_edited {
                edited.insert(relative.to_string_lossy().into_owned());
            }
        }
        value
            .report
            .blockers
            .retain(|blocker| !blocker.reason.contains("requires a choice"));
        value.report.files = planned_file_changes(
            &value.local_root,
            &value.remote_view,
            &value.stage,
            |path| excluded(path, value.resources) || active_excluded_path(path, &value.active),
        )?;
        for file in &mut value.report.files {
            if edited.contains(&file.path) {
                file.resolution = "edited".into();
            }
        }
        value.conflicts.clear();
        Ok(())
    }

    fn apply(
        &self,
        prepared: Prepared,
        local: &Path,
        remote_root: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<()> {
        let Prepared::Codex(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        let exclude =
            |p: &Path| excluded(p, value.resources) || active_excluded_path(p, &value.active);
        let _sync_guards = transport.sync_guards(
            &value.state_root,
            &value.local_node_id,
            &value.remote_node_id,
            "codex",
            value.resources,
        )?;
        transport.ensure_no_pending_transaction(&value.state_root, "codex")?;
        let _guard = if value.resources.sessions() {
            Some(CodexGuards::acquire(local, remote_root, transport)?)
        } else {
            None
        };
        if value.resources.sessions() {
            let current_active = active_writer_ids(local, remote_root, transport)?;
            let newly_active = current_active
                .difference(&value.active)
                .cloned()
                .collect::<Vec<_>>();
            if !newly_active.is_empty() {
                bail!(
                    "new Codex writers became active after preview ({}); rerun sync so they can be excluded",
                    newly_active.join(",")
                );
            }
        }
        if inventory(local, exclude)?.generation != value.local_fingerprint {
            bail!("local Codex data changed after preview");
        }
        let current_remote: crate::core::Inventory =
            transport.remote_request(&RemoteRequest::Inventory {
                root: remote_root.to_owned(),
                agent: "codex".to_owned(),
                resources: value.resources,
                excluded_ids: value.active.iter().cloned().collect(),
                peer_id: value.local_node_id.clone(),
            })?;
        if current_remote.generation != value.remote_fingerprint {
            bail!("remote Codex data changed after preview");
        }
        if value.resources.sessions() && value.active.is_empty() {
            reconcile_catalog(None, false)?;
            reconcile_catalog(Some(transport), false)?;
        }
        let local_payload = value.temp.path().join("local-payload");
        let remote_payload = value.temp.path().join("remote-payload");
        let local_payload_count = build_sparse_payload(
            &value.stage,
            &local_payload,
            &value.report.files,
            PayloadSide::Local,
        )?;
        let remote_payload_count = build_sparse_payload(
            &value.stage,
            &remote_payload,
            &value.report.files,
            PayloadSide::Remote,
        )?;
        let stamp = stamp();
        let local_backup = backup_local(local, value.resources, &stamp)?;
        let remote_backup = backup_remote(remote_root, value.resources, &stamp, transport)?;
        if value.resources.sessions() && value.active.is_empty() {
            backup_state(local, &stamp)?;
            remote_state(transport, remote_root, &stamp, "backup", None)?;
        }
        let result_inventory = inventory(&value.stage, exclude)?;
        let mut journal = crate::state::TransactionJournal::new(
            "codex",
            value.resources,
            &value.local_node_id,
            &value.remote_node_id,
            &value.local_fingerprint,
            &value.remote_fingerprint,
            &crate::state::content_hash(&result_inventory),
            &local_backup,
            &remote_backup,
        );
        transport.save_transaction_pair(&value.state_root, &journal)?;
        install_local(&local_payload, local, &transport.rsync)?;
        journal.phase = crate::state::TransactionPhase::LocalApplied;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        transport.push(&remote_payload, remote_root)?;
        journal.phase = crate::state::TransactionPhase::RemoteApplied;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        verify_selected(&value.stage, local, value.resources, &value.active, "local")?;
        let verified_remote: crate::core::Inventory =
            transport.remote_request(&RemoteRequest::Inventory {
                root: remote_root.to_owned(),
                agent: "codex".to_owned(),
                resources: value.resources,
                excluded_ids: value.active.iter().cloned().collect(),
                peer_id: value.local_node_id.clone(),
            })?;
        verify_remote_inventory(
            &value.stage,
            &verified_remote,
            value.resources,
            &value.active,
        )?;
        journal.phase = crate::state::TransactionPhase::Verified;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        drop(_guard);
        if value.resources.sessions() && value.active.is_empty() {
            let local_count = reconcile_catalog(None, true)?;
            let remote_count = reconcile_catalog(Some(transport), true)?;
            let local_changed = repair_state(local, &value.metadata)?;
            let remote_changed = remote_state(
                transport,
                remote_root,
                &stamp,
                "repair",
                Some(&value.metadata),
            )?;
            println!(
                "catalog: local={local_count}, remote={remote_count}; times repaired: local={local_changed}, remote={remote_changed}"
            );
        }
        if !value.active.is_empty() {
            println!(
                "warning: skipped active Codex sessions {}; history/index and catalog repair deferred",
                value.active.iter().cloned().collect::<Vec<_>>().join(",")
            );
        }
        let final_inventory = inventory(local, exclude)?;
        let local_checkpoint = crate::state::Checkpoint::new(
            "codex",
            value.resources,
            &transport.host,
            final_inventory,
        );
        let mut remote_checkpoint = local_checkpoint.clone();
        remote_checkpoint.peer = value.local_node_id;
        remote_checkpoint.inventory = verified_remote;
        remote_checkpoint.result_content_hash =
            crate::state::content_hash(&remote_checkpoint.inventory);
        if remote_checkpoint.result_content_hash != local_checkpoint.result_content_hash {
            bail!("cannot checkpoint divergent Codex results");
        }
        let _: Value = transport.remote_request(&RemoteRequest::SaveCheckpoint {
            checkpoint: remote_checkpoint,
        })?;
        crate::state::save(&value.state_root, &local_checkpoint)?;
        transport.clear_transaction_pair(&value.state_root, &journal)?;
        for message in transport.prune_backup_pair(
            local,
            remote_root,
            BackupKind::Codex,
            options.backup_retention,
            &stamp,
        ) {
            println!("{message}");
        }
        println!(
            "complete: Codex synchronized and verified; sparse payloads: local={local_payload_count}, remote={remote_payload_count}; backups: local={}, remote={}:{}",
            local_backup.display(),
            transport.host,
            remote_backup
        );
        Ok(())
    }
}

fn edit_codex_conflict(conflict: &CodexConflict, root: &Path) -> Result<Vec<u8>> {
    let name = conflict
        .local_relative
        .file_name()
        .and_then(|name| name.to_str())
        .context("conflict path has no UTF-8 filename")?;
    let mut edited = edit_conflict_documents(
        root,
        &[EditDocument {
            name,
            local: &conflict.local_bytes,
            remote: &conflict.remote_bytes,
            remote_label: "REMOTE",
        }],
    )?;
    let bytes = edited.remove(0);
    if conflict.resource == "session" {
        validate_session_choice(&conflict.local_relative, &conflict.key, &bytes)?;
    }
    Ok(bytes)
}

fn validate_session_choice(relative: &Path, expected_id: &str, bytes: &[u8]) -> Result<Times> {
    let lines = split_lines(bytes)?;
    let (id, times) = validate_rollout(relative, &lines)?;
    if id != expected_id {
        bail!("edited rollout session id changed from {expected_id} to {id}");
    }
    Ok(times)
}

fn stage_codex_choice(
    prepared: &mut CodexPrepared,
    conflict: &CodexConflict,
    relative: &Path,
    bytes: &[u8],
) -> Result<()> {
    if conflict.local_relative != relative {
        let old = prepared.stage.join(&conflict.local_relative);
        if old.exists() {
            fs::remove_file(old)?;
        }
    }
    if conflict.remote_relative != relative {
        let old = prepared.stage.join(&conflict.remote_relative);
        if old.exists() {
            fs::remove_file(old)?;
        }
    }
    let target = prepared.stage.join(relative);
    private_dir(target.parent().context("conflict target has no parent")?)?;
    fs::write(&target, bytes)?;
    if conflict.resource == "session" {
        let times = validate_session_choice(relative, &conflict.key, bytes)?;
        filetime::set_file_mtime(
            &target,
            filetime::FileTime::from_unix_time(
                times.updated_at_ms / 1000,
                ((times.updated_at_ms % 1000) * 1_000_000) as u32,
            ),
        )?;
        prepared.metadata.insert(conflict.key.clone(), times);
    }
    Ok(())
}

pub(crate) fn archive_excluded(p: &Path, r: ResourceSelection) -> bool {
    let n = p
        .components()
        .next()
        .map(|v| v.as_os_str().to_string_lossy());
    let memory = n.as_deref() == Some("memories");
    let session = matches!(
        n.as_deref(),
        Some("sessions" | "archived_sessions" | "history.jsonl" | "session_index.jsonl")
    );
    let private_memory_metadata = memory
        && p.components()
            .skip(1)
            .any(|c| c.as_os_str() == ".git" || c.as_os_str() == ".omx");
    (!r.memory() && memory)
        || (!r.sessions() && session)
        || (!memory && !session)
        || private_memory_metadata
        || p.to_string_lossy().contains("sync-backups")
}

fn excluded(p: &Path, r: ResourceSelection) -> bool {
    archive_excluded(p, r)
}

fn active_excluded_path(path: &Path, active: &BTreeSet<String>) -> bool {
    let path = path.to_string_lossy();
    active.iter().any(|id| path.contains(id))
        || (!active.is_empty() && matches!(path.as_ref(), "history.jsonl" | "session_index.jsonl"))
}

fn scan_sessions(root: &Path, active: &BTreeSet<String>) -> Result<BTreeMap<String, Session>> {
    let mut out = BTreeMap::new();
    for area in ["sessions", "archived_sessions"] {
        let base = root.join(area);
        if !base.exists() {
            continue;
        }
        for entry in WalkDir::new(&base).follow_links(false) {
            let e = entry?;
            if !e.file_type().is_file()
                || e.path().extension().and_then(|v| v.to_str()) != Some("jsonl")
            {
                continue;
            }
            if active
                .iter()
                .any(|id| e.path().to_string_lossy().contains(id))
            {
                continue;
            }
            let bytes = fs::read(e.path())?;
            let lines = split_lines(&bytes)?;
            let (id, times) = validate_rollout(e.path(), &lines)?;
            let rel = e.path().strip_prefix(root)?.to_path_buf();
            if out
                .insert(
                    id.clone(),
                    Session {
                        relative: rel,
                        lines,
                        times,
                    },
                )
                .is_some()
            {
                bail!("duplicate Codex session id {id}")
            }
        }
    }
    Ok(out)
}
fn split_lines(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            bail!("blank Codex JSONL record")
        }
        serde_json::from_slice::<Value>(line)?;
        out.push(line.to_vec())
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!("Codex JSONL must end with newline")
    }
    Ok(out)
}
fn validate_rollout(path: &Path, lines: &[Vec<u8>]) -> Result<(String, Times)> {
    let first: Value = serde_json::from_slice(lines.first().context("empty rollout")?)?;
    if first["type"] != "session_meta" {
        bail!("first Codex record is not session_meta: {}", path.display())
    }
    let payload = &first["payload"];
    let id = payload
        .get("id")
        .or_else(|| payload.get("session_id"))
        .and_then(Value::as_str)
        .context("missing Codex session id")?
        .to_owned();
    uuid::Uuid::parse_str(&id)?;
    if !path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .contains(&id)
    {
        bail!("Codex filename/session id mismatch")
    }
    let mut ordinals = Vec::new();
    let mut event = Vec::new();
    let mut recency = Vec::new();
    for line in lines {
        let v: Value = serde_json::from_slice(line)?;
        if let Some(o) = v.get("ordinal").and_then(Value::as_u64) {
            ordinals.push(o)
        }
        if let Some(ms) = timestamp_ms(
            v.get("timestamp")
                .or_else(|| v.pointer("/payload/timestamp")),
        ) {
            event.push(ms);
            if v["type"] == "task_started"
                || v["type"] == "user_message"
                || v.pointer("/payload/type") == Some(&Value::String("user_message".into()))
            {
                recency.push(ms)
            }
        }
    }
    if !ordinals.is_empty()
        && (ordinals.len() != lines.len()
            || ordinals.iter().enumerate().any(|(i, v)| *v != i as u64))
    {
        bail!("invalid mixed/non-contiguous Codex ordinals")
    }
    let created = timestamp_ms(payload.get("timestamp"))
        .or_else(|| uuid7_ms(&id))
        .context("missing Codex creation time")?;
    let updated = event.into_iter().max().unwrap_or(created);
    let recent = recency.into_iter().max().unwrap_or(updated);
    Ok((
        id,
        Times {
            created_at_ms: created,
            updated_at_ms: updated,
            recency_at_ms: recent,
        },
    ))
}
fn timestamp_ms(v: Option<&Value>) -> Option<i64> {
    let v = v?;
    if let Some(n) = v.as_i64() {
        return Some(if n > 10_000_000_000 { n } else { n * 1000 });
    }
    let s = v.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis())
}
fn uuid7_ms(id: &str) -> Option<i64> {
    let u = uuid::Uuid::parse_str(id).ok()?;
    if u.get_version_num() != 7 {
        return None;
    }
    let b = u.as_bytes();
    Some(
        (((b[0] as u64) << 40)
            | ((b[1] as u64) << 32)
            | ((b[2] as u64) << 24)
            | ((b[3] as u64) << 16)
            | ((b[4] as u64) << 8)
            | b[5] as u64) as i64,
    )
}

fn build_stage(
    local: &Path,
    remote: &Path,
    stage: &Path,
    r: ResourceSelection,
    active: &BTreeSet<String>,
    peer: &str,
    strategy: ConflictStrategy,
) -> Result<(PlanReport, BTreeMap<String, Times>, Vec<CodexConflict>)> {
    let mut report = PlanReport {
        agent: "codex".into(),
        peer: peer.into(),
        resources: Vec::new(),
        conflict_strategy: Some(strategy),
        ..Default::default()
    };
    let mut metadata = BTreeMap::new();
    let mut conflicts = Vec::new();
    if r.sessions() {
        report.resources.push("sessions".into());
        let a = scan_sessions(local, active)?;
        let b = scan_sessions(remote, active)?;
        for id in a.keys().chain(b.keys()).cloned().collect::<BTreeSet<_>>() {
            let l = a.get(&id);
            let q = b.get(&id);
            let selected = match (l, q) {
                (Some(x), Some(y)) if x.relative != y.relative => {
                    report.blockers.push(Blocker {
                        resource: "sessions".into(),
                        path: id.clone(),
                        reason: "active/archive path differs".into(),
                    });
                    continue;
                }
                (Some(x), Some(y)) if x.lines == y.lines => {
                    report.identical += 1;
                    x
                }
                (Some(x), Some(y)) if prefix(&x.lines, &y.lines) => {
                    report.advances += 1;
                    y
                }
                (Some(x), Some(y)) if prefix(&y.lines, &x.lines) => {
                    report.advances += 1;
                    x
                }
                (Some(x), Some(y)) => match strategy {
                    ConflictStrategy::Local => x,
                    ConflictStrategy::Remote => y,
                    ConflictStrategy::Ask => {
                        conflicts.push(CodexConflict {
                            resource: "session",
                            key: id.clone(),
                            local_relative: x.relative.clone(),
                            remote_relative: y.relative.clone(),
                            local_bytes: x.lines.concat(),
                            remote_bytes: y.lines.concat(),
                        });
                        report.blockers.push(Blocker {
                            resource: "sessions".into(),
                            path: id.clone(),
                            reason: "Codex rollout requires a choice".into(),
                        });
                        continue;
                    }
                },
                (Some(x), None) => {
                    report.remote_additions += 1;
                    x
                }
                (None, Some(y)) => {
                    report.local_additions += 1;
                    y
                }
                _ => unreachable!(),
            };
            let dst = stage.join(&selected.relative);
            private_dir(dst.parent().unwrap())?;
            fs::write(&dst, selected.lines.concat())?;
            filetime::set_file_mtime(
                &dst,
                filetime::FileTime::from_unix_time(
                    selected.times.updated_at_ms / 1000,
                    ((selected.times.updated_at_ms % 1000) * 1_000_000) as u32,
                ),
            )?;
            metadata.insert(id, selected.times.clone());
        }
        if active.is_empty() {
            merge_json_file(
                &local.join("history.jsonl"),
                &remote.join("history.jsonl"),
                &stage.join("history.jsonl"),
                None,
            )?;
            merge_json_file(
                &local.join("session_index.jsonl"),
                &remote.join("session_index.jsonl"),
                &stage.join("session_index.jsonl"),
                Some("id"),
            )?;
        } else {
            report.notes.push(format!(
                "WARNING: active sessions skipped ({}); history/index and catalog repair deferred",
                active.iter().cloned().collect::<Vec<_>>().join(",")
            ));
        }
    }
    if r.memory() {
        report.resources.push("memory".into());
        merge_codex_memory(local, remote, stage, &mut report, strategy, &mut conflicts)?;
    }
    Ok((report, metadata, conflicts))
}
fn prefix(a: &[Vec<u8>], b: &[Vec<u8>]) -> bool {
    a.len() < b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}
fn merge_json_file(a: &Path, b: &Path, out: &Path, key: Option<&str>) -> Result<()> {
    let mut records: BTreeMap<String, Value> = BTreeMap::new();
    for p in [a, b] {
        if !p.exists() {
            continue;
        }
        for line in fs::read_to_string(p)?.lines() {
            let v: Value = serde_json::from_str(line)?;
            let k = key
                .and_then(|k| v.get(k))
                .map(Value::to_string)
                .unwrap_or_else(|| bytes_sha256(serde_json::to_string(&v).unwrap().as_bytes()));
            let newer = v
                .get("updated_at")
                .or_else(|| v.get("ts"))
                .map(Value::to_string)
                .unwrap_or_default();
            let old = records
                .get(&k)
                .and_then(|x| x.get("updated_at").or_else(|| x.get("ts")))
                .map(Value::to_string)
                .unwrap_or_default();
            if !records.contains_key(&k) || newer > old {
                records.insert(k, v);
            }
        }
    }
    if let Some(parent) = out.parent() {
        private_dir(parent)?
    }
    let mut text = String::new();
    for v in records.values() {
        text.push_str(&serde_json::to_string(v)?);
        text.push('\n')
    }
    fs::write(out, text)?;
    Ok(())
}

fn merge_codex_memory(
    local: &Path,
    remote: &Path,
    stage: &Path,
    report: &mut PlanReport,
    strategy: ConflictStrategy,
    conflicts: &mut Vec<CodexConflict>,
) -> Result<()> {
    let a = local.join("memories");
    let b = remote.join("memories");
    let paths = memory_paths(&a)?
        .union(&memory_paths(&b)?)
        .cloned()
        .collect::<Vec<_>>();
    for rel in paths {
        let l = a.join(&rel);
        let r = b.join(&rel);
        let out = stage.join("memories").join(&rel);
        match (l.exists(), r.exists()) {
            (true, false) => {
                report.remote_additions += 1;
                copy(&l, &out)?
            }
            (false, true) => {
                report.local_additions += 1;
                copy(&r, &out)?
            }
            (true, true) => {
                let lb = fs::read(&l)?;
                let rb = fs::read(&r)?;
                if lb == rb {
                    report.identical += 1;
                    copy(&l, &out)?
                } else if rel == Path::new("raw_memories.md") {
                    fs::create_dir_all(out.parent().unwrap())?;
                    fs::write(
                        out,
                        merge_blocks(
                            &String::from_utf8(lb)?,
                            &String::from_utf8(rb)?,
                            "## Thread: ",
                        ),
                    )?
                } else if rel == Path::new("MEMORY.md") {
                    fs::create_dir_all(out.parent().unwrap())?;
                    fs::write(
                        out,
                        merge_blocks(&String::from_utf8(lb)?, &String::from_utf8(rb)?, "### "),
                    )?
                } else if rel == Path::new("memory_summary.md") {
                    fs::create_dir_all(out.parent().unwrap())?;
                    fs::write(
                        out,
                        format!(
                            "# Synchronized memory\n\n## Local view\n\n{}\n## Remote view\n\n{}",
                            String::from_utf8(lb)?,
                            String::from_utf8(rb)?
                        ),
                    )?
                } else {
                    match strategy {
                        ConflictStrategy::Local => copy(&l, &out)?,
                        ConflictStrategy::Remote => copy(&r, &out)?,
                        ConflictStrategy::Ask => {
                            conflicts.push(CodexConflict {
                                resource: "memory",
                                key: rel.display().to_string(),
                                local_relative: Path::new("memories").join(&rel),
                                remote_relative: Path::new("memories").join(&rel),
                                local_bytes: lb,
                                remote_bytes: rb,
                            });
                            report.blockers.push(Blocker {
                                resource: "memory".into(),
                                path: rel.display().to_string(),
                                reason: "Codex memory leaf requires a choice".into(),
                            })
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
fn memory_paths(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut s = BTreeSet::new();
    if root.exists() {
        for e in WalkDir::new(root).follow_links(false) {
            let e = e?;
            if e.file_type().is_symlink() {
                bail!("symlink in Codex memory")
            };
            if e.file_type().is_file() {
                let r = e.path().strip_prefix(root)?.to_path_buf();
                if !r.to_string_lossy().contains("sync-backups")
                    && !r
                        .components()
                        .any(|c| c.as_os_str() == ".git" || c.as_os_str() == ".omx")
                {
                    s.insert(r);
                }
            }
        }
    }
    Ok(s)
}
fn copy(a: &Path, b: &Path) -> Result<()> {
    if let Some(p) = b.parent() {
        private_dir(p)?
    }
    fs::copy(a, b)?;
    Ok(())
}
fn merge_blocks(a: &str, b: &str, heading: &str) -> String {
    let mut preamble = String::new();
    let mut blocks: BTreeMap<String, String> = BTreeMap::new();
    for text in [a, b] {
        let mut current = String::new();
        for line in text.lines() {
            if line.starts_with(heading) {
                if !current.is_empty() {
                    let key = current.lines().next().unwrap_or("").to_owned();
                    if blocks.get(&key).map(String::len).unwrap_or(0) < current.len() {
                        blocks.insert(key, current.clone());
                    }
                }
                current.clear();
            }
            if current.is_empty() && !line.starts_with(heading) {
                if preamble.is_empty() {
                    preamble.push_str(line);
                    preamble.push('\n')
                }
            } else {
                current.push_str(line);
                current.push('\n')
            }
        }
        if !current.is_empty() {
            let key = current.lines().next().unwrap_or("").to_owned();
            if blocks.get(&key).map(String::len).unwrap_or(0) < current.len() {
                blocks.insert(key, current);
            }
        }
    }
    format!(
        "{}\n{}",
        preamble.trim_end(),
        blocks.into_values().collect::<Vec<_>>().join("\n")
    )
}

fn active_writer_ids(local: &Path, remote: &str, t: &SshTransport) -> Result<BTreeSet<String>> {
    let mut s = local_active_writer_ids(local)?;
    let remote_ids: Vec<String> = t.remote_request(&RemoteRequest::CodexActiveWriters {
        root: remote.to_owned(),
    })?;
    for id in remote_ids {
        s.insert(id);
    }
    Ok(s)
}

pub(crate) fn local_active_writer_ids(local: &Path) -> Result<BTreeSet<String>> {
    let mut s = BTreeSet::new();
    let d = local.join("thread-writer-locks");
    if d.exists() {
        for e in fs::read_dir(d)? {
            let e = e?;
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(id) = find_uuid(&n) {
                let f = OpenOptions::new().read(true).write(true).open(e.path())?;
                if file_lock_is_held(&f)? {
                    s.insert(id);
                }
            }
        }
    }
    Ok(s)
}

pub(crate) fn validate_archive_snapshot(root: &Path, resources: ResourceSelection) -> Result<()> {
    if resources.sessions() {
        scan_sessions(root, &BTreeSet::new())?;
    }
    Ok(())
}

pub(crate) fn archive_backup(
    root: &Path,
    resources: ResourceSelection,
    archive_stamp: &str,
) -> Result<PathBuf> {
    backup_local(root, resources, archive_stamp)
}
fn find_uuid(s: &str) -> Option<String> {
    for part in s.split(|c: char| !(c.is_ascii_hexdigit() || c == '-')) {
        if uuid::Uuid::parse_str(part).is_ok() {
            return Some(part.to_owned());
        }
    }
    None
}

struct CodexGuards {
    local: File,
    remote: RemoteGuard,
}
impl CodexGuards {
    fn acquire(local: &Path, remote: &str, t: &SshTransport) -> Result<Self> {
        let dir = local.join("thread-writer-locks");
        private_dir(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(".coordination.lock"))?;
        file.lock_exclusive()?;
        let remote = t.remote_guard(&RemoteRequest::HoldCoordinationLock {
            root: remote.to_owned(),
        })?;
        Ok(Self {
            local: file,
            remote,
        })
    }
}
impl Drop for CodexGuards {
    fn drop(&mut self) {
        let _ = &self.remote;
        let _ = FileExt::unlock(&self.local);
    }
}

fn backup_members(r: ResourceSelection) -> Vec<&'static str> {
    let mut members = Vec::new();
    if r.sessions() {
        members.extend([
            "sessions",
            "archived_sessions",
            "history.jsonl",
            "session_index.jsonl",
        ]);
    }
    if r.memory() {
        members.push("memories");
    }
    members
}

fn backup_local(root: &Path, r: ResourceSelection, stamp: &str) -> Result<PathBuf> {
    let d = root.join("sync-backups");
    private_dir(&d)?;
    let o = d.join(format!("before-{stamp}.tar.gz"));
    let members = backup_members(r)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    create_backup(root, &o, &members)?;
    Ok(o)
}
fn backup_remote(
    root: &str,
    r: ResourceSelection,
    stamp: &str,
    t: &SshTransport,
) -> Result<String> {
    #[derive(Deserialize)]
    struct BackupResult {
        path: String,
    }
    let value: BackupResult = t.remote_request(&RemoteRequest::Backup {
        root: root.to_owned(),
        backup_dir: "sync-backups".to_owned(),
        stamp: stamp.to_owned(),
        members: backup_members(r).into_iter().map(str::to_owned).collect(),
    })?;
    Ok(value.path)
}
fn install_local(stage: &Path, root: &Path, rsync: &str) -> Result<()> {
    let status = Command::new(rsync)
        .arg("-a")
        .arg(format!("{}/", stage.display()))
        .arg(format!("{}/", root.display()))
        .status()?;
    if !status.success() {
        bail!("local Codex install failed")
    }
    Ok(())
}
fn verify_selected(
    stage: &Path,
    actual: &Path,
    r: ResourceSelection,
    active: &BTreeSet<String>,
    side: &str,
) -> Result<()> {
    let exclude = |p: &Path| excluded(p, r) || active_excluded_path(p, active);
    let a = manifest(stage, exclude)?;
    let b = manifest(actual, exclude)?;
    if a != b {
        bail!("{side} final file set or content differs from the staged manifest");
    }
    Ok(())
}

fn verify_remote_inventory(
    stage: &Path,
    actual: &crate::core::Inventory,
    resources: ResourceSelection,
    active: &BTreeSet<String>,
) -> Result<()> {
    let expected = inventory(stage, |path| {
        excluded(path, resources) || active_excluded_path(path, active)
    })?;
    if expected.content_manifest() != actual.content_manifest() {
        bail!("remote final file set or content differs from the staged manifest");
    }
    Ok(())
}

fn reconcile_catalog(t: Option<&SshTransport>, scan: bool) -> Result<usize> {
    let mut c = if let Some(t) = t {
        let mut c = Command::new(&t.ssh);
        c.arg(&t.host)
            .arg("exec codex app-server --listen stdio://");
        c
    } else {
        let mut c = Command::new("codex");
        c.args(["app-server", "--listen", "stdio://"]);
        c
    };
    let mut child = c
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    rpc(
        &mut input,
        &mut output,
        1,
        "initialize",
        json!({"clientInfo":{"name":"agent-sync","version":env!("CARGO_PKG_VERSION")}}),
    )?;
    let mut total = 0;
    let mut request_id = 2;
    for archived in [false, true] {
        let mut cursor: Option<String> = None;
        loop {
            let r = rpc(
                &mut input,
                &mut output,
                request_id,
                "thread/list",
                json!({"archived":archived,"cursor":cursor,"limit":1000,"useStateDbOnly":!scan}),
            )?;
            request_id += 1;
            total += r
                .get("data")
                .and_then(Value::as_array)
                .context("invalid thread/list data")?
                .len();
            cursor = r
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if !scan || cursor.is_none() {
                break;
            }
        }
    }
    drop(input);
    let _ = child.wait();
    Ok(total)
}
fn rpc(
    input: &mut ChildStdin,
    output: &mut BufReader<ChildStdout>,
    id: i64,
    method: &str,
    params: Value,
) -> Result<Value> {
    writeln!(
        input,
        "{}",
        json!({"id":id,"method":method,"params":params})
    )?;
    input.flush()?;
    loop {
        let mut line = String::new();
        if output.read_line(&mut line)? == 0 {
            bail!("app-server ended during {method}")
        }
        let v: Value = serde_json::from_str(&line)?;
        if v.get("id").and_then(Value::as_i64) == Some(id) {
            if let Some(e) = v.get("error") {
                bail!("app-server {method}: {e}")
            }
            return v.get("result").cloned().context("missing RPC result");
        }
    }
}
fn state_db(root: &Path) -> Result<PathBuf> {
    let mut c = Vec::new();
    for e in fs::read_dir(root)? {
        let e = e?;
        let n = e.file_name().to_string_lossy().into_owned();
        if let Some(v) = n
            .strip_prefix("state_")
            .and_then(|s| s.strip_suffix(".sqlite"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            c.push((v, e.path()))
        }
    }
    c.into_iter()
        .max_by_key(|v| v.0)
        .map(|v| v.1)
        .context("no state_*.sqlite")
}
fn backup_state(root: &Path, stamp: &str) -> Result<PathBuf> {
    let source = Connection::open(state_db(root)?)?;
    let out = root
        .join("sync-backups")
        .join(format!("state-before-{stamp}.sqlite"));
    source.backup("main", &out, None)?;
    Ok(out)
}
fn repair_state(root: &Path, m: &BTreeMap<String, Times>) -> Result<usize> {
    let mut c = Connection::open(state_db(root)?)?;
    let tx = c.transaction()?;
    let mut changed = 0;
    for (id, v) in m {
        changed+=tx.execute("UPDATE threads SET created_at=?1,created_at_ms=?2,updated_at=?3,updated_at_ms=?4,recency_at=?5,recency_at_ms=?6 WHERE id=?7 AND (created_at_ms!=?2 OR updated_at_ms!=?4 OR recency_at_ms!=?6)",params![v.created_at_ms/1000,v.created_at_ms,v.updated_at_ms/1000,v.updated_at_ms,v.recency_at_ms/1000,v.recency_at_ms,id])?;
    }
    tx.commit()?;
    Ok(changed)
}
fn remote_state(
    t: &SshTransport,
    root: &str,
    stamp: &str,
    mode: &str,
    data: Option<&BTreeMap<String, Times>>,
) -> Result<String> {
    let times = data.map(|items| {
        items
            .iter()
            .map(|(id, value)| {
                (
                    id.clone(),
                    StateTimes {
                        created_at_ms: value.created_at_ms,
                        updated_at_ms: value.updated_at_ms,
                        recency_at_ms: value.recency_at_ms,
                    },
                )
            })
            .collect()
    });
    let value: Value = t.remote_request(&RemoteRequest::CodexState {
        root: root.to_owned(),
        stamp: stamp.to_owned(),
        times,
    })?;
    match mode {
        "backup" => value["path"]
            .as_str()
            .map(str::to_owned)
            .context("remote state backup omitted path"),
        "repair" => value["changed"]
            .as_u64()
            .map(|value| value.to_string())
            .context("remote state repair omitted count"),
        _ => bail!("unknown remote state mode: {mode}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uuid7_time() {
        assert!(uuid7_ms("019fe9a3-6ea4-71e1-bfce-ddfc8243ef05").is_some());
    }
    #[test]
    fn prefix_is_strict() {
        assert!(prefix(&[b"a".to_vec()], &[b"a".to_vec(), b"b".to_vec()]));
        assert!(!prefix(&[b"a".to_vec()], &[b"a".to_vec()]));
    }

    #[test]
    fn active_rollout_is_excluded_from_the_file_plan() {
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        let active = BTreeSet::from([id.to_owned()]);
        assert!(active_excluded_path(
            Path::new(&format!("sessions/2026/08/11/rollout-{id}.jsonl")),
            &active
        ));
        assert!(!active_excluded_path(
            Path::new("memories/MEMORY.md"),
            &active
        ));
        assert!(active_excluded_path(Path::new("history.jsonl"), &active));
        assert!(active_excluded_path(
            Path::new("session_index.jsonl"),
            &active
        ));
    }

    #[test]
    fn validates_rollout_ordinals_and_times() {
        let temp = tempfile::tempdir().unwrap();
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        let path = temp.path().join(format!("rollout-{id}.jsonl"));
        let lines = vec![
            format!("{{\"type\":\"session_meta\",\"ordinal\":0,\"payload\":{{\"id\":\"{id}\",\"timestamp\":\"2026-08-01T00:00:00Z\"}}}}\n").into_bytes(),
            b"{\"type\":\"task_started\",\"ordinal\":1,\"timestamp\":\"2026-08-01T01:00:00Z\"}\n".to_vec(),
        ];
        let (_, times) = validate_rollout(&path, &lines).unwrap();
        assert_eq!(times.updated_at_ms, 1_785_546_000_000);
        assert_eq!(times.recency_at_ms, times.updated_at_ms);
    }

    #[test]
    fn rejects_mixed_ordinals() {
        let temp = tempfile::tempdir().unwrap();
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        let path = temp.path().join(format!("rollout-{id}.jsonl"));
        let lines = vec![
            format!(
                "{{\"type\":\"session_meta\",\"ordinal\":0,\"payload\":{{\"id\":\"{id}\"}}}}\n"
            )
            .into_bytes(),
            b"{\"type\":\"event\",\"timestamp\":\"2026-08-01T00:00:00Z\"}\n".to_vec(),
        ];
        assert!(validate_rollout(&path, &lines).is_err());
    }

    fn write_rollout(root: &Path, id: &str, message: &str) -> PathBuf {
        let path = root
            .join("sessions/2026/08/11")
            .join(format!("rollout-2026-08-11T00-00-00-{id}.jsonl"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            format!(
                "{{\"type\":\"session_meta\",\"ordinal\":0,\"payload\":{{\"id\":\"{id}\",\"timestamp\":\"2026-08-11T00:00:00Z\"}}}}\n{{\"type\":\"event\",\"ordinal\":1,\"timestamp\":\"2026-08-11T00:01:00Z\",\"payload\":{{\"message\":\"{message}\"}}}}\n"
            ),
        )
        .unwrap();
        path
    }

    #[test]
    fn ask_strategy_exposes_codex_rollout_conflict_for_interactive_choice() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        write_rollout(&local, id, "local");
        write_rollout(&remote, id, "remote");

        let (report, _, conflicts) = build_stage(
            &local,
            &remote,
            &stage,
            ResourceSelection::Sessions,
            &BTreeSet::new(),
            "mini",
            ConflictStrategy::Ask,
        )
        .unwrap();

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].key, id);
        assert!(report.blockers[0].reason.contains("requires a choice"));
        assert!(!stage.join(&conflicts[0].local_relative).exists());
    }

    #[test]
    fn active_codex_session_is_warned_and_skipped_without_blocking_others() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        let active_id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        let inactive_id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef06";
        let active_path = write_rollout(&local, active_id, "still writing");
        let inactive_path = write_rollout(&local, inactive_id, "stable");
        fs::create_dir_all(&remote).unwrap();
        fs::write(local.join("history.jsonl"), "{\"id\":\"local\"}\n").unwrap();
        fs::write(remote.join("history.jsonl"), "{\"id\":\"remote\"}\n").unwrap();
        let active = BTreeSet::from([active_id.to_owned()]);

        let (report, metadata, conflicts) = build_stage(
            &local,
            &remote,
            &stage,
            ResourceSelection::Sessions,
            &active,
            "mini",
            ConflictStrategy::Ask,
        )
        .unwrap();

        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());
        assert!(report.notes.iter().any(|note| {
            note.contains("WARNING: active sessions skipped") && note.contains(active_id)
        }));
        assert!(
            !stage
                .join(active_path.strip_prefix(&local).unwrap())
                .exists()
        );
        assert!(
            stage
                .join(inactive_path.strip_prefix(&local).unwrap())
                .exists()
        );
        assert!(!stage.join("history.jsonl").exists());
        assert!(!stage.join("session_index.jsonl").exists());
        assert!(!metadata.contains_key(active_id));
        assert!(metadata.contains_key(inactive_id));
    }

    #[test]
    fn explicit_codex_strategy_resolves_rollout_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        let local_path = write_rollout(&local, id, "local");
        write_rollout(&remote, id, "remote");
        for (name, strategy, expected) in [
            ("local-stage", ConflictStrategy::Local, "local"),
            ("remote-stage", ConflictStrategy::Remote, "remote"),
        ] {
            let stage = temp.path().join(name);
            let (report, _, conflicts) = build_stage(
                &local,
                &remote,
                &stage,
                ResourceSelection::Sessions,
                &BTreeSet::new(),
                "mini",
                strategy,
            )
            .unwrap();
            assert!(report.blockers.is_empty());
            assert!(conflicts.is_empty());
            let relative = local_path.strip_prefix(&local).unwrap();
            assert!(
                fs::read_to_string(stage.join(relative))
                    .unwrap()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn codex_memory_leaf_conflict_uses_the_selected_strategy() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        fs::create_dir_all(local.join("memories/skills/example")).unwrap();
        fs::create_dir_all(remote.join("memories/skills/example")).unwrap();
        fs::write(local.join("memories/skills/example/SKILL.md"), "local\n").unwrap();
        fs::write(remote.join("memories/skills/example/SKILL.md"), "remote\n").unwrap();

        let ask_stage = temp.path().join("ask-stage");
        let (report, _, conflicts) = build_stage(
            &local,
            &remote,
            &ask_stage,
            ResourceSelection::Memory,
            &BTreeSet::new(),
            "mini",
            ConflictStrategy::Ask,
        )
        .unwrap();
        assert_eq!(conflicts.len(), 1);
        assert!(report.blockers[0].reason.contains("requires a choice"));

        let local_stage = temp.path().join("local-stage");
        let (report, _, conflicts) = build_stage(
            &local,
            &remote,
            &local_stage,
            ResourceSelection::Memory,
            &BTreeSet::new(),
            "mini",
            ConflictStrategy::Local,
        )
        .unwrap();
        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());
        assert_eq!(
            fs::read_to_string(local_stage.join("memories/skills/example/SKILL.md")).unwrap(),
            "local\n"
        );
    }

    #[test]
    fn memory_block_merge_prefers_richer_duplicate() {
        let merged = merge_blocks(
            "# Memory\n\n## Thread: abc\nshort\n",
            "# Memory\n\n## Thread: abc\nlonger details\n",
            "## Thread: ",
        );
        assert!(merged.contains("longer details"));
        assert!(!merged.contains("\nshort\n"));
    }

    #[test]
    fn final_verification_ignores_private_memory_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let stage = temp.path().join("stage");
        let actual = temp.path().join("actual");
        fs::create_dir_all(stage.join("memories")).unwrap();
        fs::create_dir_all(actual.join("memories/.git")).unwrap();
        fs::create_dir_all(actual.join("memories/.omx")).unwrap();
        fs::create_dir_all(actual.join("memories/nested/.git")).unwrap();
        fs::write(stage.join("memories/MEMORY.md"), "shared\n").unwrap();
        fs::write(actual.join("memories/MEMORY.md"), "shared\n").unwrap();
        fs::write(actual.join("memories/.git/config"), "private\n").unwrap();
        fs::write(actual.join("memories/.omx/state"), "private\n").unwrap();
        fs::write(actual.join("memories/nested/.git/config"), "private\n").unwrap();

        verify_selected(
            &stage,
            &actual,
            ResourceSelection::Memory,
            &BTreeSet::new(),
            "local",
        )
        .unwrap();
    }
}
