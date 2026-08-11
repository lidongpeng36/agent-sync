use super::{Adapter, Prepared};
use crate::core::{
    Blocker, FileAction, FileChange, Inventory, InventoryEntry, PlanReport, ResourceSelection,
    SyncOptions, bytes_sha256, print_planned_diff, private_dir, shorten_middle, stamp,
};
use crate::remote::Request as RemoteRequest;
use crate::transport::SshTransport;
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

pub struct OpenCodeAdapter;

#[derive(Deserialize)]
struct OpenCodeWritersResult {
    active: bool,
}

#[derive(Deserialize)]
struct OpenCodeBackupResult {
    path: String,
}

#[derive(Clone)]
struct SessionExport {
    value: Value,
}

pub struct OpenCodePrepared {
    pub report: PlanReport,
    temp: TempDir,
    local_snapshot: PathBuf,
    remote_snapshot: PathBuf,
    stage: PathBuf,
    local_inventory: Inventory,
    remote_inventory: Inventory,
    result_fingerprint: BTreeMap<String, String>,
    state_root: PathBuf,
    local_node_id: String,
    remote_node_id: String,
}

pub(super) fn print_diff(prepared: &OpenCodePrepared, _local: &Path) -> Result<()> {
    println!(
        "# agent-sync: agent=opencode peer={} status={}",
        prepared.report.peer,
        if prepared.report.blockers.is_empty() {
            "ready"
        } else {
            "action-required"
        }
    );
    print_planned_diff(
        &prepared.local_snapshot,
        &prepared.remote_snapshot,
        &prepared.stage,
        |_| false,
    )
}

impl SessionExport {
    fn parse(bytes: &[u8], expected_id: &str) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes).context("decode OpenCode export")?;
        let id = value
            .pointer("/info/id")
            .and_then(Value::as_str)
            .context("OpenCode export omitted info.id")?;
        if id != expected_id {
            bail!("OpenCode export id mismatch: expected {expected_id}, found {id}");
        }
        let messages = value
            .get("messages")
            .and_then(Value::as_array)
            .context("OpenCode export omitted messages")?;
        let mut message_ids = BTreeSet::new();
        let mut part_ids = BTreeSet::new();
        for message in messages {
            let info = message
                .get("info")
                .and_then(Value::as_object)
                .context("OpenCode message omitted info")?;
            let message_id = info
                .get("id")
                .and_then(Value::as_str)
                .context("OpenCode message omitted id")?;
            if info.get("sessionID").and_then(Value::as_str) != Some(expected_id) {
                bail!("OpenCode message {message_id} references another session");
            }
            if !message_ids.insert(message_id.to_owned()) {
                bail!("duplicate OpenCode message id: {message_id}");
            }
            for part in message
                .get("parts")
                .and_then(Value::as_array)
                .context("OpenCode message omitted parts")?
            {
                let part_id = part
                    .get("id")
                    .and_then(Value::as_str)
                    .context("OpenCode part omitted id")?;
                if part.get("sessionID").and_then(Value::as_str) != Some(expected_id)
                    || part.get("messageID").and_then(Value::as_str) != Some(message_id)
                {
                    bail!("OpenCode part {part_id} has inconsistent references");
                }
                if !part_ids.insert(part_id.to_owned()) {
                    bail!("duplicate OpenCode part id: {part_id}");
                }
            }
        }
        Ok(Self { value })
    }

    fn id(&self) -> &str {
        self.value["info"]["id"].as_str().unwrap_or_default()
    }

    fn messages(&self) -> &[Value] {
        self.value["messages"].as_array().map_or(&[], Vec::as_slice)
    }

    fn canonical_hash(&self) -> Result<String> {
        canonical_hash_value(&self.value)
    }

    fn semantically_equal(&self, other: &Self) -> bool {
        semantic_json(&self.value) == semantic_json(&other.value)
    }

    fn pretty(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(&self.value)?)
    }

    fn display_name(&self) -> String {
        let project = self.value["info"]["directory"]
            .as_str()
            .and_then(|directory| Path::new(directory).file_name())
            .and_then(|name| name.to_str())
            .unwrap_or("global");
        let title = self.value["info"]["title"]
            .as_str()
            .filter(|title| !title.trim().is_empty())
            .or_else(|| self.value["info"]["slug"].as_str())
            .unwrap_or_else(|| self.id());
        format!("{project}/{title}")
    }
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        Value::Object(items) => {
            let sorted = items
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        _ => value.clone(),
    }
}

fn semantic_json(value: &Value) -> Value {
    let mut normalized = canonical_json(value);
    if let Some(info) = normalized.get_mut("info").and_then(Value::as_object_mut) {
        // `opencode import` assigns project location from its current working
        // directory and may round the aggregate floating-point cost. These
        // fields cannot round-trip between machines and are not transcript data.
        for key in ["directory", "path", "projectID", "cost"] {
            info.remove(key);
        }
    }
    normalized
}

pub(crate) fn canonical_hash_value(value: &Value) -> Result<String> {
    Ok(bytes_sha256(&serde_json::to_vec(&semantic_json(value))?))
}

fn valid_session_id(id: &str) -> bool {
    id.starts_with("ses_")
        && id.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
}

fn local_session_ids(root: &Path) -> Result<Vec<String>> {
    let connection = Connection::open_with_flags(
        root.join("opencode.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut statement = connection.prepare("SELECT id FROM session ORDER BY id")?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

#[derive(Clone, Debug)]
struct SessionRevision {
    id: String,
    modified: i64,
    packed_counts: u64,
}

fn session_revisions(root: &Path) -> Result<Vec<SessionRevision>> {
    let connection = Connection::open_with_flags(
        root.join("opencode.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut statement = connection.prepare(
        "SELECT s.id,
                MAX(s.time_updated,
                    COALESCE((SELECT MAX(m.time_updated) FROM message m WHERE m.session_id = s.id), 0),
                    COALESCE((SELECT MAX(p.time_updated) FROM part p WHERE p.session_id = s.id), 0)),
                (SELECT COUNT(*) FROM message m WHERE m.session_id = s.id),
                (SELECT COUNT(*) FROM part p WHERE p.session_id = s.id)
           FROM session s
          ORDER BY s.id",
    )?;
    statement
        .query_map([], |row| {
            let message_count = row.get::<_, u64>(2)?;
            let part_count = row.get::<_, u64>(3)?;
            if message_count > u32::MAX.into() || part_count > u32::MAX.into() {
                return Err(rusqlite::Error::IntegralValueOutOfRange(
                    2,
                    message_count as i64,
                ));
            }
            Ok(SessionRevision {
                id: row.get(0)?,
                modified: row.get(1)?,
                packed_counts: (message_count << 32) | part_count,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(crate) fn cached_inventory(root: &Path, previous: Option<&Inventory>) -> Result<Inventory> {
    let previous = previous.map(Inventory::by_path).unwrap_or_default();
    let revisions = session_revisions(root)?;
    let mut hashes = BTreeMap::new();
    let mut dirty = Vec::new();
    let mut reused = 0;
    for revision in &revisions {
        let path = format!("sessions/{}.json", revision.id);
        if let Some(entry) = previous.get(path.as_str())
            && entry.size == revision.packed_counts
            && entry.modified_ns == revision.modified
        {
            hashes.insert(revision.id.clone(), entry.sha256.clone());
            reused += 1;
        } else {
            dirty.push(revision.id.clone());
        }
    }
    for (id, session) in export_local(&dirty)? {
        hashes.insert(id, session.canonical_hash()?);
    }
    let entries = revisions
        .into_iter()
        .map(|revision| {
            Ok(InventoryEntry {
                path: format!("sessions/{}.json", revision.id),
                sha256: hashes
                    .remove(&revision.id)
                    .context("OpenCode inventory hash was not computed")?,
                size: revision.packed_counts,
                modified_ns: revision.modified,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Inventory::new(entries, reused))
}

fn inventory_fingerprint(inventory: &Inventory) -> BTreeMap<String, String> {
    inventory
        .entries
        .iter()
        .filter_map(|entry| {
            Path::new(&entry.path)
                .file_stem()
                .and_then(|value| value.to_str())
                .map(|id| (id.to_owned(), entry.sha256.clone()))
        })
        .collect()
}

fn export_local_session(id: &str) -> Result<SessionExport> {
    if !valid_session_id(id) {
        bail!("unsafe OpenCode session id: {id:?}");
    }
    let output_file = tempfile::NamedTempFile::new()?;
    let output = Command::new("opencode")
        .args(["export", id])
        .stdout(Stdio::from(output_file.reopen()?))
        .output()?;
    if !output.status.success() {
        bail!(
            "OpenCode export failed for {id}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    SessionExport::parse(&fs::read(output_file.path())?, id)
        .with_context(|| format!("decode local OpenCode session {id}"))
}

fn export_local(ids: &[String]) -> Result<BTreeMap<String, SessionExport>> {
    let mut sessions = BTreeMap::new();
    for id in ids {
        sessions.insert(id.clone(), export_local_session(id)?);
    }
    Ok(sessions)
}

fn export_remote(
    ids: &[String],
    transport: &SshTransport,
) -> Result<BTreeMap<String, SessionExport>> {
    let exports: BTreeMap<String, Value> =
        transport.remote_request(&RemoteRequest::OpenCodeExports { ids: ids.to_vec() })?;
    exports
        .into_iter()
        .map(|(id, value)| {
            let bytes = serde_json::to_vec(&value)?;
            Ok((id.clone(), SessionExport::parse(&bytes, &id)?))
        })
        .collect()
}

fn is_prefix(shorter: &SessionExport, longer: &SessionExport) -> bool {
    shorter.messages().len() <= longer.messages().len()
        && shorter
            .messages()
            .iter()
            .zip(longer.messages())
            .all(|(left, right)| canonical_json(left) == canonical_json(right))
}

fn common_prefix(left: &SessionExport, right: &SessionExport) -> usize {
    left.messages()
        .iter()
        .zip(right.messages())
        .take_while(|(left, right)| canonical_json(left) == canonical_json(right))
        .count()
}

fn branch_token(session: &SessionExport, index: usize) -> String {
    session
        .messages()
        .get(index)
        .and_then(|message| message.pointer("/info/id"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| session.id())
        .to_owned()
}

fn deterministic_id(prefix: &str, seed: &str) -> String {
    format!("{prefix}_{}", &bytes_sha256(seed.as_bytes())[..26])
}

fn rewrite_fork(session: &SessionExport, fork_id: &str) -> Result<SessionExport> {
    let mut value = session.value.clone();
    value["info"]["id"] = Value::String(fork_id.to_owned());
    if let Some(title) = value["info"]["title"].as_str() {
        value["info"]["title"] = Value::String(format!("{title} [fork]"));
    }
    let messages = value["messages"]
        .as_array_mut()
        .context("OpenCode fork omitted messages")?;
    let message_map = messages
        .iter()
        .filter_map(|message| message.pointer("/info/id").and_then(Value::as_str))
        .map(|old| {
            (
                old.to_owned(),
                deterministic_id("msg", &format!("{fork_id}:{old}")),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for message in messages {
        let info = message["info"]
            .as_object_mut()
            .context("OpenCode fork message omitted info")?;
        let old_message = info["id"]
            .as_str()
            .context("OpenCode fork message omitted id")?
            .to_owned();
        info.insert(
            "id".into(),
            Value::String(message_map[&old_message].clone()),
        );
        info.insert("sessionID".into(), Value::String(fork_id.to_owned()));
        if let Some(parent) = info.get("parentID").and_then(Value::as_str)
            && let Some(rewritten) = message_map.get(parent)
        {
            info.insert("parentID".into(), Value::String(rewritten.clone()));
        }
        for part in message["parts"]
            .as_array_mut()
            .context("OpenCode fork message omitted parts")?
        {
            let old_part = part["id"]
                .as_str()
                .context("OpenCode fork part omitted id")?
                .to_owned();
            part["id"] = Value::String(deterministic_id("prt", &format!("{fork_id}:{old_part}")));
            part["messageID"] = Value::String(message_map[&old_message].clone());
            part["sessionID"] = Value::String(fork_id.to_owned());
        }
    }
    SessionExport::parse(&serde_json::to_vec(&value)?, fork_id)
}

fn insert_branch(
    result: &mut BTreeMap<String, SessionExport>,
    id: String,
    candidate: SessionExport,
    notes: &mut Vec<String>,
) -> Result<()> {
    let Some(existing) = result.get(&id).cloned() else {
        result.insert(id, candidate);
        return Ok(());
    };
    if existing.semantically_equal(&candidate) {
        return Ok(());
    }
    if is_prefix(&existing, &candidate) {
        result.insert(id, candidate);
        return Ok(());
    }
    if is_prefix(&candidate, &existing) {
        return Ok(());
    }
    let common = common_prefix(&existing, &candidate);
    let existing_token = branch_token(&existing, common);
    let candidate_token = branch_token(&candidate, common);
    let (original, fork_source, fork_token) = if existing_token <= candidate_token {
        (existing, candidate, candidate_token)
    } else {
        (candidate, existing, existing_token)
    };
    result.insert(id.clone(), original);
    let fork_id = deterministic_id("ses", &format!("{id}:{fork_token}"));
    let fork = rewrite_fork(&fork_source, &fork_id)?;
    notes.push(format!("session forked: {id} -> {fork_id}"));
    insert_branch(result, fork_id, fork, notes)
}

fn merge_exports(
    local: &BTreeMap<String, SessionExport>,
    remote: &BTreeMap<String, SessionExport>,
    report: &mut PlanReport,
) -> Result<BTreeMap<String, SessionExport>> {
    let mut result = BTreeMap::new();
    for (id, session) in local {
        insert_branch(&mut result, id.clone(), session.clone(), &mut report.notes)?;
    }
    for (id, session) in remote {
        insert_branch(&mut result, id.clone(), session.clone(), &mut report.notes)?;
    }
    for id in local.keys().chain(remote.keys()).collect::<BTreeSet<_>>() {
        match (local.get(id), remote.get(id)) {
            (Some(left), Some(right)) if left.semantically_equal(right) => report.identical += 1,
            (Some(left), Some(right)) if is_prefix(left, right) || is_prefix(right, left) => {
                report.advances += 1;
            }
            (Some(_), Some(_)) => {}
            (Some(_), None) => report.remote_additions += 1,
            (None, Some(_)) => report.local_additions += 1,
            (None, None) => unreachable!(),
        }
    }
    Ok(result)
}

fn write_exports(root: &Path, sessions: &BTreeMap<String, SessionExport>) -> Result<()> {
    let directory = root.join("sessions");
    private_dir(&directory)?;
    for (id, session) in sessions {
        fs::write(directory.join(format!("{id}.json")), session.pretty()?)?;
    }
    Ok(())
}

pub(crate) fn archive_snapshot(root: &Path, destination: &Path) -> Result<()> {
    ensure_cli_root(root)?;
    let sessions = export_local(&local_session_ids(root)?)?;
    write_exports(destination, &sessions)
}

pub(crate) fn validate_archive_snapshot(root: &Path) -> Result<()> {
    let directory = root.join("sessions");
    if !directory.exists() {
        bail!("OpenCode archive omitted sessions directory");
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            bail!("unsupported OpenCode archive entry: {}", path.display());
        }
        let id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("invalid OpenCode archive filename")?;
        SessionExport::parse(&fs::read(&path)?, id)?;
    }
    Ok(())
}

pub(crate) fn archive_session_hashes(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let directory = root.join("sessions");
    if !directory.exists() {
        return Ok(result);
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("invalid OpenCode archive filename")?;
        let session = SessionExport::parse(&fs::read(&path)?, id)?;
        result.insert(id.to_owned(), session.canonical_hash()?);
    }
    Ok(result)
}

pub(crate) fn current_session_hashes(root: &Path) -> Result<BTreeMap<String, String>> {
    ensure_cli_root(root)?;
    export_local(&local_session_ids(root)?)?
        .into_iter()
        .map(|(id, session)| Ok((id, session.canonical_hash()?)))
        .collect::<Result<_>>()
}

pub(crate) fn apply_archive_snapshot(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root.join("sessions"))? {
        import_local(&entry?.path())?;
    }
    Ok(())
}

pub(crate) fn archive_has_writers(root: &Path) -> Result<bool> {
    local_writers(root)
}

pub(crate) fn archive_backup(root: &Path, archive_stamp: &str) -> Result<PathBuf> {
    ensure_cli_root(root)?;
    local_backup(root, archive_stamp)
}

fn ensure_cli_root(root: &Path) -> Result<()> {
    let output = Command::new("opencode").args(["db", "path"]).output()?;
    if !output.status.success() {
        bail!(
            "`opencode db path` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let configured = fs::canonicalize(root.join("opencode.db"))?;
    let reported = fs::canonicalize(PathBuf::from(String::from_utf8(output.stdout)?.trim()))?;
    if reported != configured {
        bail!(
            "configured OpenCode root differs from `opencode db path`: configured={}, reported={}",
            configured.display(),
            reported.display()
        );
    }
    Ok(())
}

fn fingerprint(sessions: &BTreeMap<String, SessionExport>) -> Result<BTreeMap<String, String>> {
    sessions
        .iter()
        .map(|(id, session)| Ok((id.clone(), session.canonical_hash()?)))
        .collect()
}

fn session_action(current: Option<&SessionExport>, result: &SessionExport) -> FileAction {
    match current {
        None => FileAction::Create,
        Some(current) if current.semantically_equal(result) => FileAction::Unchanged,
        Some(_) => FileAction::Replace,
    }
}

fn planned_session_changes(
    local: &BTreeMap<String, SessionExport>,
    remote: &BTreeMap<String, SessionExport>,
    result: &BTreeMap<String, SessionExport>,
) -> Result<Vec<FileChange>> {
    let mut changes = Vec::new();
    for (id, merged) in result {
        let local_session = local.get(id);
        let remote_session = remote.get(id);
        let local_action = session_action(local_session, merged);
        let remote_action = session_action(remote_session, merged);
        if local_action == FileAction::Unchanged && remote_action == FileAction::Unchanged {
            continue;
        }
        let resolution = if local_session.is_some_and(|value| value.semantically_equal(merged))
            && remote_session.is_some_and(|value| value.semantically_equal(merged))
        {
            "identical"
        } else if local_session.is_some_and(|value| value.semantically_equal(merged)) {
            "local"
        } else if remote_session.is_some_and(|value| value.semantically_equal(merged)) {
            "remote"
        } else if local_session.is_none() && remote_session.is_none() {
            "generated"
        } else {
            "merged"
        };
        changes.push(FileChange {
            resource: "sessions".into(),
            path: format!("sessions/{id}.json"),
            display_path: id.clone(),
            local: local_action,
            remote: remote_action,
            resolution: resolution.into(),
            local_sha256: local_session
                .map(SessionExport::canonical_hash)
                .transpose()?,
            remote_sha256: remote_session
                .map(SessionExport::canonical_hash)
                .transpose()?,
            result_sha256: Some(merged.canonical_hash()?),
        });
    }
    Ok(changes)
}

fn lsof_command() -> &'static str {
    if Path::new("/usr/sbin/lsof").exists() {
        "/usr/sbin/lsof"
    } else {
        "lsof"
    }
}

fn local_writers(root: &Path) -> Result<bool> {
    let output = Command::new(lsof_command())
        .args(["-Fpf"])
        .arg(root.join("opencode.db"))
        .output()?;
    let mut process = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if line.starts_with('p') {
            process = true;
        } else if process
            && line.starts_with('f')
            && line[1..].chars().take_while(char::is_ascii_digit).count() > 0
            && line
                .chars()
                .any(|character| character == 'w' || character == 'u')
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn local_backup(root: &Path, backup_stamp: &str) -> Result<PathBuf> {
    let directory = root.join("agent-sync-backups");
    private_dir(&directory)?;
    let destination = directory.join(format!("before-{backup_stamp}.db"));
    let connection = Connection::open(root.join("opencode.db"))?;
    connection.execute(
        "VACUUM INTO ?1",
        params![destination.to_string_lossy().as_ref()],
    )?;
    Ok(destination)
}

fn import_local(path: &Path) -> Result<()> {
    let output = Command::new("opencode").arg("import").arg(path).output()?;
    if !output.status.success() {
        bail!(
            "OpenCode import failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn import_remote(bytes: &[u8], transport: &SshTransport) -> Result<()> {
    transport.ssh_with_input(remote_import_script(), bytes)?;
    Ok(())
}

fn remote_import_script() -> &'static str {
    "set -eu; umask 077; p=$(mktemp \"${TMPDIR:-/tmp}/agent-sync-opencode-import.XXXXXX\"); trap 'rm -f \"$p\"' EXIT; cat > \"$p\"; opencode import \"$p\""
}

fn decorate_paths(
    files: &mut [crate::core::FileChange],
    sessions: &BTreeMap<String, SessionExport>,
) {
    for file in files {
        let Some(id) = Path::new(&file.path)
            .file_stem()
            .and_then(|value| value.to_str())
        else {
            continue;
        };
        if let Some(session) = sessions.get(id) {
            file.display_path = shorten_middle(&session.display_name(), 68);
        }
    }
}

impl Adapter for OpenCodeAdapter {
    fn doctor(&self, local: &Path, remote: &str, transport: &SshTransport) -> Result<()> {
        if !local.join("opencode.db").is_file() {
            bail!("OpenCode database does not exist: {}", local.display());
        }
        for command in [&transport.ssh, "opencode", lsof_command()] {
            if !SshTransport::command_exists(command) {
                bail!("required local command not found: {command}");
            }
        }
        let output = Command::new("opencode").args(["db", "path"]).output()?;
        if !output.status.success() {
            bail!(
                "`opencode db path` failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let configured = fs::canonicalize(local.join("opencode.db"))?;
        let reported = fs::canonicalize(PathBuf::from(String::from_utf8(output.stdout)?.trim()))?;
        if reported != configured {
            bail!(
                "configured OpenCode root differs from `opencode db path`: configured={}, reported={}",
                configured.display(),
                reported.display()
            );
        }
        let _: Value = transport.remote_request(&RemoteRequest::Doctor {
            root: remote.to_owned(),
            agent: "opencode".into(),
        })?;
        Ok(())
    }

    fn prepare(
        &self,
        local: &Path,
        remote: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<Prepared> {
        if options.resources == ResourceSelection::Memory {
            bail!("OpenCode does not expose a separate memory resource; use --only sessions");
        }
        let temp = tempfile::Builder::new()
            .prefix("agent-sync-opencode-")
            .tempdir()?;
        let state_root = crate::state::state_root(options)?;
        let local_node_id = crate::state::node_id(&state_root)?;
        let remote_node_id = transport.remote_node_id()?;
        let _scan_guard = transport.remote_guard(&RemoteRequest::HoldSyncLock {
            agent: "opencode".to_owned(),
            resources: options.resources,
        })?;
        let previous_local =
            crate::state::load(&state_root, "opencode", &transport.host, options.resources)?;
        let local_inventory =
            cached_inventory(local, previous_local.as_ref().map(|value| &value.inventory))?;
        let remote_inventory: Inventory =
            transport.remote_request(&RemoteRequest::OpenCodeInventory {
                root: remote.to_owned(),
                resources: options.resources,
                peer_id: local_node_id.clone(),
                previous: None,
            })?;
        let local_fingerprint = inventory_fingerprint(&local_inventory);
        let remote_fingerprint = inventory_fingerprint(&remote_inventory);
        let local_changed = local_fingerprint
            .iter()
            .filter(|(id, hash)| remote_fingerprint.get(*id) != Some(*hash))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let remote_changed = remote_fingerprint
            .iter()
            .filter(|(id, hash)| local_fingerprint.get(*id) != Some(*hash))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let local_sessions = export_local(&local_changed)?;
        let remote_sessions = export_remote(&remote_changed, transport)?;
        let local_snapshot = temp.path().join("local");
        let remote_snapshot = temp.path().join("remote");
        let stage = temp.path().join("stage");
        write_exports(&local_snapshot, &local_sessions)?;
        write_exports(&remote_snapshot, &remote_sessions)?;
        let mut report = PlanReport {
            agent: "opencode".into(),
            peer: transport.host.clone(),
            resources: vec!["sessions".into()],
            ..PlanReport::default()
        };
        report
            .notes
            .push("OpenCode has no separate memory resource; credentials are excluded".into());
        report.notes.push(format!(
            "manifest: hashes reused local={}/{}, remote={}/{}; sessions exported local={}, remote={}",
            local_inventory.reused_entries,
            local_inventory.entries.len(),
            remote_inventory.reused_entries,
            remote_inventory.entries.len(),
            local_changed.len(),
            remote_changed.len(),
        ));
        report.identical = local_fingerprint
            .iter()
            .filter(|(id, hash)| remote_fingerprint.get(*id) == Some(*hash))
            .count();
        let result = merge_exports(&local_sessions, &remote_sessions, &mut report)?;
        write_exports(&stage, &result)?;
        report.files = planned_session_changes(&local_sessions, &remote_sessions, &result)?;
        decorate_paths(&mut report.files, &result);
        let mut result_fingerprint = local_fingerprint
            .iter()
            .filter(|(id, hash)| remote_fingerprint.get(*id) == Some(*hash))
            .map(|(id, hash)| (id.clone(), hash.clone()))
            .collect::<BTreeMap<_, _>>();
        result_fingerprint.extend(fingerprint(&result)?);
        if options.apply {
            let remote_writers: OpenCodeWritersResult =
                transport.remote_request(&RemoteRequest::OpenCodeWriters {
                    root: remote.to_owned(),
                })?;
            if local_writers(local)? || remote_writers.active {
                report.blockers.push(Blocker {
                    resource: "sessions".into(),
                    path: "opencode.db".into(),
                    reason: "active OpenCode writers must exit before apply".into(),
                });
            }
        }
        Ok(Prepared::OpenCode(OpenCodePrepared {
            report,
            temp,
            local_snapshot,
            remote_snapshot,
            stage,
            local_inventory,
            remote_inventory,
            result_fingerprint,
            state_root,
            local_node_id,
            remote_node_id,
        }))
    }

    fn resolve_interactive(&self, _prepared: &mut Prepared, _tty: bool) -> Result<()> {
        Ok(())
    }

    fn apply(
        &self,
        prepared: Prepared,
        local: &Path,
        remote: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<()> {
        let Prepared::OpenCode(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        let _sync_guards = transport.sync_guards(
            &value.state_root,
            &value.local_node_id,
            &value.remote_node_id,
            "opencode",
            options.resources,
        )?;
        transport.ensure_no_pending_transaction(&value.state_root, "opencode")?;
        let current_local = cached_inventory(local, Some(&value.local_inventory))?;
        let current_remote: Inventory =
            transport.remote_request(&RemoteRequest::OpenCodeInventory {
                root: remote.to_owned(),
                resources: options.resources,
                peer_id: value.local_node_id.clone(),
                previous: Some(value.remote_inventory.clone()),
            })?;
        if inventory_fingerprint(&current_local) != inventory_fingerprint(&value.local_inventory) {
            bail!("local OpenCode sessions changed after preview");
        }
        if inventory_fingerprint(&current_remote) != inventory_fingerprint(&value.remote_inventory)
        {
            bail!("remote OpenCode sessions changed after preview");
        }
        let remote_writers: OpenCodeWritersResult =
            transport.remote_request(&RemoteRequest::OpenCodeWriters {
                root: remote.to_owned(),
            })?;
        if local_writers(local)? || remote_writers.active {
            bail!("refusing apply while OpenCode writers are active");
        }
        thread::sleep(Duration::from_secs_f64(options.stability_seconds));
        if local_writers(local)? {
            bail!("a local OpenCode writer became active");
        }
        let remote_writers: OpenCodeWritersResult =
            transport.remote_request(&RemoteRequest::OpenCodeWriters {
                root: remote.to_owned(),
            })?;
        if remote_writers.active {
            bail!("a remote OpenCode writer became active");
        }

        let backup_stamp = stamp();
        let local_backup = local_backup(local, &backup_stamp)?;
        let remote_backup: OpenCodeBackupResult =
            transport.remote_request(&RemoteRequest::OpenCodeBackup {
                root: remote.to_owned(),
                stamp: backup_stamp,
            })?;
        let mut journal = crate::state::TransactionJournal::new(
            "opencode",
            options.resources,
            &value.local_node_id,
            &value.remote_node_id,
            &value.local_inventory.generation,
            &value.remote_inventory.generation,
            &bytes_sha256(&serde_json::to_vec(&value.result_fingerprint)?),
            &local_backup,
            &remote_backup.path,
        );
        transport.save_transaction_pair(&value.state_root, &journal)?;
        for file in &value.report.files {
            let path = value.stage.join(&file.path);
            if !matches!(file.local, FileAction::Unchanged) {
                import_local(&path)?;
            }
        }
        journal.phase = crate::state::TransactionPhase::LocalApplied;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        for file in &value.report.files {
            let path = value.stage.join(&file.path);
            if !matches!(file.remote, FileAction::Unchanged) {
                import_remote(&fs::read(&path)?, transport)?;
            }
        }
        journal.phase = crate::state::TransactionPhase::RemoteApplied;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        let verified_local = cached_inventory(local, Some(&value.local_inventory))?;
        let verified_remote: Inventory =
            transport.remote_request(&RemoteRequest::OpenCodeInventory {
                root: remote.to_owned(),
                resources: options.resources,
                peer_id: value.local_node_id.clone(),
                previous: Some(value.remote_inventory.clone()),
            })?;
        let verified_local_fingerprint = inventory_fingerprint(&verified_local);
        let verified_remote_fingerprint = inventory_fingerprint(&verified_remote);
        if verified_local_fingerprint != value.result_fingerprint {
            bail!("local OpenCode sessions differ after import");
        }
        if verified_remote_fingerprint != value.result_fingerprint {
            bail!("remote OpenCode sessions differ after import");
        }
        journal.phase = crate::state::TransactionPhase::Verified;
        transport.save_transaction_pair(&value.state_root, &journal)?;
        let local_checkpoint = crate::state::Checkpoint::new(
            "opencode",
            options.resources,
            &transport.host,
            verified_local,
        );
        let mut remote_checkpoint = local_checkpoint.clone();
        remote_checkpoint.peer = value.local_node_id.clone();
        remote_checkpoint.inventory = verified_remote;
        remote_checkpoint.result_content_hash =
            crate::state::content_hash(&remote_checkpoint.inventory);
        if remote_checkpoint.result_content_hash != local_checkpoint.result_content_hash {
            bail!("cannot checkpoint divergent OpenCode results");
        }
        let _: Value = transport.remote_request(&RemoteRequest::SaveCheckpoint {
            checkpoint: remote_checkpoint,
        })?;
        crate::state::save(&value.state_root, &local_checkpoint)?;
        transport.clear_transaction_pair(&value.state_root, &journal)?;
        println!(
            "complete: OpenCode sessions synchronized and verified; backups: local={}, remote={}:{}",
            local_backup.display(),
            transport.host,
            remote_backup.path
        );
        drop(value.temp);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn session(id: &str, message_ids: &[&str]) -> SessionExport {
        let messages = message_ids
            .iter()
            .map(|message| {
                json!({
                    "info": {"id": message, "sessionID": id},
                    "parts": [{
                        "id": format!("prt-{message}"),
                        "messageID": message,
                        "sessionID": id,
                        "type": "text",
                        "text": message,
                    }],
                })
            })
            .collect::<Vec<_>>();
        SessionExport::parse(
            &serde_json::to_vec(&json!({
                "info": {
                    "id": id,
                    "directory": "/repo/project",
                    "title": "test",
                    "time": {"updated": 1},
                },
                "messages": messages,
            }))
            .unwrap(),
            id,
        )
        .unwrap()
    }

    #[test]
    fn unchanged_sqlite_revision_reuses_checkpoint_hash_without_export() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("opencode.db");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session (id TEXT PRIMARY KEY, time_updated INTEGER NOT NULL);
                 CREATE TABLE message (
                     id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     time_updated INTEGER NOT NULL
                 );
                 CREATE TABLE part (
                     id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     time_updated INTEGER NOT NULL
                 );
                 INSERT INTO session VALUES ('ses_cached', 10);
                 INSERT INTO message VALUES ('msg_cached', 'ses_cached', 20);
                 INSERT INTO part VALUES ('prt_cached', 'ses_cached', 30);",
            )
            .unwrap();
        drop(connection);
        let previous = Inventory::new(
            vec![InventoryEntry {
                path: "sessions/ses_cached.json".into(),
                sha256: "cached-hash".into(),
                size: (1_u64 << 32) | 1,
                modified_ns: 30,
            }],
            0,
        );

        let inventory = cached_inventory(temp.path(), Some(&previous)).unwrap();

        assert_eq!(inventory.reused_entries, 1);
        assert_eq!(inventory.entries[0].sha256, "cached-hash");
    }

    #[test]
    fn remote_import_uses_a_regular_private_temp_file() {
        let script = remote_import_script();
        assert!(script.contains("umask 077"));
        assert!(script.contains("mktemp"));
        assert!(script.contains("trap 'rm -f"));
        assert!(script.contains("cat > \"$p\""));
        assert!(script.contains("opencode import \"$p\""));
        assert!(!script.contains("/dev/stdin"));
    }

    #[test]
    fn canonical_hash_ignores_json_object_key_order() {
        let left = SessionExport::parse(
            br#"{"info":{"id":"ses_test","title":"test"},"messages":[]}"#,
            "ses_test",
        )
        .unwrap();
        let right = SessionExport::parse(
            br#"{"messages":[],"info":{"title":"test","id":"ses_test"}}"#,
            "ses_test",
        )
        .unwrap();
        assert_eq!(
            left.canonical_hash().unwrap(),
            right.canonical_hash().unwrap()
        );
        assert!(left.semantically_equal(&right));

        let mut report = PlanReport::default();
        let merged = merge_exports(
            &BTreeMap::from([("ses_test".into(), left)]),
            &BTreeMap::from([("ses_test".into(), right)]),
            &mut report,
        )
        .unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(report.identical, 1);
        assert_eq!(report.advances, 0);
    }

    #[test]
    fn prefix_comparison_ignores_message_object_key_order() {
        let left = session("ses_test", &["msg_a"]);
        let mut right = left.clone();
        let message = right.value["messages"][0].as_object_mut().unwrap();
        let info = message.remove("info").unwrap();
        message.insert("info".into(), info);

        assert!(is_prefix(&left, &right));
        assert!(is_prefix(&right, &left));
        assert_eq!(common_prefix(&left, &right), 1);
    }

    #[test]
    fn imported_machine_metadata_does_not_create_perpetual_updates() {
        let local = session("ses_test", &["msg_a"]);
        let mut remote = local.clone();
        remote.value["info"]["directory"] = Value::String("/remote/project".into());
        remote.value["info"]["path"] = Value::String("remote/project".into());
        remote.value["info"]["projectID"] = Value::String("global".into());
        remote.value["info"]["cost"] = serde_json::json!(0.10000000000000002);

        assert!(local.semantically_equal(&remote));
        let changes = planned_session_changes(
            &BTreeMap::from([("ses_test".into(), local)]),
            &BTreeMap::from([("ses_test".into(), remote)]),
            &BTreeMap::from([("ses_test".into(), session("ses_test", &["msg_a"]))]),
        )
        .unwrap();
        assert!(changes.is_empty());
    }

    #[test]
    fn strict_prefix_selects_longer_session() {
        let short = session("ses_test", &["msg_a"]);
        let long = session("ses_test", &["msg_a", "msg_b"]);
        assert!(is_prefix(&short, &long));
        assert!(!is_prefix(&long, &short));
    }

    #[test]
    fn divergent_session_fork_is_deterministic_and_rewrites_references() {
        let local = BTreeMap::from([(
            "ses_test".into(),
            session("ses_test", &["msg_common", "msg_local"]),
        )]);
        let remote = BTreeMap::from([(
            "ses_test".into(),
            session("ses_test", &["msg_common", "msg_remote"]),
        )]);
        let mut first_report = PlanReport::default();
        let first = merge_exports(&local, &remote, &mut first_report).unwrap();
        let mut second_report = PlanReport::default();
        let second = merge_exports(&remote, &local, &mut second_report).unwrap();
        assert_eq!(
            first.keys().collect::<Vec<_>>(),
            second.keys().collect::<Vec<_>>()
        );
        assert_eq!(first.len(), 2);
        let fork = first
            .iter()
            .find(|(id, _)| id.as_str() != "ses_test")
            .unwrap();
        for message in fork.1.messages() {
            assert_eq!(message["info"]["sessionID"], fork.0.as_str());
            for part in message["parts"].as_array().unwrap() {
                assert_eq!(part["sessionID"], fork.0.as_str());
                assert_eq!(part["messageID"], message["info"]["id"]);
            }
        }
    }

    #[test]
    fn existing_fork_is_reused_when_an_unsynced_machine_rejoins() {
        let alpha = session("ses_test", &["msg_common", "msg_alpha"]);
        let beta = session("ses_test", &["msg_common", "msg_beta"]);
        let mut initial_report = PlanReport::default();
        let converged = merge_exports(
            &BTreeMap::from([("ses_test".into(), alpha)]),
            &BTreeMap::from([("ses_test".into(), beta.clone())]),
            &mut initial_report,
        )
        .unwrap();
        assert_eq!(converged.len(), 2);

        let stale_machine = BTreeMap::from([("ses_test".into(), beta)]);
        for (left, right) in [(&stale_machine, &converged), (&converged, &stale_machine)] {
            let mut report = PlanReport::default();
            let merged = merge_exports(left, right, &mut report).unwrap();
            assert_eq!(merged.len(), 2);
            assert_eq!(
                fingerprint(&merged).unwrap(),
                fingerprint(&converged).unwrap()
            );
        }
    }
}
