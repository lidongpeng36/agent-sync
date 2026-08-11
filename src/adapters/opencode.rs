use super::{Adapter, Prepared};
use crate::core::{
    Blocker, FileAction, PlanReport, ResourceSelection, SyncOptions, bytes_sha256,
    planned_file_changes, print_planned_diff, private_dir, shorten_middle, stamp,
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
    local_fingerprint: BTreeMap<String, String>,
    remote_fingerprint: BTreeMap<String, String>,
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
        Ok(bytes_sha256(&serde_json::to_vec(&self.value)?))
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
    let mut sessions = BTreeMap::new();
    for id in ids {
        if !valid_session_id(id) {
            bail!("unsafe remote OpenCode session id: {id:?}");
        }
        let script = format!(
            "set -eu; umask 077; p=$(mktemp \"${{TMPDIR:-/tmp}}/agent-sync-opencode.XXXXXX\"); trap 'rm -f \"$p\"' EXIT; opencode export {id} > \"$p\"; cat \"$p\""
        );
        let output = transport.ssh(&script)?;
        sessions.insert(
            id.clone(),
            SessionExport::parse(&output.stdout, id)
                .with_context(|| format!("decode remote OpenCode session {id}"))?,
        );
    }
    Ok(sessions)
}

fn is_prefix(shorter: &SessionExport, longer: &SessionExport) -> bool {
    shorter.messages().len() <= longer.messages().len()
        && shorter
            .messages()
            .iter()
            .zip(longer.messages())
            .all(|(left, right)| left == right)
}

fn common_prefix(left: &SessionExport, right: &SessionExport) -> usize {
    left.messages()
        .iter()
        .zip(right.messages())
        .take_while(|(left, right)| left == right)
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
    if existing.value == candidate.value {
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
            (Some(left), Some(right)) if left.value == right.value => report.identical += 1,
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

fn fingerprint(sessions: &BTreeMap<String, SessionExport>) -> Result<BTreeMap<String, String>> {
    sessions
        .iter()
        .map(|(id, session)| Ok((id.clone(), session.canonical_hash()?)))
        .collect()
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
    transport.ssh_with_input("opencode import /dev/stdin", bytes)?;
    Ok(())
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
        let local_ids = local_session_ids(local)?;
        let remote_ids: Vec<String> =
            transport.remote_request(&RemoteRequest::OpenCodeSessionIds {
                root: remote.to_owned(),
            })?;
        let local_sessions = export_local(&local_ids)?;
        let remote_sessions = export_remote(&remote_ids, transport)?;
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
        let result = merge_exports(&local_sessions, &remote_sessions, &mut report)?;
        write_exports(&stage, &result)?;
        report.files = planned_file_changes(&local_snapshot, &remote_snapshot, &stage, |_| false)?;
        decorate_paths(&mut report.files, &result);
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
            local_fingerprint: fingerprint(&local_sessions)?,
            remote_fingerprint: fingerprint(&remote_sessions)?,
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
        let current_local = export_local(&local_session_ids(local)?)?;
        let current_remote_ids: Vec<String> =
            transport.remote_request(&RemoteRequest::OpenCodeSessionIds {
                root: remote.to_owned(),
            })?;
        let current_remote = export_remote(&current_remote_ids, transport)?;
        if fingerprint(&current_local)? != value.local_fingerprint {
            bail!("local OpenCode sessions changed after preview");
        }
        if fingerprint(&current_remote)? != value.remote_fingerprint {
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
        for file in &value.report.files {
            let path = value.stage.join(&file.path);
            if !matches!(file.local, FileAction::Unchanged) {
                import_local(&path)?;
            }
            if !matches!(file.remote, FileAction::Unchanged) {
                import_remote(&fs::read(&path)?, transport)?;
            }
        }

        let expected = value
            .stage
            .join("sessions")
            .read_dir()?
            .map(|entry| {
                let path = entry?.path();
                let id = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .context("invalid staged OpenCode filename")?
                    .to_owned();
                Ok((id.clone(), SessionExport::parse(&fs::read(path)?, &id)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let verified_local = export_local(&local_session_ids(local)?)?;
        let verified_remote_ids: Vec<String> =
            transport.remote_request(&RemoteRequest::OpenCodeSessionIds {
                root: remote.to_owned(),
            })?;
        let verified_remote = export_remote(&verified_remote_ids, transport)?;
        for (id, expected_session) in expected {
            for (side, actual) in [
                ("local", verified_local.get(&id)),
                ("remote", verified_remote.get(&id)),
            ] {
                let actual =
                    actual.with_context(|| format!("{side} omitted OpenCode session {id}"))?;
                if actual.canonical_hash()? != expected_session.canonical_hash()? {
                    bail!("{side} OpenCode session differs after import: {id}");
                }
            }
        }
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
