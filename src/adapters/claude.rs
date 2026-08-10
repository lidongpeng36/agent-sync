use super::{Adapter, Prepared};
use crate::core::{
    Blocker, ConflictStrategy, PlanReport, ResourceSelection, SyncOptions, bytes_sha256,
    cache_path, copy_file_atomic, fingerprint, manifest, planned_file_changes, print_planned_diff,
    private_dir, safe_relative, sha256, stamp,
};
use crate::remote::{MtimeUpdate, Request as RemoteRequest, create_backup};
use crate::transport::SshTransport;
use anyhow::{Context, Result, bail};
use chrono::DateTime;
use filetime::FileTime;
use regex::Regex;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, UNIX_EPOCH};
use tempfile::TempDir;
use walkdir::WalkDir;

pub struct ClaudeAdapter;

#[derive(Clone)]
struct MemoryConflict {
    project: String,
    target: String,
    local: String,
    remote: String,
}

pub struct ClaudePrepared {
    pub report: PlanReport,
    temp: TempDir,
    stage: PathBuf,
    remote_snapshot: PathBuf,
    local_fingerprint: String,
    remote_fingerprint: String,
    conflicts: Vec<MemoryConflict>,
    choices: BTreeMap<(String, String), Side>,
    resources: ResourceSelection,
}

pub(super) fn print_diff(prepared: &ClaudePrepared, local: &Path) -> Result<()> {
    println!(
        "# agent-sync: agent=claude peer={} mode={}",
        prepared.report.peer,
        if prepared.report.blockers.is_empty() {
            "ready"
        } else {
            "blocked"
        }
    );
    for blocker in &prepared.report.blockers {
        println!(
            "# BLOCKED [{}] {}: {}",
            blocker.resource, blocker.path, blocker.reason
        );
    }
    print_planned_diff(local, &prepared.remote_snapshot, &prepared.stage, |path| {
        excluded(path, prepared.resources)
    })
}

#[derive(Clone, Copy)]
enum Side {
    Local,
    Remote,
}

impl Adapter for ClaudeAdapter {
    fn doctor(&self, local: &Path, remote: &str, transport: &SshTransport) -> Result<()> {
        if !local.exists() {
            bail!("Claude root does not exist: {}", local.display());
        }
        for command in [&transport.ssh, &transport.rsync, "/usr/sbin/lsof"] {
            if !SshTransport::command_exists(command) {
                bail!("required local command not found: {command}");
            }
        }
        let _: Value = transport.remote_request(&RemoteRequest::Doctor {
            root: remote.to_owned(),
            agent: "claude".to_owned(),
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
            .prefix("agent-sync-claude-")
            .tempdir()?;
        let remote = cache_path(options, "claude", &transport.host)?;
        pull_claude(transport, remote_root, &remote, options.resources)?;
        let stage = temp.path().join("stage");
        private_dir(&stage)?;
        let (report, conflicts) = build_stage(
            local,
            &remote,
            &stage,
            options.resources,
            &BTreeMap::new(),
            &transport.host,
            options.conflict_strategy,
        )?;
        let exclude = |p: &Path| excluded(p, options.resources);
        let remote_fingerprint = fingerprint(&remote, exclude)?;
        Ok(Prepared::Claude(ClaudePrepared {
            report,
            temp,
            stage,
            remote_snapshot: remote,
            local_fingerprint: fingerprint(local, exclude)?,
            remote_fingerprint,
            conflicts,
            choices: BTreeMap::new(),
            resources: options.resources,
        }))
    }

    fn resolve_interactive(&self, prepared: &mut Prepared, tty: bool) -> Result<()> {
        let Prepared::Claude(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        if value.conflicts.is_empty() {
            return Ok(());
        }
        if !tty {
            return Ok(());
        }
        for conflict in value.conflicts.clone() {
            println!(
                "Claude memory index conflict [{}/{}]",
                conflict.project, conflict.target
            );
            println!("  [l] {}", conflict.local.replace('\n', " "));
            println!("  [r] {}", conflict.remote.replace('\n', " "));
            loop {
                print!("choose l/r/q: ");
                io::stdout().flush()?;
                let mut answer = String::new();
                io::stdin().read_line(&mut answer)?;
                match answer.trim().to_ascii_lowercase().as_str() {
                    "l" | "local" => {
                        value.choices.insert(
                            (conflict.project.clone(), conflict.target.clone()),
                            Side::Local,
                        );
                        break;
                    }
                    "r" | "remote" => {
                        value.choices.insert(
                            (conflict.project.clone(), conflict.target.clone()),
                            Side::Remote,
                        );
                        break;
                    }
                    "q" | "quit" => bail!("cancelled by user"),
                    _ => {}
                }
            }
        }
        fs::remove_dir_all(&value.stage)?;
        private_dir(&value.stage)?;
        let (report, _) = build_stage_from_choices(
            &value.remote_snapshot,
            &value.stage,
            value.resources,
            &value.choices,
            &value.report.peer,
            ConflictStrategy::Merge,
        )?;
        value.report = report;
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
        let Prepared::Claude(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        let exclude = |p: &Path| excluded(p, value.resources);
        if fingerprint(local, exclude)? != value.local_fingerprint {
            bail!("local Claude data changed after preview");
        }
        let recheck = value.temp.path().join("remote-recheck");
        pull_claude(transport, remote_root, &recheck, value.resources)?;
        if fingerprint(&recheck, exclude)? != value.remote_fingerprint {
            bail!("remote Claude data changed after preview");
        }
        ensure_no_writers(local, remote_root, transport)?;
        thread::sleep(Duration::from_secs_f64(options.stability_seconds));
        if fingerprint(local, exclude)? != value.local_fingerprint {
            bail!("local Claude writer is active");
        }
        let stable = value.temp.path().join("remote-stable");
        pull_claude(transport, remote_root, &stable, value.resources)?;
        if fingerprint(&stable, exclude)? != value.remote_fingerprint {
            bail!("remote Claude writer is active");
        }
        ensure_no_writers(local, remote_root, transport)?;

        let stamp = stamp();
        let local_backup = backup_local(local, value.resources, &stamp)?;
        let remote_backup = backup_remote(remote_root, value.resources, &stamp, transport)?;
        install_local(&value.stage, local, value.resources, &transport.rsync)?;
        transport.push(&value.stage, remote_root)?;
        if value.resources.sessions() {
            normalize_local_mtimes(&value.stage, local)?;
            normalize_remote_mtimes(&value.stage, remote_root, transport)?;
        }
        let verified = value.temp.path().join("remote-verified");
        pull_claude(transport, remote_root, &verified, value.resources)?;
        verify_selected(&value.stage, local, value.resources, "local")?;
        verify_selected(&value.stage, &verified, value.resources, "remote")?;
        if value.resources.sessions() {
            verify_event_mtimes(local, "local")?;
            verify_event_mtimes(&verified, "remote")?;
        }
        println!(
            "complete: Claude synchronized and verified; backups: local={}, remote={}:{}",
            local_backup.display(),
            transport.host,
            remote_backup
        );
        Ok(())
    }
}

fn pull_claude(
    transport: &SshTransport,
    root: &str,
    destination: &Path,
    resources: ResourceSelection,
) -> Result<()> {
    let filters = match resources {
        ResourceSelection::All => vec![
            "--include=/projects/",
            "--include=/projects/*/",
            "--include=/projects/*/memory/",
            "--include=/projects/*/memory/*.md",
            "--exclude=/projects/*/memory/***",
            "--include=/projects/***",
            "--exclude=*",
        ],
        ResourceSelection::Sessions => vec![
            "--exclude=/projects/*/memory/***",
            "--include=/projects/***",
            "--exclude=*",
        ],
        ResourceSelection::Memory => vec![
            "--include=/projects/",
            "--include=/projects/*/",
            "--include=/projects/*/memory/",
            "--include=/projects/*/memory/*.md",
            "--exclude=*",
        ],
    };
    transport.pull(root, destination, &filters)?;
    if resources.sessions() {
        refresh_remote_mtimes(transport, root, destination, resources)?;
    }
    Ok(())
}

fn refresh_remote_mtimes(
    transport: &SshTransport,
    root: &str,
    snapshot: &Path,
    resources: ResourceSelection,
) -> Result<()> {
    let mtimes: BTreeMap<String, i64> = transport.remote_request(&RemoteRequest::ClaudeMtimes {
        root: root.to_owned(),
        include_memory: resources.memory(),
    })?;
    for (relative, ns) in mtimes {
        let relative = PathBuf::from(relative);
        safe_relative(&relative)?;
        let path = snapshot.join(relative);
        if path.exists() {
            filetime::set_file_mtime(
                path,
                FileTime::from_unix_time(ns / 1_000_000_000, (ns % 1_000_000_000) as u32),
            )?;
        }
    }
    Ok(())
}

fn excluded(path: &Path, resources: ResourceSelection) -> bool {
    let parts: Vec<_> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect();
    if parts.first().map(|v| v.as_ref()) != Some("projects") {
        return true;
    }
    let memory = parts.get(2).map(|v| v.as_ref()) == Some("memory");
    let managed_memory = memory
        && parts.len() == 4
        && Path::new(parts[3].as_ref())
            .extension()
            .and_then(|value| value.to_str())
            == Some("md");
    if memory && !managed_memory {
        return true;
    }
    (memory && !resources.memory())
        || (!memory && !resources.sessions())
        || parts.iter().any(|v| v.as_ref().contains("sync-backups"))
}

fn session_files(root: &Path) -> Result<BTreeMap<PathBuf, FileRecord>> {
    let mut out = BTreeMap::new();
    let projects = root.join("projects");
    if !projects.exists() {
        return Ok(out);
    }
    for entry in WalkDir::new(&projects)
        .follow_links(false)
        .sort_by_file_name()
    {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(root)?.to_path_buf();
        safe_relative(&relative)?;
        if relative.components().nth(2).map(|c| c.as_os_str()) == Some("memory".as_ref()) {
            continue;
        }
        let event_ns = if entry.path().extension().and_then(|v| v.to_str()) == Some("jsonl") {
            Some(validate_claude_jsonl(entry.path(), &relative)?)
        } else {
            None
        };
        out.insert(
            relative,
            FileRecord {
                sha: sha256(entry.path())?,
                path: entry.path().to_path_buf(),
                size: entry.metadata()?.len(),
                mtime_ns: entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_nanos() as i64,
                event_ns,
            },
        );
    }
    Ok(out)
}

struct FileRecord {
    sha: String,
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    event_ns: Option<i64>,
}

fn validate_claude_jsonl(path: &Path, relative: &Path) -> Result<i64> {
    let parts: Vec<_> = relative
        .iter()
        .map(|v| v.to_string_lossy().into_owned())
        .collect();
    let file = relative
        .file_stem()
        .and_then(|v| v.to_str())
        .context("invalid JSONL filename")?;
    let (session, agent) = if parts.len() == 3 {
        (file.to_owned(), None)
    } else if parts.len() == 5 && parts[3] == "subagents" && file.starts_with("agent-") {
        (
            parts[2].clone(),
            Some(file.trim_start_matches("agent-").to_owned()),
        )
    } else {
        bail!("unsupported Claude JSONL path: {}", relative.display());
    };
    uuid::Uuid::parse_str(&session).context("invalid session UUID in path")?;
    let mut session_ids = BTreeSet::new();
    let mut agent_ids = BTreeSet::new();
    let mut record_ids = BTreeSet::new();
    let mut times = Vec::new();
    let text = fs::read_to_string(path)?;
    for (number, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            bail!("{}:{} blank JSONL record", relative.display(), number + 1);
        }
        let item: Value = serde_json::from_str(line)
            .with_context(|| format!("{}:{} invalid JSON", relative.display(), number + 1))?;
        let object = item
            .as_object()
            .context("Claude JSONL record is not an object")?;
        if let Some(value) = object.get("sessionId").and_then(Value::as_str) {
            session_ids.insert(value.to_owned());
        }
        if let Some(value) = object.get("agentId").and_then(Value::as_str) {
            agent_ids.insert(value.to_owned());
        }
        if let Some(value) = object.get("uuid").and_then(Value::as_str) {
            uuid::Uuid::parse_str(value).context("invalid Claude record UUID")?;
            if !record_ids.insert(value.to_owned()) {
                bail!("duplicate Claude record UUID {value}");
            }
        }
        if let Some(value) = object.get("timestamp") {
            let text = value.as_str().context("invalid Claude timestamp")?;
            times.push(
                DateTime::parse_from_rfc3339(text)?
                    .timestamp_nanos_opt()
                    .context("timestamp out of range")?,
            );
        }
    }
    if session_ids != BTreeSet::from([session]) {
        bail!(
            "sessionId does not match Claude path: {}",
            relative.display()
        );
    }
    if let Some(expected) = agent {
        if agent_ids != BTreeSet::from([expected]) {
            bail!("agentId does not match Claude path: {}", relative.display());
        }
    }
    times
        .into_iter()
        .max()
        .context("Claude JSONL has no event timestamp")
}

fn build_stage(
    local: &Path,
    remote: &Path,
    stage: &Path,
    resources: ResourceSelection,
    choices: &BTreeMap<(String, String), Side>,
    peer: &str,
    strategy: ConflictStrategy,
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    build_stage_full(local, remote, stage, resources, choices, peer, strategy)
}

fn build_stage_from_choices(
    remote: &Path,
    stage: &Path,
    resources: ResourceSelection,
    choices: &BTreeMap<(String, String), Side>,
    peer: &str,
    strategy: ConflictStrategy,
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    // The original local tree is retained next to the remote snapshot by prepare.
    let local = stage.parent().context("stage parent")?.join("local-copy");
    build_stage_full(&local, remote, stage, resources, choices, peer, strategy)
}

fn build_stage_full(
    local: &Path,
    remote: &Path,
    stage: &Path,
    resources: ResourceSelection,
    choices: &BTreeMap<(String, String), Side>,
    peer: &str,
    strategy: ConflictStrategy,
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    let mut report = PlanReport {
        agent: "claude".into(),
        peer: peer.into(),
        resources: Vec::new(),
        conflict_strategy: Some(strategy),
        ..Default::default()
    };
    let mut conflicts = Vec::new();
    if resources.sessions() {
        report.resources.push("sessions".into());
        let left = session_files(local)?;
        let right = session_files(remote)?;
        let mut divergent_sessions = BTreeSet::new();
        for (path, local_file) in &left {
            let Some((project, session)) = main_session_identity(path) else {
                continue;
            };
            let Some(remote_file) = right.get(path) else {
                continue;
            };
            if local_file.sha != remote_file.sha
                && !file_prefix(local_file, remote_file)?
                && !file_prefix(remote_file, local_file)?
            {
                divergent_sessions.insert((project, session));
            }
        }
        let local_repairs = left
            .values()
            .filter(|file| file.event_ns.is_some() && file.event_ns != Some(file.mtime_ns))
            .count();
        let remote_repairs = right
            .values()
            .filter(|file| file.event_ns.is_some() && file.event_ns != Some(file.mtime_ns))
            .count();
        report.metadata_repairs += local_repairs + remote_repairs;
        report.notes.push(format!(
            "JSONL mtimes to normalize: local={local_repairs}, remote={remote_repairs}"
        ));
        let mut metadata_only = 0;
        for path in left.keys().chain(right.keys()).collect::<BTreeSet<_>>() {
            if session_bundle_identity(path)
                .is_some_and(|identity| divergent_sessions.contains(&identity))
            {
                continue;
            }
            let a = left.get(path);
            let b = right.get(path);
            let selected = match (a, b) {
                (Some(a), Some(b)) if a.sha == b.sha => {
                    report.identical += 1;
                    if a.mtime_ns != b.mtime_ns {
                        metadata_only += 1;
                    }
                    a
                }
                (Some(a), Some(b))
                    if path.extension().and_then(|v| v.to_str()) == Some("jsonl")
                        && file_prefix(a, b)? =>
                {
                    report.advances += 1;
                    b
                }
                (Some(a), Some(b))
                    if path.extension().and_then(|v| v.to_str()) == Some("jsonl")
                        && file_prefix(b, a)? =>
                {
                    report.advances += 1;
                    a
                }
                (Some(_), Some(_)) => {
                    report.blockers.push(Blocker {
                        resource: "sessions".into(),
                        path: path.display().to_string(),
                        reason: "same-path content diverged".into(),
                    });
                    continue;
                }
                (Some(a), None) => {
                    report.remote_additions += 1;
                    a
                }
                (None, Some(b)) => {
                    report.local_additions += 1;
                    b
                }
                _ => unreachable!(),
            };
            let dest = stage.join(path);
            private_dir(dest.parent().unwrap())?;
            fs::copy(&selected.path, &dest)?;
            if let Some(ns) = selected.event_ns {
                filetime::set_file_mtime(
                    &dest,
                    FileTime::from_unix_time(ns / 1_000_000_000, (ns % 1_000_000_000) as u32),
                )?;
            }
        }
        for (project, session) in divergent_sessions {
            match strategy {
                ConflictStrategy::Local => {
                    copy_session_bundle(local, stage, &project, &session, &session)?;
                    report.notes.push(format!(
                        "local session wins: projects/{project}/{session}.jsonl"
                    ));
                }
                ConflictStrategy::Remote => {
                    copy_session_bundle(remote, stage, &project, &session, &session)?;
                    report.notes.push(format!(
                        "remote session wins: projects/{project}/{session}.jsonl"
                    ));
                }
                ConflictStrategy::Merge => {
                    copy_session_bundle(local, stage, &project, &session, &session)?;
                    let fork =
                        merge_session_candidate(remote, stage, &project, &session, &session)?;
                    report.notes.push(format!(
                        "session forked: projects/{project}/{session}.jsonl -> {fork}.jsonl"
                    ));
                }
            }
        }
        report
            .notes
            .push(format!("metadata-only differences: {metadata_only}"));
    }
    if resources.memory() {
        report.resources.push("memory".into());
        merge_memories(
            local,
            remote,
            stage,
            choices,
            strategy,
            &mut report,
            &mut conflicts,
        )?;
    }
    report.files = planned_file_changes(local, remote, stage, |path| excluded(path, resources))?;
    for blocker in report
        .blockers
        .iter()
        .filter(|blocker| blocker.resource == "memory")
    {
        let Some((project, target)) = blocker.path.rsplit_once('/') else {
            continue;
        };
        let entry = format!("projects/{project}/memory/{target}");
        let index = format!("projects/{project}/memory/MEMORY.md");
        for file in &mut report.files {
            if file.path == entry || file.path == index {
                file.resolution = "unresolved".into();
            }
        }
    }
    // Preserve a local copy for interactive rebuild without reopening mutable source.
    let copy = stage.parent().unwrap().join("local-copy");
    if !conflicts.is_empty() && !copy.exists() {
        copy_tree_selected(local, &copy, resources)?;
    }
    Ok((report, conflicts))
}

fn file_prefix(shorter: &FileRecord, longer: &FileRecord) -> Result<bool> {
    if shorter.size >= longer.size {
        return Ok(false);
    }
    let mut left = BufReader::new(File::open(&shorter.path)?);
    let mut right = BufReader::new(File::open(&longer.path)?);
    let mut a = [0_u8; 64 * 1024];
    let mut b = [0_u8; 64 * 1024];
    loop {
        let count = left.read(&mut a)?;
        if count == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..count])?;
        if a[..count] != b[..count] {
            return Ok(false);
        }
    }
}

fn main_session_identity(path: &Path) -> Option<(String, String)> {
    let parts = path.iter().collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "projects" || path.extension()?.to_str()? != "jsonl" {
        return None;
    }
    let session = path.file_stem()?.to_str()?.to_owned();
    uuid::Uuid::parse_str(&session).ok()?;
    Some((parts[1].to_string_lossy().into_owned(), session))
}

fn session_bundle_identity(path: &Path) -> Option<(String, String)> {
    let parts = path.iter().collect::<Vec<_>>();
    if parts.len() < 3 || parts[0] != "projects" {
        return None;
    }
    let third = parts[2].to_string_lossy();
    let session = third.strip_suffix(".jsonl").unwrap_or(&third);
    uuid::Uuid::parse_str(session).ok()?;
    Some((parts[1].to_string_lossy().into_owned(), session.to_owned()))
}

fn copy_session_bundle(
    source: &Path,
    stage: &Path,
    project: &str,
    source_id: &str,
    target_id: &str,
) -> Result<()> {
    let source_project = source.join("projects").join(project);
    let target_project = stage.join("projects").join(project);
    let source_main = source_project.join(format!("{source_id}.jsonl"));
    if !source_main.exists() {
        bail!(
            "missing Claude session transcript: {}",
            source_main.display()
        );
    }
    copy_session_member(
        &source_main,
        &target_project.join(format!("{target_id}.jsonl")),
        stage,
        source_id,
        target_id,
    )?;

    let source_sidecars = source_project.join(source_id);
    if source_sidecars.exists() {
        for entry in WalkDir::new(&source_sidecars)
            .follow_links(false)
            .sort_by_file_name()
        {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry.path().strip_prefix(&source_sidecars)?;
            copy_session_member(
                entry.path(),
                &target_project.join(target_id).join(relative),
                stage,
                source_id,
                target_id,
            )?;
        }
    }
    Ok(())
}

fn copy_session_member(
    source: &Path,
    destination: &Path,
    stage: &Path,
    source_id: &str,
    target_id: &str,
) -> Result<()> {
    private_dir(destination.parent().context("session member parent")?)?;
    if source.extension().and_then(|value| value.to_str()) != Some("jsonl") {
        fs::copy(source, destination)?;
        return Ok(());
    }
    if source_id == target_id {
        fs::copy(source, destination)?;
        let relative = destination.strip_prefix(stage)?;
        let event_ns = validate_claude_jsonl(destination, relative)?;
        filetime::set_file_mtime(
            destination,
            FileTime::from_unix_time(event_ns / 1_000_000_000, (event_ns % 1_000_000_000) as u32),
        )?;
        return Ok(());
    }
    let mut output = String::new();
    for (number, line) in fs::read_to_string(source)?.lines().enumerate() {
        if line.trim().is_empty() {
            bail!("{}:{} blank JSONL record", source.display(), number + 1);
        }
        let mut value: Value = serde_json::from_str(line)?;
        let object = value
            .as_object_mut()
            .context("Claude JSONL record is not an object")?;
        for key in ["sessionId", "session_id"] {
            if object.get(key).and_then(Value::as_str) == Some(source_id) {
                object.insert(key.to_owned(), Value::String(target_id.to_owned()));
            }
        }
        output.push_str(&serde_json::to_string(&value)?);
        output.push('\n');
    }
    fs::write(destination, output)?;
    let relative = destination.strip_prefix(stage)?;
    let event_ns = validate_claude_jsonl(destination, relative)?;
    filetime::set_file_mtime(
        destination,
        FileTime::from_unix_time(event_ns / 1_000_000_000, (event_ns % 1_000_000_000) as u32),
    )?;
    Ok(())
}

fn session_record(path: &Path, relative: &Path) -> Result<FileRecord> {
    let metadata = fs::metadata(path)?;
    Ok(FileRecord {
        sha: sha256(path)?,
        path: path.to_owned(),
        size: metadata.len(),
        mtime_ns: metadata.modified()?.duration_since(UNIX_EPOCH)?.as_nanos() as i64,
        event_ns: Some(validate_claude_jsonl(path, relative)?),
    })
}

fn remove_session_bundle(stage: &Path, project: &str, session: &str) -> Result<()> {
    let project = stage.join("projects").join(project);
    let main = project.join(format!("{session}.jsonl"));
    if main.exists() {
        fs::remove_file(main)?;
    }
    let sidecars = project.join(session);
    if sidecars.exists() {
        fs::remove_dir_all(sidecars)?;
    }
    Ok(())
}

fn fork_session_id(parent: &str, canonical: &Path, candidate: &Path) -> Result<String> {
    let canonical_lines = fs::read_to_string(canonical)?;
    let candidate_lines = fs::read_to_string(candidate)?;
    let candidate_line = canonical_lines
        .lines()
        .zip(candidate_lines.lines())
        .find_map(|(left, right)| (left != right).then_some(right))
        .or_else(|| candidate_lines.lines().nth(canonical_lines.lines().count()))
        .context("divergent Claude session has no candidate event")?;
    let value: Value = serde_json::from_str(candidate_line)?;
    let discriminator = value
        .get("uuid")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| bytes_sha256(candidate_line.as_bytes()));
    let namespace = uuid::Uuid::parse_str(parent)?;
    Ok(uuid::Uuid::new_v5(&namespace, discriminator.as_bytes()).to_string())
}

fn merge_session_candidate(
    source: &Path,
    stage: &Path,
    project: &str,
    source_id: &str,
    target_id: &str,
) -> Result<String> {
    let candidate_root = tempfile::tempdir()?;
    copy_session_bundle(source, candidate_root.path(), project, source_id, target_id)?;
    let candidate_main = candidate_root
        .path()
        .join("projects")
        .join(project)
        .join(format!("{target_id}.jsonl"));
    let stage_relative = PathBuf::from("projects")
        .join(project)
        .join(format!("{target_id}.jsonl"));
    let stage_main = stage.join(&stage_relative);
    if !stage_main.exists() {
        copy_session_bundle(candidate_root.path(), stage, project, target_id, target_id)?;
        return Ok(target_id.to_owned());
    }

    let current = session_record(&stage_main, &stage_relative)?;
    let candidate = session_record(&candidate_main, &stage_relative)?;
    if current.sha == candidate.sha || file_prefix(&candidate, &current)? {
        return Ok(target_id.to_owned());
    }
    if file_prefix(&current, &candidate)? {
        remove_session_bundle(stage, project, target_id)?;
        copy_session_bundle(candidate_root.path(), stage, project, target_id, target_id)?;
        return Ok(target_id.to_owned());
    }

    let child_id = fork_session_id(target_id, &stage_main, &candidate_main)?;
    merge_session_candidate(candidate_root.path(), stage, project, target_id, &child_id)
}

fn copy_tree_selected(source: &Path, dest: &Path, resources: ResourceSelection) -> Result<()> {
    for (path, _) in manifest(source, |p| excluded(p, resources))? {
        let rel = PathBuf::from(path);
        copy_file_atomic(&source.join(&rel), &dest.join(rel))?;
    }
    Ok(())
}

fn merge_memories(
    local: &Path,
    remote: &Path,
    stage: &Path,
    choices: &BTreeMap<(String, String), Side>,
    strategy: ConflictStrategy,
    report: &mut PlanReport,
    conflicts: &mut Vec<MemoryConflict>,
) -> Result<()> {
    let projects = project_names(local)?
        .union(&project_names(remote)?)
        .cloned()
        .collect::<Vec<_>>();
    for project in projects {
        let lm = local.join("projects").join(&project).join("memory");
        let rm = remote.join("projects").join(&project).join("memory");
        let lf = memory_files(&lm)?;
        let rf = memory_files(&rm)?;
        let local_index_exists = lm.join("MEMORY.md").exists();
        let remote_index_exists = rm.join("MEMORY.md").exists();
        if lf.is_empty() && rf.is_empty() && !local_index_exists && !remote_index_exists {
            continue;
        }
        let li = memory_index(&lm, &lf)?;
        let ri = memory_index(&rm, &rf)?;
        let mut selected = BTreeMap::new();
        for target in lf.keys().chain(rf.keys()).cloned().collect::<BTreeSet<_>>() {
            if lf.contains_key(&target) && !rf.contains_key(&target) {
                report.remote_additions += 1;
            }
            if rf.contains_key(&target) && !lf.contains_key(&target) {
                report.local_additions += 1;
            }
            let left = li.items.get(&target);
            let right = ri.items.get(&target);
            let local_file = lf.get(&target);
            let remote_file = rf.get(&target);
            let content_differs = match (local_file, remote_file) {
                (Some(local), Some(remote)) => sha256(local)? != sha256(remote)?,
                _ => false,
            };
            let index_differs = matches!((left, right), (Some(a), Some(b)) if a != b);
            let mut merged_content = None;
            let automatic_side = match strategy {
                ConflictStrategy::Local => Some(Side::Local),
                ConflictStrategy::Remote => Some(Side::Remote),
                ConflictStrategy::Merge => match (local_file, remote_file) {
                    (Some(local), Some(remote)) if content_differs => {
                        let local_bytes = fs::read(local)?;
                        let remote_bytes = fs::read(remote)?;
                        if remote_bytes.starts_with(&local_bytes) {
                            Some(Side::Remote)
                        } else if local_bytes.starts_with(&remote_bytes) {
                            Some(Side::Local)
                        } else {
                            merged_content = merge_markdown_sections(
                                &fs::read_to_string(local)?,
                                &fs::read_to_string(remote)?,
                            );
                            None
                        }
                    }
                    _ => None,
                },
            };
            let explicit_choice = choices.get(&(project.clone(), target.clone())).copied();
            if explicit_choice.is_some() {
                merged_content = None;
            }
            let mut side = explicit_choice.or(automatic_side);
            let mut unresolved = false;
            if local_file.is_some()
                && remote_file.is_some()
                && ((content_differs && merged_content.is_none()) || index_differs)
                && side.is_none()
            {
                let local_block = left.cloned().unwrap_or_default();
                let remote_block = right.cloned().unwrap_or_default();
                conflicts.push(MemoryConflict {
                    project: project.clone(),
                    target: target.clone(),
                    local: local_block,
                    remote: remote_block,
                });
                report.blockers.push(Blocker {
                    resource: "memory".into(),
                    path: format!("{project}/{target}"),
                    reason: "memory content or index requires a choice".into(),
                });
                side = Some(Side::Remote);
                merged_content = None;
                unresolved = true;
            }
            let source = match side {
                Some(Side::Local) => local_file.or(remote_file),
                Some(Side::Remote) | None => remote_file.or(local_file),
            }
            .context("memory source disappeared")?;
            let block = match match side {
                Some(Side::Local) => left.or(right),
                Some(Side::Remote) | None => right.or(left),
            } {
                Some(block) => block.clone(),
                None => synthesize_block(source, &target)?,
            };
            if content_differs
                && !unresolved
                && let Some(selected_side) = side
            {
                let label = match selected_side {
                    Side::Local => "local",
                    Side::Remote => "remote",
                };
                report
                    .notes
                    .push(format!("{label} memory wins: {project}/{target}"));
            }
            let dest = stage
                .join("projects")
                .join(&project)
                .join("memory")
                .join(&target);
            if let Some(content) = merged_content {
                private_dir(dest.parent().context("memory destination parent")?)?;
                fs::write(&dest, content)?;
                report
                    .notes
                    .push(format!("memory blocks merged: {project}/{target}"));
            } else {
                copy_file_atomic(source, &dest)?;
            }
            let mtime = fs::metadata(source)?.modified()?;
            selected.insert(target, (block, mtime));
        }
        let mut ordered: Vec<_> = selected.into_iter().collect();
        ordered.sort_by_key(|(name, (_, time))| (*time, name.clone()));
        let preamble = if !ri.preamble.trim().is_empty() {
            &ri.preamble
        } else {
            &li.preamble
        };
        let mut index = preamble.trim_end().to_owned();
        if !index.is_empty() {
            index.push_str("\n\n");
        }
        for (_, (block, _)) in ordered {
            index.push_str(block.trim_end());
            index.push('\n');
        }
        let path = stage
            .join("projects")
            .join(&project)
            .join("memory/MEMORY.md");
        private_dir(path.parent().unwrap())?;
        fs::write(path, index)?;
    }
    Ok(())
}

fn markdown_sections(text: &str) -> Option<Vec<(String, String)>> {
    let mut sections = Vec::new();
    let mut key = String::new();
    let mut body = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        let hashes = trimmed.chars().take_while(|value| *value == '#').count();
        let heading = (1..=6).contains(&hashes)
            && trimmed
                .chars()
                .nth(hashes)
                .is_some_and(|value| value == ' ');
        if heading {
            if sections.iter().any(|(existing, _)| existing == &key) {
                return None;
            }
            sections.push((key, body));
            key = trimmed.to_owned();
            body = String::new();
        }
        body.push_str(line);
        body.push('\n');
    }
    if sections.iter().any(|(existing, _)| existing == &key) {
        return None;
    }
    sections.push((key, body));
    Some(sections)
}

fn merge_markdown_sections(local: &str, remote: &str) -> Option<String> {
    let local = markdown_sections(local)?;
    let remote = markdown_sections(remote)?;
    let remote_map = remote.iter().cloned().collect::<BTreeMap<_, _>>();
    let local_map = local.iter().cloned().collect::<BTreeMap<_, _>>();
    for (key, local_body) in &local_map {
        if let Some(remote_body) = remote_map.get(key)
            && remote_body != local_body
        {
            return None;
        }
    }
    let mut output = String::new();
    for (_, body) in &local {
        output.push_str(body);
    }
    for (key, body) in &remote {
        if !local_map.contains_key(key) {
            output.push_str(body);
        }
    }
    Some(output)
}

fn project_names(root: &Path) -> Result<BTreeSet<String>> {
    let mut s = BTreeSet::new();
    let p = root.join("projects");
    if p.exists() {
        for e in fs::read_dir(p)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                s.insert(e.file_name().to_string_lossy().into_owned());
            }
        }
    }
    Ok(s)
}
fn memory_files(root: &Path) -> Result<BTreeMap<String, PathBuf>> {
    let mut m = BTreeMap::new();
    if root.exists() {
        for e in fs::read_dir(root)? {
            let e = e?;
            let n = e.file_name().to_string_lossy().into_owned();
            if e.file_type()?.is_file() && n.ends_with(".md") && n != "MEMORY.md" {
                m.insert(n, e.path());
            }
        }
    }
    Ok(m)
}
struct MemoryIndex {
    preamble: String,
    items: BTreeMap<String, String>,
}

fn memory_index(root: &Path, files: &BTreeMap<String, PathBuf>) -> Result<MemoryIndex> {
    let mut items = BTreeMap::new();
    let mut preamble = String::new();
    let path = root.join("MEMORY.md");
    if path.exists() {
        let item_re = Regex::new(r"^- \[[^]]+\]\(([^)]+\.md)\)(?:\s+—\s+.*)?$")?;
        let text = fs::read_to_string(path)?;
        let mut current: Vec<String> = Vec::new();
        for line in text.lines() {
            if item_re.is_match(line) && !current.is_empty() {
                insert_index_block(&mut items, &current, files, &item_re)?;
                current.clear();
            }
            if current.is_empty() && !item_re.is_match(line) && items.is_empty() {
                preamble.push_str(line);
                preamble.push('\n');
            } else {
                current.push(line.to_owned());
            }
        }
        if !current.is_empty() {
            insert_index_block(&mut items, &current, files, &item_re)?;
        }
    }
    for (name, file) in files {
        if !items.contains_key(name) {
            items.insert(name.clone(), synthesize_block(file, name)?);
        }
    }
    Ok(MemoryIndex { preamble, items })
}

fn insert_index_block(
    items: &mut BTreeMap<String, String>,
    lines: &[String],
    files: &BTreeMap<String, PathBuf>,
    item_re: &Regex,
) -> Result<()> {
    let captures = item_re
        .captures(&lines[0])
        .context("invalid Claude memory index item")?;
    let target = captures[1].to_owned();
    if !files.contains_key(&target) {
        bail!("dangling Claude memory index: {target}");
    }
    if items
        .insert(target.clone(), lines.join("\n") + "\n")
        .is_some()
    {
        bail!("duplicate Claude memory index: {target}");
    }
    Ok(())
}

fn synthesize_block(path: &Path, target: &str) -> Result<String> {
    let text = fs::read_to_string(path)?;
    let name_re = Regex::new(r#"(?m)^name:\s*['\"]?([^'\"\n]+)"#)?;
    let desc_re = Regex::new(r#"(?m)^description:\s*['\"]?([^'\"\n]+)"#)?;
    let name = name_re
        .captures(&text)
        .map(|c| c[1].trim().to_owned())
        .filter(|s| !s.is_empty())
        .context(format!("unindexed memory lacks name: {}", path.display()))?;
    let description = desc_re
        .captures(&text)
        .map(|c| c[1].trim().to_owned())
        .filter(|s| !s.is_empty())
        .context(format!(
            "unindexed memory lacks description: {}",
            path.display()
        ))?;
    Ok(format!("- [{name}]({target}) — {description}\n"))
}

fn ensure_no_writers(local: &Path, remote: &str, transport: &SshTransport) -> Result<()> {
    let local_out = Command::new("/usr/sbin/lsof")
        .args(["-Fpf", "+D"])
        .arg(local.join("projects"))
        .output()?;
    if !parse_lsof_writers(&String::from_utf8_lossy(&local_out.stdout)).is_empty() {
        bail!("local Claude files are open")
    }
    let value: Value = transport.remote_request(&RemoteRequest::ClaudeWriters {
        root: remote.to_owned(),
    })?;
    if value["active"].as_bool().unwrap_or(true) {
        bail!("remote Claude files are open")
    }
    Ok(())
}

fn parse_lsof_writers(output: &str) -> BTreeSet<String> {
    let re = Regex::new(r"^f\d+[wu].*").expect("static regex");
    let mut current = None;
    let mut writers = BTreeSet::new();
    for line in output.lines() {
        if let Some(pid) = line.strip_prefix('p') {
            current = Some(pid.to_owned());
        } else if re.is_match(line) {
            if let Some(pid) = &current {
                writers.insert(pid.clone());
            }
        }
    }
    writers
}

fn verify_event_mtimes(root: &Path, side: &str) -> Result<()> {
    for (path, file) in session_files(root)? {
        if let Some(event) = file.event_ns {
            if file.mtime_ns != event {
                bail!(
                    "{side} event mtime verification failed for {}",
                    path.display()
                );
            }
        }
    }
    Ok(())
}

fn mtime_requests(stage: &Path) -> Result<Vec<MtimeUpdate>> {
    let mut requests = Vec::new();
    for (path, file) in session_files(stage)? {
        if let Some(event) = file.event_ns {
            requests.push(MtimeUpdate {
                path: path.to_string_lossy().into_owned(),
                sha256: file.sha,
                mtime_ns: event,
            });
        }
    }
    Ok(requests)
}

fn normalize_local_mtimes(stage: &Path, root: &Path) -> Result<()> {
    for request in mtime_requests(stage)? {
        let path = root.join(&request.path);
        if sha256(&path)? != request.sha256 {
            bail!(
                "local content changed before mtime normalization: {}",
                request.path
            );
        }
        filetime::set_file_mtime(
            path,
            FileTime::from_unix_time(
                request.mtime_ns / 1_000_000_000,
                (request.mtime_ns % 1_000_000_000) as u32,
            ),
        )?;
    }
    Ok(())
}

fn normalize_remote_mtimes(stage: &Path, root: &str, transport: &SshTransport) -> Result<()> {
    let requests = mtime_requests(stage)?;
    let _: Value = transport.remote_request(&RemoteRequest::SetMtimes {
        root: root.to_owned(),
        items: requests,
    })?;
    Ok(())
}

fn backup_local(root: &Path, resources: ResourceSelection, stamp: &str) -> Result<PathBuf> {
    let dir = root.join("agent-sync-backups");
    private_dir(&dir)?;
    let out = dir.join(format!("before-{stamp}.tar.gz"));
    let _ = resources;
    create_backup(root, &out, &["projects".to_owned()])?;
    Ok(out)
}
fn backup_remote(
    root: &str,
    _resources: ResourceSelection,
    stamp: &str,
    t: &SshTransport,
) -> Result<String> {
    #[derive(serde::Deserialize)]
    struct BackupResult {
        path: String,
    }
    let value: BackupResult = t.remote_request(&RemoteRequest::Backup {
        root: root.to_owned(),
        backup_dir: "agent-sync-backups".to_owned(),
        stamp: stamp.to_owned(),
        members: vec!["projects".to_owned()],
    })?;
    Ok(value.path)
}
fn install_local(
    stage: &Path,
    root: &Path,
    _resources: ResourceSelection,
    rsync: &str,
) -> Result<()> {
    let status = Command::new(rsync)
        .arg("-a")
        .arg(format!("{}/", stage.display()))
        .arg(format!("{}/", root.display()))
        .status()?;
    if !status.success() {
        bail!("local rsync install failed")
    }
    Ok(())
}
fn verify_selected(stage: &Path, actual: &Path, r: ResourceSelection, side: &str) -> Result<()> {
    let a = manifest(stage, |p| excluded(p, r))?;
    let b = manifest(actual, |p| excluded(p, r))?;
    if a != b {
        bail!("{side} final file set or content differs from the staged manifest");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_session(root: &Path, project: &str, session: &str, events: &[(&str, &str)]) {
        let project = root.join("projects").join(project);
        fs::create_dir_all(&project).unwrap();
        let mut text = String::new();
        for (uuid, marker) in events {
            text.push_str(
                &serde_json::to_string(&serde_json::json!({
                    "type": "user",
                    "uuid": uuid,
                    "sessionId": session,
                    "timestamp": "2026-08-11T00:00:00Z",
                    "marker": marker,
                }))
                .unwrap(),
            );
            text.push('\n');
        }
        fs::write(project.join(format!("{session}.jsonl")), text).unwrap();
    }

    fn write_memory(root: &Path, project: &str, body: &str, description: &str) {
        let memory = root.join("projects").join(project).join("memory");
        fs::create_dir_all(&memory).unwrap();
        fs::write(
            memory.join("facts.md"),
            format!("---\nname: facts\ndescription: {description}\n---\n\n{body}\n"),
        )
        .unwrap();
        fs::write(
            memory.join("MEMORY.md"),
            format!("# Memory\n\n- [facts](facts.md) — {description}\n"),
        )
        .unwrap();
    }

    #[test]
    fn streams_strict_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let short = temp.path().join("short");
        let long = temp.path().join("long");
        fs::write(&short, b"one\n").unwrap();
        fs::write(&long, b"one\ntwo\n").unwrap();
        let a = FileRecord {
            sha: String::new(),
            path: short,
            size: 4,
            mtime_ns: 0,
            event_ns: None,
        };
        let b = FileRecord {
            sha: String::new(),
            path: long,
            size: 8,
            mtime_ns: 0,
            event_ns: None,
        };
        assert!(file_prefix(&a, &b).unwrap());
        assert!(!file_prefix(&b, &a).unwrap());
    }

    #[test]
    fn merge_strategy_forks_divergent_sessions_and_rewrites_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        let project = "project";
        let session = "11111111-1111-4111-8111-111111111111";
        let common = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        write_session(
            &local,
            project,
            session,
            &[
                (common, "common"),
                ("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "local"),
            ],
        );
        write_session(
            &remote,
            project,
            session,
            &[
                (common, "common"),
                ("cccccccc-cccc-4ccc-8ccc-cccccccccccc", "remote"),
            ],
        );
        let remote_tool = remote
            .join("projects")
            .join(project)
            .join(session)
            .join("tool-results/result.txt");
        fs::create_dir_all(remote_tool.parent().unwrap()).unwrap();
        fs::write(&remote_tool, "remote tool result\n").unwrap();

        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Sessions,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());

        let project_stage = stage.join("projects").join(project);
        let main_files = fs::read_dir(&project_stage)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                (path.extension().and_then(|value| value.to_str()) == Some("jsonl")).then_some(path)
            })
            .collect::<Vec<_>>();
        assert_eq!(main_files.len(), 2);
        let fork = main_files
            .iter()
            .find(|path| path.file_stem().unwrap() != session)
            .unwrap();
        let fork_id = fork.file_stem().unwrap().to_string_lossy();
        let fork_text = fs::read_to_string(fork).unwrap();
        assert!(fork_text.contains("\"marker\":\"remote\""));
        assert!(fork_text.lines().all(|line| {
            serde_json::from_str::<Value>(line).unwrap()["sessionId"] == fork_id.as_ref()
        }));
        assert_eq!(
            fs::read_to_string(
                project_stage
                    .join(fork_id.as_ref())
                    .join("tool-results/result.txt")
            )
            .unwrap(),
            "remote tool result\n"
        );
    }

    #[test]
    fn existing_fork_is_advanced_instead_of_duplicated() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let first_stage = temp.path().join("first-stage");
        let second_remote = temp.path().join("second-remote");
        let second_stage = temp.path().join("second-stage");
        let project = "project";
        let session = "11111111-1111-4111-8111-111111111111";
        let common = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let remote_event = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
        write_session(
            &local,
            project,
            session,
            &[
                (common, "common"),
                ("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "local"),
            ],
        );
        write_session(
            &remote,
            project,
            session,
            &[(common, "common"), (remote_event, "remote")],
        );
        build_stage_full(
            &local,
            &remote,
            &first_stage,
            ResourceSelection::Sessions,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        let first_fork = fs::read_dir(first_stage.join("projects").join(project))
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                (path.extension().and_then(|value| value.to_str()) == Some("jsonl")
                    && path.file_stem().unwrap() != session)
                    .then_some(path.file_stem().unwrap().to_string_lossy().into_owned())
            })
            .next()
            .unwrap();

        write_session(
            &second_remote,
            project,
            session,
            &[
                (common, "common"),
                (remote_event, "remote"),
                ("dddddddd-dddd-4ddd-8ddd-dddddddddddd", "extended"),
            ],
        );
        build_stage_full(
            &first_stage,
            &second_remote,
            &second_stage,
            ResourceSelection::Sessions,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();

        let project_stage = second_stage.join("projects").join(project);
        let main_files = fs::read_dir(&project_stage)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                (path.extension().and_then(|value| value.to_str()) == Some("jsonl")).then_some(path)
            })
            .collect::<Vec<_>>();
        assert_eq!(main_files.len(), 2);
        let advanced =
            fs::read_to_string(project_stage.join(format!("{first_fork}.jsonl"))).unwrap();
        assert!(advanced.contains("\"marker\":\"extended\""));
    }

    #[test]
    fn local_memory_strategy_selects_content_and_index_as_a_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        write_memory(&local, "project", "local body", "local description");
        write_memory(&remote, "project", "remote body", "remote description");

        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Memory,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Local,
        )
        .unwrap();
        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());
        let memory = stage.join("projects/project/memory");
        assert!(
            fs::read_to_string(memory.join("facts.md"))
                .unwrap()
                .contains("local body")
        );
        assert!(
            fs::read_to_string(memory.join("MEMORY.md"))
                .unwrap()
                .contains("local description")
        );
    }

    #[test]
    fn merge_memory_strategy_blocks_ambiguous_content() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        write_memory(&local, "project", "disk is full", "local description");
        write_memory(&remote, "project", "disk is roomy", "remote description");

        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Memory,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        assert_eq!(report.blockers.len(), 1);
        assert_eq!(conflicts.len(), 1);
        assert!(!report.notes.iter().any(|note| note.contains("memory wins")));
        assert!(report.files.iter().any(|file| {
            file.path.ends_with("/memory/facts.md") && file.resolution == "unresolved"
        }));
        assert!(report.files.iter().any(|file| {
            file.path.ends_with("/memory/MEMORY.md") && file.resolution == "unresolved"
        }));
    }

    #[test]
    fn unmanaged_memory_files_are_excluded_and_empty_projects_are_not_generated() {
        assert!(excluded(
            Path::new("projects/project/memory/backup.bak"),
            ResourceSelection::All
        ));
        assert!(excluded(
            Path::new("projects/project/memory/nested/facts.md"),
            ResourceSelection::All
        ));
        assert!(!excluded(
            Path::new("projects/project/memory/facts.md"),
            ResourceSelection::All
        ));

        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        fs::create_dir_all(local.join("projects/project")).unwrap();
        fs::create_dir_all(remote.join("projects/project")).unwrap();
        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Memory,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        assert!(report.files.is_empty());
        assert!(conflicts.is_empty());
        assert!(!stage.join("projects/project/memory/MEMORY.md").exists());
    }

    #[test]
    fn merge_memory_strategy_combines_independent_heading_blocks() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        write_memory(
            &local,
            "project",
            "# Local facts\n\nlocal detail",
            "shared description",
        );
        write_memory(
            &remote,
            "project",
            "# Remote facts\n\nremote detail",
            "shared description",
        );

        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Memory,
            &BTreeMap::new(),
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());
        let merged = fs::read_to_string(stage.join("projects/project/memory/facts.md")).unwrap();
        assert!(merged.contains("local detail"));
        assert!(merged.contains("remote detail"));
    }

    #[test]
    fn explicit_memory_choice_overrides_possible_block_merge() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let stage = temp.path().join("stage");
        write_memory(
            &local,
            "project",
            "# Local facts\n\nlocal detail",
            "local description",
        );
        write_memory(
            &remote,
            "project",
            "# Remote facts\n\nremote detail",
            "remote description",
        );
        let choices =
            BTreeMap::from([(("project".to_owned(), "facts.md".to_owned()), Side::Local)]);
        let (report, conflicts) = build_stage_full(
            &local,
            &remote,
            &stage,
            ResourceSelection::Memory,
            &choices,
            "mini",
            ConflictStrategy::Merge,
        )
        .unwrap();
        assert!(report.blockers.is_empty());
        assert!(conflicts.is_empty());
        let memory = stage.join("projects/project/memory");
        let content = fs::read_to_string(memory.join("facts.md")).unwrap();
        assert!(content.contains("local detail"));
        assert!(!content.contains("remote detail"));
        assert!(
            fs::read_to_string(memory.join("MEMORY.md"))
                .unwrap()
                .contains("local description")
        );
    }

    #[test]
    fn memory_index_preserves_preamble_and_multiline_block() {
        let temp = tempfile::tempdir().unwrap();
        let topic = temp.path().join("topic.md");
        fs::write(&topic, "---\nname: Topic\ndescription: Desc\n---\n").unwrap();
        fs::write(
            temp.path().join("MEMORY.md"),
            "# Memory\n\n- [Topic](topic.md) — Desc\n  continuation\n",
        )
        .unwrap();
        let files = BTreeMap::from([("topic.md".to_owned(), topic)]);
        let parsed = memory_index(temp.path(), &files).unwrap();
        assert_eq!(parsed.preamble, "# Memory\n\n");
        assert_eq!(
            parsed.items["topic.md"],
            "- [Topic](topic.md) — Desc\n  continuation\n"
        );
    }

    #[test]
    fn lsof_parser_ignores_read_only_descriptors() {
        let output = "p10\nf3r\np11\nf8u\np12\nf4w\n";
        assert_eq!(
            parse_lsof_writers(output),
            BTreeSet::from(["11".to_owned(), "12".to_owned()])
        );
    }

    #[test]
    fn validates_session_identity_and_event_time() {
        let temp = tempfile::tempdir().unwrap();
        let id = "11111111-1111-4111-8111-111111111111";
        let dir = temp.path().join("projects/project");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.jsonl"));
        fs::write(&path, format!("{{\"sessionId\":\"{id}\",\"uuid\":\"22222222-2222-4222-8222-222222222222\",\"timestamp\":\"2026-08-01T00:00:00Z\"}}\n")).unwrap();
        let relative = path.strip_prefix(temp.path()).unwrap();
        assert_eq!(
            validate_claude_jsonl(&path, relative).unwrap(),
            1_785_542_400_000_000_000
        );
    }

    #[test]
    fn rejects_session_id_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("projects/project/11111111-1111-4111-8111-111111111111.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "{\"sessionId\":\"22222222-2222-4222-8222-222222222222\",\"timestamp\":\"2026-08-01T00:00:00Z\"}\n").unwrap();
        assert!(validate_claude_jsonl(&path, path.strip_prefix(temp.path()).unwrap()).is_err());
    }
}
