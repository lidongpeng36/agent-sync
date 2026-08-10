use super::{Adapter, Prepared};
use crate::core::{
    Blocker, PlanReport, ResourceSelection, SyncOptions, cache_path, copy_file_atomic, fingerprint,
    manifest, private_dir, safe_relative, sha256, stamp,
};
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
        let root = SshTransport::shell_quote(remote);
        if !transport.remote_ok(&format!("test -d {root} && command -v python3 >/dev/null && command -v tar >/dev/null && command -v lsof >/dev/null"))? {
            bail!("remote Claude root or required command is missing");
        }
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
        ResourceSelection::All => vec!["--include=/projects/***", "--exclude=*"],
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
    let python = r#"import json,os,pathlib,sys
root=pathlib.Path(sys.argv[1]).expanduser(); include_memory=sys.argv[2]=='1'; out={}
base=root/'projects'
if base.exists():
 for current,dirs,files in os.walk(base):
  p=pathlib.Path(current)
  rel=p.relative_to(base)
  if len(rel.parts)>=2 and rel.parts[1]=='memory' and not include_memory:
   dirs[:]=[];continue
  for name in files:
   path=p/name;out[(pathlib.Path('projects')/rel/name).as_posix()]=path.stat().st_mtime_ns
print(json.dumps(out,separators=(',',':')))"#;
    let script = format!(
        "python3 -c {} {} {}",
        SshTransport::shell_quote(python),
        SshTransport::shell_quote(root),
        if resources.memory() { "1" } else { "0" }
    );
    let output = transport.ssh(&script)?;
    let mtimes: BTreeMap<String, i64> = serde_json::from_slice(&output.stdout)?;
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
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    build_stage_full(local, remote, stage, resources, choices, peer)
}

fn build_stage_from_choices(
    remote: &Path,
    stage: &Path,
    resources: ResourceSelection,
    choices: &BTreeMap<(String, String), Side>,
    peer: &str,
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    // The original local tree is retained next to the remote snapshot by prepare.
    let local = stage.parent().context("stage parent")?.join("local-copy");
    build_stage_full(&local, remote, stage, resources, choices, peer)
}

fn build_stage_full(
    local: &Path,
    remote: &Path,
    stage: &Path,
    resources: ResourceSelection,
    choices: &BTreeMap<(String, String), Side>,
    peer: &str,
) -> Result<(PlanReport, Vec<MemoryConflict>)> {
    let mut report = PlanReport {
        agent: "claude".into(),
        peer: peer.into(),
        resources: Vec::new(),
        ..Default::default()
    };
    let mut conflicts = Vec::new();
    if resources.sessions() {
        report.resources.push("sessions".into());
        let left = session_files(local)?;
        let right = session_files(remote)?;
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
        report
            .notes
            .push(format!("metadata-only differences: {metadata_only}"));
    }
    if resources.memory() {
        report.resources.push("memory".into());
        merge_memories(local, remote, stage, choices, &mut report, &mut conflicts)?;
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
        let li = memory_index(&lm, &lf)?;
        let ri = memory_index(&rm, &rf)?;
        let mut selected = BTreeMap::new();
        for target in lf.keys().chain(rf.keys()).cloned().collect::<BTreeSet<_>>() {
            let source = rf.get(&target).or_else(|| lf.get(&target)).unwrap();
            if lf.contains_key(&target) && !rf.contains_key(&target) {
                report.remote_additions += 1;
            }
            if rf.contains_key(&target) && !lf.contains_key(&target) {
                report.local_additions += 1;
            }
            if let (Some(local_file), Some(remote_file)) = (lf.get(&target), rf.get(&target)) {
                if sha256(local_file)? != sha256(remote_file)? {
                    report
                        .notes
                        .push(format!("remote content wins: {project}/{target}"));
                }
            }
            let left = li.items.get(&target);
            let right = ri.items.get(&target);
            let block = match (left, right) {
                (Some(a), Some(b)) if a != b => {
                    match choices.get(&(project.clone(), target.clone())) {
                        Some(Side::Local) => a.clone(),
                        Some(Side::Remote) => b.clone(),
                        None => {
                            conflicts.push(MemoryConflict {
                                project: project.clone(),
                                target: target.clone(),
                                local: a.clone(),
                                remote: b.clone(),
                            });
                            report.blockers.push(Blocker {
                                resource: "memory".into(),
                                path: format!("{project}/{target}"),
                                reason: "index description requires a choice".into(),
                            });
                            b.clone()
                        }
                    }
                }
                (Some(a), _) => a.clone(),
                (_, Some(b)) => b.clone(),
                _ => synthesize_block(source, &target)?,
            };
            let dest = stage
                .join("projects")
                .join(&project)
                .join("memory")
                .join(&target);
            copy_file_atomic(source, &dest)?;
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
    let python = r#"import re,subprocess,sys
p=subprocess.run(['lsof','-Fpf','+D',sys.argv[1]],text=True,capture_output=True)
cur=None; writers=set()
for line in p.stdout.splitlines():
 if line.startswith('p'): cur=line[1:]
 elif cur and re.fullmatch(r'f\d+[wu].*',line): writers.add(cur)
raise SystemExit(1 if writers else 0)"#;
    let script = format!(
        "python3 -c {} {}/projects",
        SshTransport::shell_quote(python),
        SshTransport::shell_quote(remote)
    );
    if !transport.remote_ok(&script)? {
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

#[derive(serde::Serialize)]
struct MtimeRequest {
    path: String,
    sha256: String,
    mtime_ns: i64,
}

fn mtime_requests(stage: &Path) -> Result<Vec<MtimeRequest>> {
    let mut requests = Vec::new();
    for (path, file) in session_files(stage)? {
        if let Some(event) = file.event_ns {
            requests.push(MtimeRequest {
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
    let python = r#"import hashlib,json,os,pathlib,sys
root=pathlib.Path(sys.argv[1]).expanduser()
for item in json.load(sys.stdin):
 p=root/pathlib.PurePosixPath(item['path'])
 if hashlib.sha256(p.read_bytes()).hexdigest()!=item['sha256']: raise SystemExit('content changed: '+item['path'])
 ns=int(item['mtime_ns']);os.utime(p,ns=(ns,ns))"#;
    let script = format!(
        "python3 -c {} {}",
        SshTransport::shell_quote(python),
        SshTransport::shell_quote(root)
    );
    let mut child = Command::new(&transport.ssh)
        .arg(&transport.host)
        .arg(script)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    serde_json::to_writer(
        child.stdin.as_mut().context("remote mtime stdin")?,
        &requests,
    )?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!(
            "remote mtime normalization failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn backup_local(root: &Path, resources: ResourceSelection, stamp: &str) -> Result<PathBuf> {
    let dir = root.join("agent-sync-backups");
    private_dir(&dir)?;
    let out = dir.join(format!("before-{stamp}.tar.gz"));
    let mut c = Command::new("tar");
    c.args(["-czf"]).arg(&out).arg("-C").arg(root);
    let _ = resources;
    c.arg("projects");
    if !c.status()?.success() {
        bail!("local backup failed")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&out, fs::Permissions::from_mode(0o600))?;
    }
    Ok(out)
}
fn backup_remote(
    root: &str,
    _resources: ResourceSelection,
    stamp: &str,
    t: &SshTransport,
) -> Result<String> {
    let d = format!("{root}/agent-sync-backups");
    let out = format!("{d}/before-{stamp}.tar.gz");
    let s = format!(
        "set -e; umask 077; mkdir -p {d}; tar -czf {o}.partial -C {r} projects; tar -tzf {o}.partial >/dev/null; mv {o}.partial {o}",
        d = SshTransport::shell_quote(&d),
        o = SshTransport::shell_quote(&out),
        r = SshTransport::shell_quote(root)
    );
    t.ssh(&s)?;
    Ok(out)
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
