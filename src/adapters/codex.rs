use super::{Adapter, Prepared};
use crate::core::{
    Blocker, PlanReport, ResourceSelection, SyncOptions, bytes_sha256, cache_path, fingerprint,
    manifest, private_dir, stamp,
};
use crate::transport::SshTransport;
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
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use tempfile::TempDir;
use walkdir::WalkDir;

pub struct CodexAdapter;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Times {
    created_at_ms: i64,
    updated_at_ms: i64,
    recency_at_ms: i64,
}

struct Session {
    relative: PathBuf,
    lines: Vec<Vec<u8>>,
    times: Times,
}

pub struct CodexPrepared {
    pub report: PlanReport,
    temp: TempDir,
    stage: PathBuf,
    local_fingerprint: String,
    remote_fingerprint: String,
    resources: ResourceSelection,
    metadata: BTreeMap<String, Times>,
    active: BTreeSet<String>,
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
        let root = SshTransport::shell_quote(remote);
        if !transport.remote_ok(&format!("test -d {root} && command -v python3 >/dev/null && command -v tar >/dev/null && command -v codex >/dev/null"))? {
            bail!("remote Codex root or required command is missing");
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
            .prefix("agent-sync-codex-")
            .tempdir()?;
        let remote = cache_path(options, "codex", &transport.host)?;
        pull_codex(transport, remote_root, &remote, options.resources)?;
        let stage = temp.path().join("stage");
        private_dir(&stage)?;
        let active = active_writer_ids(local, remote_root, transport)?;
        let (mut report, metadata) = build_stage(
            local,
            &remote,
            &stage,
            options.resources,
            &active,
            &transport.host,
        )?;
        if options.apply && options.resources.sessions() && !active.is_empty() {
            report.blockers.push(Blocker {
                resource: "sessions".into(),
                path: active.iter().cloned().collect::<Vec<_>>().join(","),
                reason: "active Codex writers must exit before apply".into(),
            });
        }
        let exclude = |p: &Path| excluded(p, options.resources);
        let remote_fingerprint = fingerprint(&remote, exclude)?;
        Ok(Prepared::Codex(CodexPrepared {
            report,
            temp,
            stage,
            local_fingerprint: fingerprint(local, exclude)?,
            remote_fingerprint,
            resources: options.resources,
            metadata,
            active,
        }))
    }

    fn resolve_interactive(&self, _prepared: &mut Prepared, _tty: bool) -> Result<()> {
        Ok(())
    }

    fn apply(
        &self,
        prepared: Prepared,
        local: &Path,
        remote_root: &str,
        transport: &SshTransport,
        _options: &SyncOptions,
    ) -> Result<()> {
        let Prepared::Codex(value) = prepared else {
            bail!("adapter/prepared plan mismatch");
        };
        if value.resources.sessions() && !value.active.is_empty() {
            bail!("refusing apply while Codex writers are active");
        }
        let exclude = |p: &Path| excluded(p, value.resources);
        let _guard = if value.resources.sessions() {
            Some(CodexGuards::acquire(local, remote_root, transport)?)
        } else {
            None
        };
        if value.resources.sessions()
            && !active_writer_ids(local, remote_root, transport)?.is_empty()
        {
            bail!("a Codex writer became active after preview");
        }
        if fingerprint(local, exclude)? != value.local_fingerprint {
            bail!("local Codex data changed after preview");
        }
        let check = value.temp.path().join("remote-recheck");
        pull_codex(transport, remote_root, &check, value.resources)?;
        if fingerprint(&check, exclude)? != value.remote_fingerprint {
            bail!("remote Codex data changed after preview");
        }
        if value.resources.sessions() {
            reconcile_catalog(None, false)?;
            reconcile_catalog(Some(transport), false)?;
        }
        let stamp = stamp();
        let local_backup = backup_local(local, value.resources, &stamp)?;
        let remote_backup = backup_remote(remote_root, value.resources, &stamp, transport)?;
        if value.resources.sessions() {
            backup_state(local, &stamp)?;
            remote_state(transport, remote_root, &stamp, "backup", None)?;
        }
        install_local(&value.stage, local, &transport.rsync)?;
        transport.push(&value.stage, remote_root)?;
        let verified = value.temp.path().join("remote-verified");
        pull_codex(transport, remote_root, &verified, value.resources)?;
        verify_selected(&value.stage, local, value.resources, "local")?;
        verify_selected(&value.stage, &verified, value.resources, "remote")?;
        drop(_guard);
        if value.resources.sessions() {
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
        println!(
            "complete: Codex synchronized and verified; backups: local={}, remote={}:{}",
            local_backup.display(),
            transport.host,
            remote_backup
        );
        Ok(())
    }
}

fn pull_codex(t: &SshTransport, root: &str, dest: &Path, r: ResourceSelection) -> Result<()> {
    let mut f = vec![
        "--exclude=/sync-backups/***",
        "--exclude=/memories/.git/***",
        "--exclude=/memories/.omx/***",
    ];
    if r.sessions() {
        f.extend([
            "--include=/sessions/***",
            "--include=/archived_sessions/***",
            "--include=/history.jsonl",
            "--include=/session_index.jsonl",
        ]);
    }
    if r.memory() {
        f.push("--include=/memories/***");
    }
    f.push("--exclude=*");
    t.pull(root, dest, &f)
}
fn excluded(p: &Path, r: ResourceSelection) -> bool {
    let n = p
        .components()
        .next()
        .map(|v| v.as_os_str().to_string_lossy());
    let memory = n.as_deref() == Some("memories");
    let session = matches!(
        n.as_deref(),
        Some("sessions" | "archived_sessions" | "history.jsonl" | "session_index.jsonl")
    );
    (!r.memory() && memory)
        || (!r.sessions() && session)
        || (!memory && !session)
        || p.to_string_lossy().contains("sync-backups")
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
            let bytes = fs::read(e.path())?;
            let lines = split_lines(&bytes)?;
            let (id, times) = validate_rollout(e.path(), &lines)?;
            if active.contains(&id) {
                continue;
            }
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
) -> Result<(PlanReport, BTreeMap<String, Times>)> {
    let mut report = PlanReport {
        agent: "codex".into(),
        peer: peer.into(),
        resources: Vec::new(),
        ..Default::default()
    };
    let mut metadata = BTreeMap::new();
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
                (Some(_), Some(_)) => {
                    report.blockers.push(Blocker {
                        resource: "sessions".into(),
                        path: id.clone(),
                        reason: "rollout diverged".into(),
                    });
                    continue;
                }
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
        report
            .notes
            .push(format!("active sessions excluded: {}", active.len()));
    }
    if r.memory() {
        report.resources.push("memory".into());
        merge_codex_memory(local, remote, stage, &mut report)?;
    }
    Ok((report, metadata))
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
                    report.blockers.push(Blocker {
                        resource: "memory".into(),
                        path: rel.display().to_string(),
                        reason: "same-path memory leaf differs".into(),
                    })
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

const ACTIVE_PY: &str = r#"import fcntl,json,os,pathlib,re,sys
root=pathlib.Path(sys.argv[1]).expanduser(); active=[]
for p in root.glob('thread-writer-locks/*.lock'):
 m=re.search(r'([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})',p.name)
 if not m: continue
 fd=os.open(p,os.O_RDWR)
 try:
  try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError: active.append(m.group(1))
  else: fcntl.flock(fd,fcntl.LOCK_UN)
 finally: os.close(fd)
print(json.dumps(active))"#;
fn active_writer_ids(local: &Path, remote: &str, t: &SshTransport) -> Result<BTreeSet<String>> {
    let mut s = BTreeSet::new();
    let d = local.join("thread-writer-locks");
    if d.exists() {
        for e in fs::read_dir(d)? {
            let e = e?;
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(id) = find_uuid(&n) {
                let f = OpenOptions::new().read(true).write(true).open(e.path())?;
                if f.try_lock_exclusive().is_err() {
                    s.insert(id);
                }
            }
        }
    }
    let script = format!(
        "python3 -c {} {}",
        SshTransport::shell_quote(ACTIVE_PY),
        SshTransport::shell_quote(remote)
    );
    let o = t.ssh(&script)?;
    for id in serde_json::from_slice::<Vec<String>>(&o.stdout)? {
        s.insert(id);
    }
    Ok(s)
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
    remote: Child,
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
        let py = "import fcntl,os,pathlib,sys; p=pathlib.Path(sys.argv[1]).expanduser()/'thread-writer-locks/.coordination.lock';p.parent.mkdir(parents=True,exist_ok=True);f=os.open(p,os.O_CREAT|os.O_RDWR,0o600);fcntl.flock(f,fcntl.LOCK_EX);print('ready',flush=True);sys.stdin.read()";
        let script = format!(
            "exec python3 -c {} {}",
            SshTransport::shell_quote(py),
            SshTransport::shell_quote(remote)
        );
        let mut child = Command::new(&t.ssh)
            .arg(&t.host)
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line)?;
        if line.trim() != "ready" {
            bail!("remote coordination lock failed")
        }
        Ok(Self {
            local: file,
            remote: child,
        })
    }
}
impl Drop for CodexGuards {
    fn drop(&mut self) {
        let _ = self.remote.stdin.take();
        let _ = self.remote.wait();
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
    let mut command = Command::new("tar");
    command.args(["-czf"]).arg(&o).arg("-C").arg(root);
    for member in backup_members(r) {
        if root.join(member).exists() {
            command.arg(member);
        }
    }
    let status = command.status()?;
    if !status.success() {
        bail!("local Codex backup failed")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&o, fs::Permissions::from_mode(0o600))?;
    }
    Ok(o)
}
fn backup_remote(
    root: &str,
    r: ResourceSelection,
    stamp: &str,
    t: &SshTransport,
) -> Result<String> {
    let d = format!("{root}/sync-backups");
    let o = format!("{d}/before-{stamp}.tar.gz");
    let python = r#"import pathlib,sys,tarfile
root=pathlib.Path(sys.argv[1]).expanduser();out=pathlib.Path(sys.argv[2]).expanduser();members=sys.argv[3:]
out.parent.mkdir(parents=True,exist_ok=True);partial=pathlib.Path(str(out)+'.partial')
try:
 with tarfile.open(partial,'w:gz') as tar:
  for name in members:
   path=root/name
   if path.exists():tar.add(path,arcname=name)
 partial.chmod(0o600);partial.replace(out)
finally:
 partial.unlink(missing_ok=True)"#;
    let args = backup_members(r)
        .into_iter()
        .map(SshTransport::shell_quote)
        .collect::<Vec<_>>()
        .join(" ");
    let s = format!(
        "python3 -c {} {} {} {}",
        SshTransport::shell_quote(python),
        SshTransport::shell_quote(root),
        SshTransport::shell_quote(&o),
        args
    );
    t.ssh(&s)?;
    Ok(o)
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
fn verify_selected(stage: &Path, actual: &Path, r: ResourceSelection, side: &str) -> Result<()> {
    let a = manifest(stage, |p| excluded(p, r))?;
    let b = manifest(actual, |p| excluded(p, r))?;
    if a != b {
        bail!("{side} final file set or content differs from the staged manifest");
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
const STATE_PY: &str = r#"import json,pathlib,re,sqlite3,sys
root=pathlib.Path(sys.argv[1]).expanduser(); stamp=sys.argv[2]; mode=sys.argv[3]
c=[]
for p in root.glob('state_*.sqlite'):
 m=re.fullmatch(r'state_(\d+)\.sqlite',p.name)
 if m:c.append((int(m.group(1)),p))
if not c:raise SystemExit('no state db')
db=max(c)[1];backup=root/'sync-backups'/f'state-before-{stamp}.sqlite'
if mode=='backup':
 backup.parent.mkdir(parents=True,exist_ok=True)
 with sqlite3.connect(db) as s,sqlite3.connect(backup) as d:s.backup(d)
 backup.chmod(0o600);print(backup)
else:
 data=json.load(sys.stdin);changed=0
 with sqlite3.connect(db) as con:
  for sid,v in data.items():
   c=con.execute('UPDATE threads SET created_at=?,created_at_ms=?,updated_at=?,updated_at_ms=?,recency_at=?,recency_at_ms=? WHERE id=? AND (created_at_ms!=? OR updated_at_ms!=? OR recency_at_ms!=?)',(v['created_at_ms']//1000,v['created_at_ms'],v['updated_at_ms']//1000,v['updated_at_ms'],v['recency_at_ms']//1000,v['recency_at_ms'],sid,v['created_at_ms'],v['updated_at_ms'],v['recency_at_ms']))
   changed+=c.rowcount
 print(changed)"#;
fn remote_state(
    t: &SshTransport,
    root: &str,
    stamp: &str,
    mode: &str,
    data: Option<&BTreeMap<String, Times>>,
) -> Result<String> {
    let script = format!(
        "python3 -c {} {} {} {}",
        SshTransport::shell_quote(STATE_PY),
        SshTransport::shell_quote(root),
        SshTransport::shell_quote(stamp),
        SshTransport::shell_quote(mode)
    );
    let mut c = Command::new(&t.ssh);
    c.arg(&t.host)
        .arg(script)
        .stdin(if data.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = c.spawn()?;
    if let Some(d) = data {
        serde_json::to_writer(child.stdin.as_mut().unwrap(), d)?;
    }
    let o = child.wait_with_output()?;
    if !o.status.success() {
        bail!(
            "remote state operation failed: {}",
            String::from_utf8_lossy(&o.stderr)
        )
    }
    Ok(String::from_utf8(o.stdout)?.trim().to_owned())
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
}
