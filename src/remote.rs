use crate::core::file_lock_is_held;
use anyhow::{Context, Result, bail};
use filetime::FileTime;
use fs2::FileExt;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Ping,
    Doctor {
        root: String,
        agent: String,
    },
    ClaudeMtimes {
        root: String,
        include_memory: bool,
    },
    ClaudeWriters {
        root: String,
    },
    SetMtimes {
        root: String,
        items: Vec<MtimeUpdate>,
    },
    CodexActiveWriters {
        root: String,
    },
    OpenCodeSessionIds {
        root: String,
    },
    OpenCodeWriters {
        root: String,
    },
    OpenCodeBackup {
        root: String,
        stamp: String,
    },
    HoldCoordinationLock {
        root: String,
    },
    Backup {
        root: String,
        backup_dir: String,
        stamp: String,
        members: Vec<String>,
    },
    CodexState {
        root: String,
        stamp: String,
        times: Option<BTreeMap<String, StateTimes>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MtimeUpdate {
    pub path: String,
    pub sha256: String,
    pub mtime_ns: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateTimes {
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub recency_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub protocol: u32,
    pub ok: bool,
    #[serde(default)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    fn success<T: Serialize>(value: T) -> Result<Self> {
        Ok(Self {
            protocol: PROTOCOL_VERSION,
            ok: true,
            value: serde_json::to_value(value)?,
            error: None,
        })
    }

    fn failure(error: anyhow::Error) -> Self {
        Self {
            protocol: PROTOCOL_VERSION,
            ok: false,
            value: Value::Null,
            error: Some(format!("{error:#}")),
        }
    }
}

pub fn serve(protocol: u32) -> Result<()> {
    if protocol != PROTOCOL_VERSION {
        bail!("unsupported protocol {protocol}; expected {PROTOCOL_VERSION}");
    }
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut line = String::new();
    input.read_line(&mut line)?;
    if line.is_empty() {
        bail!("missing remote request");
    }
    let request: Request = serde_json::from_str(&line).context("decode remote request")?;
    let hold = matches!(request, Request::HoldCoordinationLock { .. });
    let result = dispatch(request);
    let response = match result {
        Ok(value) => Response::success(value)?,
        Err(error) => Response::failure(error),
    };
    serde_json::to_writer(io::stdout().lock(), &response)?;
    println!();
    io::stdout().flush()?;
    if hold && response.ok {
        let mut sink = Vec::new();
        input.read_to_end(&mut sink)?;
    }
    Ok(())
}

fn dispatch(request: Request) -> Result<Value> {
    match request {
        Request::Ping => Ok(serde_json::json!({
            "protocol": PROTOCOL_VERSION,
            "version": env!("CARGO_PKG_VERSION"),
            "executable_sha256": executable_sha256()?,
        })),
        Request::Doctor { root, agent } => {
            let root = expand_root(&root)?;
            if !root.is_dir() {
                bail!("remote {agent} root does not exist: {}", root.display());
            }
            match agent.as_str() {
                "codex" if Command::new("codex").arg("--version").output().is_err() => {
                    bail!("remote codex command is missing")
                }
                "claude" if Command::new("lsof").arg("-v").output().is_err() => {
                    bail!("remote lsof command is missing")
                }
                "opencode" if Command::new("opencode").arg("--version").output().is_err() => {
                    bail!("remote opencode command is missing")
                }
                "opencode" if Command::new("lsof").arg("-v").output().is_err() => {
                    bail!("remote lsof command is missing")
                }
                "opencode" => {
                    let output = Command::new("opencode").args(["db", "path"]).output()?;
                    if !output.status.success() {
                        bail!("remote `opencode db path` failed");
                    }
                    let reported = PathBuf::from(String::from_utf8(output.stdout)?.trim());
                    let configured = fs::canonicalize(root.join("opencode.db"))?;
                    let reported = fs::canonicalize(reported)?;
                    if reported != configured {
                        bail!(
                            "remote OpenCode root differs from `opencode db path`: configured={}, reported={}",
                            configured.display(),
                            reported.display()
                        );
                    }
                }
                "codex" | "claude" => {}
                _ => bail!("unknown agent: {agent}"),
            }
            Ok(serde_json::json!({ "ready": true }))
        }
        Request::ClaudeMtimes {
            root,
            include_memory,
        } => Ok(serde_json::to_value(claude_mtimes(
            &expand_root(&root)?,
            include_memory,
        )?)?),
        Request::ClaudeWriters { root } => Ok(serde_json::json!({
            "active": claude_writers(&expand_root(&root)?)?,
        })),
        Request::SetMtimes { root, items } => {
            set_mtimes(&expand_root(&root)?, &items)?;
            Ok(serde_json::json!({ "updated": items.len() }))
        }
        Request::CodexActiveWriters { root } => Ok(serde_json::to_value(codex_active_writers(
            &expand_root(&root)?,
        )?)?),
        Request::OpenCodeSessionIds { root } => Ok(serde_json::to_value(opencode_session_ids(
            &expand_root(&root)?,
        )?)?),
        Request::OpenCodeWriters { root } => Ok(serde_json::json!({
            "active": sqlite_writers(&expand_root(&root)?.join("opencode.db"))?,
        })),
        Request::OpenCodeBackup { root, stamp } => Ok(serde_json::json!({
            "path": opencode_backup(&expand_root(&root)?, &stamp)?,
        })),
        Request::HoldCoordinationLock { root } => {
            let root = expand_root(&root)?;
            let path = root.join("thread-writer-locks/.coordination.lock");
            if let Some(parent) = path.parent() {
                private_dir(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            file.lock_exclusive()?;
            // Keep the descriptor alive until serve() observes EOF.
            std::mem::forget(file);
            Ok(serde_json::json!({ "locked": true }))
        }
        Request::Backup {
            root,
            backup_dir,
            stamp,
            members,
        } => {
            let root = expand_root(&root)?;
            let out = root
                .join(checked_relative(&backup_dir)?)
                .join(format!("before-{stamp}.tar.gz"));
            create_backup(&root, &out, &members)?;
            Ok(serde_json::json!({ "path": out.to_string_lossy() }))
        }
        Request::CodexState { root, stamp, times } => {
            let root = expand_root(&root)?;
            if let Some(times) = times {
                Ok(serde_json::json!({ "changed": repair_state(&root, &times)? }))
            } else {
                Ok(serde_json::json!({
                    "path": backup_state(&root, &stamp)?.to_string_lossy()
                }))
            }
        }
    }
}

fn expand_root(root: &str) -> Result<PathBuf> {
    if root == "~" {
        return dirs::home_dir().context("remote home directory is unavailable");
    }
    if let Some(rest) = root.strip_prefix("~/") {
        return Ok(dirs::home_dir()
            .context("remote home directory is unavailable")?
            .join(rest));
    }
    Ok(PathBuf::from(root))
}

fn checked_relative(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!("unsafe relative path: {value}");
    }
    Ok(path)
}

fn private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    io::copy(&mut file, &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

fn executable_sha256() -> Result<String> {
    sha256(&std::env::current_exe()?)
}

fn claude_mtimes(root: &Path, include_memory: bool) -> Result<BTreeMap<String, i64>> {
    let base = root.join("projects");
    let mut result = BTreeMap::new();
    if !base.exists() {
        return Ok(result);
    }
    for entry in WalkDir::new(&base).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(&base)?;
        let is_memory = relative
            .components()
            .nth(1)
            .is_some_and(|v| v.as_os_str() == "memory");
        if is_memory && !include_memory {
            continue;
        }
        let ns = entry
            .metadata()?
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?;
        result.insert(
            Path::new("projects")
                .join(relative)
                .to_string_lossy()
                .into_owned(),
            i64::try_from(ns.as_nanos()).context("mtime exceeds i64")?,
        );
    }
    Ok(result)
}

fn claude_writers(root: &Path) -> Result<bool> {
    let output = Command::new("lsof")
        .args(["-Fpf", "+D"])
        .arg(root.join("projects"))
        .output()
        .context("run remote lsof")?;
    let mut current = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if line.starts_with('p') {
            current = true;
        } else if current
            && line.starts_with('f')
            && line[1..].chars().take_while(char::is_ascii_digit).count() > 0
            && line.chars().any(|c| c == 'w' || c == 'u')
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn set_mtimes(root: &Path, items: &[MtimeUpdate]) -> Result<()> {
    for item in items {
        let relative = checked_relative(&item.path)?;
        let path = root.join(relative);
        if sha256(&path)? != item.sha256 {
            bail!("content changed before mtime normalization: {}", item.path);
        }
        filetime::set_file_mtime(
            path,
            FileTime::from_unix_time(
                item.mtime_ns / 1_000_000_000,
                (item.mtime_ns % 1_000_000_000) as u32,
            ),
        )?;
    }
    Ok(())
}

fn codex_active_writers(root: &Path) -> Result<Vec<String>> {
    let directory = root.join("thread-writer-locks");
    let mut active = Vec::new();
    if !directory.exists() {
        return Ok(active);
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(id) = name
            .split(|c: char| !(c.is_ascii_hexdigit() || c == '-'))
            .find(|part| uuid::Uuid::parse_str(part).is_ok())
        else {
            continue;
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(entry.path())?;
        if file_lock_is_held(&file)? {
            active.push(id.to_owned());
        }
    }
    active.sort();
    Ok(active)
}

fn opencode_session_ids(root: &Path) -> Result<Vec<String>> {
    let connection = Connection::open_with_flags(
        root.join("opencode.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut statement = connection.prepare("SELECT id FROM session ORDER BY id")?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

fn sqlite_writers(database: &Path) -> Result<bool> {
    let output = Command::new("lsof")
        .args(["-Fpf"])
        .arg(database)
        .output()
        .context("run lsof for OpenCode database")?;
    let mut current = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if line.starts_with('p') {
            current = true;
        } else if current
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

fn opencode_backup(root: &Path, stamp: &str) -> Result<String> {
    let directory = root.join("agent-sync-backups");
    private_dir(&directory)?;
    let destination = directory.join(format!("before-{stamp}.db"));
    let connection = Connection::open(root.join("opencode.db"))?;
    connection.execute(
        "VACUUM INTO ?1",
        params![destination.to_string_lossy().as_ref()],
    )?;
    Ok(destination.to_string_lossy().into_owned())
}

pub fn create_backup(root: &Path, out: &Path, members: &[String]) -> Result<()> {
    if let Some(parent) = out.parent() {
        private_dir(parent)?;
    }
    let partial = PathBuf::from(format!("{}.partial", out.display()));
    let result = (|| -> Result<()> {
        let file = File::create(&partial)?;
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);
        archive.follow_symlinks(false);
        for member in members {
            let relative = checked_relative(member)?;
            let path = root.join(&relative);
            if path.is_dir() {
                archive.append_dir_all(&relative, &path)?;
            } else if path.exists() {
                archive.append_path_with_name(&path, &relative)?;
            }
        }
        let encoder = archive.into_inner()?;
        let file = encoder.finish()?;
        file.sync_all()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&partial, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(&partial, out)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&partial);
    }
    result
}

fn state_db(root: &Path) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(number) = name
            .strip_prefix("state_")
            .and_then(|v| v.strip_suffix(".sqlite"))
            .and_then(|v| v.parse::<u64>().ok())
        {
            candidates.push((number, entry.path()));
        }
    }
    candidates.sort_by_key(|value| value.0);
    candidates.pop().map(|value| value.1).context("no state db")
}

fn backup_state(root: &Path, stamp: &str) -> Result<PathBuf> {
    let source = Connection::open(state_db(root)?)?;
    let directory = root.join("sync-backups");
    private_dir(&directory)?;
    let out = directory.join(format!("state-before-{stamp}.sqlite"));
    source.backup("main", &out, None)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&out, fs::Permissions::from_mode(0o600))?;
    }
    Ok(out)
}

fn repair_state(root: &Path, times: &BTreeMap<String, StateTimes>) -> Result<usize> {
    let mut connection = Connection::open(state_db(root)?)?;
    let transaction = connection.transaction()?;
    let mut changed = 0;
    for (id, value) in times {
        changed += transaction.execute(
            "UPDATE threads SET created_at=?1,created_at_ms=?2,updated_at=?3,updated_at_ms=?4,recency_at=?5,recency_at_ms=?6 WHERE id=?7 AND (created_at_ms!=?2 OR updated_at_ms!=?4 OR recency_at_ms!=?6)",
            params![
                value.created_at_ms / 1000,
                value.created_at_ms,
                value.updated_at_ms / 1000,
                value.updated_at_ms,
                value.recency_at_ms / 1000,
                value.recency_at_ms,
                id
            ],
        )?;
    }
    transaction.commit()?;
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_relative_paths() {
        assert!(checked_relative("projects/a.jsonl").is_ok());
        assert!(checked_relative("../outside").is_err());
        assert!(checked_relative("/absolute").is_err());
    }

    #[test]
    fn backup_contains_nested_directory_files() -> Result<()> {
        let source = tempfile::tempdir()?;
        private_dir(&source.path().join("projects/example"))?;
        fs::write(
            source.path().join("projects/example/session.jsonl"),
            b"event\n",
        )?;
        let output = source.path().join("backups/before-test.tar.gz");
        create_backup(source.path(), &output, &["projects".to_owned()])?;

        let unpacked = tempfile::tempdir()?;
        let decoder = flate2::read::GzDecoder::new(File::open(output)?);
        let mut archive = tar::Archive::new(decoder);
        archive.unpack(unpacked.path())?;
        assert_eq!(
            fs::read(unpacked.path().join("projects/example/session.jsonl"))?,
            b"event\n"
        );
        Ok(())
    }
}
