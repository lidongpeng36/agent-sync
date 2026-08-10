use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::ValueEnum;
use serde::Deserialize;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Copy)]
pub enum OutputFormat {
    Human,
    Json,
    Diff,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ResourceSelection {
    All,
    Sessions,
    Memory,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lower")]
pub enum ConflictStrategy {
    Local,
    Remote,
    #[default]
    Merge,
}

impl fmt::Display for ConflictStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Merge => "merge",
        })
    }
}

impl ResourceSelection {
    pub fn sessions(self) -> bool {
        matches!(self, Self::All | Self::Sessions)
    }
    pub fn memory(self) -> bool {
        matches!(self, Self::All | Self::Memory)
    }
}

pub struct SyncOptions {
    pub apply: bool,
    pub stability_seconds: f64,
    pub cache_dir: Option<PathBuf>,
    pub resources: ResourceSelection,
    pub conflict_strategy: ConflictStrategy,
}

#[derive(Clone, Debug, Serialize)]
pub struct Blocker {
    pub resource: String,
    pub path: String,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FileAction {
    Unchanged,
    Create,
    Replace,
    Remove,
    Metadata,
}

impl fmt::Display for FileAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unchanged => "unchanged",
            Self::Create => "create",
            Self::Replace => "replace",
            Self::Remove => "remove",
            Self::Metadata => "metadata",
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FileChange {
    pub resource: String,
    pub path: String,
    pub local: FileAction,
    pub remote: FileAction,
    pub resolution: String,
    pub local_sha256: Option<String>,
    pub remote_sha256: Option<String>,
    pub result_sha256: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanReport {
    pub agent: String,
    pub peer: String,
    pub resources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_strategy: Option<ConflictStrategy>,
    pub local_additions: usize,
    pub remote_additions: usize,
    pub advances: usize,
    pub identical: usize,
    pub metadata_repairs: usize,
    pub files: Vec<FileChange>,
    pub blockers: Vec<Blocker>,
    pub notes: Vec<String>,
}

impl PlanReport {
    pub fn print(&self, format: OutputFormat) -> Result<()> {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(self)?),
            OutputFormat::Diff => {
                bail!("diff output requires a prepared synchronization plan")
            }
            OutputFormat::Human => {
                println!(
                    "agent: {}; peer: {}; resources: {}",
                    self.agent,
                    self.peer,
                    self.resources.join(",")
                );
                if let Some(strategy) = self.conflict_strategy {
                    println!("conflict strategy: {strategy}");
                }
                println!(
                    "local additions: {}; remote additions: {}; advances: {}; identical: {}",
                    self.local_additions, self.remote_additions, self.advances, self.identical
                );
                if self.metadata_repairs > 0 {
                    println!("metadata repairs: {}", self.metadata_repairs);
                }
                for note in &self.notes {
                    println!("note: {note}");
                }
                if !self.files.is_empty() {
                    println!("files:");
                    for file in &self.files {
                        println!(
                            "  [{}] {}: local={}, remote={}, result={}",
                            file.resource, file.path, file.local, file.remote, file.resolution
                        );
                    }
                }
                for blocker in &self.blockers {
                    println!(
                        "BLOCKED [{}] {}: {}",
                        blocker.resource, blocker.path, blocker.reason
                    );
                }
                println!(
                    "mode: {}",
                    if self.blockers.is_empty() {
                        "ready"
                    } else {
                        "blocked"
                    }
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PlannedFile {
    sha256: String,
    modified_ns: Option<u128>,
}

fn planned_files(
    root: &Path,
    exclude: impl Fn(&Path) -> bool + Copy,
) -> Result<BTreeMap<String, PlannedFile>> {
    let mut files = BTreeMap::new();
    for (path, sha256) in manifest(root, exclude)? {
        let absolute = root.join(&path);
        let modified_ns = fs::metadata(&absolute)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos());
        files.insert(
            path,
            PlannedFile {
                sha256,
                modified_ns,
            },
        );
    }
    Ok(files)
}

fn planned_action(
    current: Option<&PlannedFile>,
    result: Option<&PlannedFile>,
    track_metadata: bool,
) -> FileAction {
    match (current, result) {
        (None, Some(_)) => FileAction::Create,
        (Some(_), None) => FileAction::Remove,
        (Some(current), Some(result)) if current.sha256 != result.sha256 => FileAction::Replace,
        (Some(current), Some(result))
            if track_metadata && current.modified_ns != result.modified_ns =>
        {
            FileAction::Metadata
        }
        _ => FileAction::Unchanged,
    }
}

fn resource_name(path: &str) -> &'static str {
    if path.starts_with("memories/") || path.contains("/memory/") {
        "memory"
    } else {
        "sessions"
    }
}

pub fn planned_file_changes(
    local: &Path,
    remote: &Path,
    result: &Path,
    exclude: impl Fn(&Path) -> bool + Copy,
) -> Result<Vec<FileChange>> {
    let local = planned_files(local, exclude)?;
    let remote = planned_files(remote, exclude)?;
    let result = planned_files(result, exclude)?;
    let paths = local
        .keys()
        .chain(remote.keys())
        .chain(result.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut changes = Vec::new();
    for path in paths {
        let local_file = local.get(&path);
        let remote_file = remote.get(&path);
        let result_file = result.get(&path);
        let track_metadata = Path::new(&path)
            .extension()
            .and_then(|value| value.to_str())
            == Some("jsonl");
        let local_action = planned_action(local_file, result_file, track_metadata);
        let remote_action = planned_action(remote_file, result_file, track_metadata);
        if local_action == FileAction::Unchanged && remote_action == FileAction::Unchanged {
            continue;
        }
        let result_hash = result_file.map(|file| &file.sha256);
        let resolution = if result_hash.is_some()
            && result_hash == local_file.map(|file| &file.sha256)
            && result_hash == remote_file.map(|file| &file.sha256)
        {
            "identical"
        } else if result_hash.is_some() && result_hash == local_file.map(|file| &file.sha256) {
            "local"
        } else if result_hash.is_some() && result_hash == remote_file.map(|file| &file.sha256) {
            "remote"
        } else if local_file.is_none() && remote_file.is_none() {
            "generated"
        } else if result_file.is_none() {
            "removed"
        } else {
            "merged"
        };
        changes.push(FileChange {
            resource: resource_name(&path).to_owned(),
            path,
            local: local_action,
            remote: remote_action,
            resolution: resolution.to_owned(),
            local_sha256: local_file.map(|file| file.sha256.clone()),
            remote_sha256: remote_file.map(|file| file.sha256.clone()),
            result_sha256: result_file.map(|file| file.sha256.clone()),
        });
    }
    Ok(changes)
}

fn render_file_diff(
    side: &str,
    path: &str,
    current: Option<&Path>,
    result: Option<&Path>,
) -> Result<String> {
    let current_bytes = current.map(fs::read).transpose()?.unwrap_or_default();
    let result_bytes = result.map(fs::read).transpose()?.unwrap_or_default();
    let current_label = if current.is_some() {
        format!("a/{side}/{path}")
    } else {
        "/dev/null".to_owned()
    };
    let result_label = if result.is_some() {
        format!("b/result/{path}")
    } else {
        "/dev/null".to_owned()
    };
    let output = match (
        std::str::from_utf8(&current_bytes),
        std::str::from_utf8(&result_bytes),
    ) {
        (Ok(current), Ok(result)) => {
            let diff = similar::TextDiff::from_lines(current, result);
            diff.unified_diff()
                .context_radius(3)
                .header(&current_label, &result_label)
                .to_string()
        }
        _ => {
            format!(
                "--- {current_label}\n+++ {result_label}\nBinary files differ: old={}, new={}\n",
                hex::encode(Sha256::digest(&current_bytes)),
                hex::encode(Sha256::digest(&result_bytes))
            )
        }
    };
    Ok(output)
}

pub fn render_planned_diff(
    local: &Path,
    remote: &Path,
    result: &Path,
    exclude: impl Fn(&Path) -> bool + Copy,
) -> Result<String> {
    let changes = planned_file_changes(local, remote, result, exclude)?;
    let mut content_diffs = 0;
    let mut output = String::new();
    for change in changes {
        for (side, root, action) in [
            ("local", local, change.local),
            ("remote", remote, change.remote),
        ] {
            match action {
                FileAction::Create | FileAction::Replace | FileAction::Remove => {
                    let current = root.join(&change.path);
                    let final_path = result.join(&change.path);
                    output.push_str(&render_file_diff(
                        side,
                        &change.path,
                        current.exists().then_some(current.as_path()),
                        final_path.exists().then_some(final_path.as_path()),
                    )?);
                    content_diffs += 1;
                }
                FileAction::Metadata => {
                    output.push_str(&format!("# metadata-only: {side}/{}\n", change.path));
                }
                FileAction::Unchanged => {}
            }
        }
    }
    if content_diffs == 0 {
        output.push_str("# no file content differences\n");
    }
    Ok(output)
}

pub fn print_planned_diff(
    local: &Path,
    remote: &Path,
    result: &Path,
    exclude: impl Fn(&Path) -> bool + Copy,
) -> Result<()> {
    print!("{}", render_planned_diff(local, remote, result, exclude)?);
    Ok(())
}

pub fn sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub fn bytes_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn safe_relative(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("unsafe relative path: {}", path.display());
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("unsafe relative path: {}", path.display());
        }
    }
    Ok(())
}

pub fn private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn manifest(root: &Path, exclude: impl Fn(&Path) -> bool) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    if !root.exists() {
        return Ok(result);
    }
    for entry in WalkDir::new(root).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        let path = entry.path();
        let relative = path.strip_prefix(root)?;
        if relative.as_os_str().is_empty() || exclude(relative) {
            continue;
        }
        if entry.file_type().is_symlink() {
            bail!("symlink is not allowed: {}", path.display());
        }
        if entry.file_type().is_file() {
            result.insert(relative.to_string_lossy().into_owned(), sha256(path)?);
        }
    }
    Ok(result)
}

pub fn fingerprint(root: &Path, exclude: impl Fn(&Path) -> bool) -> Result<String> {
    let mut digest = Sha256::new();
    for (path, hash) in manifest(root, exclude)? {
        digest.update(path);
        digest.update([0]);
        digest.update(hash);
        digest.update([0]);
    }
    Ok(hex::encode(digest.finalize()))
}

pub fn stamp() -> String {
    Utc::now().format("%Y%m%d-%H%M%S-%6f").to_string()
}

pub fn cache_path(options: &SyncOptions, agent: &str, peer: &str) -> Result<PathBuf> {
    let base = options
        .cache_dir
        .clone()
        .or_else(|| dirs::cache_dir().map(|path| path.join("agent-sync")))
        .context("cannot determine cache directory; pass --cache-dir")?;
    let safe_peer: String = peer
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "_.-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    let resource = match options.resources {
        ResourceSelection::All => "all",
        ResourceSelection::Sessions => "sessions",
        ResourceSelection::Memory => "memory",
    };
    let path = base.join(agent).join(safe_peer).join(resource);
    private_dir(&path)?;
    Ok(path)
}

pub fn copy_file_atomic(source: &Path, destination: &Path) -> Result<()> {
    let parent = destination.parent().context("destination has no parent")?;
    private_dir(parent)?;
    let temp = destination.with_file_name(format!(
        ".{}.agent-sync-{}",
        destination
            .file_name()
            .unwrap_or_default()
            .to_string_lossy(),
        std::process::id()
    ));
    fs::copy(source, &temp)?;
    fs::rename(&temp, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unsafe_relative_paths() {
        assert!(safe_relative(Path::new("a/b")).is_ok());
        assert!(safe_relative(Path::new("../a")).is_err());
        assert!(safe_relative(Path::new("/a")).is_err());
    }

    #[test]
    fn file_plan_and_diff_cover_both_sides_and_generated_results() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let result = temp.path().join("result");
        for root in [&local, &remote, &result] {
            fs::create_dir_all(root.join("memories")).unwrap();
        }
        fs::write(local.join("memories/local.md"), "local\n").unwrap();
        fs::write(result.join("memories/local.md"), "local\n").unwrap();
        fs::write(remote.join("memories/remote.md"), "remote\n").unwrap();
        fs::write(result.join("memories/remote.md"), "remote\n").unwrap();
        fs::write(result.join("memories/merged.md"), "merged\n").unwrap();

        let changes = planned_file_changes(&local, &remote, &result, |_| false).unwrap();
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().any(|change| {
            change.path == "memories/local.md"
                && change.local == FileAction::Unchanged
                && change.remote == FileAction::Create
                && change.resolution == "local"
        }));
        assert!(changes.iter().any(|change| {
            change.path == "memories/remote.md"
                && change.local == FileAction::Create
                && change.remote == FileAction::Unchanged
                && change.resolution == "remote"
        }));
        assert!(changes.iter().any(|change| {
            change.path == "memories/merged.md" && change.resolution == "generated"
        }));

        let diff = render_planned_diff(&local, &remote, &result, |_| false).unwrap();
        assert!(diff.contains("--- /dev/null"));
        assert!(diff.contains("+++ b/result/memories/local.md"));
        assert!(diff.contains("+++ b/result/memories/remote.md"));
        assert!(diff.contains("+++ b/result/memories/merged.md"));
        assert!(diff.contains("+merged"));
    }
}
