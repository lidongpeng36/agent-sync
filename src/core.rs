use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Copy)]
pub enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ResourceSelection {
    All,
    Sessions,
    Memory,
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
}

#[derive(Clone, Debug, Serialize)]
pub struct Blocker {
    pub resource: String,
    pub path: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanReport {
    pub agent: String,
    pub peer: String,
    pub resources: Vec<String>,
    pub local_additions: usize,
    pub remote_additions: usize,
    pub advances: usize,
    pub identical: usize,
    pub metadata_repairs: usize,
    pub blockers: Vec<Blocker>,
    pub notes: Vec<String>,
}

impl PlanReport {
    pub fn print(&self, format: OutputFormat) -> Result<()> {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(self)?),
            OutputFormat::Human => {
                println!(
                    "agent: {}; peer: {}; resources: {}",
                    self.agent,
                    self.peer,
                    self.resources.join(",")
                );
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
}
