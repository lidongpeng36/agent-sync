use crate::core::{Inventory, ResourceSelection, SyncOptions, private_dir};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

const CHECKPOINT_VERSION: u32 = 1;
const TRANSACTION_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    pub version: u32,
    pub sync_id: String,
    pub agent: String,
    pub resources: ResourceSelection,
    pub peer: String,
    pub completed_at_ms: i64,
    pub result_content_hash: String,
    pub inventory: Inventory,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionPhase {
    Prepared,
    LocalApplied,
    RemoteApplied,
    Verified,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransactionJournal {
    pub version: u32,
    pub transaction_id: String,
    pub agent: String,
    pub resources: ResourceSelection,
    pub local_node_id: String,
    pub remote_node_id: String,
    pub local_generation: String,
    pub remote_generation: String,
    pub result_content_hash: String,
    pub local_backup: String,
    pub remote_backup: String,
    pub started_at_ms: i64,
    pub phase: TransactionPhase,
}

impl TransactionJournal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        agent: &str,
        resources: ResourceSelection,
        local_node_id: &str,
        remote_node_id: &str,
        local_generation: &str,
        remote_generation: &str,
        result_content_hash: &str,
        local_backup: &Path,
        remote_backup: &str,
    ) -> Self {
        let started_at_ms = Utc::now().timestamp_millis();
        let mut digest = Sha256::new();
        for value in [
            agent,
            local_node_id,
            remote_node_id,
            local_generation,
            remote_generation,
            result_content_hash,
        ] {
            digest.update(value);
            digest.update([0]);
        }
        digest.update(started_at_ms.to_le_bytes());
        Self {
            version: TRANSACTION_VERSION,
            transaction_id: hex::encode(digest.finalize()),
            agent: agent.to_owned(),
            resources,
            local_node_id: local_node_id.to_owned(),
            remote_node_id: remote_node_id.to_owned(),
            local_generation: local_generation.to_owned(),
            remote_generation: remote_generation.to_owned(),
            result_content_hash: result_content_hash.to_owned(),
            local_backup: local_backup.display().to_string(),
            remote_backup: remote_backup.to_owned(),
            started_at_ms,
            phase: TransactionPhase::Prepared,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != TRANSACTION_VERSION
            || self.transaction_id.len() != 64
            || self.agent.is_empty()
            || self.local_node_id.is_empty()
            || self.remote_node_id.is_empty()
            || self.result_content_hash.len() != 64
        {
            bail!("transaction journal is invalid");
        }
        Ok(())
    }
}

impl Checkpoint {
    pub fn new(
        agent: &str,
        resources: ResourceSelection,
        peer: &str,
        inventory: Inventory,
    ) -> Self {
        let completed_at_ms = Utc::now().timestamp_millis();
        let result_content_hash = content_hash(&inventory);
        let mut digest = Sha256::new();
        digest.update(agent);
        digest.update([0]);
        digest.update(peer);
        digest.update([0]);
        digest.update(completed_at_ms.to_le_bytes());
        digest.update([0]);
        digest.update(&result_content_hash);
        Self {
            version: CHECKPOINT_VERSION,
            sync_id: hex::encode(digest.finalize()),
            agent: agent.to_owned(),
            resources,
            peer: peer.to_owned(),
            completed_at_ms,
            result_content_hash,
            inventory,
        }
    }

    pub fn validate(&self, agent: &str, resources: ResourceSelection, peer: &str) -> Result<()> {
        if self.version != CHECKPOINT_VERSION
            || self.agent != agent
            || self.resources != resources
            || self.peer != peer
            || self.result_content_hash != content_hash(&self.inventory)
        {
            bail!("checkpoint identity or checksum is invalid");
        }
        Ok(())
    }
}

pub fn content_hash(inventory: &Inventory) -> String {
    let mut digest = Sha256::new();
    for entry in &inventory.entries {
        digest.update(&entry.path);
        digest.update([0]);
        digest.update(&entry.sha256);
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

pub fn state_root(options: &SyncOptions) -> Result<PathBuf> {
    options
        .cache_dir
        .clone()
        .or_else(|| dirs::cache_dir().map(|path| path.join("agent-sync")))
        .map(|path| path.join("state"))
        .context("cannot determine state directory; pass --cache-dir")
}

pub fn default_state_root() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|path| path.join("agent-sync/state"))
        .context("cannot determine remote state directory")
}

pub fn node_id(root: &Path) -> Result<String> {
    let path = root.join("node-id");
    if let Ok(value) = fs::read_to_string(&path) {
        let value = value.trim();
        if value.len() == 64 && value.chars().all(|character| character.is_ascii_hexdigit()) {
            return Ok(value.to_owned());
        }
    }
    private_dir(root)?;
    let mut digest = Sha256::new();
    digest.update(
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_le_bytes(),
    );
    digest.update(std::process::id().to_le_bytes());
    digest.update(std::env::current_exe()?.to_string_lossy().as_bytes());
    let value = hex::encode(digest.finalize());
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    writeln!(temporary, "{value}")?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(&path) {
        Ok(_) => Ok(value),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(fs::read_to_string(path)?.trim().to_owned())
        }
        Err(error) => Err(error.error.into()),
    }
}

pub struct SyncLock {
    _file: File,
}

pub fn acquire_sync_lock(
    root: &Path,
    agent: &str,
    _resources: ResourceSelection,
) -> Result<SyncLock> {
    let directory = root.join("locks").join(safe_component(agent)?);
    private_dir(&directory)?;
    let path = directory.join("sync.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    let started = Instant::now();
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(SyncLock { _file: file }),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if started.elapsed() >= Duration::from_secs(30) {
                    bail!("timed out waiting for sync lock {}", path.display());
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub fn load(
    root: &Path,
    agent: &str,
    peer: &str,
    resources: ResourceSelection,
) -> Result<Option<Checkpoint>> {
    let path = checkpoint_path(root, agent, peer, resources)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let checkpoint: Checkpoint = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode checkpoint {}", path.display()))?;
    checkpoint.validate(agent, resources, peer)?;
    Ok(Some(checkpoint))
}

pub fn save(root: &Path, checkpoint: &Checkpoint) -> Result<()> {
    checkpoint.validate(&checkpoint.agent, checkpoint.resources, &checkpoint.peer)?;
    let path = checkpoint_path(
        root,
        &checkpoint.agent,
        &checkpoint.peer,
        checkpoint.resources,
    )?;
    let parent = path.parent().context("checkpoint has no parent")?;
    private_dir(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut temporary, checkpoint)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(&path)
        .map_err(|error| error.error)
        .with_context(|| format!("install checkpoint {}", path.display()))?;
    Ok(())
}

pub fn load_transaction(root: &Path, agent: &str) -> Result<Option<TransactionJournal>> {
    let path = transaction_path(root, agent)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let journal: TransactionJournal = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode transaction journal {}", path.display()))?;
    journal.validate()?;
    if journal.agent != agent {
        bail!("transaction journal agent mismatch");
    }
    Ok(Some(journal))
}

pub fn save_transaction(root: &Path, journal: &TransactionJournal) -> Result<()> {
    journal.validate()?;
    let path = transaction_path(root, &journal.agent)?;
    let parent = path.parent().context("transaction journal has no parent")?;
    private_dir(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut temporary, journal)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(&path)
        .map_err(|error| error.error)
        .with_context(|| format!("install transaction journal {}", path.display()))?;
    Ok(())
}

pub fn clear_transaction(root: &Path, agent: &str, transaction_id: &str) -> Result<()> {
    let Some(current) = load_transaction(root, agent)? else {
        return Ok(());
    };
    if current.transaction_id != transaction_id {
        bail!("refusing to clear a different active transaction");
    }
    fs::remove_file(transaction_path(root, agent)?)?;
    Ok(())
}

fn transaction_path(root: &Path, agent: &str) -> Result<PathBuf> {
    Ok(root
        .join("transactions")
        .join(safe_component(agent)?)
        .join("current.json"))
}

fn checkpoint_path(
    root: &Path,
    agent: &str,
    peer: &str,
    resources: ResourceSelection,
) -> Result<PathBuf> {
    let agent = safe_component(agent)?;
    let peer = safe_component(peer)?;
    let resource = match resources {
        ResourceSelection::All => "all",
        ResourceSelection::Sessions => "sessions",
        ResourceSelection::Memory => "memory",
    };
    Ok(root.join(agent).join(peer).join(format!("{resource}.json")))
}

fn safe_component(value: &str) -> Result<String> {
    if value.is_empty() {
        bail!("empty checkpoint identity");
    }
    Ok(value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || "_.-".contains(character) {
                character
            } else {
                '_'
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::InventoryEntry;

    #[test]
    fn checkpoint_round_trip_is_scoped_to_peer_and_resource() {
        let temp = tempfile::tempdir().unwrap();
        let inventory = Inventory {
            generation: "generation".into(),
            entries: vec![InventoryEntry {
                path: "sessions/a.jsonl".into(),
                sha256: "content".into(),
                size: 12,
                modified_ns: 34,
            }],
            reused_entries: 0,
        };
        let checkpoint = Checkpoint::new("codex", ResourceSelection::Sessions, "mini", inventory);
        save(temp.path(), &checkpoint).unwrap();
        let loaded = load(temp.path(), "codex", "mini", ResourceSelection::Sessions)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.sync_id, checkpoint.sync_id);
        assert_eq!(loaded.result_content_hash, checkpoint.result_content_hash);
        assert!(
            load(temp.path(), "codex", "other", ResourceSelection::Sessions)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn transaction_journal_requires_matching_id_to_clear() {
        let temp = tempfile::tempdir().unwrap();
        let journal = TransactionJournal::new(
            "codex",
            ResourceSelection::Memory,
            "local",
            "remote",
            "local-generation",
            "remote-generation",
            &"a".repeat(64),
            Path::new("/tmp/local-backup"),
            "/tmp/remote-backup",
        );
        save_transaction(temp.path(), &journal).unwrap();
        assert_eq!(
            load_transaction(temp.path(), "codex")
                .unwrap()
                .unwrap()
                .phase,
            TransactionPhase::Prepared
        );
        assert!(clear_transaction(temp.path(), "codex", "wrong").is_err());
        clear_transaction(temp.path(), "codex", &journal.transaction_id).unwrap();
        assert!(load_transaction(temp.path(), "codex").unwrap().is_none());
    }
}
