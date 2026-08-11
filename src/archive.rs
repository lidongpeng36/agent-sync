use crate::adapters::{AgentKind, claude, codex, opencode};
use crate::core::{ResourceSelection, private_dir, safe_relative, sha256, stamp};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use tar::{Archive, Builder, EntryType, Header};
use tempfile::{NamedTempFile, TempDir};
use walkdir::WalkDir;

const FORMAT: &str = "agent-sync-portable-archive";
const VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_ENTRY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    version: u32,
    agent: String,
    resources: String,
    created_at: String,
    entries: Vec<ManifestEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestEntry {
    path: String,
    size: u64,
    sha256: String,
    mtime_ns: i64,
}

pub struct ValidatedArchive {
    _temp: TempDir,
    pub agent: AgentKind,
    pub resources: ResourceSelection,
    payload: PathBuf,
    entries: BTreeMap<String, String>,
}

pub struct ImportPlan {
    pub created: usize,
    pub identical: usize,
    pub conflicts: Vec<String>,
}

fn resource_name(resources: ResourceSelection) -> &'static str {
    match resources {
        ResourceSelection::All => "all",
        ResourceSelection::Sessions => "sessions",
        ResourceSelection::Memory => "memory",
    }
}

fn parse_resources(value: &str) -> Result<ResourceSelection> {
    match value {
        "all" => Ok(ResourceSelection::All),
        "sessions" => Ok(ResourceSelection::Sessions),
        "memory" => Ok(ResourceSelection::Memory),
        _ => bail!("unsupported archive resource selection {value:?}"),
    }
}

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    let parent = destination.parent().context("archive path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    fs::copy(source, temporary.path())?;
    private_file(temporary.path())?;
    let metadata = source.metadata()?;
    filetime::set_file_mtime(
        temporary.path(),
        filetime::FileTime::from_last_modification_time(&metadata),
    )?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    Ok(())
}

fn private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn copy_portable_files(
    source: &Path,
    destination: &Path,
    excluded: impl Fn(&Path) -> bool,
) -> Result<()> {
    for entry in WalkDir::new(source).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        let relative = entry.path().strip_prefix(source)?;
        if relative.as_os_str().is_empty() || excluded(relative) {
            continue;
        }
        safe_relative(relative)?;
        if entry.file_type().is_symlink() {
            bail!("symlink is not portable: {}", entry.path().display());
        }
        if entry.file_type().is_file() {
            copy_file(entry.path(), &destination.join(relative))?;
        }
    }
    Ok(())
}

fn build_snapshot(
    kind: AgentKind,
    root: &Path,
    destination: &Path,
    resources: ResourceSelection,
) -> Result<()> {
    private_dir(destination)?;
    match kind {
        AgentKind::Codex => {
            copy_portable_files(root, destination, |path| {
                codex::archive_excluded(path, resources)
            })?;
            codex::validate_archive_snapshot(destination, resources)
        }
        AgentKind::Claude => {
            copy_portable_files(root, destination, |path| {
                claude::archive_excluded(path, resources)
            })?;
            claude::validate_archive_snapshot(destination, resources)
        }
        AgentKind::Opencode => {
            if resources == ResourceSelection::Memory {
                bail!("OpenCode does not expose a separate memory resource");
            }
            opencode::archive_snapshot(root, destination)?;
            opencode::validate_archive_snapshot(destination)
        }
    }
}

fn snapshot_manifest(root: &Path) -> Result<Vec<ManifestEntry>> {
    let mut entries = Vec::new();
    for entry in WalkDir::new(root).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(root)?;
        safe_relative(relative)?;
        entries.push(ManifestEntry {
            path: relative.to_string_lossy().into_owned(),
            size: entry.metadata()?.len(),
            sha256: sha256(entry.path())?,
            mtime_ns: entry
                .metadata()?
                .modified()?
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .try_into()
                .context("file mtime exceeds archive range")?,
        });
    }
    Ok(entries)
}

pub fn export(
    kind: AgentKind,
    root: &Path,
    output: &Path,
    resources: ResourceSelection,
    force: bool,
) -> Result<()> {
    if output.exists() && !force {
        bail!(
            "archive already exists: {}; pass --force to replace it",
            output.display()
        );
    }
    let temp = tempfile::Builder::new()
        .prefix("agent-sync-export-")
        .tempdir()?;
    let payload = temp.path().join("payload");
    build_snapshot(kind, root, &payload, resources)?;
    let manifest = Manifest {
        format: FORMAT.into(),
        version: VERSION,
        agent: kind.to_string(),
        resources: resource_name(resources).into(),
        created_at: Utc::now().to_rfc3339(),
        entries: snapshot_manifest(&payload)?,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;

    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    {
        let encoder = GzEncoder::new(temporary.as_file_mut(), Compression::default());
        let mut builder = Builder::new(encoder);
        let mut header = Header::new_gnu();
        header.set_size(manifest_bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        builder.append_data(&mut header, "manifest.json", manifest_bytes.as_slice())?;
        for entry in &manifest.entries {
            builder.append_path_with_name(
                payload.join(&entry.path),
                Path::new("payload").join(&entry.path),
            )?;
        }
        let encoder = builder.into_inner()?;
        encoder.finish()?;
    }
    temporary.as_file().sync_all()?;
    temporary.persist(output).map_err(|error| error.error)?;
    println!(
        "exported: agent={kind}; resources={}; files={}; sha256={}; output={}",
        resource_name(resources),
        manifest.entries.len(),
        sha256(output)?,
        output.display()
    );
    Ok(())
}

pub fn validate(input: &Path) -> Result<ValidatedArchive> {
    let file = File::open(input).with_context(|| format!("open archive {}", input.display()))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    let temp = tempfile::Builder::new()
        .prefix("agent-sync-import-")
        .tempdir()?;
    let payload = temp.path().join("payload");
    private_dir(&payload)?;
    let mut manifest_bytes = None;
    let mut seen = BTreeSet::new();
    let mut total = 0_u64;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        safe_relative(&path)?;
        if !seen.insert(path.clone()) {
            bail!("duplicate archive entry: {}", path.display());
        }
        if entry.header().entry_type() != EntryType::Regular {
            bail!("archive contains non-regular entry: {}", path.display());
        }
        let size = entry.size();
        if size > MAX_ENTRY_BYTES {
            bail!("archive entry is too large: {}", path.display());
        }
        total = total.checked_add(size).context("archive size overflow")?;
        if total > MAX_TOTAL_BYTES {
            bail!("archive expands beyond the safety limit");
        }
        if path == Path::new("manifest.json") {
            if size > MAX_MANIFEST_BYTES {
                bail!("archive manifest is too large");
            }
            let mut bytes = Vec::with_capacity(size as usize);
            entry.read_to_end(&mut bytes)?;
            manifest_bytes = Some(bytes);
            continue;
        }
        let relative = path
            .strip_prefix("payload")
            .with_context(|| format!("unexpected archive entry: {}", path.display()))?;
        safe_relative(relative)?;
        let destination = payload.join(relative);
        private_dir(
            destination
                .parent()
                .context("archive entry has no parent")?,
        )?;
        entry.unpack(&destination)?;
        private_file(&destination)?;
    }
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes.context("archive omitted manifest.json")?)?;
    if manifest.format != FORMAT || manifest.version != VERSION {
        bail!("unsupported archive format or version");
    }
    let agent = AgentKind::parse(&manifest.agent)
        .with_context(|| format!("unsupported archive agent {:?}", manifest.agent))?;
    let resources = parse_resources(&manifest.resources)?;
    if agent == AgentKind::Opencode && resources == ResourceSelection::Memory {
        bail!("OpenCode archive cannot contain only memory");
    }
    let mut expected = BTreeMap::new();
    for entry in manifest.entries {
        let relative = PathBuf::from(&entry.path);
        safe_relative(&relative)?;
        if expected
            .insert(entry.path.clone(), entry.sha256.clone())
            .is_some()
        {
            bail!("duplicate manifest path: {}", entry.path);
        }
        let path = payload.join(&relative);
        let metadata = path
            .metadata()
            .with_context(|| format!("archive omitted payload/{}", entry.path))?;
        if metadata.len() != entry.size || sha256(&path)? != entry.sha256 {
            bail!("archive checksum mismatch: {}", entry.path);
        }
        if entry.mtime_ns < 0 {
            bail!("archive contains a negative mtime: {}", entry.path);
        }
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_unix_time(
                entry.mtime_ns / 1_000_000_000,
                (entry.mtime_ns % 1_000_000_000) as u32,
            ),
        )?;
    }
    let actual = snapshot_manifest(&payload)?
        .into_iter()
        .map(|entry| (entry.path, entry.sha256))
        .collect::<BTreeMap<_, _>>();
    if actual != expected {
        bail!("archive payload contains unlisted or missing files");
    }
    match agent {
        AgentKind::Codex => {
            if expected
                .keys()
                .any(|path| codex::archive_excluded(Path::new(path), resources))
            {
                bail!("archive contains unsupported Codex paths");
            }
            codex::validate_archive_snapshot(&payload, resources)?;
        }
        AgentKind::Claude => {
            if expected
                .keys()
                .any(|path| claude::archive_excluded(Path::new(path), resources))
            {
                bail!("archive contains unsupported Claude paths");
            }
            claude::validate_archive_snapshot(&payload, resources)?;
        }
        AgentKind::Opencode => opencode::validate_archive_snapshot(&payload)?,
    }
    Ok(ValidatedArchive {
        _temp: temp,
        agent,
        resources,
        payload,
        entries: expected,
    })
}

pub fn plan_import(archive: &ValidatedArchive, root: &Path) -> Result<ImportPlan> {
    let current = match archive.agent {
        AgentKind::Opencode => opencode::current_session_hashes(root)?,
        _ => archive
            .entries
            .keys()
            .filter_map(|relative| {
                let path = root.join(relative);
                path.is_file()
                    .then(|| Ok((relative.clone(), sha256(&path)?)))
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
    };
    let incoming = match archive.agent {
        AgentKind::Opencode => opencode::archive_session_hashes(&archive.payload)?,
        _ => archive.entries.clone(),
    };
    let mut plan = ImportPlan {
        created: 0,
        identical: 0,
        conflicts: Vec::new(),
    };
    for (path, hash) in incoming {
        match current.get(&path) {
            None => plan.created += 1,
            Some(current) if current == &hash => plan.identical += 1,
            Some(_) => plan.conflicts.push(path),
        }
    }
    Ok(plan)
}

fn apply_files(archive: &ValidatedArchive, root: &Path, force: bool) -> Result<()> {
    for (relative, expected_hash) in &archive.entries {
        let source = archive.payload.join(relative);
        let destination = root.join(relative);
        if destination.is_file() && sha256(&destination)? != *expected_hash && !force {
            bail!("refusing to overwrite differing file: {relative}");
        }
        if !destination.is_file() || sha256(&destination)? != *expected_hash {
            copy_file(&source, &destination)?;
        }
    }
    Ok(())
}

pub fn apply_import(archive: &ValidatedArchive, root: &Path, force: bool) -> Result<PathBuf> {
    let plan = plan_import(archive, root)?;
    if !plan.conflicts.is_empty() && !force {
        bail!(
            "{} existing items differ; inspect the preview and pass --force to overwrite",
            plan.conflicts.len()
        );
    }
    match archive.agent {
        AgentKind::Codex if !codex::local_active_writer_ids(root)?.is_empty() => {
            bail!("refusing import while Codex sessions are active")
        }
        AgentKind::Claude if claude::local_has_writers(root)? => {
            bail!("refusing import while Claude files are open")
        }
        AgentKind::Opencode if opencode::archive_has_writers(root)? => {
            bail!("refusing import while OpenCode is writing its database")
        }
        _ => {}
    }
    let archive_stamp = stamp();
    let backup = match archive.agent {
        AgentKind::Codex => codex::archive_backup(root, archive.resources, &archive_stamp)?,
        AgentKind::Claude => claude::archive_backup(root, archive.resources, &archive_stamp)?,
        AgentKind::Opencode => opencode::archive_backup(root, &archive_stamp)?,
    };
    match archive.agent {
        AgentKind::Codex | AgentKind::Claude => apply_files(archive, root, force)?,
        AgentKind::Opencode => opencode::apply_archive_snapshot(&archive.payload)?,
    }
    match archive.agent {
        AgentKind::Codex => codex::validate_archive_snapshot(root, archive.resources)?,
        AgentKind::Claude => claude::validate_archive_snapshot(root, archive.resources)?,
        AgentKind::Opencode => {}
    }
    let verified = plan_import(archive, root)?;
    if !verified.conflicts.is_empty() || verified.created != 0 {
        bail!("import verification failed after apply");
    }
    println!(
        "imported and verified: agent={}; files={}; backup={}",
        archive.agent,
        archive.entries.len(),
        backup.display()
    );
    Ok(backup)
}

pub fn print_plan(input: &Path, archive: &ValidatedArchive, plan: &ImportPlan) -> Result<()> {
    println!(
        "archive: {}; sha256={}; agent={}; resources={}",
        input.display(),
        sha256(input)?,
        archive.agent,
        resource_name(archive.resources)
    );
    println!(
        "validated: files={}; create={}; identical={}; conflicts={}",
        archive.entries.len(),
        plan.created,
        plan.identical,
        plan.conflicts.len()
    );
    for path in &plan.conflicts {
        println!("  ! {path}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_memory_round_trip_uses_one_validated_file() {
        let source = tempfile::tempdir().unwrap();
        private_dir(&source.path().join("memories")).unwrap();
        fs::write(
            source.path().join("memories/MEMORY.md"),
            "# durable memory\n",
        )
        .unwrap();
        fs::write(source.path().join("auth.json"), "secret").unwrap();
        let output_dir = tempfile::tempdir().unwrap();
        let archive_path = output_dir.path().join("codex.agent-sync.tar.gz");

        export(
            AgentKind::Codex,
            source.path(),
            &archive_path,
            ResourceSelection::Memory,
            false,
        )
        .unwrap();
        assert!(archive_path.is_file());
        assert!(
            export(
                AgentKind::Codex,
                source.path(),
                &archive_path,
                ResourceSelection::Memory,
                false,
            )
            .is_err()
        );
        export(
            AgentKind::Codex,
            source.path(),
            &archive_path,
            ResourceSelection::Memory,
            true,
        )
        .unwrap();

        let archive = validate(&archive_path).unwrap();
        assert_eq!(archive.agent, AgentKind::Codex);
        assert_eq!(archive.resources, ResourceSelection::Memory);
        assert!(archive.entries.contains_key("memories/MEMORY.md"));
        assert!(!archive.entries.contains_key("auth.json"));

        let target = tempfile::tempdir().unwrap();
        let plan = plan_import(&archive, target.path()).unwrap();
        assert_eq!(plan.created, 1);
        assert!(plan.conflicts.is_empty());
        let backup = apply_import(&archive, target.path(), false).unwrap();
        assert!(backup.is_file());
        assert_eq!(
            fs::read_to_string(target.path().join("memories/MEMORY.md")).unwrap(),
            "# durable memory\n"
        );
        let verified = plan_import(&archive, target.path()).unwrap();
        assert_eq!(verified.identical, 1);
        assert_eq!(verified.created, 0);
        assert!(verified.conflicts.is_empty());

        fs::write(target.path().join("memories/MEMORY.md"), "different\n").unwrap();
        let conflict = plan_import(&archive, target.path()).unwrap();
        assert_eq!(conflict.conflicts, vec!["memories/MEMORY.md"]);
        assert!(apply_import(&archive, target.path(), false).is_err());
        apply_import(&archive, target.path(), true).unwrap();
        assert_eq!(
            fs::read_to_string(target.path().join("memories/MEMORY.md")).unwrap(),
            "# durable memory\n"
        );
    }

    #[test]
    fn truncated_archive_fails_validation() {
        let source = tempfile::tempdir().unwrap();
        private_dir(&source.path().join("memories")).unwrap();
        fs::write(source.path().join("memories/MEMORY.md"), "memory\n").unwrap();
        let output_dir = tempfile::tempdir().unwrap();
        let archive_path = output_dir.path().join("archive.tar.gz");
        export(
            AgentKind::Codex,
            source.path(),
            &archive_path,
            ResourceSelection::Memory,
            false,
        )
        .unwrap();
        let size = archive_path.metadata().unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&archive_path)
            .unwrap()
            .set_len(size / 2)
            .unwrap();
        assert!(validate(&archive_path).is_err());
    }
}
