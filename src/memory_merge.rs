//! Durable, paired Codex Markdown merge bases. These are not scan hash caches.
use crate::core::{ResourceSelection, bytes_sha256, private_dir, safe_relative};
use crate::state::{TransactionJournal, TransactionPhase};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use similar::{Algorithm, DiffTag, capture_diff_slices};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Endpoint {
    pub node: String,
    pub root: String,
}

impl Endpoint {
    pub fn new(node: String, root: &Path) -> Result<Self> {
        Ok(Self {
            node,
            root: fs::canonicalize(root)?
                .to_str()
                .context("non-UTF-8 Codex root")?
                .to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub endpoints: [Endpoint; 2],
    pub resources: ResourceSelection,
}

impl Scope {
    pub fn new(a: Endpoint, b: Endpoint, resources: ResourceSelection) -> Result<Self> {
        let mut endpoints = [a, b];
        endpoints.sort();
        let scope = Self {
            endpoints,
            resources,
        };
        scope.validate()?;
        Ok(scope)
    }

    fn validate(&self) -> Result<()> {
        if !self.resources.memory() || self.endpoints[0] >= self.endpoints[1] {
            bail!("invalid memory baseline scope");
        }
        for endpoint in &self.endpoints {
            if endpoint.node.len() != 64
                || !endpoint.node.bytes().all(|b| b.is_ascii_hexdigit())
                || !Path::new(&endpoint.root).is_absolute()
            {
                bail!("invalid memory baseline endpoint");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    version: u32,
    pub scope: Scope,
    pub transaction_id: String,
    pub files: BTreeMap<String, String>,
    pub excluded_ids: BTreeSet<String>,
    checksum: String,
}

#[derive(Serialize, Deserialize)]
pub struct View {
    pub scope: Scope,
    pub baseline: Option<Baseline>,
}

pub fn eligible(path: &Path) -> bool {
    safe_relative(path).is_ok()
        && path
            .components()
            .next()
            .is_some_and(|p| p.as_os_str() == "memories")
        && path.extension().is_some_and(|ext| ext == "md")
        && !crate::adapters::codex::archive_excluded(path, ResourceSelection::Memory)
}

impl Baseline {
    #[cfg(test)]
    pub fn capture(scope: Scope, transaction_id: String, root: &Path) -> Result<Self> {
        Self::capture_excluding(scope, transaction_id, root, &BTreeSet::new())
    }

    pub fn capture_excluding(
        scope: Scope,
        transaction_id: String,
        root: &Path,
        excluded_ids: &BTreeSet<String>,
    ) -> Result<Self> {
        let mut files = BTreeMap::new();
        for entry in walkdir::WalkDir::new(root.join("memories")).follow_links(false) {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error)
                    if error
                        .io_error()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let relative = entry.path().strip_prefix(root)?;
            if entry.file_type().is_file()
                && eligible(relative)
                && !excluded_ids
                    .iter()
                    .any(|id| relative.to_string_lossy().contains(id))
            {
                // Binary/non-UTF-8 Markdown remains an explicit conflict.
                if let Ok(text) = String::from_utf8(fs::read(entry.path())?)
                    && !text.contains('\0')
                {
                    files.insert(
                        relative
                            .to_str()
                            .context("non-UTF-8 memory path")?
                            .to_owned(),
                        text,
                    );
                }
            }
        }
        let mut value = Self {
            version: 1,
            scope,
            transaction_id,
            files,
            excluded_ids: excluded_ids.clone(),
            checksum: String::new(),
        };
        value.checksum = value.digest()?;
        value.validate()?;
        Ok(value)
    }

    fn digest(&self) -> Result<String> {
        Ok(bytes_sha256(&serde_json::to_vec(&(
            self.version,
            &self.scope,
            &self.transaction_id,
            &self.files,
            &self.excluded_ids,
        ))?))
    }

    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self
            .excluded_ids
            .iter()
            .any(|id| uuid::Uuid::parse_str(id).is_err())
            || self
                .files
                .keys()
                .any(|p| self.excluded_ids.iter().any(|id| p.contains(id)))
            || self.version != 1
            || self.transaction_id.len() != 64
            || !self.transaction_id.bytes().all(|b| b.is_ascii_hexdigit())
            || self.files.keys().any(|p| !eligible(Path::new(p)))
            || self.checksum != self.digest()?
        {
            bail!("invalid memory baseline identity, paths or checksum");
        }
        Ok(())
    }
}

pub fn storage_root() -> Result<PathBuf> {
    dirs::data_local_dir()
        .map(|p| p.join("agent-sync/memory-baselines"))
        .context("cannot determine durable memory baseline directory")
}

fn path(root: &Path, scope: &Scope) -> Result<PathBuf> {
    scope.validate()?;
    Ok(root.join(format!(
        "{}.json",
        bytes_sha256(&serde_json::to_vec(scope)?)
    )))
}

pub fn load(root: &Path, scope: &Scope) -> Result<Option<Baseline>> {
    let bytes = match fs::read(path(root, scope)?) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let Ok(value) = serde_json::from_slice::<Baseline>(&bytes) else {
        return Ok(None);
    };
    if value.scope != *scope || value.validate().is_err() {
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn common(local: Option<&Baseline>, remote: Option<&Baseline>) -> Option<Baseline> {
    match (local, remote) {
        (Some(a), Some(b)) if a == b && a.validate().is_ok() => Some(a.clone()),
        _ => None,
    }
}

/// Publishing a base is legal only after full two-endpoint verification.
pub fn save_verified(root: &Path, value: &Baseline, journal: &TransactionJournal) -> Result<()> {
    value.validate()?;
    journal.validate()?;
    let mut nodes = [
        journal.local_node_id.as_str(),
        journal.remote_node_id.as_str(),
    ];
    nodes.sort();
    let mut scope_nodes = value.scope.endpoints.each_ref().map(|e| e.node.as_str());
    scope_nodes.sort();
    if journal.agent != "codex"
        || journal.phase != TransactionPhase::Verified
        || journal.transaction_id != value.transaction_id
        || journal.resources != value.scope.resources
        || nodes != scope_nodes
    {
        bail!("memory baseline requires its verified Codex transaction");
    }
    private_dir(root)?;
    let mut tmp = tempfile::NamedTempFile::new_in(root)?;
    serde_json::to_writer(&mut tmp, value)?;
    tmp.write_all(b"\n")?;
    tmp.as_file().sync_all()?;
    tmp.persist(path(root, &value.scope)?)
        .map_err(|e| e.error)?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

/// Conservative line-based diff3: accept disjoint edits, reject ambiguous order.
pub fn merge(base: &str, local: &str, remote: &str) -> Option<String> {
    if [base, local, remote].iter().any(|text| text.contains('\0')) {
        return None;
    }
    if local == remote {
        return Some(local.to_owned());
    }
    if local == base {
        return Some(remote.to_owned());
    }
    if remote == base {
        return Some(local.to_owned());
    }
    let original: Vec<_> = base.split_inclusive('\n').collect();
    let mut edits = Vec::new();
    for text in [local, remote] {
        let lines: Vec<_> = text.split_inclusive('\n').collect();
        for op in capture_diff_slices(Algorithm::Myers, &original, &lines) {
            if op.tag() != DiffTag::Equal {
                let range = op.old_range();
                edits.push((range.start, range.end, lines[op.new_range()].concat()));
            }
        }
    }
    edits.sort();
    edits.dedup();
    for pair in edits.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        if b.0 < a.1 || ((a.0 == a.1 || b.0 == b.1) && b.0 <= a.1) {
            return None;
        }
    }
    let mut result = String::new();
    let mut cursor = 0;
    for (start, end, replacement) in edits {
        result.push_str(&original[cursor..start].concat());
        result.push_str(&replacement);
        cursor = end;
    }
    result.push_str(&original[cursor..].concat());
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realistic_memory_scenarios_distinguish_text_conflicts_from_semantic_review() {
        #[derive(Deserialize)]
        struct Scenario {
            name: String,
            description: String,
            base: String,
            local: String,
            remote: String,
            merged: Option<String>,
            #[serde(default)]
            semantic_review_required: bool,
        }
        let scenarios: Vec<Scenario> = serde_json::from_str(include_str!(
            "../tests/fixtures/codex-memory/scenarios.json"
        ))
        .unwrap();
        for case in scenarios {
            let forward = merge(&case.base, &case.local, &case.remote);
            let reverse = merge(&case.base, &case.remote, &case.local);
            assert_eq!(forward, case.merged, "{}: {}", case.name, case.description);
            assert_eq!(reverse, forward, "direction changed result: {}", case.name);
            if let Some(result) = &forward {
                assert_eq!(merge(result, result, result).as_ref(), Some(result));
            }
            // This case documents a limitation, NOT approval of contradictory instructions.
            if case.semantic_review_required {
                let result = forward
                    .as_ref()
                    .expect("text-only merge currently succeeds");
                assert!(result.contains("必须 restart"));
                assert!(result.contains("禁止 restart"));
            }
        }
    }

    fn scope() -> Scope {
        Scope::new(
            Endpoint {
                node: "a".repeat(64),
                root: "/local/codex".into(),
            },
            Endpoint {
                node: "b".repeat(64),
                root: "/remote/codex".into(),
            },
            ResourceSelection::Memory,
        )
        .unwrap()
    }

    #[test]
    fn three_way_preserves_disjoint_edits_in_both_directions() {
        let cases = [
            (
                "a\nb\nc\nd\n",
                "a\nlocal\nb\nc\nd\n",
                "a\nb\nc\nremote\nd\n",
                "a\nlocal\nb\nc\nremote\nd\n",
            ),
            ("a\nb\nc\n", "A\nb\nc\n", "a\nb\nC\n", "A\nb\nC\n"),
            ("a\nb\nc\nd\n", "b\nc\nd\n", "a\nb\nc\nD\n", "b\nc\nD\n"),
            (
                "甲\r\n乙\r\n丙",
                "新甲\r\n乙\r\n丙",
                "甲\r\n乙\r\n新丙",
                "新甲\r\n乙\r\n新丙",
            ),
        ];
        for (base, local, remote, expected) in cases {
            assert_eq!(merge(base, local, remote).as_deref(), Some(expected));
            assert_eq!(merge(base, remote, local).as_deref(), Some(expected));
        }
    }

    #[test]
    fn overlaps_and_ambiguous_insertions_are_conflicts() {
        for (base, local, remote) in [
            ("a\nb\n", "A\nb\n", "other\nb\n"),
            ("a\nb\n", "b\n", "A\nb\n"),
            ("a\n", "a\nlocal\n", "a\nremote\n"),
            ("", "local", "remote"),
            ("a\nb\n", "A\nb\n", "before\na\nb\n"),
        ] {
            assert_eq!(merge(base, local, remote), None);
            assert_eq!(merge(base, remote, local), None);
        }
    }

    #[test]
    fn unilateral_and_identical_edits_converge_without_normalizing_text() {
        for (base, result) in [("a\n", ""), ("a", "a\r\nb"), ("", "新\n")] {
            assert_eq!(merge(base, base, result).as_deref(), Some(result));
            assert_eq!(merge(base, result, base).as_deref(), Some(result));
            assert_eq!(merge(base, result, result).as_deref(), Some(result));
        }
        assert_eq!(
            merge("a\nb\nc\nd\n", "A\nb\nc\nd\n", "A\nb\nc\nD\n").as_deref(),
            Some("A\nb\nc\nD\n")
        );
    }

    fn journal() -> TransactionJournal {
        let mut journal = TransactionJournal::new(
            "codex",
            ResourceSelection::Memory,
            &"a".repeat(64),
            &"b".repeat(64),
            "local",
            "remote",
            &"c".repeat(64),
            Path::new("/backup/local"),
            "/backup/remote",
        );
        journal.phase = TransactionPhase::Verified;
        journal
    }

    #[test]
    fn baseline_is_paired_scoped_checksummed_and_private() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        let store = temp.path().join("store");
        fs::create_dir_all(data.join("memories/.git")).unwrap();
        fs::write(data.join("memories/one.md"), "shared\n").unwrap();
        fs::write(data.join("memories/.git/private.md"), "excluded").unwrap();
        fs::write(data.join("memories/non-text.md"), [0xff]).unwrap();
        fs::write(data.join("memories/settings.json"), "{}").unwrap();
        let journal = journal();
        let base = Baseline::capture(scope(), journal.transaction_id.clone(), &data).unwrap();
        assert_eq!(base.files.len(), 1);
        assert!(load(&store, &scope()).unwrap().is_none());
        assert!(!store.exists()); // A preview/load never establishes a base.
        save_verified(&store, &base, &journal).unwrap();
        assert_eq!(load(&store, &scope()).unwrap(), Some(base.clone()));
        assert_eq!(common(Some(&base), Some(&base)), Some(base.clone()));
        assert!(common(Some(&base), None).is_none());
        let mut other = base.clone();
        other.transaction_id = "d".repeat(64);
        other.checksum = other.digest().unwrap();
        assert!(common(Some(&base), Some(&other)).is_none()); // Interrupted paired publish.
        let swapped = Scope::new(
            scope().endpoints[1].clone(),
            scope().endpoints[0].clone(),
            ResourceSelection::Memory,
        )
        .unwrap();
        assert_eq!(load(&store, &swapped).unwrap(), Some(base.clone()));
        for scoped in [
            Scope::new(
                scope().endpoints[0].clone(),
                scope().endpoints[1].clone(),
                ResourceSelection::All,
            )
            .unwrap(),
            Scope::new(
                scope().endpoints[0].clone(),
                Endpoint {
                    node: "b".repeat(64),
                    root: "/other".into(),
                },
                ResourceSelection::Memory,
            )
            .unwrap(),
            Scope::new(
                scope().endpoints[0].clone(),
                Endpoint {
                    node: "e".repeat(64),
                    root: "/remote/codex".into(),
                },
                ResourceSelection::Memory,
            )
            .unwrap(),
        ] {
            assert!(load(&store, &scoped).unwrap().is_none());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path(&store, &scope()).unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        other
            .files
            .insert("memories/one.md".into(), "tampered".into());
        fs::write(
            path(&store, &scope()).unwrap(),
            serde_json::to_vec(&other).unwrap(),
        )
        .unwrap();
        assert!(load(&store, &scope()).unwrap().is_none());
        fs::write(path(&store, &scope()).unwrap(), "broken JSON").unwrap();
        assert!(load(&store, &scope()).unwrap().is_none());
        other
            .files
            .insert("memories/../../secret.md".into(), "bad".into());
        other.checksum = other.digest().unwrap();
        assert!(other.validate().is_err());
    }

    #[test]
    fn excluded_active_paths_never_enter_a_new_baseline() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("memories")).unwrap();
        let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
        fs::write(temp.path().join(format!("memories/{id}.md")), "active").unwrap();
        fs::write(temp.path().join("memories/stable.md"), "stable").unwrap();
        let baseline = Baseline::capture_excluding(
            scope(),
            journal().transaction_id,
            temp.path(),
            &BTreeSet::from([id.into()]),
        )
        .unwrap();
        assert_eq!(baseline.files.len(), 1);
        assert!(baseline.files.contains_key("memories/stable.md"));
    }

    #[test]
    fn failed_or_unrelated_transaction_cannot_advance_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let store = temp.path().join("store");
        let mut journal = journal();
        let base = Baseline::capture(scope(), journal.transaction_id.clone(), temp.path()).unwrap();
        for phase in [
            TransactionPhase::Prepared,
            TransactionPhase::LocalApplied,
            TransactionPhase::RemoteApplied,
        ] {
            journal.phase = phase;
            assert!(save_verified(&store, &base, &journal).is_err());
            assert!(!store.exists());
        }
        journal.phase = TransactionPhase::Verified;
        journal.transaction_id = "e".repeat(64);
        assert!(save_verified(&store, &base, &journal).is_err());
    }
}
