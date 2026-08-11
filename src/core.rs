use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::ValueEnum;
use fs2::FileExt;
use serde::Deserialize;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write as IoWrite};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use walkdir::WalkDir;

#[derive(Clone, Copy)]
pub enum OutputFormat {
    Human,
    Json,
    Diff,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceSelection {
    All,
    Sessions,
    Memory,
}

pub enum InteractiveChoice<T> {
    Local,
    Remote,
    Edited(T),
}

pub struct EditDocument<'a> {
    pub name: &'a str,
    pub local: &'a [u8],
    pub remote: &'a [u8],
    pub remote_label: &'a str,
}

pub fn file_lock_is_held(file: &File) -> Result<bool> {
    match file.try_lock_exclusive() {
        Ok(()) => {
            FileExt::unlock(file)?;
            Ok(false)
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(true),
        Err(error) => Err(error).context("probe writer lock"),
    }
}

fn conflict_markers(local: &str, remote: &str, remote_label: &str) -> String {
    format!(
        "<<<<<<< LOCAL\n{}{}=======\n{}{}>>>>>>> {remote_label}\n",
        local,
        if local.ends_with('\n') { "" } else { "\n" },
        remote,
        if remote.ends_with('\n') { "" } else { "\n" },
    )
}

fn has_conflict_markers(content: &str) -> bool {
    content.lines().any(|line| {
        line.starts_with("<<<<<<< ") || line == "=======" || line.starts_with(">>>>>>> ")
    })
}

pub fn choose_interactively<T>(
    heading: &str,
    local_label: &str,
    remote_label: &str,
    mut edit: impl FnMut() -> Result<T>,
) -> Result<InteractiveChoice<T>> {
    println!("{heading}");
    println!("  [l] {local_label}");
    println!("  [r] {remote_label}");
    loop {
        print!("choose [l]ocal / [r]emote / [e]dit / [q]uit: ");
        io::stdout().flush()?;
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer)? == 0 {
            bail!("cancelled by user");
        }
        match answer.trim().to_ascii_lowercase().as_str() {
            "l" | "local" => return Ok(InteractiveChoice::Local),
            "r" | "remote" => return Ok(InteractiveChoice::Remote),
            "e" | "edit" => match edit() {
                Ok(value) => return Ok(InteractiveChoice::Edited(value)),
                Err(error) => println!("edit not accepted: {error:#}"),
            },
            "q" | "quit" => bail!("cancelled by user"),
            _ => println!("Please choose l, r, e, or q."),
        }
    }
}

pub fn edit_conflict_documents(
    root: &Path,
    documents: &[EditDocument<'_>],
) -> Result<Vec<Vec<u8>>> {
    private_dir(root)?;
    let mut paths = Vec::new();
    for document in documents {
        let local =
            std::str::from_utf8(document.local).context("local conflict content is not UTF-8")?;
        let remote =
            std::str::from_utf8(document.remote).context("remote conflict content is not UTF-8")?;
        let content = if local == remote {
            local.to_owned()
        } else {
            conflict_markers(local, remote, document.remote_label)
        };
        let path = root.join(document.name);
        fs::write(&path, content)?;
        paths.push(path);
    }
    let editor = std::env::var("VISUAL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "vi".into());
    let mut parts = editor.split_whitespace();
    let program = parts.next().context("editor command is empty")?;
    println!(
        "opening {} with {editor}",
        paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(" and ")
    );
    let status = Command::new(program)
        .args(parts)
        .args(&paths)
        .status()
        .with_context(|| format!("start editor {program}"))?;
    if !status.success() {
        bail!("editor exited with {status}");
    }
    paths
        .iter()
        .map(|path| {
            let bytes = fs::read(path)?;
            let text = std::str::from_utf8(&bytes).context("edited content is not UTF-8")?;
            if has_conflict_markers(text) {
                bail!("conflict markers remain; resolve all <<<<<<<, =======, and >>>>>>> lines");
            }
            Ok(bytes)
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lower")]
pub enum ConflictStrategy {
    Local,
    Remote,
    #[default]
    #[serde(alias = "merge")]
    #[value(alias = "merge")]
    Ask,
}

impl fmt::Display for ConflictStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Ask => "ask",
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct InventoryEntry {
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub modified_ns: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Inventory {
    pub generation: String,
    pub entries: Vec<InventoryEntry>,
    #[serde(default)]
    pub reused_entries: usize,
}

impl Inventory {
    pub fn new(entries: Vec<InventoryEntry>, reused_entries: usize) -> Self {
        let mut digest = Sha256::new();
        for entry in &entries {
            digest.update(&entry.path);
            digest.update([0]);
            digest.update(&entry.sha256);
            digest.update([0]);
            digest.update(entry.size.to_le_bytes());
            digest.update(entry.modified_ns.to_le_bytes());
        }
        Self {
            generation: hex::encode(digest.finalize()),
            entries,
            reused_entries,
        }
    }

    pub fn by_path(&self) -> BTreeMap<&str, &InventoryEntry> {
        self.entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect()
    }

    pub fn content_manifest(&self) -> BTreeMap<&str, &str> {
        self.entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.sha256.as_str()))
            .collect()
    }
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

#[derive(Clone, Copy)]
pub enum PayloadSide {
    Local,
    Remote,
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
    pub display_path: String,
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
    fn render_human(&self) -> String {
        let mut output = String::new();
        writeln!(
            output,
            "agent: {}; peer: {}; resources: {}",
            self.agent,
            self.peer,
            self.resources.join(",")
        )
        .unwrap();
        if let Some(strategy) = self.conflict_strategy {
            writeln!(output, "conflict strategy: {strategy}").unwrap();
        }
        writeln!(
            output,
            "local additions: {}; remote additions: {}; advances: {}; identical: {}",
            self.local_additions, self.remote_additions, self.advances, self.identical
        )
        .unwrap();
        if self.metadata_repairs > 0 {
            writeln!(output, "metadata repairs: {}", self.metadata_repairs).unwrap();
        }
        for note in &self.notes {
            writeln!(output, "note: {note}").unwrap();
        }
        if !self.files.is_empty() {
            let path_width = self
                .files
                .iter()
                .map(|file| UnicodeWidthStr::width(file.display_path.as_str()))
                .max()
                .unwrap_or(24)
                .clamp(24, 68);
            writeln!(output, "files ({}):", self.files.len()).unwrap();
            writeln!(
                output,
                "{}",
                table_row("PATH", "LOCAL", "REMOTE", "RESULT", path_width)
            )
            .unwrap();
            for file in &self.files {
                writeln!(
                    output,
                    "{}",
                    table_row(
                        &file.display_path,
                        action_symbol(file.local),
                        action_symbol(file.remote),
                        resolution_symbol(&file.resolution),
                        path_width,
                    )
                )
                .unwrap();
            }
            writeln!(
                output,
                "  side actions: = unchanged  + create  ↻ update content  ~ metadata only  − remove"
            )
            .unwrap();
            writeln!(
                output,
                "  results: L use local  R use remote  M merged  E edited  = same  ✦ generated  ? choose"
            )
            .unwrap();
            writeln!(
                output,
                "  ↻ means that --apply updates that side to the staged result; it does not mean merge."
            )
            .unwrap();
        }
        if self.blockers.is_empty() {
            writeln!(output, "status: ready").unwrap();
            return output;
        }

        writeln!(output, "action required ({}):", self.blockers.len()).unwrap();
        for blocker in &self.blockers {
            let display_path = self
                .files
                .iter()
                .find(|file| {
                    file.resource == blocker.resource
                        && file.resolution == "unresolved"
                        && blocker
                            .path
                            .rsplit('/')
                            .next()
                            .is_some_and(|target| file.path.ends_with(target))
                })
                .map(|file| file.display_path.as_str())
                .unwrap_or(&blocker.path);
            writeln!(output, "  ? {display_path}").unwrap();
            writeln!(output, "    {}", blocker_explanation(blocker)).unwrap();
        }
        let has_choice = self
            .blockers
            .iter()
            .any(|blocker| blocker.reason.contains("requires a choice"));
        let has_active_writer = self
            .blockers
            .iter()
            .any(|blocker| blocker.reason.contains("active"));
        writeln!(output, "next steps:").unwrap();
        writeln!(
            output,
            "  inspect full content diff: agent-sync sync {} {} -f diff",
            self.agent, self.peer
        )
        .unwrap();
        if has_choice {
            writeln!(
                output,
                "  resolve per conflict:      agent-sync sync {} {} --apply",
                self.agent, self.peer
            )
            .unwrap();
            writeln!(
                output,
                "    choose l (local), r (remote), or e ($EDITOR), then confirm with [Y/n]"
            )
            .unwrap();
            writeln!(
                output,
                "  use local for all:         agent-sync sync {} {} -s local --apply",
                self.agent, self.peer
            )
            .unwrap();
            writeln!(
                output,
                "  use remote for all:        agent-sync sync {} {} -s remote --apply",
                self.agent, self.peer
            )
            .unwrap();
        }
        if has_active_writer {
            writeln!(
                output,
                "  after closing the writer:  agent-sync sync {} {} --apply",
                self.agent, self.peer
            )
            .unwrap();
            if self.resources.iter().any(|resource| resource == "memory") {
                writeln!(
                    output,
                    "  sync memory meanwhile:     agent-sync sync {} {} --only memory --apply",
                    self.agent, self.peer
                )
                .unwrap();
            }
        }
        writeln!(output, "status: action required; no files changed").unwrap();
        output
    }

    pub fn print(&self, format: OutputFormat) -> Result<()> {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(self)?),
            OutputFormat::Diff => {
                bail!("diff output requires a prepared synchronization plan")
            }
            OutputFormat::Human => print!("{}", self.render_human()),
        }
        Ok(())
    }
}

fn action_symbol(action: FileAction) -> &'static str {
    match action {
        FileAction::Unchanged => "=",
        FileAction::Create => "+",
        FileAction::Replace => "↻",
        FileAction::Remove => "−",
        FileAction::Metadata => "~",
    }
}

fn pad_right(value: &str, width: usize) -> String {
    let padding = width.saturating_sub(UnicodeWidthStr::width(value));
    format!("{value}{}", " ".repeat(padding))
}

fn center(value: &str, width: usize) -> String {
    let padding = width.saturating_sub(UnicodeWidthStr::width(value));
    let left = padding / 2;
    format!(
        "{}{}{}",
        " ".repeat(left),
        value,
        " ".repeat(padding - left)
    )
}

fn table_row(path: &str, local: &str, remote: &str, result: &str, path_width: usize) -> String {
    format!(
        "  {}  {}  {}  {}",
        pad_right(path, path_width),
        center(local, 5),
        center(remote, 6),
        center(result, 6),
    )
}

fn resolution_symbol(resolution: &str) -> &'static str {
    match resolution {
        "local" => "L",
        "remote" => "R",
        "merged" => "M",
        "edited" => "E",
        "identical" => "=",
        "generated" => "✦",
        "removed" => "−",
        "unresolved" => "?",
        _ => "?",
    }
}

fn blocker_explanation(blocker: &Blocker) -> &str {
    if blocker.reason.contains("requires a choice") {
        "Automatic merge could not safely combine the memory content or its index."
    } else if blocker.reason.contains("active") {
        "A session is still active; close its writer and run sync again."
    } else {
        &blocker.reason
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

pub fn shorten_middle(value: &str, maximum: usize) -> String {
    if UnicodeWidthStr::width(value) <= maximum {
        return value.to_owned();
    }
    let ellipsis_width = UnicodeWidthChar::width('…').unwrap_or(1);
    let available = maximum.saturating_sub(ellipsis_width);
    let suffix_budget = available / 3;
    let prefix_budget = available.saturating_sub(suffix_budget);
    let mut prefix = String::new();
    let mut prefix_width = 0;
    for character in value.chars() {
        let width = UnicodeWidthChar::width(character).unwrap_or(0);
        if prefix_width + width > prefix_budget {
            break;
        }
        prefix.push(character);
        prefix_width += width;
    }
    let mut suffix = Vec::new();
    let mut suffix_width = 0;
    for character in value.chars().rev() {
        let width = UnicodeWidthChar::width(character).unwrap_or(0);
        if suffix_width + width > suffix_budget {
            break;
        }
        suffix.push(character);
        suffix_width += width;
    }
    suffix.reverse();
    format!("{prefix}…{}", suffix.into_iter().collect::<String>())
}

fn fallback_project_name(slug: &str) -> String {
    slug.trim_matches('-')
        .rsplit('-')
        .find(|part| !part.is_empty())
        .unwrap_or(slug)
        .to_owned()
}

fn session_name(project: &str, session: &str, roots: [&Path; 3]) -> (String, String) {
    let mut project_name = None;
    let mut title = None;
    for root in roots {
        let transcript = root
            .join("projects")
            .join(project)
            .join(format!("{session}.jsonl"));
        let Ok(file) = File::open(transcript) else {
            continue;
        };
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if let Some(cwd) = value.get("cwd").and_then(serde_json::Value::as_str)
                && let Some(name) = Path::new(cwd).file_name().and_then(|name| name.to_str())
            {
                project_name = Some(name.to_owned());
            }
            if let Some(name) = value
                .get("aiTitle")
                .or_else(|| value.get("slug"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.trim().is_empty())
            {
                title = Some(name.to_owned());
            }
        }
        if project_name.is_some() && title.is_some() {
            break;
        }
    }
    (
        project_name.unwrap_or_else(|| fallback_project_name(project)),
        title.unwrap_or_else(|| session.chars().take(8).collect()),
    )
}

fn friendly_path(
    path: &str,
    roots: [&Path; 3],
    sessions: &mut BTreeMap<(String, String), (String, String)>,
) -> String {
    let parts = Path::new(path)
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if parts.first().map(String::as_str) != Some("projects") || parts.len() < 3 {
        return shorten_middle(path, 68);
    }
    let project = &parts[1];
    if parts[2] == "memory" {
        let target = parts
            .get(3)
            .map(|name| name.trim_end_matches(".md"))
            .unwrap_or("memory");
        let target = if target == "MEMORY" { "index" } else { target };
        return shorten_middle(
            &format!("{}/memory/{target}", fallback_project_name(project)),
            68,
        );
    }
    let session = parts[2].trim_end_matches(".jsonl");
    if uuid::Uuid::parse_str(session).is_err() {
        return shorten_middle(path, 68);
    }
    let (project_name, session_title) = sessions
        .entry((project.clone(), session.to_owned()))
        .or_insert_with(|| session_name(project, session, roots));
    let mut display = format!(
        "{}/{}",
        shorten_middle(project_name, 20),
        shorten_middle(session_title, 32)
    );
    if !parts[2].ends_with(".jsonl") {
        match parts.get(3).map(String::as_str) {
            Some("subagents") => {
                let agent_file = parts.get(4);
                let agent = agent_file
                    .map(|name| {
                        name.trim_start_matches("agent-")
                            .split('.')
                            .next()
                            .unwrap_or(name)
                    })
                    .unwrap_or("unknown");
                display.push_str(&format!("/subagent-{}", shorten_middle(agent, 10)));
                if agent_file.is_some_and(|name| name.ends_with(".meta.json")) {
                    display.push_str("/meta");
                }
            }
            Some("tool-results") => {
                let tool = parts.get(4).map(String::as_str).unwrap_or("result");
                display.push_str(&format!("/tool-{}", shorten_middle(tool, 12)));
            }
            Some(other) => display.push_str(&format!("/{}", shorten_middle(other, 14))),
            None => {}
        }
    }
    shorten_middle(&display, 68)
}

pub fn planned_file_changes(
    local: &Path,
    remote: &Path,
    result: &Path,
    exclude: impl Fn(&Path) -> bool + Copy,
) -> Result<Vec<FileChange>> {
    let local_root = local;
    let remote_root = remote;
    let result_root = result;
    let local = planned_files(local_root, exclude)?;
    let remote = planned_files(remote_root, exclude)?;
    let result = planned_files(result_root, exclude)?;
    let paths = local
        .keys()
        .chain(remote.keys())
        .chain(result.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut changes = Vec::new();
    let mut session_names = BTreeMap::new();
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
            display_path: friendly_path(
                &path,
                [result_root, local_root, remote_root],
                &mut session_names,
            ),
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

pub fn inventory(root: &Path, exclude: impl Fn(&Path) -> bool) -> Result<Inventory> {
    inventory_cached(root, exclude, None).map(|(inventory, _)| inventory)
}

pub fn inventory_cached(
    root: &Path,
    exclude: impl Fn(&Path) -> bool,
    previous: Option<&Inventory>,
) -> Result<(Inventory, usize)> {
    let mut entries = Vec::new();
    let previous = previous.map(Inventory::by_path).unwrap_or_default();
    let mut reused = 0;
    if root.exists() {
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
            if !entry.file_type().is_file() {
                continue;
            }
            safe_relative(relative)?;
            let metadata = entry.metadata()?;
            let modified = metadata.modified()?.duration_since(UNIX_EPOCH)?;
            let relative = relative.to_string_lossy().into_owned();
            let size = metadata.len();
            let modified_ns = modified.as_nanos() as i64;
            let sha256 = if let Some(previous) = previous
                .get(relative.as_str())
                .filter(|entry| entry.size == size && entry.modified_ns == modified_ns)
            {
                reused += 1;
                previous.sha256.clone()
            } else {
                sha256(path)?
            };
            entries.push(InventoryEntry {
                path: relative,
                sha256,
                size,
                modified_ns,
            });
        }
    }
    Ok((Inventory::new(entries, reused), reused))
}

pub fn inventory_transfer_paths(local: &Inventory, remote: &Inventory) -> Vec<String> {
    let local_hashes = local
        .entries
        .iter()
        .map(|entry| entry.sha256.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    remote
        .entries
        .iter()
        .filter(|entry| !local_hashes.contains(entry.sha256.as_str()))
        .map(|entry| entry.path.clone())
        .collect()
}

pub fn seed_remote_deltas(
    local_root: &Path,
    local: &Inventory,
    remote_view: &Path,
    remote: &Inventory,
    transfer_paths: &[String],
) -> Result<usize> {
    let local = local.by_path();
    let remote = remote.by_path();
    let mut seeded = 0;
    for path in transfer_paths {
        let Some(local_entry) = local.get(path.as_str()) else {
            continue;
        };
        let Some(remote_entry) = remote.get(path.as_str()) else {
            continue;
        };
        if local_entry.sha256 == remote_entry.sha256 {
            continue;
        }
        let destination = remote_view.join(path);
        private_dir(destination.parent().context("delta path has no parent")?)?;
        fs::copy(local_root.join(path), destination)?;
        seeded += 1;
    }
    Ok(seeded)
}

pub fn complete_remote_view(
    local_root: &Path,
    local: &Inventory,
    remote_view: &Path,
    remote: &Inventory,
) -> Result<()> {
    let mut local_by_hash = BTreeMap::new();
    for entry in &local.entries {
        local_by_hash.entry(entry.sha256.as_str()).or_insert(entry);
    }
    for entry in &remote.entries {
        let destination = remote_view.join(&entry.path);
        if !destination.exists() {
            let source_entry = local_by_hash
                .get(entry.sha256.as_str())
                .with_context(|| format!("remote object was not transferred: {}", entry.path))?;
            let source = local_root.join(&source_entry.path);
            private_dir(
                destination
                    .parent()
                    .context("inventory path has no parent")?,
            )?;
            if source_entry.modified_ns == entry.modified_ns {
                fs::hard_link(&source, &destination)
                    .or_else(|_| fs::copy(&source, &destination).map(|_| ()))?;
            } else {
                fs::copy(&source, &destination)?;
            }
        }
        let actual_hash = sha256(&destination)?;
        if actual_hash != entry.sha256 {
            bail!(
                "remote object checksum mismatch for {}: expected {}, got {}",
                entry.path,
                entry.sha256,
                actual_hash
            );
        }
        let seconds = entry.modified_ns.div_euclid(1_000_000_000);
        let nanos = entry.modified_ns.rem_euclid(1_000_000_000) as u32;
        let current = fs::metadata(&destination)?
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_nanos() as i64;
        if current != entry.modified_ns {
            filetime::set_file_mtime(
                &destination,
                filetime::FileTime::from_unix_time(seconds, nanos),
            )?;
        }
    }
    Ok(())
}

pub fn stamp() -> String {
    Utc::now().format("%Y%m%d-%H%M%S-%6f").to_string()
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

pub fn build_sparse_payload(
    result: &Path,
    destination: &Path,
    files: &[FileChange],
    side: PayloadSide,
) -> Result<usize> {
    private_dir(destination)?;
    let mut count = 0;
    for file in files {
        let action = match side {
            PayloadSide::Local => file.local,
            PayloadSide::Remote => file.remote,
        };
        match action {
            FileAction::Unchanged => continue,
            FileAction::Remove => {
                bail!(
                    "sparse payload cannot represent removal safely yet: {}",
                    file.path
                )
            }
            FileAction::Create | FileAction::Replace | FileAction::Metadata => {}
        }
        let relative = Path::new(&file.path);
        safe_relative(relative)?;
        let source = result.join(relative);
        if !source.is_file() {
            bail!("staged payload source is missing: {}", source.display());
        }
        copy_file_atomic(&source, &destination.join(relative))?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_editor_uses_git_style_conflict_markers() {
        let text = conflict_markers("local\n", "remote\n", "REMOTE mini");
        assert!(text.starts_with("<<<<<<< LOCAL\n"));
        assert!(text.contains("\n=======\n"));
        assert!(text.ends_with(">>>>>>> REMOTE mini\n"));
        assert!(has_conflict_markers(&text));
        assert!(!has_conflict_markers("resolved\n"));
    }
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

    #[test]
    fn human_report_uses_symbols_and_actionable_conflict_guidance() {
        let report = PlanReport {
            agent: "claude".into(),
            peer: "mini".into(),
            resources: vec!["memory".into()],
            conflict_strategy: Some(ConflictStrategy::Ask),
            files: vec![FileChange {
                resource: "memory".into(),
                path: "projects/-Users-lidongpeng-udeer-udeer/memory/facts.md".into(),
                display_path: "udeer/memory/facts".into(),
                local: FileAction::Replace,
                remote: FileAction::Unchanged,
                resolution: "unresolved".into(),
                local_sha256: Some("local".into()),
                remote_sha256: Some("remote".into()),
                result_sha256: Some("staged".into()),
            }],
            blockers: vec![Blocker {
                resource: "memory".into(),
                path: "-Users-lidongpeng-udeer-udeer/facts.md".into(),
                reason: "memory content or index requires a choice".into(),
            }],
            ..PlanReport::default()
        };

        let rendered = report.render_human();
        assert!(rendered.contains("PATH"));
        assert!(rendered.contains("LOCAL  REMOTE  RESULT"));
        assert!(rendered.contains("udeer/memory/facts"));
        assert!(rendered.contains("↻ update content"));
        assert!(rendered.contains("? choose"));
        assert!(rendered.contains("action required (1):"));
        assert!(rendered.contains("agent-sync sync claude mini -f diff"));
        assert!(rendered.contains("agent-sync sync claude mini --apply"));
        assert!(rendered.contains("-s local --apply"));
        assert!(!rendered.contains("-s ask --apply"));
        assert!(!rendered.contains("BLOCKED"));
        assert!(!rendered.contains("result=unresolved"));
    }

    #[test]
    fn blocking_writer_guidance_does_not_claim_conflict_strategy_can_resolve_it() {
        let report = PlanReport {
            agent: "opencode".into(),
            peer: "mini".into(),
            resources: vec!["sessions".into()],
            blockers: vec![Blocker {
                resource: "sessions".into(),
                path: "opencode.db".into(),
                reason: "active OpenCode writers must exit before apply".into(),
            }],
            ..PlanReport::default()
        };

        let rendered = report.render_human();
        assert!(rendered.contains("after closing the writer:"));
        assert!(!rendered.contains("--only memory --apply"));
        assert!(!rendered.contains("resolve per conflict:"));
        assert!(!rendered.contains("-s ask"));
        assert!(!rendered.contains("-s local"));
        assert!(!rendered.contains("-s remote"));
    }

    #[test]
    fn table_columns_use_terminal_width_for_mixed_language_paths() {
        let header = table_row("PATH", "LOCAL", "REMOTE", "RESULT", 36);
        let ascii = table_row("udeer/release-v3", "=", "↻", "L", 36);
        let chinese = table_row("项目/调查OpenCode配额错误", "↻", "=", "E", 36);

        let expected_width = UnicodeWidthStr::width(header.as_str());
        assert_eq!(UnicodeWidthStr::width(ascii.as_str()), expected_width);
        assert_eq!(UnicodeWidthStr::width(chinese.as_str()), expected_width);
        assert_eq!(UnicodeWidthStr::width(pad_right("中文", 12).as_str()), 12);
        assert_eq!(UnicodeWidthStr::width(center("✦", 6).as_str()), 6);

        let shortened = shorten_middle("项目目录/调查OpenCode配额错误和URL脱敏问题", 24);
        assert!(shortened.contains('…'));
        assert!(UnicodeWidthStr::width(shortened.as_str()) <= 24);
    }

    #[test]
    fn claude_session_display_path_uses_project_and_title() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let remote = temp.path().join("remote");
        let result = temp.path().join("result");
        let project = "-Users-lidongpeng-udeer-udeer";
        let session = "95277219-228f-4257-bb4d-a676546cdd07";
        for root in [&local, &remote, &result] {
            fs::create_dir_all(root.join("projects").join(project)).unwrap();
        }
        fs::write(
            result
                .join("projects")
                .join(project)
                .join(format!("{session}.jsonl")),
            r#"{"cwd":"/Users/lidongpeng/udeer/udeer","slug":"hidden-sprouting-crescent"}"#,
        )
        .unwrap();
        let subagents = result
            .join("projects")
            .join(project)
            .join(session)
            .join("subagents");
        fs::create_dir_all(&subagents).unwrap();
        fs::write(subagents.join("agent-a62839bf499f.jsonl"), "{}\n").unwrap();
        fs::write(subagents.join("agent-a62839bf499f.meta.json"), "{}\n").unwrap();

        let changes = planned_file_changes(&local, &remote, &result, |_| false).unwrap();
        assert_eq!(changes.len(), 3);
        assert!(
            changes
                .iter()
                .any(|change| change.display_path == "udeer/hidden-sprouting-crescent")
        );
        assert!(changes.iter().any(|change| {
            change.display_path == "udeer/hidden-sprouting-crescent/subagent-a62839…99f"
        }));
        assert!(changes.iter().any(|change| {
            change.display_path == "udeer/hidden-sprouting-crescent/subagent-a62839…99f/meta"
        }));
    }

    #[test]
    fn inventory_reuses_unchanged_hashes_and_materializes_by_content() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let view = temp.path().join("view");
        private_dir(&local).unwrap();
        private_dir(&view).unwrap();
        fs::write(local.join("local-name.jsonl"), "same\n").unwrap();

        let first = inventory(&local, |_| false).unwrap();
        let (second, reused) = inventory_cached(&local, |_| false, Some(&first)).unwrap();
        assert_eq!(reused, 1);
        assert_eq!(second.reused_entries, 1);

        let source = &second.entries[0];
        let remote = Inventory {
            generation: "remote".into(),
            entries: vec![InventoryEntry {
                path: "remote-name.jsonl".into(),
                sha256: source.sha256.clone(),
                size: source.size,
                modified_ns: source.modified_ns,
            }],
            reused_entries: 0,
        };
        assert!(inventory_transfer_paths(&second, &remote).is_empty());
        complete_remote_view(&local, &second, &view, &remote).unwrap();
        assert_eq!(fs::read(view.join("remote-name.jsonl")).unwrap(), b"same\n");
    }

    #[test]
    fn differing_same_path_is_seeded_for_rsync_delta_transfer() {
        let temp = tempfile::tempdir().unwrap();
        let local = temp.path().join("local");
        let view = temp.path().join("view");
        private_dir(&local).unwrap();
        private_dir(&view).unwrap();
        fs::write(local.join("session.jsonl"), "prefix\n").unwrap();
        let local_inventory = inventory(&local, |_| false).unwrap();
        let remote_inventory = Inventory {
            generation: "remote".into(),
            entries: vec![InventoryEntry {
                path: "session.jsonl".into(),
                sha256: "different".into(),
                size: 20,
                modified_ns: 10,
            }],
            reused_entries: 0,
        };
        let transfer = inventory_transfer_paths(&local_inventory, &remote_inventory);
        assert_eq!(transfer, ["session.jsonl"]);
        assert_eq!(
            seed_remote_deltas(
                &local,
                &local_inventory,
                &view,
                &remote_inventory,
                &transfer
            )
            .unwrap(),
            1
        );
        assert_eq!(fs::read(view.join("session.jsonl")).unwrap(), b"prefix\n");
    }

    #[test]
    fn sparse_payload_contains_only_files_changed_on_selected_side() {
        let temp = tempfile::tempdir().unwrap();
        let result = temp.path().join("result");
        let local_payload = temp.path().join("local-payload");
        private_dir(&result).unwrap();
        fs::write(result.join("local.jsonl"), "local\n").unwrap();
        fs::write(result.join("remote.jsonl"), "remote\n").unwrap();
        let files = vec![
            FileChange {
                resource: "sessions".into(),
                path: "local.jsonl".into(),
                display_path: "local.jsonl".into(),
                local: FileAction::Create,
                remote: FileAction::Unchanged,
                resolution: "local".into(),
                local_sha256: None,
                remote_sha256: None,
                result_sha256: None,
            },
            FileChange {
                resource: "sessions".into(),
                path: "remote.jsonl".into(),
                display_path: "remote.jsonl".into(),
                local: FileAction::Unchanged,
                remote: FileAction::Create,
                resolution: "remote".into(),
                local_sha256: None,
                remote_sha256: None,
                result_sha256: None,
            },
        ];
        assert_eq!(
            build_sparse_payload(&result, &local_payload, &files, PayloadSide::Local).unwrap(),
            1
        );
        assert!(local_payload.join("local.jsonl").is_file());
        assert!(!local_payload.join("remote.jsonl").exists());
    }
}
