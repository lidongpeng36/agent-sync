//! Root-scoped Claude writer attribution. Unattributed writers fail closed.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs, path::Path, process::Command};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Activity {
    pub sessions: BTreeSet<String>,
    pub projects: BTreeSet<String>,
}

impl Activity {
    pub fn merge(&mut self, other: Self) {
        self.sessions.extend(other.sessions);
        self.projects.extend(other.projects);
    }

    pub fn validate(&self) -> Result<()> {
        for id in &self.sessions {
            uuid::Uuid::parse_str(id)?;
        }
        for project in &self.projects {
            crate::core::safe_relative(Path::new(project))?;
            if Path::new(project).components().count() != 1 {
                bail!("invalid Claude project exclusion");
            }
        }
        Ok(())
    }

    pub fn excludes(&self, path: &Path) -> bool {
        if let Some((_, id)) = super::claude::session_bundle_identity(path) {
            return self.sessions.contains(&id);
        }
        path.components().nth(1).is_some_and(|p| {
            self.projects
                .contains(p.as_os_str().to_string_lossy().as_ref())
        })
    }

    pub fn covers(&self, other: &Self) -> bool {
        other.sessions.is_subset(&self.sessions) && other.projects.is_subset(&self.projects)
    }
}

pub(crate) fn map_projects(root: &Path, activity: &mut Activity) -> Result<()> {
    let projects = root.join("projects");
    if !projects.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(projects)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if activity.sessions.iter().any(|id| {
            entry.path().join(format!("{id}.jsonl")).exists() || entry.path().join(id).is_dir()
        }) {
            activity
                .projects
                .insert(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(())
}

fn process_exists(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // Signal 0 checks existence/permission without delivering a signal.
    unsafe {
        libc::kill(pid as i32, 0) == 0
            || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

pub(crate) fn detect(root: &Path) -> Result<Activity> {
    let root = fs::canonicalize(root)?;
    let mut activity = Activity::default();
    let registry = root.join("sessions");
    if registry.is_dir() {
        for entry in fs::read_dir(registry)? {
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            if entry.file_type()?.is_symlink() {
                bail!("symlink in Claude session registry");
            }
            let text = match fs::read(entry.path()) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let value: serde_json::Value = serde_json::from_slice(&text)
                .context("invalid Claude session registry; retry after writers settle")?;
            let pid = value["pid"]
                .as_u64()
                .and_then(|p| u32::try_from(p).ok())
                .context("Claude session registry lacks a valid pid")?;
            if !process_exists(pid) {
                continue;
            }
            let id = value["sessionId"]
                .as_str()
                .context("cannot attribute live Claude process to a session")?;
            uuid::Uuid::parse_str(id).context("invalid live Claude session ID")?;
            activity.sessions.insert(id.to_owned());
        }
    }
    let projects = root.join("projects");
    if !projects.is_dir() {
        return Ok(activity);
    }
    let output = Command::new("lsof")
        .args(["-F0pafn", "+D"])
        .arg(&projects)
        .output()
        .context("inspect Claude write descriptors")?;
    // macOS lsof can return 1 for a +D scan even when some files match.
    // Only a clean 0/1 result is usable; diagnostics or other statuses fail closed.
    if !matches!(output.status.code(), Some(0 | 1)) || !output.stderr.is_empty() {
        bail!("cannot inspect Claude writers; lsof failed");
    }
    let mut writing = false;
    for field in output.stdout.split(|b| *b == 0) {
        let field = std::str::from_utf8(field)?.trim_start_matches('\n');
        if field.starts_with('p') {
            writing = false;
        }
        if let Some(fd) = field.strip_prefix('f') {
            let suffix = fd.trim_start_matches(|c: char| c.is_ascii_digit());
            writing = suffix.starts_with('w') || suffix.starts_with('u');
        }
        if let Some(access) = field.strip_prefix('a') {
            writing = access == "w" || access == "u";
        }
        if writing && let Some(name) = field.strip_prefix('n') {
            let relative = Path::new(name)
                .strip_prefix(&root)
                .context("unattributed Claude writer outside selected root")?;
            if let Some((project, id)) = super::claude::session_bundle_identity(relative) {
                activity.sessions.insert(id);
                activity.projects.insert(project);
            } else if let Some(project) = relative.components().nth(1) {
                // Memory/index writers are deferred with their entire shared project data.
                activity
                    .projects
                    .insert(project.as_os_str().to_string_lossy().into_owned());
            } else {
                bail!("unattributed Claude writer blocks apply");
            }
        }
    }
    let names: Vec<_> = fs::read_dir(&projects)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    for id in &activity.sessions {
        let matching: Vec<_> = names
            .iter()
            .filter(|project| {
                let p = projects.join(project);
                p.join(format!("{id}.jsonl")).exists() || p.join(id).is_dir()
            })
            .cloned()
            .collect();
        if matching.is_empty() {
            // A just-started session may not have written its first record yet.
            activity.projects.extend(names.iter().cloned());
        } else {
            activity.projects.extend(matching);
        }
    }
    activity.validate()?;
    Ok(activity)
}

#[cfg(test)]
mod tests {
    use super::*;
    const ACTIVE: &str = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
    const IDLE: &str = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef06";

    #[test]
    fn live_registry_excludes_bundle_and_shared_project_data_only() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("sessions")).unwrap();
        fs::create_dir_all(temp.path().join("projects/project")).unwrap();
        fs::write(
            temp.path().join(format!("projects/project/{ACTIVE}.jsonl")),
            "incomplete live JSONL",
        )
        .unwrap();
        fs::write(
            temp.path().join("sessions/live.json"),
            serde_json::json!({"pid":std::process::id(),"sessionId":ACTIVE}).to_string(),
        )
        .unwrap();
        let a = detect(temp.path()).unwrap();
        for path in [
            format!("projects/project/{ACTIVE}.jsonl"),
            format!("projects/project/{ACTIVE}/subagents/agent-abc.jsonl"),
            "projects/project/memory/MEMORY.md".into(),
            "projects/project/sessions-index.json".into(),
        ] {
            assert!(a.excludes(Path::new(&path)), "{path}");
        }
        assert!(!a.excludes(Path::new(&format!("projects/project/{IDLE}.jsonl"))));
        assert!(!a.excludes(Path::new("projects/unrelated/memory/MEMORY.md")));
        let mut newer = a.clone();
        newer.sessions.insert(IDLE.into());
        assert!(!a.covers(&newer));
        fs::write(
            temp.path().join("sessions/live.json"),
            serde_json::json!({"pid":0,"sessionId":ACTIVE}).to_string(),
        )
        .unwrap();
        assert!(detect(temp.path()).unwrap().sessions.is_empty());
    }

    #[test]
    fn actual_lsof_access_fields_detect_writes_and_ignore_reads() {
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("projects/project/{ACTIVE}.jsonl"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut writer = fs::File::create(&path).unwrap();
        writer.write_all(b"live").unwrap();
        assert!(detect(temp.path()).unwrap().sessions.contains(ACTIVE));
        drop(writer);
        let _reader = fs::File::open(&path).unwrap();
        assert!(detect(temp.path()).unwrap().sessions.is_empty());
    }

    #[test]
    fn peer_session_ids_map_to_project_aliases_without_path_substrings() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("projects/local-alias")).unwrap();
        fs::write(
            temp.path()
                .join(format!("projects/local-alias/{ACTIVE}.jsonl")),
            "live",
        )
        .unwrap();
        let mut a = Activity {
            sessions: BTreeSet::from([ACTIVE.into()]),
            ..Default::default()
        };
        map_projects(temp.path(), &mut a).unwrap();
        assert!(a.projects.contains("local-alias"));
        assert!(!a.excludes(Path::new(&format!(
            "projects/local-alias/{IDLE}/tool-results/{ACTIVE}.txt"
        ))));
        a.projects.insert("../outside".into());
        assert!(a.validate().is_err());
    }
}
