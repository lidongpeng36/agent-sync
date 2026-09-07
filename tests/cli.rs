use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;

#[test]
fn lists_built_in_adapters() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .arg("adapters")
        .assert()
        .success()
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"))
        .stdout(predicate::str::contains("opencode"));
}

#[test]
fn sync_requires_known_agent() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["sync", "unknown", "mini"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown agent \"unknown\""));
}

#[test]
fn yes_requires_apply() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["sync", "codex", "mini", "--yes"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--apply"));
}

#[test]
fn import_force_requires_apply() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["import", "codex", "archive.tar.gz", "--force"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--apply"));
}

#[test]
fn help_documents_read_only_default() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["sync", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Without this flag the command is read-only",
        ));
}

#[test]
fn remote_helper_negotiates_the_typed_protocol() {
    let output = Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["__remote", "--protocol", "5"])
        .write_stdin("{\"op\":\"ping\"}\n")
        .output()
        .unwrap();
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["protocol"], 5);
    assert_eq!(response["ok"], true);
    assert_eq!(response["value"]["protocol"], 5);
    assert!(response["value"]["executable_sha256"].as_str().is_some());
}

#[test]
fn remote_helper_rejects_an_old_protocol() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["__remote", "--protocol", "4"])
        .write_stdin("{\"op\":\"ping\"}\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unsupported protocol 4; expected 5",
        ));
}

#[cfg(unix)]
#[test]
fn codex_memory_three_way_transaction_converges_and_failed_apply_keeps_base() {
    use std::fs;
    use std::path::Path;

    let temp = tempfile::tempdir().unwrap();
    // Memory-only sync probes Codex's presence but must never invoke a real agent.
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let codex = bin.join("codex");
    fs::write(
        &codex,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo codex-test; else exit 99; fi\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let test_path = std::env::join_paths(paths).unwrap();
    let local_home = temp.path().join("local");
    let remote_home = temp.path().join("remote");
    let local = local_home.join("codex");
    let remote = remote_home.join("codex");
    let rel = "memories/skills/example/SKILL.md";
    for root in [&local, &remote] {
        fs::create_dir_all(root.join("memories/skills/example")).unwrap();
        fs::write(root.join(rel), "a\nb\nc\nd\n").unwrap();
    }
    let ssh = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/transport/local-ssh.py");
    let run_from = |reverse: bool, extra: &[&str]| {
        let (local_home, remote_home, local, remote) = if reverse {
            (&remote_home, &local_home, &remote, &local)
        } else {
            (&local_home, &remote_home, &local, &remote)
        };
        let mut command = Command::cargo_bin("agent-sync").unwrap();
        command
            .env("PATH", &test_path)
            .env("HOME", local_home)
            .env("XDG_CACHE_HOME", local_home.join(".cache"))
            .env("XDG_DATA_HOME", local_home.join(".local/share"))
            .env("XDG_CONFIG_HOME", local_home.join(".config"))
            .env("AGENT_SYNC_TEST_REMOTE_HOME", remote_home)
            .env_remove("AGENT_SYNC_CONFIG")
            .args([
                "sync", "codex", "fixture", "-o", "memory", "-s", "ask", "-t", "0",
            ])
            .arg("--local-root")
            .arg(local)
            .arg("--remote-root")
            .arg(remote)
            .arg("--ssh")
            .arg(&ssh)
            .args(extra);
        command.output().unwrap()
    };
    let run = |extra: &[&str]| run_from(false, extra);
    let successful = |output: &std::process::Output| {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let baselines = || {
        walkdir::WalkDir::new(temp.path())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| {
                e.file_type().is_file()
                    && e.path()
                        .parent()
                        .is_some_and(|p| p.ends_with("memory-baselines"))
            })
            .map(|e| (e.path().to_owned(), fs::read(e.path()).unwrap()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let preview = run(&["-f", "json"]);
    successful(&preview);
    assert!(baselines().is_empty()); // Read-only preview cannot establish trust.
    successful(&run(&["--apply", "--yes"]));
    let initial = baselines();
    assert_eq!(initial.len(), 2);
    assert_eq!(initial.values().next(), initial.values().nth(1));

    fs::write(local.join(rel), "a\nlocal\nb\nc\nd\n").unwrap();
    fs::write(remote.join(rel), "a\nb\nc\nremote\nd\n").unwrap();
    let preview = run(&["-f", "json"]);
    successful(&preview);
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(plan["blockers"].as_array().unwrap().len(), 0);
    assert!(
        plan["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap().contains("memory three-way merge"))
    );
    assert_eq!(baselines(), initial);
    successful(&run(&["--apply", "--yes"]));
    let merged = "a\nlocal\nb\nc\nremote\nd\n";
    assert_eq!(fs::read_to_string(local.join(rel)).unwrap(), merged);
    assert_eq!(fs::read_to_string(remote.join(rel)).unwrap(), merged);
    let verified = baselines();
    assert_ne!(initial, verified);
    assert_eq!(verified.values().next(), verified.values().nth(1));
    let preview = run(&["-f", "json"]);
    successful(&preview);
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert!(plan["files"].as_array().unwrap().is_empty());
    assert!(plan["notes"].as_array().unwrap().iter().any(|v| {
        v.as_str()
            .unwrap()
            .contains("hashes reused local=1/1, remote=1/1")
    }));

    let reverse = run_from(true, &["-f", "json"]);
    successful(&reverse);
    let reverse_plan: Value = serde_json::from_slice(&reverse.stdout).unwrap();
    assert!(reverse_plan["files"].as_array().unwrap().is_empty());
    assert!(!reverse_plan["notes"].as_array().unwrap().iter().any(|v| {
        v.as_str()
            .unwrap()
            .contains("no matching verified baseline")
    }));

    fs::write(local.join(rel), "different local\n").unwrap();
    fs::write(remote.join(rel), "different remote\n").unwrap();
    let preview = run(&["-f", "json"]);
    assert_eq!(preview.status.code(), Some(2));
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(plan["blockers"].as_array().unwrap().len(), 1);
    assert_eq!(baselines(), verified);

    // A one-sided change is safe to plan, but a failed write must not advance trust.
    fs::write(remote.join(rel), merged).unwrap();
    fs::write(remote_home.join("fail-push"), "").unwrap();
    let failed = run(&["--apply", "--yes"]);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("rsync push failed"));
    assert_eq!(baselines(), verified);
    fs::remove_file(remote_home.join("fail-push")).unwrap();
    let retry = run(&["--apply", "--yes"]);
    assert!(!retry.status.success());
    assert!(String::from_utf8_lossy(&retry.stderr).contains("unfinished"));
    assert_eq!(baselines(), verified);
}
