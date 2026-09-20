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
        .args(["__remote", "--protocol", "7"])
        .write_stdin("{\"op\":\"ping\"}\n")
        .output()
        .unwrap();
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["protocol"], 7);
    assert_eq!(response["ok"], true);
    assert_eq!(response["value"]["protocol"], 7);
    assert!(response["value"]["executable_sha256"].as_str().is_some());
}

#[test]
fn remote_helper_rejects_an_old_protocol() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .args(["__remote", "--protocol", "6"])
        .write_stdin("{\"op\":\"ping\"}\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unsupported protocol 6; expected 7",
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

#[cfg(unix)]
#[test]
fn codex_semantic_session_transaction_repairs_only_selected_roots() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let codex = bin.join("codex");
    fs::write(
        &codex,
        r#"#!/usr/bin/env python3
import json, os, pathlib, sys
if '--version' in sys.argv:
    print('codex-test'); sys.exit(0)
root = pathlib.Path(os.environ['CODEX_HOME'])
(root/'catalog-root').write_text(str(root))
for line in sys.stdin:
    req = json.loads(line)
    if req['method'] == 'initialize': result = {}
    elif req['method'] == 'thread/read':
        thread = req['params']['threadId']
        # Simulate a runtime touching a selected rollout's mtime during repair.
        # Final verification must cover metadata as well as exact content.
        if thread == '019fe9a3-6ea4-71e1-bfce-ddfc8243ef05':
            for rollout in (root/'sessions').rglob('*' + thread + '.jsonl'):
                os.utime(rollout, (1700000000, 1700000000))
        result = {'thread': {'id': thread}}
    else: raise RuntimeError('unexpected RPC')
    print(json.dumps({'id': req['id'], 'result': result}), flush=True)
"#,
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let path = std::env::join_paths(paths).unwrap();
    let lh = temp.path().join("local-home");
    let rh = temp.path().join("remote-home");
    let local = lh.join("isolated-codex");
    let remote = rh.join("isolated-codex");
    let id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef05";
    let rel = format!("sessions/2026/08/11/rollout-{id}.jsonl");
    let first = serde_json::json!({"type":"session_meta","ordinal":0,
        "payload":{"id":id,"timestamp":"2026-08-11T00:00:00Z","history_mode":"paginated"}});
    let setting = serde_json::json!({"type":"event_msg","ordinal":1,
        "payload":{"type":"thread_settings_applied","thread_settings":{}}});
    for root in [&local, &remote] {
        fs::create_dir_all(root.join("sessions/2026/08/11")).unwrap();
        let db = rusqlite::Connection::open(root.join("state_5.sqlite")).unwrap();
        db.execute_batch("CREATE TABLE threads (id TEXT PRIMARY KEY, created_at INTEGER, created_at_ms INTEGER, updated_at INTEGER, updated_at_ms INTEGER, recency_at INTEGER, recency_at_ms INTEGER);").unwrap();
    }
    fs::write(local.join(&rel), format!("{first}\n{setting}\n")).unwrap();
    let mut setting_empty = setting.clone();
    setting_empty["payload"]["thread_settings"]["disabled_plugin_ids"] = serde_json::json!([]);
    let advanced = format!(
        "{first}\n{setting_empty}\n{{\"type\":\"event_msg\",\"ordinal\":2,\"payload\":{{\"type\":\"task_complete\"}}}}\n"
    );
    fs::write(remote.join(&rel), &advanced).unwrap();
    // Same-size JSON reorderings with the staged mtime must still be transferred.
    let equal_id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef06";
    let equal_rel = format!("sessions/2026/08/11/rollout-{equal_id}.jsonl");
    let left = format!(
        "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{equal_id}\",\"timestamp\":\"2026-08-11T00:00:00Z\"}}}}\n"
    );
    let right = format!(
        "{{\"payload\":{{\"timestamp\":\"2026-08-11T00:00:00Z\",\"id\":\"{equal_id}\"}},\"type\":\"session_meta\"}}\n"
    );
    assert_eq!(left.len(), right.len());
    for (root, text) in [(&local, &left), (&remote, &right)] {
        fs::write(root.join(&equal_rel), text).unwrap();
        filetime::set_file_mtime(
            root.join(&equal_rel),
            filetime::FileTime::from_unix_time(1786406400, 0),
        )
        .unwrap();
    }
    let reverse_id = "019fe9a3-6ea4-71e1-bfce-ddfc8243ef07";
    let reverse_rel = format!("sessions/2026/08/11/rollout-{reverse_id}.jsonl");
    for (root, text) in [(&local, &right), (&remote, &left)] {
        fs::write(root.join(&reverse_rel), text.replace(equal_id, reverse_id)).unwrap();
        filetime::set_file_mtime(
            root.join(&reverse_rel),
            filetime::FileTime::from_unix_time(1786406400, 0),
        )
        .unwrap();
    }
    let ssh = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/transport/local-ssh.py");
    let run = |args: &[&str]| {
        Command::cargo_bin("agent-sync")
            .unwrap()
            .env("PATH", &path)
            .env("HOME", &lh)
            .env("CODEX_HOME", lh.join("must-not-be-used"))
            .env("XDG_CACHE_HOME", lh.join(".cache"))
            .env("XDG_DATA_HOME", lh.join(".local/share"))
            .env("XDG_CONFIG_HOME", lh.join(".config"))
            .env("AGENT_SYNC_TEST_REMOTE_HOME", &rh)
            .env_remove("AGENT_SYNC_CONFIG")
            .args(["sync", "codex", "fixture", "-o", "sessions", "-t", "0"])
            .arg("--local-root")
            .arg(&local)
            .arg("--remote-root")
            .arg(&remote)
            .arg("--ssh")
            .arg(&ssh)
            .args(args)
            .output()
            .unwrap()
    };
    // A mixed-format pair must stop before runtime/catalog work, even with an
    // explicit whole-side policy and --yes. Migration guidance names real roots.
    let mut legacy: Vec<Value> = advanced
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    legacy[0]["payload"]["history_mode"] = serde_json::json!("legacy");
    for record in &mut legacy {
        record.as_object_mut().unwrap().remove("ordinal");
    }
    let legacy = legacy
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>();
    fs::write(remote.join(&rel), &legacy).unwrap();
    for strategy in ["ask", "local", "remote"] {
        let blocked = run(&["-f", "json", "-s", strategy, "--apply", "--yes"]);
        assert_eq!(
            blocked.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&blocked.stderr)
        );
        let plan: Value = serde_json::from_slice(&blocked.stdout).unwrap();
        assert_eq!(plan["blockers"].as_array().unwrap().len(), 1);
        assert!(
            plan["blockers"][0]["reason"]
                .as_str()
                .unwrap()
                .contains("history format mismatch")
        );
        let notes = plan["notes"].to_string();
        assert!(notes.contains("remote legacy=1"));
        assert!(notes.contains(remote.to_str().unwrap()));
        assert!(notes.contains("codex migrate-rollouts --apply"));
        assert_eq!(
            fs::read_to_string(local.join(&rel)).unwrap(),
            format!("{first}\n{setting}\n")
        );
        assert_eq!(fs::read_to_string(remote.join(&rel)).unwrap(), legacy);
        for root in [&local, &remote] {
            assert!(!root.join("catalog-root").exists());
        }
    }
    // Once official migration has produced the same format, ordinary append
    // merging, transaction verification, and a converged rerun still work.
    fs::write(remote.join(&rel), &advanced).unwrap();
    let preview = run(&["-f", "json"]);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(plan["advances"], 1);
    assert_eq!(plan["blockers"], serde_json::json!([]));
    assert!(!local.join("catalog-root").exists());
    let applied = run(&["--apply", "--yes"]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    for root in [&local, &remote] {
        assert_eq!(fs::read_to_string(root.join(&rel)).unwrap(), advanced);
        assert_eq!(
            fs::read_to_string(root.join("catalog-root")).unwrap(),
            root.to_str().unwrap()
        );
    }
    assert!(!lh.join("must-not-be-used").exists());
    let rerun = run(&["-f", "json"]);
    assert!(rerun.status.success());
    let plan: Value = serde_json::from_slice(&rerun.stdout).unwrap();
    assert_eq!(plan["advances"], 0);
    assert_eq!(plan["identical"], 3);
    assert_eq!(plan["files"], serde_json::json!([]));
    assert_eq!(
        fs::read(local.join(&reverse_rel)).unwrap(),
        fs::read(remote.join(&reverse_rel)).unwrap()
    );
    assert_eq!(
        fs::read(local.join(&equal_rel)).unwrap(),
        fs::read(remote.join(&equal_rel)).unwrap()
    );
    assert_eq!(plan["blockers"], serde_json::json!([]));
}

#[cfg(unix)]
#[test]
fn configured_memory_backend_stages_then_converges_without_codex_runtime() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    // Any accidental runtime dependency makes the test fail.
    let unavailable = bin.join("codex");
    fs::write(&unavailable, "#!/bin/sh\nexit 99\n").unwrap();
    fs::set_permissions(&unavailable, fs::Permissions::from_mode(0o700)).unwrap();
    let resolver = bin.join("merge-agent");
    fs::write(
        &resolver,
        r#"#!/usr/bin/env python3
import json,pathlib,sys
prompt=sys.stdin.read()
fingerprint=prompt.split('Input fingerprint: ')[1].split('\n')[0]
result={'input_sha256':fingerprint,'merged':'# Memory\nalpha\nbeta\n','conflicts':[]}
pathlib.Path(sys.argv[sys.argv.index('--output-last-message')+1]).write_text(json.dumps(result))
"#,
    )
    .unwrap();
    fs::set_permissions(&resolver, fs::Permissions::from_mode(0o700)).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let path = std::env::join_paths(paths).unwrap();
    let lh = temp.path().join("local-home");
    let rh = temp.path().join("remote-home");
    let local = lh.join("codex");
    let remote = rh.join("codex");
    for root in [&local, &remote] {
        fs::create_dir_all(root.join("memories")).unwrap();
    }
    fs::write(local.join("memories/MEMORY.md"), "# Memory\nalpha\n").unwrap();
    fs::write(remote.join("memories/MEMORY.md"), "# Memory\nbeta\n").unwrap();
    let config = temp.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "[memory_merge]\nbackend = 'codex'\ncommand = {:?}\n",
            resolver.to_str().unwrap()
        ),
    )
    .unwrap();
    let ssh = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/transport/local-ssh.py");
    let run = |args: &[&str]| {
        Command::cargo_bin("agent-sync")
            .unwrap()
            .env("PATH", &path)
            .env("HOME", &lh)
            .env("XDG_CACHE_HOME", lh.join(".cache"))
            .env("XDG_DATA_HOME", lh.join(".local/share"))
            .env("XDG_CONFIG_HOME", lh.join(".config"))
            .env("AGENT_SYNC_TEST_REMOTE_HOME", &rh)
            .env_remove("AGENT_SYNC_CONFIG")
            .arg("--config")
            .arg(&config)
            .args(["sync", "codex", "fixture", "-o", "memory", "-t", "0"])
            .arg("--local-root")
            .arg(&local)
            .arg("--remote-root")
            .arg(&remote)
            .arg("--ssh")
            .arg(&ssh)
            .args(args)
            .output()
            .unwrap()
    };
    let preview = run(&["-f", "json"]);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(plan["blockers"], serde_json::json!([]));
    assert_eq!(
        fs::read_to_string(local.join("memories/MEMORY.md")).unwrap(),
        "# Memory\nalpha\n"
    );
    let apply = run(&["--apply", "--yes"]);
    assert!(
        apply.status.success(),
        "{}",
        String::from_utf8_lossy(&apply.stderr)
    );
    for root in [&local, &remote] {
        assert_eq!(
            fs::read_to_string(root.join("memories/MEMORY.md")).unwrap(),
            "# Memory\nalpha\nbeta\n"
        );
    }
    fs::write(&resolver, "#!/bin/sh\necho secret-test-key >&2\nexit 99\n").unwrap();
    let rerun = run(&["-f", "json"]);
    assert!(rerun.status.success());
    let plan: Value = serde_json::from_slice(&rerun.stdout).unwrap();
    assert_eq!(plan["files"], serde_json::json!([]));
    fs::write(remote.join("memories/MEMORY.md"), "# Memory\nchanged\n").unwrap();
    fs::write(
        local.join("memories/MEMORY.md"),
        "# Memory\nlocal changed\n",
    )
    .unwrap();
    let failed = run(&["-f", "json"]);
    assert_eq!(failed.status.code(), Some(2));
    let plan: Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(plan["blockers"].as_array().unwrap().len(), 1);
    assert!(!String::from_utf8_lossy(&failed.stdout).contains("secret-test-key"));
    assert!(!String::from_utf8_lossy(&failed.stderr).contains("secret-test-key"));
    assert_eq!(
        fs::read_to_string(local.join("memories/MEMORY.md")).unwrap(),
        "# Memory\nlocal changed\n"
    );
}

#[cfg(unix)]
#[test]
fn cross_file_review_repairs_identical_inputs_and_persists_verified_policy() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let resolver = bin.join("reviewer");
    fs::write(&resolver,r#"#!/usr/bin/env python3
import json,pathlib,sys
prompt=sys.stdin.read();data=json.loads(prompt.split('Input JSON:\n')[1]);fingerprint=prompt.split('Input fingerprint: ')[1].split('\n')[0]
assert data['policy']=='codex-linked-memory-consistency-v1'
u=data['units'];cat=next(v for v in u.values() if v['kind']=='catalog');raw=next(v for v in u.values() if v['kind']=='raw' and not v['writable'])
result={'input_sha256':fingerprint,'edits':[{'unit':cat['id'],'before':'The dry-run was not executed.','after':'The dry-run ran; actual prune was not run.','evidence':[{'unit':raw['id'],'quote':'Executed dry-run: 8 files would be pruned (144 MB). No actual prune.'}]}],'conflicts':[]}
pathlib.Path(sys.argv[sys.argv.index('--output-last-message')+1]).write_text(json.dumps(result))
"#).unwrap();
    fs::set_permissions(&resolver, fs::Permissions::from_mode(0o700)).unwrap();
    let lh = temp.path().join("local");
    let rh = temp.path().join("remote");
    let local = lh.join("codex");
    let remote = rh.join("codex");
    let sid = "01a06746-a744-7162-b68e-71bf6f57265a";
    let wrong = format!(
        "# Task Group: Cleanup\n- rollout_summaries/cleanup.md (thread_id={sid})\n- The dry-run was not executed.\n"
    );
    let raw = format!(
        "# Raw Memories\n\n## Thread `{sid}`\nExecuted dry-run: 8 files would be pruned (144 MB). No actual prune.\n"
    );
    for root in [&local, &remote] {
        fs::create_dir_all(root.join("memories")).unwrap();
        fs::write(root.join("memories/MEMORY.md"), &wrong).unwrap();
        fs::write(root.join("memories/raw_memories.md"), &raw).unwrap();
    }
    let config = temp.path().join("config.toml");
    fs::write(&config, "[memory_merge]\nbackend='builtin'\n").unwrap();
    let ssh = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/transport/local-ssh.py");
    let run = |args: &[&str]| {
        Command::cargo_bin("agent-sync")
            .unwrap()
            .env("HOME", &lh)
            .env("XDG_CACHE_HOME", lh.join(".cache"))
            .env("XDG_DATA_HOME", lh.join(".local/share"))
            .env("XDG_CONFIG_HOME", lh.join(".config"))
            .env("AGENT_SYNC_TEST_REMOTE_HOME", &rh)
            .env_remove("AGENT_SYNC_CONFIG")
            .arg("--config")
            .arg(&config)
            .args(["sync", "codex", "fixture", "-o", "memory", "-t", "0"])
            .arg("--local-root")
            .arg(&local)
            .arg("--remote-root")
            .arg(&remote)
            .arg("--ssh")
            .arg(&ssh)
            .args(args)
            .output()
            .unwrap()
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
    let first = run(&["--apply", "--yes"]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(baselines().values().all(|b| {
        serde_json::from_slice::<Value>(b)
            .unwrap()
            .get("review_policy")
            .is_none()
    }));
    fs::write(
        &config,
        format!(
            "[memory_merge]\nbackend='codex'\ncommand={:?}\n",
            resolver.to_str().unwrap()
        ),
    )
    .unwrap();
    let preview = run(&["-f", "json"]);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let plan: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(plan["files"].as_array().unwrap().len(), 1);
    assert_eq!(
        fs::read_to_string(local.join("memories/MEMORY.md")).unwrap(),
        wrong
    );
    let applied = run(&["--apply", "--yes"]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    for root in [&local, &remote] {
        assert!(
            fs::read_to_string(root.join("memories/MEMORY.md"))
                .unwrap()
                .contains("The dry-run ran; actual prune was not run.")
        );
    }
    let verified = baselines();
    assert_eq!(verified.len(), 2);
    assert!(verified.values().all(
        |b| serde_json::from_slice::<Value>(b).unwrap()["review_policy"]
            == "codex-linked-memory-consistency-v1"
    ));
    fs::write(&resolver, "#!/bin/sh\nexit 99\n").unwrap();
    let rerun = run(&["-f", "json"]);
    assert!(
        rerun.status.success(),
        "{}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    let plan: Value = serde_json::from_slice(&rerun.stdout).unwrap();
    assert_eq!(plan["files"], serde_json::json!([]));
    for root in [&local, &remote] {
        fs::write(
            root.join("memories/raw_memories.md"),
            format!("{raw}Changed evidence.\n"),
        )
        .unwrap();
    }
    let failed = run(&["--apply", "--yes", "-f", "json"]);
    assert_eq!(failed.status.code(), Some(2));
    let plan: Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(plan["blockers"][0]["resource"], "memory-consistency");
    assert_eq!(baselines(), verified);
}
