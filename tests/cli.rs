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
        .stderr(predicate::str::contains("invalid value 'unknown'"));
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
        .args(["__remote", "--protocol", "1"])
        .write_stdin("{\"op\":\"ping\"}\n")
        .output()
        .unwrap();
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["protocol"], 1);
    assert_eq!(response["ok"], true);
    assert_eq!(response["value"]["protocol"], 1);
    assert!(response["value"]["executable_sha256"].as_str().is_some());
}
