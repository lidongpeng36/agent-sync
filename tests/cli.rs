use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn lists_built_in_adapters() {
    Command::cargo_bin("agent-sync")
        .unwrap()
        .arg("adapters")
        .assert()
        .success()
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"));
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
