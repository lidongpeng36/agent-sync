use crate::core::{ResourceSelection, private_dir, safe_relative};
use crate::remote::{BackupKind, BackupPruneResult, PROTOCOL_VERSION, Request, Response};
use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferStats {
    pub wire_sent: Option<u64>,
    pub wire_received: Option<u64>,
    pub literal_data: Option<u64>,
    pub matched_data: Option<u64>,
}

#[derive(Clone)]
pub struct SshTransport {
    pub host: String,
    pub ssh: String,
    pub rsync: String,
    bandwidth_limit_kbps: Option<u64>,
    helper: Arc<OnceLock<String>>,
}

impl SshTransport {
    pub fn new(
        host: String,
        ssh: String,
        rsync: String,
        bandwidth_limit_kbps: Option<u64>,
    ) -> Self {
        Self {
            host,
            ssh,
            rsync,
            bandwidth_limit_kbps,
            helper: Arc::new(OnceLock::new()),
        }
    }

    pub fn command_exists(command: &str) -> bool {
        Command::new(command).arg("--version").output().is_ok()
    }

    pub fn ssh(&self, script: &str) -> Result<Output> {
        let output = Command::new(&self.ssh)
            .arg(&self.host)
            .arg(script)
            .output()
            .with_context(|| format!("run {} {}", self.ssh, self.host))?;
        if !output.status.success() {
            bail!(
                "remote command failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(output)
    }

    pub fn ssh_with_input(&self, script: &str, input: &[u8]) -> Result<Output> {
        let mut child = Command::new(&self.ssh)
            .arg(&self.host)
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("run {} {}", self.ssh, self.host))?;
        child
            .stdin
            .take()
            .context("remote command stdin")?
            .write_all(input)?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            bail!(
                "remote command failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(output)
    }

    pub fn pull_files(
        &self,
        remote_root: &str,
        local: &Path,
        paths: &[String],
    ) -> Result<TransferStats> {
        private_dir(local)?;
        if paths.is_empty() {
            return Ok(TransferStats {
                wire_sent: Some(0),
                wire_received: Some(0),
                literal_data: Some(0),
                matched_data: Some(0),
            });
        }
        let mut list = tempfile::NamedTempFile::new()?;
        for path in paths {
            let relative = Path::new(path);
            safe_relative(relative)?;
            if path.contains(['\n', '\r']) {
                bail!("unsafe newline in transfer path: {path:?}");
            }
            writeln!(list, "{path}")?;
        }
        list.flush()?;
        let mut command = Command::new(&self.rsync);
        self.configure_rsync(&mut command);
        command
            .arg("--stats")
            .arg("--checksum")
            .arg(format!("--files-from={}", list.path().display()))
            .args(["-e", &self.ssh])
            .arg(format!(
                "{}:{}/",
                self.host,
                remote_root.trim_end_matches('/')
            ))
            .arg(format!("{}/", local.display()));
        let output = command
            .output()
            .with_context(|| format!("run {}", self.rsync))?;
        if !output.status.success() {
            bail!(
                "rsync selective pull failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(parse_transfer_stats(&String::from_utf8_lossy(
            &output.stdout,
        )))
    }

    pub fn push(&self, source: &Path, remote_root: &str) -> Result<()> {
        let mut command = Command::new(&self.rsync);
        self.configure_rsync(&mut command);
        let output = command
            .args(["-e", &self.ssh])
            .arg(format!("{}/", source.display()))
            .arg(format!(
                "{}:{}/",
                self.host,
                remote_root.trim_end_matches('/')
            ))
            .output()?;
        if !output.status.success() {
            bail!(
                "rsync push failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    fn configure_rsync(&self, command: &mut Command) {
        command.args(["-a", "--compress"]);
        if let Some(limit) = self.bandwidth_limit_kbps {
            command.arg(format!("--bwlimit={limit}"));
        }
    }

    pub fn ensure_remote_helper(&self) -> Result<&str> {
        if let Some(path) = self.helper.get() {
            return Ok(path);
        }
        let path = remote_helper_path();
        let local_exe = std::env::current_exe().context("resolve current agent-sync executable")?;
        let local_hash = crate::core::sha256(&local_exe)?;
        let current = self
            .raw_request::<serde_json::Value>(&path, &Request::Ping)
            .ok()
            .and_then(|value| {
                value
                    .get("executable_sha256")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            });
        if current.as_deref() != Some(&local_hash) {
            self.check_remote_platform()?;
            self.upload_helper(&local_exe)?;
            let value: serde_json::Value = self.raw_request(&path, &Request::Ping)?;
            let remote_hash = value
                .get("executable_sha256")
                .and_then(|value| value.as_str())
                .context("remote helper ping omitted executable hash")?;
            if remote_hash != local_hash {
                bail!("remote helper checksum differs after upload");
            }
        }
        let _ = self.helper.set(path);
        Ok(self.helper.get().expect("helper path was initialized"))
    }

    pub fn remote_request<T: DeserializeOwned>(&self, request: &Request) -> Result<T> {
        let path = self.ensure_remote_helper()?.to_owned();
        self.raw_request(&path, request)
    }

    pub fn remote_guard(&self, request: &Request) -> Result<RemoteGuard> {
        let path = self.ensure_remote_helper()?.to_owned();
        let mut child = self.spawn_helper(&path)?;
        serde_json::to_writer(
            child.stdin.as_mut().context("remote helper stdin")?,
            request,
        )?;
        child
            .stdin
            .as_mut()
            .context("remote helper stdin")?
            .write_all(b"\n")?;
        child.stdin.as_mut().unwrap().flush()?;
        let mut line = String::new();
        BufReader::new(child.stdout.take().context("remote helper stdout")?)
            .read_line(&mut line)?;
        let response: Response = serde_json::from_str(&line)
            .with_context(|| format!("decode remote helper response: {}", line.trim()))?;
        validate_response(&response)?;
        Ok(RemoteGuard { child })
    }

    pub fn remote_node_id(&self) -> Result<String> {
        let value: serde_json::Value = self.remote_request(&Request::NodeId)?;
        value
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .context("remote helper omitted node id")
    }

    pub fn sync_guards(
        &self,
        local_state_root: &Path,
        local_node_id: &str,
        remote_node_id: &str,
        agent: &str,
        resources: ResourceSelection,
    ) -> Result<SyncGuards> {
        let request = Request::HoldSyncLock {
            agent: agent.to_owned(),
            resources,
        };
        let (local, remote) = if local_node_id <= remote_node_id {
            let local = crate::state::acquire_sync_lock(local_state_root, agent, resources)?;
            let remote = self.remote_guard(&request)?;
            (local, remote)
        } else {
            let remote = self.remote_guard(&request)?;
            let local = crate::state::acquire_sync_lock(local_state_root, agent, resources)?;
            (local, remote)
        };
        Ok(SyncGuards {
            _local: local,
            _remote: remote,
        })
    }

    pub fn ensure_no_pending_transaction(
        &self,
        local_state_root: &Path,
        agent: &str,
    ) -> Result<()> {
        let local = crate::state::load_transaction(local_state_root, agent)?;
        let remote: Option<crate::state::TransactionJournal> =
            self.remote_request(&Request::GetTransaction {
                agent: agent.to_owned(),
            })?;
        if local.is_none() && remote.is_none() {
            return Ok(());
        }
        let ids = local
            .iter()
            .chain(remote.iter())
            .map(|journal| journal.transaction_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let verified = local
            .iter()
            .chain(remote.iter())
            .any(|journal| journal.phase == crate::state::TransactionPhase::Verified);
        if ids.len() == 1 && verified {
            let transaction_id = ids.into_iter().next().expect("transaction id exists");
            let _: serde_json::Value = self.remote_request(&Request::ClearTransaction {
                agent: agent.to_owned(),
                transaction_id: transaction_id.to_owned(),
            })?;
            if local.is_some() {
                crate::state::clear_transaction(local_state_root, agent, transaction_id)?;
            }
            return Ok(());
        }
        let describe = |journal: &crate::state::TransactionJournal| {
            format!(
                "id={}, phase={:?}, local_backup={}, remote_backup={}",
                journal.transaction_id, journal.phase, journal.local_backup, journal.remote_backup
            )
        };
        let detail = match (local.as_ref(), remote.as_ref()) {
            (Some(local), Some(remote)) if local.transaction_id == remote.transaction_id => {
                describe(local)
            }
            (Some(local), Some(remote)) => {
                format!("local [{}]; remote [{}]", describe(local), describe(remote))
            }
            (Some(local), None) => format!("local [{}]; remote missing", describe(local)),
            (None, Some(remote)) => format!("local missing; remote [{}]", describe(remote)),
            (None, None) => unreachable!(),
        };
        bail!("unfinished {agent} sync transaction; recovery required before continuing: {detail}")
    }

    pub fn save_transaction_pair(
        &self,
        local_state_root: &Path,
        journal: &crate::state::TransactionJournal,
    ) -> Result<()> {
        let _: serde_json::Value = self.remote_request(&Request::PutTransaction {
            journal: journal.clone(),
        })?;
        crate::state::save_transaction(local_state_root, journal)
    }

    pub fn clear_transaction_pair(
        &self,
        local_state_root: &Path,
        journal: &crate::state::TransactionJournal,
    ) -> Result<()> {
        let _: serde_json::Value = self.remote_request(&Request::ClearTransaction {
            agent: journal.agent.clone(),
            transaction_id: journal.transaction_id.clone(),
        })?;
        crate::state::clear_transaction(local_state_root, &journal.agent, &journal.transaction_id)
    }

    pub fn prune_backup_pair(
        &self,
        local_root: &Path,
        remote_root: &str,
        agent: BackupKind,
        keep: usize,
        protected_stamp: &str,
    ) -> Vec<String> {
        let mut messages = Vec::new();
        match crate::remote::prune_backups(local_root, agent, keep, protected_stamp) {
            Ok(result) => messages.push(prune_message("local", result)),
            Err(error) => messages.push(format!(
                "warning: local backup retention stopped before completion: {error:#}"
            )),
        }
        match self.remote_request::<BackupPruneResult>(&Request::PruneBackups {
            root: remote_root.to_owned(),
            agent,
            keep,
            protected_stamp: protected_stamp.to_owned(),
        }) {
            Ok(result) => messages.push(prune_message(&format!("remote {}", self.host), result)),
            Err(error) => messages.push(format!(
                "warning: remote backup retention on {} stopped before completion: {error:#}",
                self.host
            )),
        }
        messages
    }

    fn raw_request<T: DeserializeOwned>(&self, path: &str, request: &Request) -> Result<T> {
        let mut child = self.spawn_helper(path)?;
        serde_json::to_writer(
            child.stdin.as_mut().context("remote helper stdin")?,
            request,
        )?;
        child
            .stdin
            .take()
            .context("remote helper stdin")?
            .write_all(b"\n")?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            bail!(
                "remote helper failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let response: Response = serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "decode remote helper response: {}",
                String::from_utf8_lossy(&output.stdout).trim()
            )
        })?;
        validate_response(&response)?;
        serde_json::from_value(response.value).context("decode remote helper result")
    }

    fn spawn_helper(&self, path: &str) -> Result<Child> {
        Command::new(&self.ssh)
            .arg(&self.host)
            .arg(format!(
                "exec \"{path}\" __remote --protocol {PROTOCOL_VERSION}"
            ))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start remote helper on {}", self.host))
    }

    fn check_remote_platform(&self) -> Result<()> {
        let output = self.ssh("uname -s; uname -m")?;
        let values: Vec<_> = String::from_utf8(output.stdout)?
            .lines()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect();
        if values.len() != 2 {
            bail!("remote platform probe returned unexpected output");
        }
        let local_os = match std::env::consts::OS {
            "macos" => "Darwin",
            "linux" => "Linux",
            value => value,
        };
        let local_arch = match std::env::consts::ARCH {
            "aarch64" => "arm64",
            value => value,
        };
        if values[0] != local_os || values[1] != local_arch {
            bail!(
                "remote helper bootstrap requires the same platform; local={local_os}/{local_arch}, remote={}/{}",
                values[0],
                values[1]
            );
        }
        Ok(())
    }

    fn upload_helper(&self, local_exe: &Path) -> Result<()> {
        let version = env!("CARGO_PKG_VERSION");
        let script = format!(
            "set -eu; umask 077; d=\"$HOME/.cache/agent-sync/remotes/v{version}\"; mkdir -p \"$d\"; p=\"$d/agent-sync.partial.$$\"; trap 'rm -f \"$p\"' EXIT; cat > \"$p\"; chmod 700 \"$p\"; mv \"$p\" \"$d/agent-sync\"; trap - EXIT"
        );
        let mut child = Command::new(&self.ssh)
            .arg(&self.host)
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut source = File::open(local_exe)?;
        std::io::copy(
            &mut source,
            child.stdin.as_mut().context("remote helper upload stdin")?,
        )?;
        drop(child.stdin.take());
        let output = child.wait_with_output()?;
        if !output.status.success() {
            bail!(
                "remote helper upload failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }
}

fn prune_message(endpoint: &str, result: BackupPruneResult) -> String {
    format!(
        "backup retention: {endpoint} retained {} set(s), removed {} file(s) ({} bytes)",
        result.retained_sets, result.removed_files, result.removed_bytes
    )
}

fn parse_transfer_stats(output: &str) -> TransferStats {
    let mut result = TransferStats::default();
    for line in output.lines() {
        let Some((label, value)) = line.split_once(':') else {
            continue;
        };
        let value = value
            .trim()
            .chars()
            .take_while(|character| character.is_ascii_digit() || *character == ',')
            .filter(char::is_ascii_digit)
            .collect::<String>()
            .parse::<u64>()
            .ok();
        match label.trim().to_ascii_lowercase().as_str() {
            "total bytes sent" | "total sent" => result.wire_sent = value,
            "total bytes received" | "total received" => result.wire_received = value,
            "literal data" | "unmatched data" => result.literal_data = value,
            "matched data" => result.matched_data = value,
            _ => {}
        }
    }
    result
}

pub struct RemoteGuard {
    child: Child,
}

pub struct SyncGuards {
    _local: crate::state::SyncLock,
    _remote: RemoteGuard,
}

impl Drop for RemoteGuard {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}

fn remote_helper_path() -> String {
    format!(
        "$HOME/.cache/agent-sync/remotes/v{}/agent-sync",
        env!("CARGO_PKG_VERSION")
    )
}

fn validate_response(response: &Response) -> Result<()> {
    if response.protocol != PROTOCOL_VERSION {
        bail!(
            "remote protocol mismatch: local={}, remote={}",
            PROTOCOL_VERSION,
            response.protocol
        );
    }
    if !response.ok {
        bail!(
            "remote operation failed: {}",
            response.error.as_deref().unwrap_or("unknown error")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsync_transfers_are_compressed_and_optionally_limited() {
        let transport =
            SshTransport::new("mini".into(), "ssh".into(), "rsync".into(), Some(12_345));
        let mut command = Command::new("rsync");
        transport.configure_rsync(&mut command);
        let args = command
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args, ["-a", "--compress", "--bwlimit=12345"]);
    }

    #[test]
    fn parses_rsync_wire_and_delta_statistics() {
        let stats = parse_transfer_stats(
            "Unmatched data: 1,024 B\nMatched data: 8,192 B\nTotal sent: 321 B\nTotal received: 654 B\n",
        );
        assert_eq!(stats.literal_data, Some(1_024));
        assert_eq!(stats.matched_data, Some(8_192));
        assert_eq!(stats.wire_sent, Some(321));
        assert_eq!(stats.wire_received, Some(654));
    }
}
