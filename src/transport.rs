use crate::core::private_dir;
use anyhow::{Context, Result, bail};
use std::path::Path;
use std::process::{Command, Output};

#[derive(Clone)]
pub struct SshTransport {
    pub host: String,
    pub ssh: String,
    pub rsync: String,
}

impl SshTransport {
    pub fn new(host: String, ssh: String, rsync: String) -> Self {
        Self { host, ssh, rsync }
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

    pub fn remote_ok(&self, script: &str) -> Result<bool> {
        let status = Command::new(&self.ssh)
            .arg(&self.host)
            .arg(script)
            .status()?;
        Ok(status.success())
    }

    pub fn pull(&self, remote_root: &str, local: &Path, filters: &[&str]) -> Result<()> {
        private_dir(local)?;
        let mut command = Command::new(&self.rsync);
        command.args(["-a", "--delete", "--delete-excluded", "--prune-empty-dirs"]);
        for filter in filters {
            command.arg(filter);
        }
        command.args(["-e", &self.ssh]);
        command
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
                "rsync pull failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub fn push(&self, source: &Path, remote_root: &str) -> Result<()> {
        let output = Command::new(&self.rsync)
            .args(["-a", "-e", &self.ssh])
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

    pub fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}
