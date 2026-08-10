mod claude;
mod codex;

use crate::core::{OutputFormat, PlanReport, SyncOptions};
use crate::transport::SshTransport;
use anyhow::Result;
use clap::ValueEnum;
use std::fmt;
use std::path::Path;

pub use claude::ClaudePrepared;
pub use codex::CodexPrepared;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AgentKind {
    Codex,
    Claude,
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        })
    }
}

pub enum Prepared {
    Claude(ClaudePrepared),
    Codex(CodexPrepared),
}

impl Prepared {
    pub fn report(&self) -> &PlanReport {
        match self {
            Self::Claude(v) => &v.report,
            Self::Codex(v) => &v.report,
        }
    }
    pub fn print(&self, output: OutputFormat, local_root: &Path) -> Result<()> {
        match (self, output) {
            (Self::Claude(value), OutputFormat::Diff) => claude::print_diff(value, local_root),
            (Self::Codex(value), OutputFormat::Diff) => codex::print_diff(value, local_root),
            _ => self.report().print(output),
        }
    }
    pub fn blocked(&self) -> bool {
        !self.report().blockers.is_empty()
    }
}

pub trait Adapter {
    fn doctor(&self, local_root: &Path, remote_root: &str, transport: &SshTransport) -> Result<()>;
    fn prepare(
        &self,
        local_root: &Path,
        remote_root: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<Prepared>;
    fn resolve_interactive(&self, prepared: &mut Prepared, tty: bool) -> Result<()>;
    fn apply(
        &self,
        prepared: Prepared,
        local_root: &Path,
        remote_root: &str,
        transport: &SshTransport,
        options: &SyncOptions,
    ) -> Result<()>;
}

static CLAUDE: claude::ClaudeAdapter = claude::ClaudeAdapter;
static CODEX: codex::CodexAdapter = codex::CodexAdapter;

pub fn adapter_for(kind: AgentKind) -> &'static dyn Adapter {
    match kind {
        AgentKind::Claude => &CLAUDE,
        AgentKind::Codex => &CODEX,
    }
}
