pub(crate) mod claude;
pub(crate) mod codex;
pub(crate) mod opencode;

use crate::core::{OutputFormat, PlanReport, SyncOptions};
use crate::transport::SshTransport;
use anyhow::Result;
use clap::ValueEnum;
use std::fmt;
use std::path::Path;

pub use claude::ClaudePrepared;
pub use codex::CodexPrepared;
pub use opencode::OpenCodePrepared;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AgentKind {
    Codex,
    Claude,
    Opencode,
}

impl AgentKind {
    pub fn parse(value: &str) -> Option<Self> {
        Self::from_str(value, true).ok()
    }

    pub const fn all() -> [Self; 3] {
        [Self::Codex, Self::Claude, Self::Opencode]
    }
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Opencode => "opencode",
        })
    }
}

pub enum Prepared {
    Claude(ClaudePrepared),
    Codex(CodexPrepared),
    OpenCode(OpenCodePrepared),
}

impl Prepared {
    pub fn report(&self) -> &PlanReport {
        match self {
            Self::Claude(v) => &v.report,
            Self::Codex(v) => &v.report,
            Self::OpenCode(v) => &v.report,
        }
    }
    pub fn print(&self, output: OutputFormat, local_root: &Path) -> Result<()> {
        match (self, output) {
            (Self::Claude(value), OutputFormat::Diff) => claude::print_diff(value, local_root),
            (Self::Codex(value), OutputFormat::Diff) => codex::print_diff(value, local_root),
            (Self::OpenCode(value), OutputFormat::Diff) => opencode::print_diff(value, local_root),
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
static OPENCODE: opencode::OpenCodeAdapter = opencode::OpenCodeAdapter;

pub fn adapter_for(kind: AgentKind) -> &'static dyn Adapter {
    match kind {
        AgentKind::Claude => &CLAUDE,
        AgentKind::Codex => &CODEX,
        AgentKind::Opencode => &OPENCODE,
    }
}
