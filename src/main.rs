mod adapters;
mod archive;
mod config;
mod core;
mod remote;
mod state;
mod transport;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use adapters::{AgentKind, adapter_for};
use config::Config;
use core::{ConflictStrategy, OutputFormat, ResourceSelection, SyncOptions};
use transport::SshTransport;

#[derive(Parser)]
#[command(name = "agent-sync", version, about)]
struct Cli {
    #[arg(short = 'c', long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug)]
struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cancelled by user")
    }
}

impl std::error::Error for Cancelled {}

#[derive(Subcommand)]
enum Command {
    /// Preview or apply a bidirectional synchronization.
    #[command(
        visible_alias = "s",
        after_help = "Recommended order: agent-sync sync <AGENT> [PEER] [OPTIONS]\n\
Options may also appear before or between positional arguments.\n\n\
Examples:\n  agent-sync sync claude mini -o sessions\n  agent-sync s claude -f diff       # uses default_peer\n  agent-sync s codex mini -a\n  agent-sync s mini                 # infers agent from current directory"
    )]
    Sync(SyncArgs),
    /// Check paths, commands, SSH connectivity, and adapter-specific dependencies.
    #[command(
        visible_alias = "d",
        after_help = "Examples:\n  agent-sync doctor claude mini\n  agent-sync d claude       # uses default_peer"
    )]
    Doctor(TargetArgs),
    /// Export portable local sessions/memory into one checksummed archive.
    Export(ExportArgs),
    /// Validate, preview, or apply one portable local archive.
    Import(ImportArgs),
    /// List built-in adapters and their capabilities.
    #[command(visible_alias = "a")]
    Adapters,
    /// Internal typed RPC endpoint used over SSH.
    #[command(name = "__remote", hide = true)]
    Remote(RemoteArgs),
}

#[derive(Args)]
struct RemoteArgs {
    #[arg(long)]
    protocol: u32,
}

#[derive(Args)]
struct TargetArgs {
    /// Agent name, or peer when agent is inferred from the current directory.
    agent_or_peer: Option<String>,
    /// SSH peer name or alias when the agent is explicit.
    peer: Option<String>,
    #[arg(short = 'l', long)]
    local_root: Option<PathBuf>,
    #[arg(short = 'r', long)]
    remote_root: Option<String>,
    #[arg(short = 'S', long)]
    ssh: Option<String>,
    #[arg(short = 'R', long)]
    rsync: Option<String>,
    /// Limit rsync traffic in KiB/s so other SSH sessions remain responsive.
    #[arg(long, value_parser = parse_bandwidth_limit)]
    bwlimit: Option<u64>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Only {
    Sessions,
    Memory,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Human,
    Json,
    Diff,
}

#[derive(Args)]
struct SyncArgs {
    #[command(flatten)]
    target: TargetArgs,
    #[arg(short = 'o', long)]
    only: Option<Only>,
    /// Apply the displayed plan. Without this flag the command is read-only.
    #[arg(short = 'a', long)]
    apply: bool,
    /// Confirm a non-interactive apply. Does not resolve conflicts.
    #[arg(short = 'y', long, requires = "apply")]
    yes: bool,
    #[arg(short = 't', long, default_value_t = 2.0, value_parser = parse_stability)]
    stability_seconds: f64,
    /// Directory for per-peer checkpoints, scan hashes, node identity, and sync locks.
    #[arg(short = 'C', long)]
    cache_dir: Option<PathBuf>,
    #[arg(short = 'f', long, value_enum, default_value_t = Format::Human)]
    format: Format,
    /// Resolve irreconcilable conflicts by asking or preferring one side.
    #[arg(short = 's', long, value_enum)]
    conflict_strategy: Option<ConflictStrategy>,
}

#[derive(Args)]
struct ArchiveTargetArgs {
    /// Archive file, or agent name when followed by FILE.
    #[arg(value_name = "AGENT_OR_FILE")]
    agent_or_file: String,
    /// Archive file when AGENT is explicit.
    #[arg(value_name = "FILE")]
    file: Option<PathBuf>,
    #[arg(short = 'l', long)]
    local_root: Option<PathBuf>,
}

#[derive(Args)]
struct ExportArgs {
    #[command(flatten)]
    target: ArchiveTargetArgs,
    #[arg(short = 'o', long)]
    only: Option<Only>,
    /// Replace an existing output archive.
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
struct ImportArgs {
    #[command(flatten)]
    target: ArchiveTargetArgs,
    /// Apply the validated import. Without this flag the command is read-only.
    #[arg(short = 'a', long)]
    apply: bool,
    /// Confirm a non-interactive import.
    #[arg(short = 'y', long, requires = "apply")]
    yes: bool,
    /// Allow differing existing items to be overwritten.
    #[arg(long, requires = "apply")]
    force: bool,
}

fn parse_stability(value: &str) -> Result<f64, String> {
    let value: f64 = value.parse().map_err(|_| "must be a number".to_owned())?;
    if (0.0..=60.0).contains(&value) {
        Ok(value)
    } else {
        Err("must be between 0 and 60".to_owned())
    }
}

fn parse_bandwidth_limit(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| "must be a positive number of KiB/s".to_owned())
}

#[derive(Serialize)]
struct AdapterInfo<'a> {
    name: &'a str,
    resources: &'a [&'a str],
}

fn resolve_target(
    args: &TargetArgs,
    config: &Config,
) -> Result<(AgentKind, PathBuf, String, SshTransport, String)> {
    let current_dir = std::env::current_dir().context("determine current directory")?;
    let (agent, peer) = match (&args.agent_or_peer, &args.peer) {
        (Some(agent), Some(peer)) => (
            AgentKind::parse(agent).with_context(|| {
                format!("unknown agent {agent:?}; expected codex, claude, or opencode")
            })?,
            Some(peer.as_str()),
        ),
        (Some(value), None) => match AgentKind::parse(value) {
            Some(agent) => (agent, None),
            None => (config.infer_agent(&current_dir)?, Some(value.as_str())),
        },
        (None, Some(_)) => unreachable!("peer cannot be present without the first positional"),
        (None, None) => (config.infer_agent(&current_dir)?, None),
    };
    let peer_name = config.peer_name(peer)?;
    let resolved = config.resolve(
        agent,
        &peer_name,
        args.local_root.as_deref(),
        args.remote_root.as_deref(),
    )?;
    let ssh = args.ssh.clone().unwrap_or_else(|| resolved.ssh.clone());
    let rsync = args.rsync.clone().unwrap_or_else(|| resolved.rsync.clone());
    let bandwidth_limit_kbps = args.bwlimit.or(resolved.bandwidth_limit_kbps);
    Ok((
        agent,
        resolved.local_root,
        resolved.remote_root,
        SshTransport::new(resolved.host, ssh, rsync, bandwidth_limit_kbps),
        peer_name,
    ))
}

fn confirm(args: &SyncArgs) -> Result<()> {
    if !args.apply {
        return Ok(());
    }
    if args.yes {
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        bail!("--apply requires a TTY or explicit --yes");
    }
    loop {
        print!("Apply these changes? [Y/n] ");
        io::stdout().flush()?;
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer)? == 0 {
            return Err(Cancelled.into());
        }
        match parse_confirmation(&answer) {
            Some(true) => return Ok(()),
            Some(false) => return Err(Cancelled.into()),
            None => println!("Please answer y or n."),
        }
    }
}

fn parse_confirmation(answer: &str) -> Option<bool> {
    match answer.trim().to_ascii_lowercase().as_str() {
        "" | "y" | "yes" => Some(true),
        "n" | "no" => Some(false),
        _ => None,
    }
}

fn archive_target(
    args: &ArchiveTargetArgs,
    config: &Config,
) -> Result<(AgentKind, PathBuf, PathBuf)> {
    let current_dir = std::env::current_dir().context("determine current directory")?;
    let (agent, file) = match &args.file {
        None => (
            config.infer_agent(&current_dir)?,
            PathBuf::from(&args.agent_or_file),
        ),
        Some(file) => (
            AgentKind::parse(&args.agent_or_file).with_context(|| {
                format!(
                    "unknown agent {:?}; expected codex, claude, or opencode",
                    args.agent_or_file
                )
            })?,
            file.clone(),
        ),
    };
    Ok((
        agent,
        config.local_root(agent, args.local_root.as_deref())?,
        file,
    ))
}

fn confirm_import(args: &ImportArgs) -> Result<()> {
    if !args.apply || args.yes {
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        bail!("--apply requires a TTY or explicit --yes");
    }
    loop {
        print!("Apply this validated import? [Y/n] ");
        io::stdout().flush()?;
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer)? == 0 {
            return Err(Cancelled.into());
        }
        match parse_confirmation(&answer) {
            Some(true) => return Ok(()),
            Some(false) => return Err(Cancelled.into()),
            None => println!("Please answer y or n."),
        }
    }
}

fn run() -> Result<i32> {
    let cli = Cli::parse();
    match cli.command {
        Command::Remote(args) => {
            remote::serve(args.protocol)?;
            Ok(0)
        }
        Command::Adapters => {
            let values = [
                AdapterInfo {
                    name: "codex",
                    resources: &["sessions", "memory"],
                },
                AdapterInfo {
                    name: "claude",
                    resources: &["sessions", "memory"],
                },
                AdapterInfo {
                    name: "opencode",
                    resources: &["sessions"],
                },
            ];
            println!("{}", serde_json::to_string_pretty(&values)?);
            Ok(0)
        }
        Command::Export(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (agent, local_root, output) = archive_target(&args.target, &config)?;
            let resources = match args.only {
                None => ResourceSelection::All,
                Some(Only::Sessions) => ResourceSelection::Sessions,
                Some(Only::Memory) => ResourceSelection::Memory,
            };
            archive::export(agent, &local_root, &output, resources, args.force)?;
            Ok(0)
        }
        Command::Import(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (agent, local_root, input) = archive_target(&args.target, &config)?;
            let validated = archive::validate(&input)?;
            if validated.agent != agent {
                bail!(
                    "archive agent is {}, but target agent is {agent}",
                    validated.agent
                );
            }
            let plan = archive::plan_import(&validated, &local_root)?;
            archive::print_plan(&input, &validated, &plan)?;
            if !args.apply {
                return Ok(if plan.conflicts.is_empty() { 0 } else { 2 });
            }
            if !plan.conflicts.is_empty() && !args.force {
                bail!("import has conflicts; pass --force with --apply to overwrite them");
            }
            confirm_import(&args)?;
            archive::apply_import(&validated, &local_root, args.force)?;
            Ok(0)
        }
        Command::Doctor(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (agent, local, remote, transport, peer) = resolve_target(&args, &config)?;
            let adapter = adapter_for(agent);
            adapter.doctor(&local, &remote, &transport)?;
            println!("doctor: {agent} on {peer} is ready");
            Ok(0)
        }
        Command::Sync(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (agent, local_root, remote_root, transport, _) =
                resolve_target(&args.target, &config)?;
            let resources = match args.only {
                None => ResourceSelection::All,
                Some(Only::Sessions) => ResourceSelection::Sessions,
                Some(Only::Memory) => ResourceSelection::Memory,
            };
            let output = match args.format {
                Format::Human => OutputFormat::Human,
                Format::Json => OutputFormat::Json,
                Format::Diff => OutputFormat::Diff,
            };
            let options = SyncOptions {
                apply: args.apply,
                stability_seconds: args.stability_seconds,
                cache_dir: args.cache_dir.clone(),
                resources,
                conflict_strategy: args
                    .conflict_strategy
                    .unwrap_or_else(|| config.conflict_strategy(agent)),
            };
            let adapter = adapter_for(agent);
            adapter.doctor(&local_root, &remote_root, &transport)?;
            let mut prepared = adapter.prepare(&local_root, &remote_root, &transport, &options)?;
            prepared.print(output, &local_root)?;
            if !args.apply {
                return Ok(if prepared.blocked() { 2 } else { 0 });
            }
            let required_resolution = prepared.blocked();
            adapter.resolve_interactive(&mut prepared, io::stdin().is_terminal())?;
            if prepared.blocked() {
                return Ok(2);
            }
            if required_resolution {
                println!("resolved plan:");
                prepared.print(output, &local_root)?;
            }
            confirm(&args)?;
            adapter
                .apply(prepared, &local_root, &remote_root, &transport, &options)
                .with_context(|| format!("{agent} synchronization failed"))?;
            Ok(0)
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code as u8),
        Err(error) => {
            eprintln!("agent-sync: error: {error:#}");
            if error.downcast_ref::<Cancelled>().is_some() {
                ExitCode::from(3)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_conflict_strategy_flag_is_accepted() {
        let cli =
            Cli::try_parse_from(["agent-sync", "sync", "claude", "mini", "-s", "remote"]).unwrap();
        let Command::Sync(args) = cli.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.conflict_strategy, Some(ConflictStrategy::Remote));

        let ask =
            Cli::try_parse_from(["agent-sync", "sync", "claude", "mini", "-s", "ask"]).unwrap();
        let Command::Sync(args) = ask.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.conflict_strategy, Some(ConflictStrategy::Ask));

        let legacy =
            Cli::try_parse_from(["agent-sync", "sync", "claude", "mini", "-s", "merge"]).unwrap();
        let Command::Sync(args) = legacy.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.conflict_strategy, Some(ConflictStrategy::Ask));
    }

    #[test]
    fn command_alias_and_common_short_options_are_accepted() {
        let cli = Cli::try_parse_from([
            "agent-sync",
            "s",
            "claude",
            "-o",
            "memory",
            "-a",
            "-y",
            "-t",
            "1",
            "-C",
            "/tmp/agent-sync-test-cache",
            "-f",
            "diff",
            "-s",
            "local",
            "-l",
            "/tmp/local",
            "-r",
            ".claude",
            "-S",
            "ssh",
            "-R",
            "rsync",
        ])
        .unwrap();
        let Command::Sync(args) = cli.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.target.agent_or_peer.as_deref(), Some("claude"));
        assert!(args.target.peer.is_none());
        assert!(args.apply);
        assert!(args.yes);
        assert!(matches!(args.format, Format::Diff));
        assert_eq!(args.conflict_strategy, Some(ConflictStrategy::Local));
    }

    #[test]
    fn doctor_and_adapters_have_short_aliases() {
        let doctor = Cli::try_parse_from(["agent-sync", "d", "claude", "mini"]).unwrap();
        assert!(matches!(doctor.command, Command::Doctor(_)));
        let adapters = Cli::try_parse_from(["agent-sync", "a"]).unwrap();
        assert!(matches!(adapters.command, Command::Adapters));
    }

    #[test]
    fn options_may_surround_positional_arguments() {
        let cli = Cli::try_parse_from([
            "agent-sync",
            "sync",
            "-f",
            "json",
            "claude",
            "-o",
            "memory",
            "mini",
            "-c",
            "/tmp/config.toml",
        ])
        .unwrap();
        let Command::Sync(args) = cli.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.target.peer.as_deref(), Some("mini"));
        assert!(matches!(args.format, Format::Json));
    }

    #[test]
    fn apply_confirmation_defaults_to_yes_and_accepts_common_answers() {
        assert_eq!(parse_confirmation("\n"), Some(true));
        assert_eq!(parse_confirmation("y"), Some(true));
        assert_eq!(parse_confirmation("YES"), Some(true));
        assert_eq!(parse_confirmation("n"), Some(false));
        assert_eq!(parse_confirmation("No"), Some(false));
        assert_eq!(parse_confirmation("apply"), None);
    }

    #[test]
    fn target_positionals_allow_omitting_agent() {
        let inferred = Cli::try_parse_from(["agent-sync", "sync", "mini"]).unwrap();
        let Command::Sync(args) = inferred.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.target.agent_or_peer.as_deref(), Some("mini"));
        assert!(args.target.peer.is_none());

        let explicit = Cli::try_parse_from(["agent-sync", "sync", "codex", "mini"]).unwrap();
        let Command::Sync(args) = explicit.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.target.agent_or_peer.as_deref(), Some("codex"));
        assert_eq!(args.target.peer.as_deref(), Some("mini"));
    }

    #[test]
    fn bandwidth_limit_must_be_positive() {
        let cli = Cli::try_parse_from(["agent-sync", "sync", "codex", "mini", "--bwlimit", "8192"])
            .unwrap();
        let Command::Sync(args) = cli.command else {
            panic!("expected sync command");
        };
        assert_eq!(args.target.bwlimit, Some(8192));
        assert!(
            Cli::try_parse_from(["agent-sync", "sync", "codex", "mini", "--bwlimit", "0",])
                .is_err()
        );
    }
}
