mod adapters;
mod config;
mod core;
mod remote;
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
Examples:\n  agent-sync sync claude mini -o sessions\n  agent-sync s claude -f diff       # uses default_peer\n  agent-sync s codex mini -a"
    )]
    Sync(SyncArgs),
    /// Check paths, commands, SSH connectivity, and adapter-specific dependencies.
    #[command(
        visible_alias = "d",
        after_help = "Examples:\n  agent-sync doctor claude mini\n  agent-sync d claude       # uses default_peer"
    )]
    Doctor(TargetArgs),
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
    /// Agent adapter to use.
    agent: AgentKind,
    /// SSH peer name or alias; optional when default_peer is configured.
    peer: Option<String>,
    #[arg(short = 'l', long)]
    local_root: Option<PathBuf>,
    #[arg(short = 'r', long)]
    remote_root: Option<String>,
    #[arg(short = 'S', long)]
    ssh: Option<String>,
    #[arg(short = 'R', long)]
    rsync: Option<String>,
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
    #[arg(short = 'C', long)]
    cache_dir: Option<PathBuf>,
    #[arg(short = 'f', long, value_enum, default_value_t = Format::Human)]
    format: Format,
    /// Resolve content conflicts using local, remote, or resource-aware merge behavior.
    #[arg(short = 's', long, value_enum)]
    conflict_strategy: Option<ConflictStrategy>,
}

fn parse_stability(value: &str) -> Result<f64, String> {
    let value: f64 = value.parse().map_err(|_| "must be a number".to_owned())?;
    if (0.0..=60.0).contains(&value) {
        Ok(value)
    } else {
        Err("must be between 0 and 60".to_owned())
    }
}

#[derive(Serialize)]
struct AdapterInfo<'a> {
    name: &'a str,
    resources: [&'a str; 2],
}

fn resolve_target(
    args: &TargetArgs,
    config: &Config,
) -> Result<(PathBuf, String, SshTransport, String)> {
    let peer_name = config.peer_name(args.peer.as_deref())?;
    let resolved = config.resolve(
        args.agent,
        &peer_name,
        args.local_root.as_deref(),
        args.remote_root.as_deref(),
    )?;
    let ssh = args.ssh.clone().unwrap_or_else(|| resolved.ssh.clone());
    let rsync = args.rsync.clone().unwrap_or_else(|| resolved.rsync.clone());
    Ok((
        resolved.local_root,
        resolved.remote_root,
        SshTransport::new(resolved.host, ssh, rsync),
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
    print!("Type apply to continue: ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if answer.trim() != "apply" {
        return Err(Cancelled.into());
    }
    Ok(())
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
                    resources: ["sessions", "memory"],
                },
                AdapterInfo {
                    name: "claude",
                    resources: ["sessions", "memory"],
                },
            ];
            println!("{}", serde_json::to_string_pretty(&values)?);
            Ok(0)
        }
        Command::Doctor(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (local, remote, transport, peer) = resolve_target(&args, &config)?;
            let adapter = adapter_for(args.agent);
            adapter.doctor(&local, &remote, &transport)?;
            println!("doctor: {} on {} is ready", args.agent, peer);
            Ok(0)
        }
        Command::Sync(args) => {
            let config = Config::load(cli.config.as_deref())?;
            let (local_root, remote_root, transport, _) = resolve_target(&args.target, &config)?;
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
                    .unwrap_or_else(|| config.conflict_strategy(args.target.agent)),
            };
            let adapter = adapter_for(args.target.agent);
            adapter.doctor(&local_root, &remote_root, &transport)?;
            let mut prepared = adapter.prepare(&local_root, &remote_root, &transport, &options)?;
            prepared.print(output, &local_root)?;
            if !args.apply {
                return Ok(if prepared.blocked() { 2 } else { 0 });
            }
            adapter.resolve_interactive(&mut prepared, io::stdin().is_terminal())?;
            if prepared.blocked() {
                prepared.print(output, &local_root)?;
                return Ok(2);
            }
            confirm(&args)?;
            adapter
                .apply(prepared, &local_root, &remote_root, &transport, &options)
                .with_context(|| format!("{} synchronization failed", args.target.agent))?;
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
}
