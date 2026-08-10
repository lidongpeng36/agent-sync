mod adapters;
mod config;
mod core;
mod transport;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use adapters::{AgentKind, adapter_for};
use config::Config;
use core::{OutputFormat, ResourceSelection, SyncOptions};
use transport::SshTransport;

#[derive(Parser)]
#[command(name = "agent-sync", version, about)]
struct Cli {
    #[arg(long, global = true)]
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
    Sync(SyncArgs),
    /// Check paths, commands, SSH connectivity, and adapter-specific dependencies.
    Doctor(TargetArgs),
    /// List built-in adapters and their capabilities.
    Adapters,
}

#[derive(Args)]
struct TargetArgs {
    agent: AgentKind,
    peer: String,
    #[arg(long)]
    local_root: Option<PathBuf>,
    #[arg(long)]
    remote_root: Option<String>,
    #[arg(long)]
    ssh: Option<String>,
    #[arg(long)]
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
}

#[derive(Args)]
struct SyncArgs {
    #[command(flatten)]
    target: TargetArgs,
    #[arg(long)]
    only: Option<Only>,
    /// Apply the displayed plan. Without this flag the command is read-only.
    #[arg(long)]
    apply: bool,
    /// Confirm a non-interactive apply. Does not resolve conflicts.
    #[arg(long, requires = "apply")]
    yes: bool,
    #[arg(long, default_value_t = 2.0, value_parser = parse_stability)]
    stability_seconds: f64,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = Format::Human)]
    format: Format,
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

fn resolve_target(args: &TargetArgs, config: &Config) -> Result<(PathBuf, String, SshTransport)> {
    let resolved = config.resolve(
        args.agent,
        &args.peer,
        args.local_root.as_deref(),
        args.remote_root.as_deref(),
    )?;
    let ssh = args.ssh.clone().unwrap_or_else(|| resolved.ssh.clone());
    let rsync = args.rsync.clone().unwrap_or_else(|| resolved.rsync.clone());
    Ok((
        resolved.local_root,
        resolved.remote_root,
        SshTransport::new(resolved.host, ssh, rsync),
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
    let config = Config::load(cli.config.as_deref())?;
    match cli.command {
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
            let (local, remote, transport) = resolve_target(&args, &config)?;
            let adapter = adapter_for(args.agent);
            adapter.doctor(&local, &remote, &transport)?;
            println!("doctor: {} on {} is ready", args.agent, args.peer);
            Ok(0)
        }
        Command::Sync(args) => {
            let (local_root, remote_root, transport) = resolve_target(&args.target, &config)?;
            let resources = match args.only {
                None => ResourceSelection::All,
                Some(Only::Sessions) => ResourceSelection::Sessions,
                Some(Only::Memory) => ResourceSelection::Memory,
            };
            let output = match args.format {
                Format::Human => OutputFormat::Human,
                Format::Json => OutputFormat::Json,
            };
            let options = SyncOptions {
                apply: args.apply,
                stability_seconds: args.stability_seconds,
                cache_dir: args.cache_dir.clone(),
                resources,
            };
            let adapter = adapter_for(args.target.agent);
            adapter.doctor(&local_root, &remote_root, &transport)?;
            let mut prepared = adapter.prepare(&local_root, &remote_root, &transport, &options)?;
            prepared.print(output)?;
            if !args.apply {
                return Ok(if prepared.blocked() { 2 } else { 0 });
            }
            adapter.resolve_interactive(&mut prepared, io::stdin().is_terminal())?;
            if prepared.blocked() {
                prepared.print(output)?;
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
