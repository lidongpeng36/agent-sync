use crate::adapters::AgentKind;
use crate::core::ConflictStrategy;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "version")]
    version: u32,
    default_peer: Option<String>,
    conflict_strategy: Option<ConflictStrategy>,
    #[serde(default)]
    peers: BTreeMap<String, Peer>,
    #[serde(default)]
    agents: BTreeMap<String, Agent>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Peer {
    host: Option<String>,
    ssh: Option<String>,
    rsync: Option<String>,
    #[serde(default)]
    roots: BTreeMap<String, String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Agent {
    local_root: Option<String>,
    conflict_strategy: Option<ConflictStrategy>,
}

fn version() -> u32 {
    1
}

pub struct Resolved {
    pub host: String,
    pub ssh: String,
    pub rsync: String,
    pub local_root: PathBuf,
    pub remote_root: String,
}

impl Config {
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = explicit
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("AGENT_SYNC_CONFIG").map(PathBuf::from))
            .or_else(|| dirs::config_dir().map(|path| path.join("agent-sync/config.toml")));
        let Some(path) = path else {
            return Ok(Self {
                version: 1,
                ..Self::default()
            });
        };
        if !path.exists() {
            if explicit.is_some() {
                bail!("configuration does not exist: {}", path.display());
            }
            return Ok(Self {
                version: 1,
                ..Self::default()
            });
        }
        let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let config: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        if config.version != 1 {
            bail!("unsupported config version {}", config.version);
        }
        Ok(config)
    }

    pub fn resolve(
        &self,
        kind: AgentKind,
        peer_name: &str,
        local: Option<&Path>,
        remote: Option<&str>,
    ) -> Result<Resolved> {
        let key = kind.to_string();
        let peer = self.peers.get(peer_name);
        let host = peer
            .and_then(|p| p.host.clone())
            .unwrap_or_else(|| peer_name.to_owned());
        validate_host(&host)?;
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let default_local = match kind {
            AgentKind::Codex => home.join(".codex"),
            AgentKind::Claude => home.join(".claude"),
            AgentKind::Opencode => home.join(".local/share/opencode"),
        };
        let configured = self
            .agents
            .get(&key)
            .and_then(|a| a.local_root.as_deref())
            .map(expand_home)
            .transpose()?;
        let local_root = local
            .map(PathBuf::from)
            .or(configured)
            .unwrap_or(default_local);
        let default_remote = match kind {
            AgentKind::Codex => ".codex",
            AgentKind::Claude => ".claude",
            AgentKind::Opencode => ".local/share/opencode",
        };
        let remote_root = remote
            .map(str::to_owned)
            .or_else(|| peer.and_then(|p| p.roots.get(&key).cloned()))
            .unwrap_or_else(|| default_remote.to_owned());
        validate_remote_root(&remote_root)?;
        Ok(Resolved {
            host,
            ssh: peer
                .and_then(|p| p.ssh.clone())
                .unwrap_or_else(|| "/usr/bin/ssh".to_owned()),
            rsync: peer
                .and_then(|p| p.rsync.clone())
                .unwrap_or_else(|| "rsync".to_owned()),
            local_root,
            remote_root,
        })
    }

    pub fn peer_name(&self, command_line: Option<&str>) -> Result<String> {
        command_line
            .map(str::to_owned)
            .or_else(|| self.default_peer.clone())
            .filter(|peer| !peer.trim().is_empty())
            .context(
                "peer is required; pass <PEER> (for example: mini) or set default_peer = \"mini\" in the configuration file",
            )
    }

    pub fn conflict_strategy(&self, kind: AgentKind) -> ConflictStrategy {
        self.agents
            .get(&kind.to_string())
            .and_then(|agent| agent.conflict_strategy)
            .or(self.conflict_strategy)
            .unwrap_or_default()
    }
}

fn expand_home(value: &str) -> Result<PathBuf> {
    if value == "~" {
        return dirs::home_dir().context("cannot expand ~");
    }
    if let Some(suffix) = value.strip_prefix("~/") {
        return Ok(dirs::home_dir().context("cannot expand ~")?.join(suffix));
    }
    Ok(PathBuf::from(value))
}

fn validate_host(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.@-".contains(c))
    {
        bail!("unsafe SSH host or alias: {value:?}");
    }
    Ok(())
}

fn validate_remote_root(value: &str) -> Result<()> {
    if value.is_empty() || value.contains(['\n', '\r', '\0']) {
        bail!("unsafe remote root");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn host_validation() {
        assert!(validate_host("user@mini-1").is_ok());
        assert!(validate_host("mini;false").is_err());
    }

    #[test]
    fn conflict_strategy_defaults_to_ask_and_can_be_configured() {
        let default = Config::default();
        assert_eq!(
            default.conflict_strategy(AgentKind::Claude),
            ConflictStrategy::Ask
        );

        let configured: Config = toml::from_str(
            r#"
            version = 1
            [agents.claude]
            conflict_strategy = "local"
            "#,
        )
        .unwrap();
        assert_eq!(
            configured.conflict_strategy(AgentKind::Claude),
            ConflictStrategy::Local
        );

        let global: Config = toml::from_str(
            r#"
            version = 1
            conflict_strategy = "remote"
            "#,
        )
        .unwrap();
        assert_eq!(
            global.conflict_strategy(AgentKind::Claude),
            ConflictStrategy::Remote
        );

        let legacy: Config = toml::from_str(
            r#"
            version = 1
            [agents.claude]
            conflict_strategy = "merge"
            "#,
        )
        .unwrap();
        assert_eq!(
            legacy.conflict_strategy(AgentKind::Claude),
            ConflictStrategy::Ask
        );
    }

    #[test]
    fn command_line_peer_overrides_configured_default() {
        let configured: Config = toml::from_str(
            r#"
            version = 1
            default_peer = "mini"
            "#,
        )
        .unwrap();
        assert_eq!(configured.peer_name(None).unwrap(), "mini");
        assert_eq!(configured.peer_name(Some("dev")).unwrap(), "dev");
        assert!(Config::default().peer_name(None).is_err());
    }
}
