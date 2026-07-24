// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Layered configuration.
//!
//! Precedence (highest wins): CLI flags > environment > `./saladfingers.toml` >
//! `~/.config/saladfingers/config.toml`. The API key is resolved separately from
//! `SALAD_API_KEY` > `SALAD_API_KEY_FILE` > `~/.config/saladfingers/api-key`.
//!
//! Secrets never live in these files — storage/registry credentials are referenced
//! by the name of the environment variable that holds them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use saladfingers_api::{SaladClient, SaladClientConfig, Secret};
use serde::{Deserialize, Serialize};

/// Fully resolved configuration.
pub struct Config {
    /// Organization name.
    pub organization: String,
    /// Project name.
    pub project: String,
    /// API key.
    pub api_key: Secret,
    /// Bulk-storage backend, if configured.
    pub storage: Option<StorageConfig>,
    /// Container registry, if configured.
    pub registry: Option<RegistryConfig>,
    /// Global defaults.
    pub defaults: Defaults,
    /// Named run profiles.
    pub profiles: BTreeMap<String, Profile>,
}

/// Global defaults from the `[salad]` section.
#[derive(Debug, Clone, Default)]
pub struct Defaults {
    /// Default scheduling priority.
    pub priority: Option<String>,
    /// Default country allow-list.
    pub country_codes: Vec<String>,
}

/// S3-compatible storage backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// Region (e.g. `auto` for R2).
    #[serde(default)]
    pub region: Option<String>,
    /// Bucket name.
    pub bucket: String,
    /// Whether to use path-style addressing.
    #[serde(default)]
    pub path_style: bool,
    /// Name of the env var holding the access key.
    #[serde(default)]
    pub access_key_env: Option<String>,
    /// Name of the env var holding the secret key.
    #[serde(default)]
    pub secret_key_env: Option<String>,
    /// Maximum presigned-URL blocks per artifact. Each block is 4 GiB, so this sets the size
    /// ceiling for any single input, output, or checkpoint (`max_artifact_parts × 4 GiB`) and
    /// the cap the output collector enforces on an untrusted result envelope. Defaults to 64
    /// (256 GiB); raise it for larger artifacts. Clamped to a 4096-block (16 TiB) maximum.
    #[serde(default)]
    pub max_artifact_parts: Option<u32>,
}

impl StorageConfig {
    /// The effective per-artifact part ceiling: the configured `max_artifact_parts` clamped to
    /// a sane range, or [`crate::spec::DEFAULT_MAX_PARTS`] when unset.
    #[must_use]
    pub fn effective_max_parts(&self) -> u32 {
        self.max_artifact_parts
            .unwrap_or(crate::spec::DEFAULT_MAX_PARTS)
            .clamp(1, crate::spec::MAX_ARTIFACT_PARTS_LIMIT)
    }
}

/// Container registry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Registry base (e.g. `registry.example.com/org/salad`).
    pub base: String,
    /// Auth kind: `basic` or `docker_hub`.
    #[serde(default)]
    pub auth_kind: Option<String>,
    /// Name of the env var holding the username (used to pull images at deploy time).
    #[serde(default)]
    pub username_env: Option<String>,
    /// Name of the env var holding the password/token (used to pull images).
    #[serde(default)]
    pub password_env: Option<String>,
    /// Name of the env var holding the push username. Optional: `image push` falls
    /// back to `SALADFINGERS_REGISTRY_PUSH_USER` then to `username_env` if unset.
    #[serde(default)]
    pub push_username_env: Option<String>,
    /// Name of the env var holding the push password/token. Optional: `image push`
    /// falls back to `SALADFINGERS_REGISTRY_PUSH_PASS` then to `password_env`.
    #[serde(default)]
    pub push_password_env: Option<String>,
}

/// A named run profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Profile {
    /// Image reference or a `saladfingers.images.<name>`.
    #[serde(default)]
    pub image: Option<String>,
    /// GPU classes (names or UUIDs).
    #[serde(default)]
    pub gpu_classes: Vec<String>,
    /// vCPU count.
    #[serde(default)]
    pub cpu: Option<u32>,
    /// RAM in GB.
    #[serde(default)]
    pub memory_gb: Option<u32>,
    /// Disk in GB.
    #[serde(default)]
    pub disk_gb: Option<u64>,
    /// `/dev/shm` size in MB.
    #[serde(default)]
    pub shm_mb: Option<u32>,
    /// Extra environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Priority.
    #[serde(default)]
    pub priority: Option<String>,
    /// Max duration (e.g. `20m`).
    #[serde(default)]
    pub max_duration: Option<String>,
    /// Number of shards.
    #[serde(default)]
    pub replicas: Option<u32>,
    /// Minimum download throughput gate.
    #[serde(default)]
    pub min_download_mbps: Option<f64>,
    /// Minimum upload throughput gate.
    #[serde(default)]
    pub min_upload_mbps: Option<f64>,
    /// Artifact staging (`[profiles.<name>.artifacts]`). CLI `--input`/`--output`
    /// flags override the respective list wholesale.
    #[serde(default)]
    pub artifacts: Option<ProfileArtifacts>,
    /// Checkpoint watcher (`[profiles.<name>.checkpoint]`). `--checkpoint` overrides.
    #[serde(default)]
    pub checkpoint: Option<ProfileCheckpoint>,
}

/// Artifact staging lists for a profile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfileArtifacts {
    /// Inputs `SRC[:DST]` staged in before the run.
    #[serde(default)]
    pub pull: Vec<String>,
    /// Outputs `GLOB[:NAME]` collected after the run.
    #[serde(default)]
    pub push: Vec<String>,
}

/// Checkpoint watcher settings for a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileCheckpoint {
    /// Checkpoint directory, relative to the job workdir (absolute also accepted).
    #[serde(alias = "glob")]
    pub dir: String,
    /// Scan interval in seconds.
    #[serde(default = "default_ckpt_interval")]
    pub interval_secs: u64,
    /// A checkpoint uploads once no file changed within this many seconds.
    #[serde(default = "default_ckpt_quiesce")]
    pub quiesce_secs: u64,
}

fn default_ckpt_interval() -> u64 {
    30
}

fn default_ckpt_quiesce() -> u64 {
    15
}

/// Deserialized shape of a config file.
#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    salad: Option<SaladSection>,
    #[serde(default)]
    storage: Option<StorageConfig>,
    #[serde(default)]
    registry: Option<RegistryConfig>,
    #[serde(default)]
    profiles: BTreeMap<String, Profile>,
}

#[derive(Debug, Default, Deserialize)]
struct SaladSection {
    #[serde(default)]
    organization: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    country_codes: Vec<String>,
}

impl Config {
    /// Load and layer configuration.
    ///
    /// # Errors
    /// Returns an error if a config file cannot be parsed, or if the organization,
    /// project, or API key cannot be resolved.
    pub fn load(
        config_flag: Option<&Path>,
        org_flag: Option<&str>,
        project_flag: Option<&str>,
    ) -> Result<Self> {
        let global = load_file(global_config_path(config_flag).as_deref())?;
        let project_file = load_file(Some(Path::new("saladfingers.toml")))?;
        let merged = merge(global, project_file);
        let salad = merged.salad.unwrap_or_default();

        let organization = org_flag
            .map(str::to_string)
            .or_else(|| non_empty_env("SALADFINGERS_ORG"))
            .or(salad.organization)
            .context("organization not set (pass --org, set SALADFINGERS_ORG, or add [salad] organization to your config)")?;
        let project = project_flag
            .map(str::to_string)
            .or_else(|| non_empty_env("SALADFINGERS_PROJECT"))
            .or(salad.project)
            .context("project not set (pass --project, set SALADFINGERS_PROJECT, or add [salad] project to your config)")?;
        let api_key = resolve_api_key()?;

        Ok(Self {
            organization,
            project,
            api_key,
            storage: merged.storage,
            registry: merged.registry,
            defaults: Defaults {
                priority: salad.priority,
                country_codes: salad.country_codes,
            },
            profiles: merged.profiles,
        })
    }

    /// Build an API client from this config.
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be built.
    pub fn client(&self) -> Result<SaladClient> {
        let scfg = SaladClientConfig::new(
            self.api_key.clone(),
            self.organization.clone(),
            self.project.clone(),
        );
        SaladClient::new(scfg).context("building API client")
    }

    /// Look up a profile by name.
    ///
    /// # Errors
    /// Returns an error naming the available profiles if the name is unknown.
    pub fn profile(&self, name: &str) -> Result<&Profile> {
        self.profiles.get(name).with_context(|| {
            let available: Vec<&str> = self.profiles.keys().map(String::as_str).collect();
            format!(
                "unknown profile '{name}' (available: {})",
                available.join(", ")
            )
        })
    }
}

fn merge(base: FileConfig, over: FileConfig) -> FileConfig {
    let mut profiles = base.profiles;
    profiles.extend(over.profiles);
    FileConfig {
        salad: merge_salad(base.salad, over.salad),
        storage: over.storage.or(base.storage),
        registry: over.registry.or(base.registry),
        profiles,
    }
}

fn merge_salad(base: Option<SaladSection>, over: Option<SaladSection>) -> Option<SaladSection> {
    match (base, over) {
        (b, None) => b,
        (None, o) => o,
        (Some(b), Some(o)) => Some(SaladSection {
            organization: o.organization.or(b.organization),
            project: o.project.or(b.project),
            priority: o.priority.or(b.priority),
            country_codes: if o.country_codes.is_empty() {
                b.country_codes
            } else {
                o.country_codes
            },
        }),
    }
}

fn load_file(path: Option<&Path>) -> Result<FileConfig> {
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    match std::fs::read_to_string(path) {
        Ok(content) => {
            toml::from_str(&content).with_context(|| format!("parsing config {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(FileConfig::default()),
        Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
    }
}

fn global_config_path(config_flag: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = config_flag {
        return Some(path.to_path_buf());
    }
    if let Some(path) = non_empty_env("SALADFINGERS_CONFIG") {
        return Some(PathBuf::from(path));
    }
    config_home().map(|base| base.join("saladfingers").join("config.toml"))
}

fn config_home() -> Option<PathBuf> {
    non_empty_env("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| non_empty_env("HOME").map(|h| PathBuf::from(h).join(".config")))
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn resolve_api_key() -> Result<Secret> {
    if let Some(key) = non_empty_env("SALAD_API_KEY") {
        return Ok(Secret::new(key));
    }
    if let Some(path) = non_empty_env("SALAD_API_KEY_FILE") {
        return read_key_file(Path::new(&path));
    }
    if let Some(path) = default_key_path()
        && path.exists()
    {
        return read_key_file(&path);
    }
    bail!(
        "no API key found (set SALAD_API_KEY, SALAD_API_KEY_FILE, or write ~/.config/saladfingers/api-key with mode 0600)"
    )
}

fn read_key_file(path: &Path) -> Result<Secret> {
    if let Some(mode) = loose_key_permissions(path) {
        eprintln!(
            "warning: API key file {} is readable by other users (mode {mode:04o}); run: chmod 600 {}",
            path.display(),
            path.display()
        );
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading API key file {}", path.display()))?;
    let key = raw.trim();
    if key.is_empty() {
        bail!("API key file {} is empty", path.display());
    }
    Ok(Secret::new(key))
}

/// The permission bits of the API key file when it is group- or world-accessible
/// (`mode & 0o077 != 0`), else `None`.
///
/// `resolve_api_key`'s own error message tells users to hand-write the key file, so the
/// realistic path is `echo "$KEY" > ~/.config/saladfingers/api-key`, which lands at 0644
/// under the usual umask — any other local user can then read a key that grants full
/// control of the SaladCloud org. Callers only warn: refusing would break CI and
/// container setups that legitimately run with odd modes.
///
/// Returns `None` on non-Unix (no POSIX mode) and when the metadata cannot be read —
/// an unreadable file surfaces as a proper error from the subsequent read.
fn loose_key_permissions(path: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
        (mode & 0o077 != 0).then_some(mode)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// The default API key path (`~/.config/saladfingers/api-key`).
#[must_use]
pub fn default_key_path() -> Option<PathBuf> {
    config_home().map(|base| base.join("saladfingers").join("api-key"))
}

/// The default global config path (`~/.config/saladfingers/config.toml`).
#[must_use]
pub fn default_config_path() -> Option<PathBuf> {
    config_home().map(|base| base.join("saladfingers").join("config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layering_prefers_flags_then_env_then_file() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            r#"
[salad]
organization = "file-org"
project = "file-proj"
priority = "low"

[profiles.kernels]
image = "kernel-test"
gpu_classes = ["rtx 4090"]
cpu = 8
"#,
        )
        .unwrap();

        // SAFETY: single-threaded test.
        unsafe {
            std::env::set_var("SALAD_API_KEY", "test-key");
            std::env::remove_var("SALADFINGERS_ORG");
            std::env::remove_var("SALADFINGERS_PROJECT");
        }

        // File values used when no flag/env.
        let cfg = Config::load(Some(&cfg_path), None, None).unwrap();
        assert_eq!(cfg.organization, "file-org");
        assert_eq!(cfg.project, "file-proj");
        assert_eq!(cfg.defaults.priority.as_deref(), Some("low"));
        assert!(cfg.profile("kernels").is_ok());
        assert_eq!(cfg.profile("kernels").unwrap().cpu, Some(8));
        assert!(cfg.profile("nope").is_err());

        // Flag overrides the file.
        let cfg = Config::load(Some(&cfg_path), Some("flag-org"), None).unwrap();
        assert_eq!(cfg.organization, "flag-org");
    }

    #[test]
    fn profile_checkpoint_and_artifacts_sections_parse() {
        // These sections are documented in saladfingers.toml.example; they must parse
        // into the profile (not be silently dropped as unknown keys).
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            r#"
[salad]
organization = "o"
project = "p"

[profiles.train]
image = "trainer"
gpu_classes = ["rtx 4090"]
shm_mb = 2048

[profiles.train.checkpoint]
dir = "ckpts"
interval_secs = 120

[profiles.train.artifacts]
push = ["ckpts/latest/**:model"]
pull = ["train.py"]
"#,
        )
        .unwrap();
        // SAFETY: single-threaded test.
        unsafe {
            std::env::set_var("SALAD_API_KEY", "test-key");
        }
        let cfg = Config::load(Some(&cfg_path), None, None).unwrap();
        let p = cfg.profile("train").unwrap();
        assert_eq!(p.shm_mb, Some(2048));
        let ckpt = p.checkpoint.as_ref().expect("checkpoint section parsed");
        assert_eq!(ckpt.dir, "ckpts");
        assert_eq!(ckpt.interval_secs, 120);
        assert_eq!(ckpt.quiesce_secs, 15, "default quiesce");
        let art = p.artifacts.as_ref().expect("artifacts section parsed");
        assert_eq!(art.push, vec!["ckpts/latest/**:model"]);
        assert_eq!(art.pull, vec!["train.py"]);
    }

    /// Write `contents` to `dir/api-key` with an exact Unix mode.
    #[cfg(unix)]
    fn write_key_with_mode(dir: &Path, contents: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("api-key");
        std::fs::write(&path, contents).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn loose_key_permissions_flags_group_and_world_readable_files() {
        let dir = tempfile::tempdir().unwrap();

        // 0600 is the documented mode: owner-only, nothing to warn about.
        let tight = write_key_with_mode(dir.path(), "k\n", 0o600);
        assert_eq!(loose_key_permissions(&tight), None);

        // 0644 is what `echo "$KEY" > file` produces under the usual umask.
        let world = write_key_with_mode(dir.path(), "k\n", 0o644);
        assert_eq!(loose_key_permissions(&world), Some(0o644));

        // Group-readable only still leaks to every member of the group.
        let group = write_key_with_mode(dir.path(), "k\n", 0o640);
        assert_eq!(loose_key_permissions(&group), Some(0o640));

        // A missing file is not a permission problem — the read reports it instead.
        assert_eq!(loose_key_permissions(&dir.path().join("absent")), None);
    }

    #[cfg(unix)]
    #[test]
    fn read_key_file_warns_but_still_reads_a_world_readable_key() {
        // Refusing would break CI/container setups with odd modes, so a loose mode
        // must only warn on stderr — the key is still returned, trimmed.
        let dir = tempfile::tempdir().unwrap();
        let path = write_key_with_mode(dir.path(), "  secret-key \n", 0o644);
        assert_eq!(loose_key_permissions(&path), Some(0o644));
        let key = read_key_file(&path).expect("loose permissions warn, never refuse");
        assert_eq!(key.expose(), "secret-key");
    }
}
