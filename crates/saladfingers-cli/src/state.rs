// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Local state: the run-state directory and the GPU-class cache.
//!
//! Runs are created starting in M4; M2 defines the schema and the read/write/list
//! plumbing that `ls`, `status`, and `gc` build on, plus a TTL'd GPU-class cache so
//! name→UUID resolution does not hit the API on every invocation.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use saladfingers_api::{GpuClass, SaladClient};
use serde::{Deserialize, Serialize};

/// Schema version for the run-state files.
pub const STATE_VERSION: u32 = 1;

/// Persisted state for one run/session/serve deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    /// Schema version.
    pub v: u32,
    /// Run identifier.
    pub run_id: String,
    /// `run`, `session`, or `serve`.
    pub kind: String,
    /// When the run was created.
    pub created_at: DateTime<Utc>,
    /// Organization.
    pub org: String,
    /// Project.
    pub project: String,
    /// Profile used, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Image reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// GPU classes requested (names as given).
    #[serde(default)]
    pub gpu_classes: Vec<String>,
    /// The GPU the node actually turned out to have, once something has looked.
    ///
    /// SaladCloud never reports which class a group was placed on: the container-group
    /// object echoes back the *requested* `gpu_classes` UUID list and the instance object
    /// carries only lifecycle fields (verified against the live API). So with a
    /// first-available list of several classes, the allocation is knowable only by asking
    /// the node — and that answer is worth persisting, because it cannot be recovered once
    /// the group is gone. `None` = nobody has looked yet, or the look failed.
    ///
    /// It is an observation of the box that came up, not a standing truth. That is safe
    /// here because a session which relaunches elsewhere comes back with a fresh `boot_id`
    /// and its reaper deletes the group, so the reading cannot quietly describe a machine
    /// the session no longer runs on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_observed: Option<String>,
    /// Priority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    /// The command run on the GPU.
    #[serde(default)]
    pub command: Vec<String>,
    /// Declared output artifact names (`run` kind only). The collector allow-lists the
    /// untrusted envelope's `uploads[].name` against this set, so a hostile node cannot get
    /// the CLI to fetch an artifact the run never asked for. `None` = unknown (an older
    /// state file, or a non-`run` deployment); the collector then relies on path-shape
    /// validation alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_names: Option<Vec<String>>,
    /// The per-artifact part ceiling this run was created with (`run` kind only). Persisted so
    /// `attach` caps the untrusted envelope's reported part count at the same value the run
    /// presigned URLs for, regardless of any later config change. `None` = unknown (an older
    /// state file, or a non-`run` deployment); the collector then falls back to the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parts: Option<u32>,
    /// Shared checkpoint name this run reads and writes, if any (`--checkpoint-prefix`).
    ///
    /// Recorded so a second run cannot be aimed at a prefix a live run is already rotating
    /// — two writers sharing one slot ring would overwrite each other's checkpoints. It
    /// also lets `checkpoint show RUN_ID` say where the checkpoint actually went when the
    /// run's own prefix holds nothing, as a hint in the error; addressing it still takes
    /// `--prefix`, because a command that read local state to pick a different key would
    /// behave differently on every machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_prefix: Option<String>,
    /// The container groups backing this run.
    #[serde(default)]
    pub groups: Vec<GroupRef>,
    /// Run status: `creating|running|succeeded|failed|cancelled|detached|deleted`.
    pub status: String,
    /// Session bearer token (sessions only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_token: Option<String>,
    /// Wall-clock budget in seconds, if set — the detached reaper caps on 2× this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_secs: Option<u64>,
    /// Post-run result: exit code + billed-time/cost estimate (set on completion).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<RunResult>,
}

/// Post-run result: the worst shard exit code plus a billed-time and cost estimate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    /// Worst shard exit code.
    pub exit_code: i32,
    /// Estimated billed (running-state) seconds, summed across every machine every
    /// shard passed through — reallocations included. SaladCloud bills only the
    /// `running` state, so summing each machine's running span tracks the billed
    /// window even when the bandwidth gate reallocated the run mid-flight.
    pub billed_seconds_est: u64,
    /// Estimated cost in USD (`billed_seconds_est` × hourly price ÷ 3600). `None` if
    /// the GPU class price could not be resolved.
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub cost_est_usd: Option<Decimal>,
}

/// A container group backing a run shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRef {
    /// Group name.
    pub name: String,
    /// Shard index.
    pub shard: u32,
    /// Last observed instance/group state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_state: Option<String>,
    /// Machine ids this shard has run on.
    #[serde(default)]
    pub machine_history: Vec<String>,
    /// Billed (running-state) spans, one per machine this shard passed through. A
    /// mid-run reallocation (e.g. the bandwidth gate) closes the previous machine's
    /// span and opens a new one, so every node that reached `running` — not just the
    /// final one — is counted toward the billed-time estimate.
    #[serde(default)]
    pub running_spans: Vec<RunningSpan>,
}

/// One billed interval on a single machine: the window an instance spent in the
/// `running` state (the only billed state). `end` is `None` while the machine is
/// still running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningSpan {
    /// Machine id this span was billed on.
    pub machine_id: String,
    /// When the instance entered `running`.
    pub start: DateTime<Utc>,
    /// When the instance left `running` (reallocated away, or the shard finished).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<DateTime<Utc>>,
}

impl RunningSpan {
    /// Billed seconds in this span. An open span (`end == None`) is measured to
    /// `as_of`. Never negative.
    #[must_use]
    pub fn billed_seconds(&self, as_of: DateTime<Utc>) -> u64 {
        let end = self.end.unwrap_or(as_of);
        (end - self.start).num_seconds().max(0) as u64
    }
}

impl RunState {
    /// The group names for this run.
    #[must_use]
    pub fn group_names(&self) -> Vec<String> {
        self.groups.iter().map(|g| g.name.clone()).collect()
    }

    /// Total estimated billed (running-state) seconds across every machine every
    /// shard passed through — reallocations included. Open spans are measured to
    /// `as_of`.
    #[must_use]
    pub fn billed_seconds_est(&self, as_of: DateTime<Utc>) -> u64 {
        self.groups
            .iter()
            .flat_map(|g| &g.running_spans)
            .map(|span| span.billed_seconds(as_of))
            .sum()
    }
}

/// Serializes tests that point `XDG_STATE_HOME` at a tempdir. Env vars are process-global
/// and `set_var` is unsafe with concurrent threads: under nextest each test is its own
/// process and the lock is moot, but under plain `cargo test` two state-mutating tests in
/// one binary would otherwise race each other's directories.
#[cfg(test)]
pub(crate) static TEST_STATE_ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The base state directory (`$XDG_STATE_HOME/saladfingers` or `~/.local/state/saladfingers`).
///
/// # Errors
/// Returns an error if neither `XDG_STATE_HOME` nor `HOME` is set.
pub fn state_dir() -> Result<PathBuf> {
    if let Some(dir) = env_path("XDG_STATE_HOME") {
        return Ok(dir.join("saladfingers"));
    }
    let home = env_path("HOME")
        .context("cannot locate state dir: neither XDG_STATE_HOME nor HOME is set")?;
    Ok(home.join(".local").join("state").join("saladfingers"))
}

fn runs_dir() -> Result<PathBuf> {
    Ok(state_dir()?.join("runs"))
}

/// Path to a run's state file.
///
/// # Errors
/// Returns an error if the state directory cannot be located.
pub fn run_path(run_id: &str) -> Result<PathBuf> {
    Ok(runs_dir()?.join(format!("{run_id}.json")))
}

/// Path to a run's detached-reaper log.
///
/// # Errors
/// Returns an error if the state directory cannot be located.
pub fn reaper_log_path(run_id: &str) -> Result<PathBuf> {
    Ok(runs_dir()?.join(format!("{run_id}.reaper.log")))
}

/// Write a run's state atomically.
///
/// # Errors
/// Returns an error if the state directory cannot be created or written.
pub fn save_run(run: &RunState) -> Result<()> {
    let dir = runs_dir()?;
    ensure_private_dir(&dir)?;
    let path = dir.join(format!("{}.json", run.run_id));
    write_json_atomic(&path, run)
}

/// Load a run's state, or `None` if it does not exist.
///
/// # Errors
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_run(run_id: &str) -> Result<Option<RunState>> {
    let path = run_path(run_id)?;
    read_json_opt(&path)
}

/// Delete a run's local state file (idempotent — missing file is Ok).
///
/// # Errors
/// Returns an error if the file exists but cannot be removed.
pub fn delete_run(run_id: &str) -> Result<()> {
    let path = run_path(run_id)?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

/// List all locally tracked runs.
///
/// # Errors
/// Returns an error if the runs directory cannot be read.
pub fn list_runs() -> Result<Vec<RunState>> {
    let dir = runs_dir()?;
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", dir.display())),
    };
    let mut runs = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "json")
            && let Some(run) = read_json_opt::<RunState>(&path)?
        {
            runs.push(run);
        }
    }
    runs.sort_by_key(|r| std::cmp::Reverse(r.created_at));
    Ok(runs)
}

// ---- GPU-class cache ------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct GpuClassCache {
    cached_at: DateTime<Utc>,
    classes: Vec<GpuClass>,
}

fn gpu_cache_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("gpu-classes.json"))
}

/// GPU classes, served from a TTL'd on-disk cache. `refresh` forces a live fetch.
///
/// # Errors
/// Returns an error if the live fetch fails and no usable cache exists.
pub async fn cached_gpu_classes(
    client: &SaladClient,
    refresh: bool,
    ttl_hours: i64,
) -> Result<Vec<GpuClass>> {
    let path = gpu_cache_path()?;
    if !refresh
        && let Some(cache) = read_json_opt::<GpuClassCache>(&path)?
        && Utc::now() - cache.cached_at < Duration::hours(ttl_hours)
    {
        return Ok(cache.classes);
    }
    let classes = client.list_gpu_classes().await?;
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent).ok();
    }
    let cache = GpuClassCache {
        cached_at: Utc::now(),
        classes: classes.clone(),
    };
    let _ = write_json_atomic(&path, &cache);
    Ok(classes)
}

// ---- helpers --------------------------------------------------------------

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Create `dir` (and parents) private to the owner.
///
/// Run state can carry the session/serve bearer token, so the directory holding it must not be
/// listable or traversable by other local users. An already-existing directory from an older
/// version is tightened too, which also shields state files that were written before this.
/// Shared with `admin`, which puts the API key in the config dir.
pub(crate) fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Best-effort: a pre-existing dir keeps its old mode through `create_dir_all`.
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// Write `value` as JSON to `path` atomically, readable only by the owner.
///
/// [`RunState`] carries `agent_token` for session/serve deployments — the bearer that grants
/// exec and file access inside the container — so these files must never be world-readable.
/// The mode is set when the temp file is *created* rather than chmod-ed afterwards, so the
/// secret is never even briefly readable; `create_new` guarantees we made the file ourselves
/// (and refuses to follow a symlink planted at that path). The temp sits beside the target so
/// the rename stays atomic.
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    // A leftover temp from a crashed write would keep its old (possibly world-readable) mode,
    // because `OpenOptions::mode` only applies at creation — clear it first.
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    {
        use std::io::Write as _;
        let mut file = opts
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("writing {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

fn read_json_opt<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let value = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", path.display()))?;
            Ok(Some(value))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_state_round_trips_through_disk() {
        let _env = TEST_STATE_ENV.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: serialized by TEST_STATE_ENV against the other state-mutating tests;
        // scopes state to the tempdir.
        unsafe {
            std::env::set_var("XDG_STATE_HOME", dir.path());
        }

        let run = RunState {
            v: STATE_VERSION,
            run_id: "sf-x7k2mq".into(),
            kind: "run".into(),
            created_at: Utc::now(),
            org: "my-org".into(),
            project: "my-proj".into(),
            profile: Some("kernels".into()),
            image: Some("img@sha256:abc".into()),
            gpu_classes: vec!["rtx 4090".into()],
            gpu_observed: None,
            priority: Some("batch".into()),
            command: vec!["true".into()],
            output_names: Some(vec!["model".into(), "ckpt".into()]),
            max_parts: Some(64),
            checkpoint_prefix: None,
            groups: vec![GroupRef {
                name: "sf-x7k2mq".into(),
                shard: 0,
                last_state: Some("running".into()),
                machine_history: vec!["mach-a".into()],
                running_spans: vec![RunningSpan {
                    machine_id: "mach-a".into(),
                    start: Utc::now(),
                    end: None,
                }],
            }],
            status: "running".into(),
            agent_token: None,
            max_duration_secs: Some(2700),
            result: None,
        };
        save_run(&run).unwrap();
        let loaded = load_run("sf-x7k2mq").unwrap().unwrap();
        assert_eq!(loaded.run_id, "sf-x7k2mq");
        assert_eq!(
            loaded.output_names.as_deref(),
            Some(["model".to_string(), "ckpt".to_string()].as_slice()),
        );
        assert_eq!(loaded.max_parts, Some(64));

        // `agent_token` (the session/serve bearer) lives in this file, so neither it nor the
        // directory holding it may be readable by other local users.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let file_mode = std::fs::metadata(run_path("sf-x7k2mq").unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(file_mode & 0o777, 0o600, "state file must be owner-only");
            let dir_mode = std::fs::metadata(runs_dir().unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700, "runs dir must be owner-only");
        }
        assert_eq!(loaded.group_names(), vec!["sf-x7k2mq".to_string()]);
        assert_eq!(loaded.groups[0].running_spans.len(), 1);
        assert_eq!(list_runs().unwrap().len(), 1);
        assert!(load_run("sf-missing").unwrap().is_none());
    }
}
