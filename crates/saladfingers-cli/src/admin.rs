// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Lifecycle admin commands: `init`, `ls`, `status`, `gc`.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use saladfingers_api::{ContainerGroup, GroupStatus};

use crate::cli::{GcArgs, LsArgs, RunIdArgs};
use crate::config::{self, Config};
use crate::output::{OutputFormat, print_json, print_table, table};
use crate::{deploy, names, state};

/// `saladfingers init` — interactively write the global config.
pub fn init() -> Result<()> {
    let config_path = config::default_config_path()
        .context("cannot determine config path (set HOME or XDG_CONFIG_HOME)")?;
    eprintln!("saladfingers init — writing {}", config_path.display());

    let organization = prompt("Organization", "")?;
    if organization.is_empty() {
        bail!("organization is required");
    }
    let project = prompt("Project", "")?;
    if project.is_empty() {
        bail!("project is required");
    }
    let priority = prompt("Default priority [high|medium|low|batch]", "batch")?;

    let mut out = String::new();
    out.push_str("[salad]\n");
    out.push_str(&format!("organization = {}\n", toml_str(&organization)));
    out.push_str(&format!("project = {}\n", toml_str(&project)));
    if !priority.is_empty() {
        out.push_str(&format!("priority = {}\n", toml_str(&priority)));
    }

    if prompt_bool("Configure S3-compatible storage now?", false)? {
        let endpoint = prompt("Storage endpoint URL", "")?;
        let bucket = prompt("Bucket", "")?;
        let region = prompt("Region", "auto")?;
        out.push_str("\n[storage]\n");
        out.push_str(&format!("endpoint = {}\n", toml_str(&endpoint)));
        out.push_str(&format!("bucket = {}\n", toml_str(&bucket)));
        out.push_str(&format!("region = {}\n", toml_str(&region)));
        out.push_str("path_style = false\n");
        out.push_str("access_key_env = \"SALADFINGERS_S3_ACCESS_KEY\"\n");
        out.push_str("secret_key_env = \"SALADFINGERS_S3_SECRET_KEY\"\n");
    }

    if prompt_bool("Configure a container registry now?", false)? {
        let base = prompt("Registry base (host/namespace)", "")?;
        out.push_str("\n[registry]\n");
        out.push_str(&format!("base = {}\n", toml_str(&base)));
        out.push_str("auth_kind = \"basic\"\n");
        out.push_str("username_env = \"SALADFINGERS_REGISTRY_USER\"\n");
        out.push_str("password_env = \"SALADFINGERS_REGISTRY_PASSWORD\"\n");
    }

    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&config_path, out)
        .with_context(|| format!("writing {}", config_path.display()))?;
    eprintln!("wrote {}", config_path.display());

    let key = prompt(
        "Paste API key (blank to keep using SALAD_API_KEY / env)",
        "",
    )?;
    if !key.is_empty() {
        write_key_file(&key)?;
    }
    Ok(())
}

/// `saladfingers ls` — live container groups merged with local run states.
pub async fn ls(cfg: Config, args: LsArgs) -> Result<()> {
    let client = cfg.client()?;
    let groups = client.list_container_groups().await?;
    let runs = state::list_runs().unwrap_or_default();
    let active: HashSet<String> = runs.iter().map(|r| r.run_id.clone()).collect();

    let mut rows: Vec<GroupRow> = groups
        .iter()
        .filter(|g| args.all || names::is_sf_group(&g.name))
        .map(|g| GroupRow::from_group(g, &active))
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));

    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&rows)?,
        OutputFormat::Table => {
            if rows.is_empty() {
                eprintln!("no container groups");
                return Ok(());
            }
            let mut t = table(&["group", "run", "status", "replicas", "created"]);
            for r in &rows {
                t.add_row(vec![
                    r.name.clone(),
                    r.run_id.clone().unwrap_or_else(|| "-".into()),
                    r.status.clone(),
                    r.replicas
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "-".into()),
                    r.created.clone(),
                ]);
            }
            print_table(&t);
        }
    }
    Ok(())
}

/// `saladfingers status RUN_ID`
pub async fn status(cfg: Config, args: RunIdArgs) -> Result<()> {
    let Some(run) = state::load_run(&args.run_id)? else {
        bail!(
            "no local state for run '{}' (run state is created starting in M4)",
            args.run_id
        );
    };
    let client = cfg.client()?;

    let mut live = Vec::new();
    for group in run.group_names() {
        let status = match client.get_container_group(&group).await {
            Ok(g) => group_status_label(&g),
            Err(e) if e.is_not_found() => "deleted".to_string(),
            Err(e) => format!("error: {e}"),
        };
        live.push((group, status));
    }

    if args.json {
        print_json(&serde_json::json!({
            "run_id": run.run_id,
            "kind": run.kind,
            "status": run.status,
            "created_at": run.created_at,
            "groups": live.iter().map(|(n, s)| serde_json::json!({"group": n, "live_status": s})).collect::<Vec<_>>(),
        }))?;
    } else {
        let mut t = table(&["field", "value"]);
        t.add_row(vec!["run".to_string(), run.run_id.clone()]);
        t.add_row(vec!["kind".to_string(), run.kind.clone()]);
        t.add_row(vec!["local status".to_string(), run.status.clone()]);
        t.add_row(vec!["created".to_string(), run.created_at.to_rfc3339()]);
        for (group, status) in &live {
            t.add_row(vec![format!("group {group}"), status.clone()]);
        }
        print_table(&t);
    }
    Ok(())
}

/// `saladfingers watch RUN_ID` — a live status watcher.
///
/// Like [`status`] but loops on a token-bucket-friendly interval, printing each
/// group's state transitions (and `pulling_progress` while downloading) until every
/// group reaches a terminal state, then exits 0. Read-only: it creates and mutates
/// nothing. For a one-shot `run`, terminal means `succeeded`/`failed`/`stopped`; for
/// a `session`/`serve` box, reaching `running` is terminal-enough (matching `attach`).
/// With `--json` it streams one compact NDJSON event per transition to stdout instead
/// of the human transition log. Ctrl-C stops watching cleanly.
pub async fn watch(cfg: Config, args: RunIdArgs) -> Result<()> {
    let Some(run) = state::load_run(&args.run_id)? else {
        bail!("no local state for run '{}'", args.run_id);
    };
    let client = cfg.client()?;
    let groups = run.group_names();
    if groups.is_empty() {
        eprintln!("run '{}' has no groups to watch", run.run_id);
        return Ok(());
    }
    let running_terminal = watch_running_is_terminal(&run.kind);
    let json = args.json;
    let interval = base_interval_secs(groups.len());
    if !json {
        eprintln!(
            "watching {} ({} group(s)); Ctrl-C to stop",
            run.run_id,
            groups.len()
        );
    }

    // Per-group last-printed detail (to print only on change) and terminal flag.
    let mut last: Vec<String> = vec![String::new(); groups.len()];
    let mut done: Vec<bool> = vec![false; groups.len()];

    loop {
        for (i, name) in groups.iter().enumerate() {
            if done[i] {
                continue;
            }
            let (status_word, detail, is_done) = match client.get_container_group(name).await {
                Ok(g) => {
                    let status = g.status().unwrap_or(GroupStatus::Unknown);
                    let instances = client.list_instances(name).await.unwrap_or_default();
                    (
                        status_word(status),
                        deploy::describe(status, &instances),
                        is_group_terminal(status, running_terminal),
                    )
                }
                // A deleted/gone group is terminal for watching purposes.
                Err(e) if e.is_not_found() => ("deleted".to_string(), "deleted".to_string(), true),
                // Transient error: report it (on change) and keep polling.
                Err(e) => ("error".to_string(), format!("error: {e}"), false),
            };

            if detail != last[i] {
                if json {
                    let event = WatchEvent {
                        ts: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                        group: name,
                        shard: run.groups[i].shard,
                        status: &status_word,
                        detail: &detail,
                        done: is_done,
                    };
                    if let Ok(line) = serde_json::to_string(&event) {
                        println!("{line}");
                    }
                } else {
                    eprintln!("  {}  {name}  {detail}", Utc::now().format("%H:%M:%S"));
                }
                last[i] = detail;
            }
            done[i] = is_done;
        }

        if done.iter().all(|d| *d) {
            break;
        }

        tokio::select! {
            () = tokio::time::sleep(jittered(interval)) => {}
            _ = tokio::signal::ctrl_c() => {
                if !json {
                    eprintln!("\nstopped watching {}", run.run_id);
                }
                return Ok(());
            }
        }
    }

    if !json {
        let mut t = table(&["group", "shard", "final state"]);
        for (i, name) in groups.iter().enumerate() {
            t.add_row(vec![
                name.clone(),
                run.groups[i].shard.to_string(),
                last[i].clone(),
            ]);
        }
        print_table(&t);
    }
    Ok(())
}

/// `saladfingers gc` — reap leftover saladfingers container groups.
pub async fn gc(cfg: Config, args: GcArgs) -> Result<()> {
    let cutoff = humantime::parse_duration(&args.older_than)
        .with_context(|| format!("invalid --older-than '{}'", args.older_than))?;
    let cutoff = chrono::Duration::from_std(cutoff).context("duration out of range")?;
    let threshold = Utc::now() - cutoff;

    let client = cfg.client()?;
    let groups = client.list_container_groups().await?;
    let runs = state::list_runs().unwrap_or_default();
    // A locally-active run shields its group from gc, but only while it is plausibly still
    // running (within 2× its budget + slack). Past that its manager has almost certainly
    // died and the group is a billing leak the backstop must reap — otherwise a stale
    // "running" run (crashed CLI, dead reaper) would protect a billing group forever.
    let now = Utc::now();
    let active: HashMap<String, Duration> = runs
        .iter()
        .filter(|r| !is_terminal(&r.status))
        .map(|r| {
            (
                r.run_id.clone(),
                crate::runner::wait_hard_cap(r.max_duration_secs),
            )
        })
        .collect();

    let candidates: Vec<&ContainerGroup> = groups
        .iter()
        .filter(|g| names::is_sf_group(&g.name))
        .filter(|g| g.create_time.is_some_and(|t| t < threshold))
        .filter(|g| {
            match names::run_id_of_group(&g.name).and_then(|rid| active.get(&rid).copied()) {
                // Active run: a candidate only once the group outlives its hard cap.
                Some(cap) => g.create_time.is_some_and(|t| {
                    now.signed_duration_since(t)
                        .to_std()
                        .is_ok_and(|age| age > cap)
                }),
                // Orphan or terminal run → a candidate (already past the age threshold above).
                None => true,
            }
        })
        .collect();

    if candidates.is_empty() {
        eprintln!(
            "nothing to garbage-collect (no orphaned sf-* groups older than {})",
            args.older_than
        );
        return Ok(());
    }

    let mut t = table(&["group", "status", "created"]);
    for g in &candidates {
        t.add_row(vec![
            g.name.clone(),
            group_status_label(g),
            g.create_time.map(|t| t.to_rfc3339()).unwrap_or_default(),
        ]);
    }
    print_table(&t);

    if args.dry_run {
        eprintln!("dry run: {} group(s) would be deleted", candidates.len());
        return Ok(());
    }
    if !args.yes && !confirm(&format!("Delete {} group(s)?", candidates.len()))? {
        eprintln!("aborted");
        return Ok(());
    }

    let mut deleted = 0;
    let mut reaped_run_ids = Vec::new();
    for g in &candidates {
        match client.delete_container_group(&g.name).await {
            Ok(()) => {
                deleted += 1;
                if let Some(rid) = names::run_id_of_group(&g.name) {
                    reaped_run_ids.push(rid);
                }
            }
            Err(e) => eprintln!("failed to delete {}: {e}", g.name),
        }
    }
    eprintln!("deleted {deleted} group(s)");

    // Best-effort: reap the runs' remote artifacts under `runs/<run_id>/`. Storage is
    // optional (S4-only runs have nothing to list here), so a missing backend is fine.
    if let Some(storage) = &cfg.storage {
        reaped_run_ids.sort();
        reaped_run_ids.dedup();
        match crate::presign::S3Backend::from_config(storage) {
            Ok(backend) => {
                let http = reqwest::Client::new();
                for rid in &reaped_run_ids {
                    let prefix = format!("runs/{rid}/");
                    match backend.delete_prefix(&http, &prefix).await {
                        Ok(n) if n > 0 => eprintln!("removed {n} object(s) under {prefix}"),
                        Ok(_) => {}
                        Err(e) => eprintln!("prefix cleanup for {prefix} failed: {e}"),
                    }
                }
            }
            Err(e) => eprintln!("skipping remote cleanup (storage unavailable): {e}"),
        }
    }
    Ok(())
}

// ---- helpers --------------------------------------------------------------

#[derive(serde::Serialize)]
struct GroupRow {
    name: String,
    run_id: Option<String>,
    status: String,
    replicas: Option<u32>,
    created: String,
    tracked_locally: bool,
}

impl GroupRow {
    fn from_group(g: &ContainerGroup, active: &HashSet<String>) -> Self {
        let run_id = names::run_id_of_group(&g.name);
        let tracked_locally = run_id.as_ref().is_some_and(|r| active.contains(r));
        Self {
            name: g.name.clone(),
            run_id,
            status: group_status_label(g),
            replicas: g.replicas,
            created: g.create_time.map(|t| t.to_rfc3339()).unwrap_or_default(),
            tracked_locally,
        }
    }
}

fn group_status_label(g: &ContainerGroup) -> String {
    match g.current_state.as_ref().map(|s| s.status) {
        Some(GroupStatus::Pending) => "pending",
        Some(GroupStatus::Running) => "running",
        Some(GroupStatus::Stopped) => "stopped",
        Some(GroupStatus::Succeeded) => "succeeded",
        Some(GroupStatus::Deploying) => "deploying",
        Some(GroupStatus::Failed) => "failed",
        Some(GroupStatus::Unknown) | None => "unknown",
    }
    .to_string()
}

fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "failed" | "cancelled" | "deleted" | "reaped"
    )
}

// ---- watch helpers --------------------------------------------------------

/// One streamed NDJSON transition event emitted by `watch --json`.
#[derive(serde::Serialize)]
struct WatchEvent<'a> {
    ts: String,
    group: &'a str,
    shard: u32,
    status: &'a str,
    detail: &'a str,
    done: bool,
}

/// The lowercase word for a group status (`Running` → `"running"`).
fn status_word(status: GroupStatus) -> String {
    format!("{status:?}").to_lowercase()
}

/// Whether `running` counts as terminal for watching a run of this `kind`. A one-shot
/// `run` keeps working while `running`; a `session`/`serve` box is done once up.
fn watch_running_is_terminal(kind: &str) -> bool {
    !matches!(kind, "run")
}

/// Whether a group status is terminal for `watch`. `running_is_terminal` flips whether
/// `Running` stops the watch (true for session/serve, false for a one-shot run).
fn is_group_terminal(status: GroupStatus, running_is_terminal: bool) -> bool {
    match status {
        GroupStatus::Failed | GroupStatus::Stopped | GroupStatus::Succeeded => true,
        GroupStatus::Running => running_is_terminal,
        GroupStatus::Pending | GroupStatus::Deploying | GroupStatus::Unknown => false,
    }
}

/// Base poll interval in seconds for watching `num_groups` groups. Each cycle costs
/// TWO API calls per group (`get_container_group` + `list_instances`), so budget
/// `max(5, ceil(2N·60/120))` against a 120/min slice of the 180/min token bucket —
/// the old `(N+1)` formula assumed one call per group and undercounted by ~2×.
fn base_interval_secs(num_groups: usize) -> u64 {
    let n = num_groups as u64;
    ((2 * n * 60).div_ceil(120)).max(5)
}

/// Apply ±20% jitter to a base interval (wall-clock derived; no `rand` dependency,
/// mirroring the api crate) so many watchers don't align their polls.
fn jittered(base_secs: u64) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let frac = f64::from(nanos % 1_000) / 1_000.0; // [0, 1)
    let factor = 0.8 + 0.4 * frac; // [0.8, 1.2)
    Duration::from_secs_f64(base_secs as f64 * factor)
}

fn toml_str(s: &str) -> String {
    // Basic TOML string escaping for the values we write.
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn prompt(question: &str, default: &str) -> Result<String> {
    if default.is_empty() {
        eprint!("{question}: ");
    } else {
        eprint!("{question} [{default}]: ");
    }
    std::io::stderr().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let value = line.trim();
    Ok(if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    })
}

fn prompt_bool(question: &str, default: bool) -> Result<bool> {
    let default_str = if default { "Y/n" } else { "y/N" };
    let answer = prompt(&format!("{question} [{default_str}]"), "")?;
    Ok(match answer.to_ascii_lowercase().as_str() {
        "" => default,
        "y" | "yes" => true,
        _ => false,
    })
}

fn confirm(question: &str) -> Result<bool> {
    prompt_bool(question, false)
}

/// Write `contents` to `path` with owner-only permissions, atomically.
///
/// The mode is set when the file is *created* rather than chmod-ed after the fact: a plain
/// `fs::write` lands the secret at the umask default (typically 0644) first, and a local
/// watcher that opens it in that window keeps read access even after the chmod, because
/// permissions are checked at open. Writing a 0600 temp and renaming it into place also means
/// an existing key file is never left missing or half-written, and `create_new` guarantees we
/// created the temp ourselves rather than following a symlink planted at that path.
fn write_secret_file(path: &Path, contents: &str) -> Result<()> {
    let tmp = path.with_extension("tmp");
    // A stale temp would keep its old mode, since `OpenOptions::mode` only applies at creation.
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    {
        let mut file = opts
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("writing {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn write_key_file(key: &str) -> Result<()> {
    let path = config::default_key_path().context("cannot determine api-key path")?;
    if let Some(parent) = path.parent() {
        // The API key lives here, so the directory itself should not be traversable by others.
        state::ensure_private_dir(parent)?;
    }
    write_secret_file(&path, &format!("{key}\n"))?;
    eprintln!("wrote {} (mode 0600)", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_files_are_created_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api-key");

        write_secret_file(&path, "sk-first\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "sk-first\n");
        // Overwriting an existing key must neither lose it nor loosen the mode.
        write_secret_file(&path, "sk-second\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "sk-second\n");
        // No temp file is left behind holding a copy of the secret.
        assert!(!path.with_extension("tmp").exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "api key must be owner-only");
        }
    }

    #[test]
    fn interval_grows_with_group_count_but_never_below_five() {
        // 2 calls per group per cycle: ceil(2N*60/120) = N, floored at 5.
        assert_eq!(base_interval_secs(1), 5); // 1 → 5
        assert_eq!(base_interval_secs(5), 5); // 5 → 5
        assert_eq!(base_interval_secs(10), 10);
        assert_eq!(base_interval_secs(20), 20);
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        for _ in 0..1_000 {
            let d = jittered(10).as_secs_f64();
            assert!((8.0..=12.0).contains(&d), "jittered 10s out of band: {d}");
        }
    }

    #[test]
    fn running_is_terminal_only_for_session_and_serve() {
        assert!(!watch_running_is_terminal("run"));
        assert!(watch_running_is_terminal("session"));
        assert!(watch_running_is_terminal("serve"));
    }

    #[test]
    fn terminal_classification_depends_on_kind() {
        // A one-shot run: running is NOT terminal, but succeeded/failed/stopped are.
        assert!(!is_group_terminal(GroupStatus::Running, false));
        assert!(is_group_terminal(GroupStatus::Succeeded, false));
        assert!(is_group_terminal(GroupStatus::Failed, false));
        assert!(is_group_terminal(GroupStatus::Stopped, false));
        // A session/serve box: reaching running IS terminal.
        assert!(is_group_terminal(GroupStatus::Running, true));
        // In-flight states are never terminal.
        assert!(!is_group_terminal(GroupStatus::Pending, true));
        assert!(!is_group_terminal(GroupStatus::Deploying, false));
    }
}
