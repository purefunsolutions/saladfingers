// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers run` — one-shot batch runs on rented GPUs, plus `attach`/`cancel`.
//!
//! Sequence: resolve params → upload inputs → create N shard groups (each a
//! single-replica `sf-agent run`) → poll for the result envelope → download the
//! artifacts it lists → delete the groups → summarize. Ctrl-C cancels (stops and
//! deletes); `--detach` returns immediately and `attach` resumes the wait.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use saladfingers_api::{GroupStatus, Instance, InstanceState, RestartPolicy, SaladClient};
use saladfingers_protocol::{JobStatus, ResultEnvelope, Timings, transfer};

use crate::cli::{RunArgs, RunIdArgs};
use crate::config::{Config, Profile};
use crate::deploy::{self, GroupParams};
use crate::names;
use crate::output::{print_table, table};
use crate::presign::S3Backend;
use crate::spec::{self, CheckpointParams, GateParams, OutputRequest, SpecParams, UploadedInput};
use crate::state::{self, GroupRef, RunResult, RunState, RunningSpan};

/// Presigned-URL expiry: generous against clock skew (72 h floor, or 2× duration).
fn presign_expiry(max_duration: Option<Duration>) -> Duration {
    let floor = Duration::from_secs(72 * 3600);
    let doubled = max_duration.map(|d| d * 2).unwrap_or(floor);
    doubled.max(floor).min(Duration::from_secs(7 * 24 * 3600))
}

struct RunParams {
    command: Vec<String>,
    image: String,
    gpu_classes: Vec<String>,
    cpu: u32,
    memory_mb: u32,
    disk_gib: u64,
    shm_mb: Option<u32>,
    priority: String,
    env: BTreeMap<String, String>,
    max_duration_secs: Option<u64>,
    replicas: u32,
    country_codes: Vec<String>,
    inputs: Vec<(String, PathBuf, bool)>, // (container dest, local source, archive)
    outputs: Vec<OutputRequest>,
    max_parts: u32, // presigned-URL blocks per artifact (size ceiling); from [storage] config
    gate: Option<GateParams>,
    checkpoint: Option<CheckpointParams>,
    name_hint: Option<String>,
}

/// `saladfingers run`
pub async fn run(cfg: Config, args: RunArgs) -> Result<()> {
    let profile = args
        .profile
        .as_deref()
        .map(|p| cfg.profile(p))
        .transpose()?
        .cloned();
    let params = resolve_params(&cfg, profile.as_ref(), &args)?;
    let storage = cfg
        .storage
        .as_ref()
        .context("`run` needs an S3-compatible [storage] backend configured")?;
    let backend = S3Backend::from_config(storage)?;
    let client = cfg.client()?;
    let http = transfer::transfer_client()?;

    let run_id = names::generate_run_id();
    let shard_count = params.replicas.max(1);
    let expiry = presign_expiry(params.max_duration_secs.map(Duration::from_secs));

    // Preflight: quota.
    let quotas = client.get_quotas().await?;
    if quotas.replicas_available() < shard_count {
        bail!(
            "quota: {} replicas available, {shard_count} needed — run `saladfingers gc` or raise the quota",
            quotas.replicas_available()
        );
    }

    eprintln!(
        "run {run_id}: uploading {} input(s)...",
        params.inputs.len()
    );
    let inputs = upload_inputs(
        &backend,
        &http,
        &run_id,
        &params.inputs,
        params.max_parts,
        expiry,
    )
    .await?;

    let uuids = deploy::resolve_gpu_uuids(&client, &params.gpu_classes, false).await?;
    let priority = deploy::parse_priority(&params.priority)?;

    let mut groups = Vec::new();
    for shard in 0..shard_count {
        let name = names::group_name(&run_id, (shard_count > 1).then_some(shard));
        let job_spec = spec::build_job_spec(SpecParams {
            backend: &backend,
            run_id: &run_id,
            shard_index: shard,
            shard_count,
            command: params.command.clone(),
            env: params.env.clone(),
            inputs: &inputs,
            outputs: &params.outputs,
            max_parts: params.max_parts,
            max_duration_secs: params.max_duration_secs,
            stop_signal: None,
            gate: params.gate.as_ref().map(|g| GateParams {
                min_download_mbps: g.min_download_mbps,
                min_upload_mbps: g.min_upload_mbps,
            }),
            checkpoint: params.checkpoint.as_ref().map(|c| CheckpointParams {
                dir: c.dir.clone(),
                interval_secs: c.interval_secs,
                quiesce_secs: c.quiesce_secs,
            }),
            expiry,
        });
        let job_key = spec::job_key(&run_id, shard);
        let job_body = serde_json::to_vec(&job_spec)?;
        // Digest the exact bytes about to be uploaded — never a re-serialization, which
        // could differ and would turn the agent's check into a spurious failure.
        let job_sha256 = transfer::sha256_hex(&job_body);
        put_object(&http, &backend.presign_put(&job_key, expiry), job_body).await?;
        let job_url = backend.presign_get(&job_key, expiry);

        let env = job_env(&job_url, &run_id, shard, shard_count, job_sha256)?;

        let request = deploy::build_request(GroupParams {
            name: name.clone(),
            image: params.image.clone(),
            gpu_uuids: uuids.clone(),
            priority,
            cpu: params.cpu,
            memory_mb: params.memory_mb,
            disk_gib: params.disk_gib,
            command: Some(vec!["/bin/sf-agent".into(), "run".into()]),
            env,
            gateway_port: None,
            gateway_auth: false,
            registry_auth: deploy::registry_auth(&cfg),
            restart_policy: RestartPolicy::OnFailure,
            country_codes: params.country_codes.clone(),
            shm_mb: params.shm_mb,
        });
        eprintln!("run {run_id}: creating shard {shard} ({name})...");
        if let Err(e) = client.create_container_group(&request).await {
            // A mid-loop failure must not leak the shards already created — each is a
            // live group that would bill until something else deleted it.
            if shard > 0 {
                eprintln!(
                    "run {run_id}: shard {shard} create failed; deleting the {shard} \
                     already-created group(s)"
                );
                for done in 0..shard {
                    let prev = names::group_name(&run_id, (shard_count > 1).then_some(done));
                    let _ = deploy::delete_group(&client, &prev).await;
                }
            }
            return Err(e).context("creating shard group");
        }
        groups.push(GroupRef {
            name,
            shard,
            last_state: None,
            machine_history: Vec::new(),
            running_spans: Vec::new(),
        });
    }

    save_state(&cfg, &run_id, &params, &groups, "running");

    if args.detach {
        // A container group relaunches its container on every exit (empirical E1/E2),
        // so a detached run would bill forever after finishing. Spawn a detached
        // reaper that deletes the group(s) once the run completes or a hard cap
        // elapses. `gc` / the end-of-session quota check remain the backstop.
        match spawn_reaper(&run_id, &cfg.organization, &cfg.project) {
            Ok(()) => eprintln!(
                "run {run_id} detached; a background reaper will stop its group(s) when the run \
                 finishes.\n  follow:  saladfingers status {run_id}\n  collect: saladfingers attach {run_id}"
            ),
            Err(e) => eprintln!(
                "run {run_id} detached, but the reaper failed to start ({e:#}); \
                 stop it yourself with `saladfingers cancel {run_id}`"
            ),
        }
        return Ok(());
    }

    let hourly = deploy::gpu_hourly_price(&client, &params.gpu_classes[0], priority).await;
    let hard_cap = wait_hard_cap(params.max_duration_secs);
    let allowed_outputs: Vec<String> = params.outputs.iter().map(|o| o.name.clone()).collect();
    let exit_code = match await_and_collect(
        &client,
        &http,
        &backend,
        &run_id,
        shard_count,
        Some(&allowed_outputs),
        params.max_parts,
        expiry,
        hourly,
        hard_cap,
    )
    .await
    {
        Ok(code) => code,
        Err(e) => {
            // A poll error or the hard-cap timeout must not abandon a live, billing group.
            eprintln!("run {run_id}: aborting — stopping and deleting group(s) to halt billing");
            cleanup(&client, &run_id, shard_count).await;
            mark_run_failed(&run_id);
            return Err(e);
        }
    };
    std::process::exit(exit_code);
}

/// `saladfingers attach RUN_ID`
pub async fn attach(cfg: Config, args: RunIdArgs) -> Result<()> {
    let run = state::load_run(&args.run_id)?
        .with_context(|| format!("no local state for run '{}'", args.run_id))?;
    let storage = cfg
        .storage
        .as_ref()
        .context("`attach` needs an S3-compatible [storage] backend")?;
    let backend = S3Backend::from_config(storage)?;
    let client = cfg.client()?;
    let http = transfer::transfer_client()?;
    let shard_count = u32::try_from(run.groups.len().max(1)).unwrap_or(1);
    let expiry = presign_expiry(None);
    let hourly = if let Some(class) = run.gpu_classes.first() {
        match deploy::parse_priority(run.priority.as_deref().unwrap_or("batch")) {
            Ok(priority) => deploy::gpu_hourly_price(&client, class, priority).await,
            Err(_) => None,
        }
    } else {
        None
    };
    let hard_cap = wait_hard_cap(run.max_duration_secs);
    let exit_code = match await_and_collect(
        &client,
        &http,
        &backend,
        &run.run_id,
        shard_count,
        run.output_names.as_deref(),
        run.max_parts.unwrap_or(spec::DEFAULT_MAX_PARTS),
        expiry,
        hourly,
        hard_cap,
    )
    .await
    {
        Ok(code) => code,
        Err(e) => {
            eprintln!(
                "run {}: aborting — stopping and deleting group(s) to halt billing",
                run.run_id
            );
            cleanup(&client, &run.run_id, shard_count).await;
            mark_run_failed(&run.run_id);
            return Err(e);
        }
    };
    std::process::exit(exit_code);
}

/// `saladfingers cancel RUN_ID`
pub async fn cancel(cfg: Config, args: RunIdArgs) -> Result<()> {
    let client = cfg.client()?;
    let names: Vec<String> = match state::load_run(&args.run_id)? {
        Some(run) => run.group_names(),
        None => vec![args.run_id.clone()],
    };
    // A failed DELETE means the group may still be billing — that must be a loud,
    // non-zero exit, never a silent success. (Stop is best-effort; delete is what
    // stops billing, and it treats 404 as OK.)
    let mut failed: Vec<String> = Vec::new();
    for name in &names {
        eprintln!("stopping and deleting {name}...");
        let _ = client.stop_container_group(name).await;
        if let Err(e) = deploy::delete_group(&client, name).await {
            eprintln!("  failed to delete {name}: {e:#}");
            failed.push(name.clone());
        }
    }
    if failed.is_empty() {
        if let Ok(Some(mut run)) = state::load_run(&args.run_id) {
            run.status = "cancelled".into();
            let _ = state::save_run(&run);
        }
        Ok(())
    } else {
        // Keep the local status non-terminal so `cancel` can be retried and the
        // reaper/gc backstops still treat the run as live.
        bail!(
            "failed to delete {} group(s) ({}) — they may still be billing; retry \
             `saladfingers cancel {}` or run `saladfingers gc`",
            failed.len(),
            failed.join(", "),
            args.run_id
        );
    }
}

/// `saladfingers reap RUN_ID` — the detached reaper for `--detach` runs and sessions.
/// A container group relaunches its container on every exit (empirical E1/E2), so
/// nothing an agent does — finishing, deadman self-exit, max-duration — stops billing;
/// only group deletion does. For runs, this waits until every shard has written its
/// result envelope (job done) or a hard cap elapses. For sessions (which have no
/// envelope), it waits for the box to become useless: the group gone, the agent's
/// `boot_id` changed (a relaunch wiped all session state — deadman/max-duration
/// self-exit shows up exactly this way), or the session's own budget elapsed.
/// Then it stops + deletes the group(s). Spawned detached; also safe to run by hand.
pub async fn reap(cfg: Config, args: RunIdArgs) -> Result<()> {
    let run = state::load_run(&args.run_id)?
        .with_context(|| format!("no local state for run '{}'", args.run_id))?;
    let client = cfg.client()?;
    let backend = cfg
        .storage
        .as_ref()
        .map(S3Backend::from_config)
        .transpose()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    let shard_count = u32::try_from(run.groups.len().max(1)).unwrap_or(1);
    let is_session = run.kind == "session";
    // Hard cap. Runs: 2× the budget (+slack) — the envelope normally appears well
    // before this. Sessions: the budget itself (+15 min slack) — after max-duration the
    // agent self-exits and every relaunch is a fresh, stateless box nobody asked for.
    let hard_cap = if is_session {
        Duration::from_secs(
            run.max_duration_secs
                .unwrap_or(4 * 3600)
                .saturating_add(900)
                .min(24 * 3600),
        )
    } else {
        wait_hard_cap(run.max_duration_secs)
    };
    let start = Instant::now();
    eprintln!("reaper {}: waiting (hard cap {hard_cap:?})", run.run_id);
    let mut first_boot_id: Option<String> = None;
    loop {
        let done = if is_session {
            session_over(&cfg, &client, &run, &mut first_boot_id).await
        } else {
            match backend.as_ref() {
                // Every shard has a terminal envelope → job is done.
                Some(b) => {
                    let mut all = true;
                    for shard in 0..shard_count {
                        if !shard_terminal(&http, b, &run.run_id, shard).await {
                            all = false;
                            break;
                        }
                    }
                    all
                }
                None => false, // no storage → can't detect completion; rely on the hard cap
            }
        };
        if done || start.elapsed() >= hard_cap {
            break;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    eprintln!("reaper {}: reaping groups", run.run_id);
    cleanup(&client, &run.run_id, shard_count).await;
    if let Ok(Some(mut run)) = state::load_run(&args.run_id) {
        if run.status == "running" {
            run.status = "reaped".into();
        }
        let _ = state::save_run(&run);
    }
    Ok(())
}

/// Whether a session box is over: its group is gone, or the serving agent's `boot_id`
/// no longer matches the first one seen — the container relaunched (deadman or
/// max-duration self-exit, crash, node loss), wiping every exec and upload, so the box
/// left behind is a stateless impostor that only bills. Unreachable healthz is NOT
/// "over" (the box may be mid-boot); the hard cap bounds that case.
async fn session_over(
    cfg: &Config,
    client: &SaladClient,
    run: &RunState,
    first_boot_id: &mut Option<String>,
) -> bool {
    match client.get_container_group(&run.run_id).await {
        Err(e) if e.is_not_found() => return true, // already deleted (session rm)
        Err(_) => return false,                    // transient control-plane error
        Ok(_) => {}
    }
    if let Some(boot_id) = crate::session::probe_boot_id(cfg, client, &run.run_id).await {
        match first_boot_id {
            None => *first_boot_id = Some(boot_id),
            Some(first) if *first != boot_id => {
                eprintln!(
                    "reaper {}: boot_id changed ({first} → {boot_id}) — the box relaunched \
                     and lost all session state; reaping",
                    run.run_id
                );
                return true;
            }
            Some(_) => {}
        }
    }
    false
}

/// Whether a shard has written its (terminal) result envelope yet. The agent only
/// writes the envelope once, at completion, so its presence means the job is done.
async fn shard_terminal(
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard: u32,
) -> bool {
    let url = backend.presign_get(
        &format!("{}/result.json", spec::shard_prefix(run_id, shard)),
        Duration::from_secs(72 * 3600),
    );
    matches!(fetch_envelope(http, &url).await, Ok(Some(_)))
}

/// Spawn a detached `saladfingers reap RUN_ID` that outlives this process, so a
/// `--detach` run (or a session) stops billing when it finishes even after the CLI exits.
pub(crate) fn spawn_reaper(run_id: &str, org: &str, project: &str) -> Result<()> {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().context("locating own executable")?;
    let log =
        std::fs::File::create(state::reaper_log_path(run_id)?).context("creating reaper log")?;
    std::process::Command::new(exe)
        // Forward org/project explicitly: a run configured via --org/--project flags (rather
        // than env or config file) would otherwise spawn a reaper that resolves a different
        // org/project and never finds the group to reap — a billing leak.
        .args(["--org", org, "--project", project, "reap", run_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log.try_clone()?))
        .stderr(std::process::Stdio::from(log))
        .process_group(0) // own group: not killed by Ctrl-C / terminal close of the parent
        .spawn()
        .context("spawning detached reaper")?;
    Ok(())
}

// ---- internals ------------------------------------------------------------

/// SaladCloud caps each environment value at 1000 characters; stay safely under it.
const ENV_VALUE_MAX: usize = 960;
/// The agent reassembles `SF_JOB_URL` + `SF_JOB_URL_1..=9` (ten segments max).
const JOB_URL_MAX_PARTS: usize = 10;

/// The container environment for one shard's `sf-agent run`.
///
/// `SF_JOB_SHA256` pins the exact `JobSpec` bytes this CLI uploaded. The spec carries the
/// `command` the agent execs plus every presigned URL for the run, so anyone able to
/// overwrite that object between upload and fetch — anyone with write access to the
/// bucket, or a leaked/misscoped storage credential — would otherwise substitute their own
/// spec and get code execution on the operator's rented, operator-billed GPU along with the
/// run's inputs, outputs and checkpoints. The agent recomputes the digest over the bytes it
/// fetched with the very same [`transfer::sha256_hex`] and refuses to run on a mismatch;
/// without this variable that check silently does nothing.
///
/// A `job_url` longer than SaladCloud's 1000-char env-value cap is split across
/// `SF_JOB_URL` + `SF_JOB_URL_1..` (the agent's `job_url()` concatenates them back).
/// Typical presigned URLs fit in one value; long endpoints or session-token credentials
/// (`X-Amz-Security-Token`) are what push past the cap.
fn job_env(
    job_url: &str,
    run_id: &str,
    shard: u32,
    shard_count: u32,
    job_sha256: String,
) -> Result<BTreeMap<String, String>> {
    let mut env = BTreeMap::new();
    let chunks: Vec<&str> = job_url
        .as_bytes()
        .chunks(ENV_VALUE_MAX)
        // Presigned URLs are ASCII, so byte chunks are valid UTF-8.
        .map(|c| std::str::from_utf8(c).context("job URL is not ASCII/UTF-8"))
        .collect::<Result<_>>()?;
    if chunks.len() > JOB_URL_MAX_PARTS {
        bail!(
            "presigned job URL is {} chars — beyond the {} env values the agent reassembles",
            job_url.len(),
            JOB_URL_MAX_PARTS
        );
    }
    for (i, chunk) in chunks.iter().enumerate() {
        let key = if i == 0 {
            "SF_JOB_URL".to_string()
        } else {
            format!("SF_JOB_URL_{i}")
        };
        env.insert(key, (*chunk).to_string());
    }
    env.insert("SF_JOB_SHA256".to_string(), job_sha256);
    env.insert("SF_RUN_ID".to_string(), run_id.to_string());
    env.insert("SF_SHARD_INDEX".to_string(), shard.to_string());
    env.insert("SF_SHARD_COUNT".to_string(), shard_count.to_string());
    Ok(env)
}

/// Wall-clock cap for waiting on a run before treating it as stuck: 2× its budget + 10 min
/// slack, or 2 h if unbudgeted, capped at 24 h. The agent enforces its own max_duration, so
/// a healthy run writes its envelope well before this; the cap only bounds a run whose agent
/// never reports (crash, bad image) so its group is torn down instead of billing forever.
pub(crate) fn wait_hard_cap(max_duration_secs: Option<u64>) -> Duration {
    Duration::from_secs(
        max_duration_secs
            .map_or(2 * 3600, |d| d.saturating_mul(2).saturating_add(600))
            .min(24 * 3600),
    )
}

/// Best-effort: flip a run's local status to a terminal state after its group has been torn
/// down, so `gc`/`status` stop treating it as live.
fn mark_run_failed(run_id: &str) {
    if let Ok(Some(mut run)) = state::load_run(run_id) {
        run.status = "failed".into();
        let _ = state::save_run(&run);
    }
}

#[allow(clippy::too_many_arguments)] // internal orchestration helper; all args are distinct
async fn await_and_collect(
    client: &SaladClient,
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard_count: u32,
    allowed_outputs: Option<&[String]>,
    max_parts: u32,
    expiry: Duration,
    hourly_price: Option<Decimal>,
    hard_cap: Duration,
) -> Result<i32> {
    let mut worst = 0i32;
    let mut shard_spans: Vec<Vec<RunningSpan>> = Vec::with_capacity(shard_count as usize);
    for shard in 0..shard_count {
        let name = names::group_name(run_id, (shard_count > 1).then_some(shard));
        eprintln!("run {run_id}: waiting on shard {shard} ({name})...");
        let (result, spans) = tokio::select! {
            r = await_shard(client, http, backend, run_id, shard, shard_count, hard_cap) => r?,
            () = ctrl_c() => {
                eprintln!("\ncancelling run {run_id}...");
                cleanup(client, run_id, shard_count).await;
                std::process::exit(130);
            }
        };
        match result {
            ShardOutcome::Envelope(env) => {
                let code = shard_exit_code(&env);
                worst = worst.max(code);
                eprintln!("  shard {shard}: {:?} (exit {code})", env.status);
                // A non-exec failure (AgentError, TimedOut, …) carries its reason in
                // the envelope; surface it, or the exit code is a riddle.
                if let Some(err) = &env.error {
                    eprintln!("  shard {shard}: error: {err}");
                }
                // Delete FIRST, then download: the artifacts live in object storage
                // (the envelope is the commit record), so nothing is needed from the
                // node — but the group keeps billing for every second a potentially
                // multi-GB download would take. A failed download can always be
                // retried via `attach`; a billing group cannot be un-billed.
                let _ = deploy::delete_group(client, &name).await;
                download_outputs(
                    http,
                    backend,
                    run_id,
                    shard,
                    &env,
                    allowed_outputs,
                    max_parts,
                    expiry,
                )
                .await;
            }
            ShardOutcome::Failed => {
                worst = worst.max(1);
                eprintln!("  shard {shard}: group failed (no envelope) — keeping it for forensics");
                print_system_logs(client, &name).await;
            }
        }
        shard_spans.push(spans);
    }
    // Reallocations we observed = machines beyond the first, per shard.
    let reallocations: usize = shard_spans.iter().map(|s| s.len().saturating_sub(1)).sum();
    let (billed_seconds, cost) = finalize_run(run_id, worst, &shard_spans, hourly_price);
    summary(run_id, worst, billed_seconds, cost, reallocations);
    Ok(worst)
}

/// The process exit code a collected shard contributes to `saladfingers run`.
///
/// The envelope's `exit_code` is what the *exec* exited with, and it is authoritative
/// only where the exec outcome IS the run outcome (`Succeeded` / `Failed`). It must
/// NOT short-circuit the other statuses: on `AgentError` the exec may well have
/// exited 0 before the agent failed — e.g. a declared output that matched nothing —
/// and trusting that 0 would let a scripted `saladfingers run` (CI) mistake
/// destroyed work for success (caught live in the release validation round).
fn shard_exit_code(env: &ResultEnvelope) -> i32 {
    match env.status {
        JobStatus::Succeeded => env.exit_code.unwrap_or(0),
        JobStatus::Failed => env.exit_code.unwrap_or(1),
        JobStatus::TimedOut => 124,
        JobStatus::Interrupted => 143,
        JobStatus::AgentError => 1,
    }
}

/// The end of an envelope's billed `running` window: when outputs finished, or the
/// tightest earlier phase we recorded. SaladCloud bills only the `running` state, and
/// `agent_start` ≈ when the container starts running, so `agent_start` → this end is
/// the precise billed window for the node the run completed on.
fn envelope_running_end(t: &Timings) -> DateTime<Utc> {
    t.outputs_done
        .or(t.exec_end)
        .or(t.exec_start)
        .unwrap_or(t.agent_start)
}

/// Cost estimate in USD: hourly price × billed seconds ÷ 3600, to 6 decimal places.
fn cost_estimate(hourly_usd: Decimal, billed_secs: u64) -> Decimal {
    (hourly_usd * Decimal::from(billed_secs) / Decimal::from(3600u32)).round_dp(6)
}

/// Persist the observed running spans into the run state, sum the billed time across
/// every machine every shard passed through (reallocations included), and record the
/// final status + result estimate. Returns `(billed_seconds, cost)`.
fn finalize_run(
    run_id: &str,
    exit_code: i32,
    shard_spans: &[Vec<RunningSpan>],
    hourly_price: Option<Decimal>,
) -> (u64, Option<Decimal>) {
    let now = Utc::now();
    let status = if exit_code == 0 {
        "succeeded"
    } else {
        "failed"
    };
    if let Ok(Some(mut run)) = state::load_run(run_id) {
        for (shard, spans) in shard_spans.iter().enumerate() {
            let shard = shard as u32;
            if let Some(group) = run.groups.iter_mut().find(|g| g.shard == shard) {
                group.machine_history = spans.iter().map(|s| s.machine_id.clone()).collect();
                group.running_spans = spans.clone();
            }
        }
        let billed = run.billed_seconds_est(now);
        let cost = hourly_price.map(|hp| cost_estimate(hp, billed));
        run.status = status.to_string();
        run.result = Some(RunResult {
            exit_code,
            billed_seconds_est: billed,
            cost_est_usd: cost,
        });
        let _ = state::save_run(&run);
        (billed, cost)
    } else {
        // No local state (e.g. it was gc'd) — still report from the observed spans.
        let billed: u64 = shard_spans
            .iter()
            .flatten()
            .map(|s| s.billed_seconds(now))
            .sum();
        let cost = hourly_price.map(|hp| cost_estimate(hp, billed));
        (billed, cost)
    }
}

enum ShardOutcome {
    Envelope(Box<ResultEnvelope>),
    Failed,
}

/// Accumulates per-machine running spans for one shard as the poll loop observes the
/// group's current `running` instance. When the running `machine_id` changes (a
/// reallocation — e.g. the bandwidth gate moved the run to a new node), the previous
/// machine's span is closed at the last time it was seen running, and a new span is
/// opened. This is what lets the billed estimate count every node the shard passed
/// through, not just the final one.
#[derive(Debug, Default)]
struct SpanTracker {
    spans: Vec<RunningSpan>,
    /// Last time the currently-open span's machine was seen `running`.
    last_running: Option<DateTime<Utc>>,
}

impl SpanTracker {
    /// Adopt spans recorded by a previous process (an `attach` after the original CLI
    /// died or detached). Without this, `finalize_run` would overwrite the state's
    /// spans with only what THIS process observed, silently dropping billed time.
    fn seed(&mut self, spans: Vec<RunningSpan>) {
        self.spans = spans;
        self.last_running = None;
    }

    /// Record that `machine_id` is the instance currently in `running` at `now`.
    fn observe_running(&mut self, machine_id: &str, now: DateTime<Utc>) {
        let same = self
            .open_span()
            .is_some_and(|open| open.machine_id == machine_id);
        if same {
            self.last_running = Some(now);
            return;
        }
        // Either nothing is open, or a different machine now runs (a reallocation):
        // close the previous span at the last time we saw it running — the gap while
        // the new node allocates/downloads is free, not billed to the old node. For a
        // seeded span whose hand-off we never observed (`last_running` is None), close
        // at `now`: slightly overcounting the free allocation gap beats dropping the
        // old node's billed time entirely.
        let boundary = self.last_running.take().unwrap_or(now);
        if let Some(open) = self.open_span() {
            open.end = Some(boundary.max(open.start));
        }
        self.spans.push(RunningSpan {
            machine_id: machine_id.to_string(),
            start: now,
            end: None,
        });
        self.last_running = Some(now);
    }

    /// Close the currently-open span (shard finished, failed, or cancelled).
    fn close(&mut self) {
        if let Some(last_running) = self.last_running.take()
            && let Some(open) = self.open_span()
        {
            open.end = Some(last_running.max(open.start));
        }
    }

    /// Close the final span from the authoritative envelope timings — the tightest
    /// billed window we have for the node the run actually completed on. Discarded
    /// nodes keep their poll-observed spans.
    fn finalize_with_envelope(&mut self, env: &ResultEnvelope) {
        let start = env.timings.agent_start;
        let end = envelope_running_end(&env.timings).max(start);
        let env_machine = env.node.machine_id.clone().unwrap_or_default();
        // Only refine the open span in place when it is (or may be) the same node the
        // envelope came from. A mismatched open span — a seeded span from a machine
        // that was reallocated away while nobody watched — must be closed and kept,
        // not have its window and attribution overwritten by the final node's.
        let refines_open = self
            .open_span()
            .is_some_and(|open| env_machine.is_empty() || open.machine_id == env_machine);
        if refines_open {
            if let Some(open) = self.open_span() {
                open.start = start;
                open.end = Some(end);
            }
        } else {
            let boundary = self.last_running.take().unwrap_or(start);
            if let Some(open) = self.open_span() {
                open.end = Some(boundary.max(open.start));
            }
            // Record the final node's envelope window (also covers the case where it
            // started and finished entirely between polls).
            self.spans.push(RunningSpan {
                machine_id: env_machine,
                start,
                end: Some(end),
            });
        }
        self.last_running = None;
    }

    /// The currently-open (unclosed) span, if any.
    fn open_span(&mut self) -> Option<&mut RunningSpan> {
        self.spans.last_mut().filter(|s| s.end.is_none())
    }

    /// The accumulated spans.
    fn into_spans(self) -> Vec<RunningSpan> {
        self.spans
    }
}

/// The `machine_id` of the instance currently in `running`, if any. A shard group is
/// single-replica, so there is at most one instance.
fn running_machine_id(instances: &[Instance]) -> Option<String> {
    instances
        .iter()
        .find(|i| i.state == Some(InstanceState::Running))
        .and_then(|i| i.machine_id.clone())
}

async fn await_shard(
    client: &SaladClient,
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard: u32,
    shard_count: u32,
    hard_cap: Duration,
) -> Result<(ShardOutcome, Vec<RunningSpan>)> {
    let name = names::group_name(run_id, (shard_count > 1).then_some(shard));
    let result_get = backend.presign_get(
        &format!("{}/result.json", spec::shard_prefix(run_id, shard)),
        Duration::from_secs(72 * 3600),
    );
    let start = Instant::now();
    let mut last = String::new();
    let mut tracker = SpanTracker::default();
    // Adopt spans a previous process already observed (attach after a crash/detach),
    // so its billed time survives into the final estimate instead of being overwritten.
    if let Ok(Some(run)) = state::load_run(run_id)
        && let Some(g) = run.groups.iter().find(|g| g.shard == shard)
        && !g.running_spans.is_empty()
    {
        tracker.seed(g.running_spans.clone());
    }
    loop {
        // Envelope present = authoritative completion.
        if let Ok(Some(env)) = fetch_envelope(http, &result_get).await {
            tracker.finalize_with_envelope(&env);
            return Ok((ShardOutcome::Envelope(Box::new(env)), tracker.into_spans()));
        }
        // A transient control-plane error must not abandon a live, billing run: log it and
        // keep polling (like `list_instances` below). The hard cap still bounds a run whose
        // control plane is genuinely, persistently unreachable.
        let status = match client.get_container_group(&name).await {
            Ok(group) => group.status().unwrap_or(GroupStatus::Unknown),
            Err(e) => {
                tracing::warn!("poll of {name} failed: {e:#}; retrying");
                GroupStatus::Unknown
            }
        };
        // Track which node is billing us: a change in the running machine_id is a
        // reallocation, and every node that reached `running` accrues billed time.
        let instances = client.list_instances(&name).await.unwrap_or_default();
        if let Some(machine_id) = running_machine_id(&instances) {
            tracker.observe_running(&machine_id, Utc::now());
        }
        let key = format!("{status:?}");
        if key != last {
            eprintln!(
                "    {}  {}",
                Utc::now().format("%H:%M:%S"),
                key.to_lowercase()
            );
            last = key;
        }
        if matches!(status, GroupStatus::Failed) {
            // Grace: the envelope may still be arriving.
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Ok(Some(env)) = fetch_envelope(http, &result_get).await {
                tracker.finalize_with_envelope(&env);
                return Ok((ShardOutcome::Envelope(Box::new(env)), tracker.into_spans()));
            }
            tracker.close();
            return Ok((ShardOutcome::Failed, tracker.into_spans()));
        }
        if start.elapsed() > hard_cap {
            bail!("run timed out after {hard_cap:?} waiting on shard {shard}");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn upload_inputs(
    backend: &S3Backend,
    http: &reqwest::Client,
    run_id: &str,
    inputs: &[(String, PathBuf, bool)],
    max_parts: u32,
    expiry: Duration,
) -> Result<Vec<UploadedInput>> {
    let mut uploaded = Vec::new();
    for (index, (dest, local, archive)) in inputs.iter().enumerate() {
        let stem = spec::input_stem(run_id, index);
        let put_urls: Vec<String> = (0..max_parts)
            .map(|k| backend.presign_put(&transfer::part_key(&stem, k), expiry))
            .collect();
        let report = transfer::upload_artifact(http, local, *archive, &put_urls, &stem)
            .await
            .with_context(|| format!("uploading input {}", local.display()))?;
        let get_urls: Vec<String> = (0..report.parts)
            .map(|k| backend.presign_get(&transfer::part_key(&stem, k), expiry))
            .collect();
        uploaded.push(UploadedInput {
            dest: dest.clone(),
            archive: *archive,
            get_urls,
        });
    }
    Ok(uploaded)
}

#[allow(clippy::too_many_arguments)] // internal orchestration helper; all args are distinct
async fn download_outputs(
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard: u32,
    env: &ResultEnvelope,
    allowed_outputs: Option<&[String]>,
    max_parts: u32,
    expiry: Duration,
) {
    if env.uploads.is_empty() {
        return;
    }
    let out_dir = PathBuf::from("sf-out").join(run_id).join(shard.to_string());
    for upload in &env.uploads {
        // The envelope is written by `sf-agent` on the rented node via a presigned PUT it
        // fully controls, so every field — including `name` — is untrusted. Gate it before
        // it reaches any filesystem join or storage key.
        if let Err(reason) = admit_output(&upload.name, upload.parts, max_parts, allowed_outputs) {
            eprintln!("  warning: skipping {reason}");
            continue;
        }
        let stem = format!("{}/out/{}", spec::shard_prefix(run_id, shard), upload.name);
        let get_urls: Vec<String> = (0..upload.parts)
            .map(|k| backend.presign_get(&transfer::part_key(&stem, k), expiry))
            .collect();
        let dest = out_dir.join(&upload.name);
        match transfer::download_artifact(http, &get_urls, &dest, true, Some(&upload.sha256)).await
        {
            Ok(()) => eprintln!("  downloaded '{}' → {}", upload.name, dest.display()),
            Err(e) => eprintln!("  warning: failed to download '{}': {e}", upload.name),
        }
    }
}

/// Decide whether an untrusted envelope's artifact record may be collected, returning `Err`
/// with a reason to skip it. Three gates, most-fundamental first:
///
/// 1. **Path shape (always):** the name must be a plain relative path, so `out_dir.join(name)`
///    can never escape the `sf-out/<run>/<shard>` tree.
/// 2. **Part count (always):** the CLI only issues `max_parts` presigned PUT URLs per output
///    (the run's [`spec::DEFAULT_MAX_PARTS`]/`[storage] max_artifact_parts` ceiling), so an
///    honest artifact has at most that many parts. A larger count is a malformed or hostile
///    envelope; refuse it before it drives `(0..parts)` presigned-URL generation — a claim of
///    billions of parts would otherwise OOM the host.
/// 3. **Allow-list (when the declared set is known):** the name must be one the run actually
///    asked for. A hostile node therefore cannot make the CLI materialise an artifact — even
///    a well-formed relative one — under a name the run never declared. When the set is
///    unknown (an older state file with no recorded outputs), gates 1–2 stand alone.
fn admit_output(
    name: &str,
    parts: u32,
    max_parts: u32,
    allowed: Option<&[String]>,
) -> Result<(), String> {
    if !is_safe_relative(name) {
        return Err(format!(
            "unsafe output name {name:?} (must be a plain relative path)"
        ));
    }
    if parts > max_parts {
        return Err(format!(
            "output {name:?} claims {parts} parts (max {max_parts})"
        ));
    }
    if let Some(allowed) = allowed
        && !allowed.iter().any(|declared| declared == name)
    {
        return Err(format!(
            "unexpected output {name:?} (not a declared output of this run)"
        ));
    }
    Ok(())
}

/// Whether `name` is a safe, plain relative path: non-empty and every component an ordinary
/// segment — no root (`/…`), no drive prefix, no `..`, no `.`. Gates the attacker-controlled
/// [`ResultEnvelope`] artifact names before they are joined onto a local path or a storage
/// key, guaranteeing the result stays under the intended output directory.
fn is_safe_relative(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

async fn cleanup(client: &SaladClient, run_id: &str, shard_count: u32) {
    for shard in 0..shard_count {
        let name = names::group_name(run_id, (shard_count > 1).then_some(shard));
        let _ = client.stop_container_group(&name).await;
        let _ = deploy::delete_group(client, &name).await;
    }
}

async fn print_system_logs(client: &SaladClient, name: &str) {
    if let Ok(entries) = client.get_system_logs(name).await {
        for entry in entries.iter().take(10) {
            for event in &entry.events {
                if let Some(n) = &event.name {
                    eprintln!("    log: {n}");
                }
            }
        }
    }
}

async fn fetch_envelope(http: &reqwest::Client, url: &str) -> Result<Option<ResultEnvelope>> {
    // `without_url`: the URL is presigned; its signature must not reach error text.
    // Today's callers discard this error, but stripping it here means no future caller
    // can turn a poll failure into a leaked capability.
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    Ok(resp.json::<ResultEnvelope>().await.ok())
}

async fn put_object(http: &reqwest::Client, url: &str, body: Vec<u8>) -> Result<()> {
    // `without_url`: reqwest errors carry the full URL — for a presigned URL that is a
    // live capability (the signature is in the query string) and must not reach error
    // text. This error propagates out of `run`, and `main` returns `anyhow::Result`, so
    // `Termination` prints it with `Debug` — the whole source chain — into stderr/CI logs.
    http.put(url)
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    Ok(())
}

async fn ctrl_c() {
    let _ = tokio::signal::ctrl_c().await;
}

fn summary(
    run_id: &str,
    exit_code: i32,
    billed_seconds: u64,
    cost: Option<Decimal>,
    reallocations: usize,
) {
    let mut t = table(&["field", "value"]);
    t.add_row(vec!["run".to_string(), run_id.to_string()]);
    t.add_row(vec!["exit code".to_string(), exit_code.to_string()]);
    t.add_row(vec![
        "outcome".to_string(),
        if exit_code == 0 {
            "success".into()
        } else {
            "failure".to_string()
        },
    ]);
    t.add_row(vec![
        "billed (est)".to_string(),
        format!("{billed_seconds}s"),
    ]);
    t.add_row(vec![
        "cost (est)".to_string(),
        cost.map_or_else(|| "?".to_string(), |c| format!("${c}")),
    ]);
    // Only shown when the run bounced across nodes — billed time already sums them.
    if reallocations > 0 {
        t.add_row(vec!["reallocations".to_string(), reallocations.to_string()]);
    }
    print_table(&t);
}

fn resolve_params(cfg: &Config, profile: Option<&Profile>, args: &RunArgs) -> Result<RunParams> {
    let image = crate::image::resolve_deploy_image(
        args.image.as_deref(),
        profile.and_then(|p| p.image.as_deref()),
    )?;
    let mut gpu_classes = args.gpu_classes.clone();
    if gpu_classes.is_empty() {
        gpu_classes = profile.map(|p| p.gpu_classes.clone()).unwrap_or_default();
    }
    if gpu_classes.is_empty() {
        bail!("no GPU class (pass --gpu-class or set gpu_classes in the profile)");
    }
    let priority = args
        .priority
        .clone()
        .or_else(|| profile.and_then(|p| p.priority.clone()))
        .or_else(|| cfg.defaults.priority.clone())
        .unwrap_or_else(|| "batch".to_string());

    let mut env = profile.map(|p| p.env.clone()).unwrap_or_default();
    for kv in &args.env {
        if let Some((k, v)) = kv.split_once('=') {
            env.insert(k.to_string(), v.to_string());
        }
    }

    let max_duration_secs = args
        .max_duration
        .as_deref()
        .or_else(|| profile.and_then(|p| p.max_duration.as_deref()))
        .map(parse_duration_secs)
        .transpose()?;

    let gate = if args.no_gate {
        None
    } else {
        profile.and_then(|p| match (p.min_download_mbps, p.min_upload_mbps) {
            (None, None) => None,
            (down, up) => Some(GateParams {
                min_download_mbps: down,
                min_upload_mbps: up,
            }),
        })
    };

    // `--checkpoint` wins wholesale (with its own interval/quiesce flags); otherwise the
    // profile's `[profiles.<name>.checkpoint]` section applies.
    let checkpoint = args
        .checkpoint
        .as_ref()
        .map(|dir| CheckpointParams {
            dir: dir.clone(),
            interval_secs: args.checkpoint_interval,
            quiesce_secs: args.checkpoint_quiesce,
        })
        .or_else(|| {
            profile
                .and_then(|p| p.checkpoint.clone())
                .map(|c| CheckpointParams {
                    dir: c.dir,
                    interval_secs: c.interval_secs,
                    quiesce_secs: c.quiesce_secs,
                })
        });

    // Artifact lists: CLI flags override the profile's `artifacts.pull`/`push` wholesale.
    let input_specs = if args.inputs.is_empty() {
        profile
            .and_then(|p| p.artifacts.as_ref())
            .map(|a| a.pull.clone())
            .unwrap_or_default()
    } else {
        args.inputs.clone()
    };
    let output_specs = if args.outputs.is_empty() {
        profile
            .and_then(|p| p.artifacts.as_ref())
            .map(|a| a.push.clone())
            .unwrap_or_default()
    } else {
        args.outputs.clone()
    };

    // The API wants lowercase ISO alpha-2 country codes.
    let country_codes: Vec<String> = if args.countries.is_empty() {
        cfg.defaults.country_codes.clone()
    } else {
        args.countries.clone()
    }
    .iter()
    .map(|c| c.trim().to_lowercase())
    .filter(|c| !c.is_empty())
    .collect();

    Ok(RunParams {
        command: args.command.clone(),
        image,
        gpu_classes,
        cpu: profile.and_then(|p| p.cpu).unwrap_or(8),
        memory_mb: profile.and_then(|p| p.memory_gb).unwrap_or(16) * 1024,
        disk_gib: profile.and_then(|p| p.disk_gb).unwrap_or(20) + 5, // spool headroom
        shm_mb: profile.and_then(|p| p.shm_mb),
        priority,
        env,
        max_duration_secs,
        // An explicit --replicas overrides the profile in BOTH directions; the old
        // `max()` could only raise the count, silently multiplying spend.
        replicas: args
            .replicas
            .or_else(|| profile.and_then(|p| p.replicas))
            .unwrap_or(1),
        country_codes,
        inputs: parse_inputs(&input_specs)?,
        outputs: parse_outputs(&output_specs),
        // Presigned-URL blocks per artifact. From `[storage] max_artifact_parts` (clamped),
        // else the default; storage-less configs never reach the transfer path anyway.
        max_parts: cfg.storage.as_ref().map_or(
            spec::DEFAULT_MAX_PARTS,
            crate::config::StorageConfig::effective_max_parts,
        ),
        gate,
        checkpoint,
        name_hint: args.name_hint.clone(),
    })
}

fn parse_inputs(specs: &[String]) -> Result<Vec<(String, PathBuf, bool)>> {
    let mut out = Vec::new();
    for s in specs {
        let (src, dest) = match s.split_once(':') {
            Some((src, dest)) => (src.to_string(), dest.to_string()),
            None => {
                let name = std::path::Path::new(s)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("input");
                (s.clone(), format!("/work/{name}"))
            }
        };
        let local = PathBuf::from(&src);
        if !local.exists() {
            bail!("input source does not exist: {src}");
        }
        let archive = local.is_dir();
        out.push((dest, local, archive));
    }
    Ok(out)
}

fn parse_outputs(specs: &[String]) -> Vec<OutputRequest> {
    specs
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let (glob, name) = match s.split_once(':') {
                Some((glob, name)) => (glob.to_string(), name.to_string()),
                None => (s.clone(), format!("output{i}")),
            };
            OutputRequest {
                name,
                src_glob: glob,
                archive: true,
            }
        })
        .collect()
}

fn parse_duration_secs(s: &str) -> Result<u64> {
    Ok(humantime::parse_duration(s)
        .with_context(|| format!("invalid duration '{s}'"))?
        .as_secs())
}

fn save_state(cfg: &Config, run_id: &str, params: &RunParams, groups: &[GroupRef], status: &str) {
    let run = RunState {
        v: state::STATE_VERSION,
        run_id: run_id.to_string(),
        kind: "run".to_string(),
        created_at: Utc::now(),
        org: cfg.organization.clone(),
        project: cfg.project.clone(),
        profile: params.name_hint.clone(),
        image: Some(params.image.clone()),
        gpu_classes: params.gpu_classes.clone(),
        priority: Some(params.priority.clone()),
        command: params.command.clone(),
        // Record the declared output names so `attach` (which reconstructs from state) can
        // allow-list them against the untrusted envelope just as the foreground path does.
        output_names: Some(params.outputs.iter().map(|o| o.name.clone()).collect()),
        // Record the part ceiling this run presigned URLs for, so `attach` caps the untrusted
        // envelope at the same value regardless of any later `[storage]` config change.
        max_parts: Some(params.max_parts),
        groups: groups.to_vec(),
        status: status.to_string(),
        agent_token: None,
        max_duration_secs: params.max_duration_secs,
        result: None,
    };
    let _ = state::save_run(&run);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timings_at(start: DateTime<Utc>, exec_end_secs: i64, with_outputs: bool) -> Timings {
        Timings {
            agent_start: start,
            gate_done: None,
            inputs_done: None,
            exec_start: Some(start + chrono::Duration::seconds(2)),
            exec_end: Some(start + chrono::Duration::seconds(exec_end_secs)),
            outputs_done: with_outputs
                .then(|| start + chrono::Duration::seconds(exec_end_secs + 1)),
        }
    }

    fn envelope_for(machine: &str, timings: Timings) -> ResultEnvelope {
        let node = saladfingers_protocol::NodeInfo {
            machine_id: Some(machine.to_string()),
            ..Default::default()
        };
        ResultEnvelope {
            v: saladfingers_protocol::PROTOCOL_VERSION,
            run_id: "sf-test".into(),
            shard_index: 0,
            status: saladfingers_protocol::JobStatus::Succeeded,
            exit_code: Some(0),
            error: None,
            timings,
            node,
            uploads: vec![],
            attempts: 1,
            gate_reallocations: 0,
        }
    }

    #[test]
    fn shard_exit_code_never_reports_success_for_a_non_exec_outcome() {
        let mut env = envelope_for("m1", timings_at(Utc::now(), 5, false));

        // The live-caught case: exec exited 0, then the output upload failed (zero
        // glob matches) → AgentError with exit_code Some(0) must still be a failure.
        env.status = saladfingers_protocol::JobStatus::AgentError;
        env.exit_code = Some(0);
        assert_eq!(shard_exit_code(&env), 1);

        // TimedOut / Interrupted map to their conventional codes even if the agent
        // recorded what the exec exited with.
        env.status = saladfingers_protocol::JobStatus::TimedOut;
        assert_eq!(shard_exit_code(&env), 124);
        env.status = saladfingers_protocol::JobStatus::Interrupted;
        assert_eq!(shard_exit_code(&env), 143);

        // Where the exec outcome IS the run outcome, its code passes through…
        env.status = saladfingers_protocol::JobStatus::Failed;
        env.exit_code = Some(42);
        assert_eq!(shard_exit_code(&env), 42);
        env.status = saladfingers_protocol::JobStatus::Succeeded;
        env.exit_code = Some(0);
        assert_eq!(shard_exit_code(&env), 0);
        // …with a non-zero floor for a Failed envelope missing the code.
        env.status = saladfingers_protocol::JobStatus::Failed;
        env.exit_code = None;
        assert_eq!(shard_exit_code(&env), 1);
    }

    #[test]
    fn cost_and_billed_math() {
        // RTX 3060 @ batch: $0.04/h × 8 s = $0.0000888… → $0.000089 (6 dp).
        assert_eq!(cost_estimate(Decimal::new(4, 2), 8), Decimal::new(89, 6));
        assert_eq!(cost_estimate(Decimal::new(4, 2), 0), Decimal::ZERO);

        // The billed window ends at outputs_done when present…
        let start = Utc::now();
        assert_eq!(
            envelope_running_end(&timings_at(start, 7, true)),
            start + chrono::Duration::seconds(8)
        );
        // …else falls back to exec_end.
        assert_eq!(
            envelope_running_end(&timings_at(start, 7, false)),
            start + chrono::Duration::seconds(7)
        );
    }

    #[test]
    fn billed_and_cost_sum_across_reallocations() {
        // A shard the bandwidth gate reallocated once: it reached `running` on mach-a
        // (100 s billed) before moving to mach-b (250 s billed). The estimate must
        // count BOTH nodes (350 s), not just the final one (250 s) as the old code did.
        let base = Utc::now();
        let mkspan = |machine: &str, s: i64, e: i64| RunningSpan {
            machine_id: machine.to_string(),
            start: base + chrono::Duration::seconds(s),
            end: Some(base + chrono::Duration::seconds(e)),
        };
        let run = RunState {
            v: state::STATE_VERSION,
            run_id: "sf-realloc".into(),
            kind: "run".into(),
            created_at: base,
            org: "o".into(),
            project: "p".into(),
            profile: None,
            image: None,
            gpu_classes: vec!["rtx 4090".into()],
            priority: Some("batch".into()),
            command: vec![],
            output_names: None,
            max_parts: None,
            groups: vec![GroupRef {
                name: "sf-realloc".into(),
                shard: 0,
                last_state: Some("running".into()),
                machine_history: vec!["mach-a".into(), "mach-b".into()],
                running_spans: vec![mkspan("mach-a", 0, 100), mkspan("mach-b", 100, 350)],
            }],
            status: "succeeded".into(),
            agent_token: None,
            max_duration_secs: None,
            result: None,
        };

        let billed = run.billed_seconds_est(base + chrono::Duration::seconds(400));
        assert_eq!(billed, 350, "sums both machines, not just the final 250 s");

        // $0.36/h × 350 s = $0.035 (vs the buggy final-only $0.025).
        assert_eq!(
            cost_estimate(Decimal::new(36, 2), billed),
            Decimal::new(35, 3)
        );
        assert_ne!(
            cost_estimate(Decimal::new(36, 2), billed),
            Decimal::new(25, 3)
        );
    }

    #[test]
    fn tracker_closes_previous_span_on_reallocation() {
        let base = Utc::now();
        let at = |s: i64| base + chrono::Duration::seconds(s);
        let mut tracker = SpanTracker::default();
        tracker.observe_running("mach-a", at(0));
        tracker.observe_running("mach-a", at(10)); // same node — still one open span
        tracker.observe_running("mach-b", at(35)); // reallocation: close mach-a, open mach-b
        tracker.observe_running("mach-b", at(70));
        tracker.close(); // shard finished on mach-b at its last-seen time (70)

        let spans = tracker.into_spans();
        assert_eq!(
            spans.len(),
            2,
            "the discarded node's span is kept, not dropped"
        );
        assert_eq!(spans[0].machine_id, "mach-a");
        // mach-a closed at the last time it was seen running (10), NOT when mach-b
        // appeared (35) — the reallocation gap is free.
        assert_eq!(spans[0].billed_seconds(at(1000)), 10);
        assert_eq!(spans[1].machine_id, "mach-b");
        assert_eq!(spans[1].billed_seconds(at(1000)), 35);
        // The undercount bug would have counted only mach-b (35 s); we count both (45 s).
        let total: u64 = spans.iter().map(|s| s.billed_seconds(at(1000))).sum();
        assert_eq!(total, 45);
    }

    #[test]
    fn finalize_prefers_envelope_for_final_span() {
        let base = Utc::now();
        let at = |s: i64| base + chrono::Duration::seconds(s);
        let mut tracker = SpanTracker::default();
        tracker.observe_running("mach-a", at(0));
        tracker.observe_running("mach-a", at(100)); // discarded node ran 0..100
        tracker.observe_running("mach-b", at(130)); // reallocated to the final node

        // Envelope from the final node: precise window agent_start=132 → outputs_done=250.
        let env = envelope_for(
            "mach-b",
            Timings {
                agent_start: at(132),
                gate_done: None,
                inputs_done: None,
                exec_start: Some(at(135)),
                exec_end: Some(at(248)),
                outputs_done: Some(at(250)),
            },
        );
        tracker.finalize_with_envelope(&env);

        let spans = tracker.into_spans();
        assert_eq!(spans.len(), 2);
        // Discarded node counted from poll observation (0 → 100).
        assert_eq!(spans[0].machine_id, "mach-a");
        assert_eq!(spans[0].billed_seconds(at(1000)), 100);
        // Final node counted from the precise envelope window (132 → 250 = 118 s).
        assert_eq!(spans[1].machine_id, "mach-b");
        assert_eq!(spans[1].billed_seconds(at(1000)), 118);
    }

    #[test]
    fn seeded_spans_survive_a_new_tracker_and_close_on_the_next_sighting() {
        // `attach` after the original CLI died: the state already holds observed spans
        // (one still open). They must be preserved — and the open one closed when a
        // DIFFERENT machine is sighted — instead of being overwritten by the fresh
        // tracker, which silently dropped the earlier process's billed time.
        let base = Utc::now();
        let at = |s: i64| base + chrono::Duration::seconds(s);
        let mut tracker = SpanTracker::default();
        tracker.seed(vec![
            RunningSpan {
                machine_id: "mach-a".into(),
                start: at(0),
                end: Some(at(50)),
            },
            RunningSpan {
                machine_id: "mach-b".into(),
                start: at(80),
                end: None, // open: mach-b was running when the old process died
            },
        ]);
        // The new process first sights mach-c at t=200: mach-b's hand-off was never
        // observed, so it closes at the sighting (over the free gap, never dropped).
        tracker.observe_running("mach-c", at(200));
        tracker.close();

        let spans = tracker.into_spans();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].billed_seconds(at(1000)), 50);
        assert_eq!(spans[1].machine_id, "mach-b");
        assert_eq!(spans[1].billed_seconds(at(1000)), 120); // 80 → 200
        assert_eq!(spans[2].machine_id, "mach-c");
    }

    #[test]
    fn finalize_keeps_a_mismatched_seeded_span_instead_of_hijacking_it() {
        // Seeded open span from mach-a; the envelope arrives from mach-b before this
        // process ever observed anything. The old bug rewrote mach-a's span with
        // mach-b's window (losing mach-a's time AND misattributing the final node).
        let base = Utc::now();
        let at = |s: i64| base + chrono::Duration::seconds(s);
        let mut tracker = SpanTracker::default();
        tracker.seed(vec![RunningSpan {
            machine_id: "mach-a".into(),
            start: at(0),
            end: None,
        }]);
        let env = envelope_for(
            "mach-b",
            Timings {
                agent_start: at(300),
                gate_done: None,
                inputs_done: None,
                exec_start: Some(at(302)),
                exec_end: Some(at(390)),
                outputs_done: Some(at(400)),
            },
        );
        tracker.finalize_with_envelope(&env);

        let spans = tracker.into_spans();
        assert_eq!(spans.len(), 2, "seeded span kept, envelope span appended");
        assert_eq!(spans[0].machine_id, "mach-a");
        assert!(spans[0].end.is_some(), "stale open span must be closed");
        assert_eq!(spans[1].machine_id, "mach-b");
        assert_eq!(spans[1].billed_seconds(at(1000)), 100); // 300 → 400
    }

    /// A TCP port nothing is listening on: bind an ephemeral port, note it, drop the
    /// listener. Connecting there fails immediately and deterministically — no live
    /// server, no timeout, no network.
    fn closed_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
        // listener dropped here
    }

    #[tokio::test]
    async fn presigned_url_never_reaches_error_text() {
        // reqwest errors embed the request URL. For a presigned URL that URL *is* the
        // capability (the `X-Amz-Signature` in the query string), so it must not survive
        // into anything printable. `main` returns `anyhow::Result`, so `Termination`
        // renders a propagated error with `Debug` — anyhow's full source chain — straight
        // into stderr and CI logs. Both renderings are asserted below.
        const SENTINEL: &str = "SENTINELSIG123";
        let url = format!(
            "http://127.0.0.1:{}/runs/sf-x/job.json?X-Amz-Signature={SENTINEL}",
            closed_port()
        );
        let http = reqwest::Client::new();

        let err = put_object(&http, &url, b"{}".to_vec())
            .await
            .expect_err("a PUT to a closed port must fail");
        let alternate = format!("{err:#}");
        let debug = format!("{err:?}");
        assert!(
            !alternate.contains(SENTINEL),
            "leaked via {{:#}}: {alternate}"
        );
        assert!(!debug.contains(SENTINEL), "leaked via Debug: {debug}");
        assert!(
            !alternate.contains("X-Amz-Signature") && !debug.contains("X-Amz-Signature"),
            "the whole presigned URL must be stripped, not just its signature value"
        );

        // Same for the presigned GET the poll loop uses.
        let err = fetch_envelope(&http, &url)
            .await
            .expect_err("a GET to a closed port must fail");
        let alternate = format!("{err:#}");
        let debug = format!("{err:?}");
        assert!(
            !alternate.contains(SENTINEL),
            "leaked via {{:#}}: {alternate}"
        );
        assert!(!debug.contains(SENTINEL), "leaked via Debug: {debug}");
    }

    #[test]
    fn job_env_pins_the_digest_of_the_uploaded_spec() {
        // Stand in for the buffer `run` uploads; the digest must be taken over exactly
        // these bytes, with the same function the agent uses to verify them.
        let job_body = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "command": ["python", "train.py"],
        }))
        .unwrap();

        let env = job_env(
            "https://s3.example/runs/sf-x/job.json?X-Amz-Signature=sig",
            "sf-x",
            2,
            4,
            transfer::sha256_hex(&job_body),
        )
        .unwrap();

        // Armed: without this the agent's `SF_JOB_SHA256` check is dead code and it will
        // exec whatever command the spec URL happens to return.
        assert_eq!(
            env.get("SF_JOB_SHA256").map(String::as_str),
            Some(transfer::sha256_hex(&job_body).as_str()),
        );
        // …and it is genuinely a digest OF THOSE BYTES, not a constant: one flipped byte
        // in the spec produces a different value, which is what makes substitution fail.
        let mut tampered = job_body.clone();
        tampered.extend_from_slice(b" ");
        assert_ne!(env["SF_JOB_SHA256"], transfer::sha256_hex(&tampered));
        assert_eq!(env["SF_JOB_SHA256"].len(), 64, "lowercase-hex sha256");

        // The four pre-existing variables are unchanged.
        assert_eq!(
            env["SF_JOB_URL"],
            "https://s3.example/runs/sf-x/job.json?X-Amz-Signature=sig"
        );
        assert_eq!(env["SF_RUN_ID"], "sf-x");
        assert_eq!(env["SF_SHARD_INDEX"], "2");
        assert_eq!(env["SF_SHARD_COUNT"], "4");
        assert_eq!(env.len(), 5, "exactly the five SF_* variables");
    }

    #[test]
    fn job_env_splits_a_long_url_across_the_env_value_cap() {
        // SaladCloud caps env values at 1000 chars. A presigned URL past the cap (e.g.
        // with an X-Amz-Security-Token) must be split into SF_JOB_URL + SF_JOB_URL_1..,
        // which the agent's `job_url()` concatenates back in order.
        let long: String = format!(
            "https://s3.example/runs/sf-x/job.json?X-Amz-Security-Token={}&X-Amz-Signature=sig",
            "t".repeat(2000)
        );
        let env = job_env(&long, "sf-x", 0, 1, "0".repeat(64)).unwrap();
        // Every value respects the cap...
        for (k, v) in &env {
            assert!(v.len() <= ENV_VALUE_MAX, "{k} is {} chars", v.len());
        }
        // ...and the segments reassemble to the original URL, exactly as the agent does.
        let mut reassembled = env["SF_JOB_URL"].clone();
        for i in 1.. {
            match env.get(&format!("SF_JOB_URL_{i}")) {
                Some(part) => reassembled.push_str(part),
                None => break,
            }
        }
        assert_eq!(reassembled, long);

        // A short URL stays a single variable.
        let env = job_env("https://s3.example/j?sig", "sf-x", 0, 1, "0".repeat(64)).unwrap();
        assert!(!env.contains_key("SF_JOB_URL_1"));

        // Beyond what the agent can reassemble (10 segments) is a loud error, not a
        // silently truncated URL.
        let absurd = "x".repeat(ENV_VALUE_MAX * JOB_URL_MAX_PARTS + 1);
        assert!(job_env(&absurd, "sf-x", 0, 1, String::new()).is_err());
    }

    #[test]
    fn expiry_has_floor_and_ceiling() {
        let h = |n: u64| Duration::from_secs(n * 3600);
        assert_eq!(presign_expiry(None), h(72));
        assert_eq!(presign_expiry(Some(h(1))), h(72)); // 2h < 72h floor
        assert_eq!(presign_expiry(Some(h(48))), h(96)); // 2 × 48h
        assert_eq!(presign_expiry(Some(h(200))), h(7 * 24)); // capped at 7 days
    }

    #[test]
    fn parse_outputs_derives_names() {
        let out = parse_outputs(&["ckpts/latest:model".into(), "logs".into()]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "model");
        assert_eq!(out[0].src_glob, "ckpts/latest");
        assert_eq!(out[1].name, "output1");
    }

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_duration_secs("45m").unwrap(), 2700);
        assert_eq!(parse_duration_secs("2h").unwrap(), 7200);
        assert!(parse_duration_secs("nonsense").is_err());
    }

    #[test]
    fn parse_inputs_defaults_dest_and_detects_dir() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("data.bin");
        std::fs::write(&file, b"x").unwrap();

        let inputs = parse_inputs(&[file.to_str().unwrap().to_string()]).unwrap();
        assert_eq!(inputs[0].0, "/work/data.bin");
        assert!(!inputs[0].2, "a file is not archived as a directory");

        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let inputs = parse_inputs(&[format!("{}:/work/d", sub.display())]).unwrap();
        assert_eq!(inputs[0].0, "/work/d");
        assert!(inputs[0].2, "a directory is archived");

        assert!(parse_inputs(&["/no/such/path".into()]).is_err());
    }

    #[test]
    fn is_safe_relative_rejects_traversal_and_absolute_names() {
        // Ordinary artifact names (single- or multi-component relative paths) are accepted.
        assert!(is_safe_relative("model"));
        assert!(is_safe_relative("model.safetensors"));
        assert!(is_safe_relative("ckpts/latest/model.pt"));

        // A malicious envelope's `name` must never escape the output tree. These are the
        // shapes that would steer `out_dir.join(name)` outside `sf-out/<run>/<shard>`.
        assert!(!is_safe_relative(""));
        assert!(!is_safe_relative("/etc/cron.d/evil"));
        assert!(!is_safe_relative("../../../../../../etc/evil"));
        assert!(!is_safe_relative("ckpts/../../../evil"));
        assert!(!is_safe_relative(".."));
        assert!(!is_safe_relative("."));
        assert!(!is_safe_relative("./sneaky"));

        // Why the fix validates the name up front rather than checking the joined path:
        // `Path::starts_with` is purely lexical and does NOT resolve `..`, so a post-join
        // containment check would be fooled. `out_dir.join("../../evil")` still
        // "starts_with" out_dir yet injects parent components that escape it once resolved.
        let out_dir = PathBuf::from("sf-out").join("sf-run").join("0");
        let escaping = out_dir.join("../../evil");
        assert!(
            escaping.starts_with(&out_dir),
            "lexically looks contained..."
        );
        assert!(
            escaping
                .components()
                .any(|c| matches!(c, Component::ParentDir)),
            "...yet carries `..` that escapes once resolved",
        );
        // Accepted names carry no `..`/root, so they stay lexically within `out_dir`.
        for good in ["model", "ckpts/latest/model.pt"] {
            assert!(is_safe_relative(good));
            assert!(out_dir.join(good).starts_with(&out_dir));
        }
    }

    #[test]
    fn admit_output_enforces_shape_parts_then_allow_list() {
        let declared = ["model".to_string(), "ckpts/latest".to_string()];
        let cap = spec::DEFAULT_MAX_PARTS;

        // Unknown declared set (e.g. an older state file): the path-shape and part-count
        // floors are the only gates — any well-formed, sanely-sized relative name is admitted.
        assert!(admit_output("model", 1, cap, None).is_ok());
        assert!(admit_output("anything/goes.bin", cap, cap, None).is_ok());
        assert!(admit_output("../escape", 1, cap, None).is_err());
        assert!(admit_output("/abs", 1, cap, None).is_err());

        // Part count is bounded by the run's ceiling (the number of PUT URLs it issued), so a
        // hostile envelope cannot drive unbounded presigned-URL generation — even for a
        // declared name. The bound is the passed `max_parts`, whatever the run configured.
        let err = admit_output("model", cap + 1, cap, Some(&declared)).unwrap_err();
        assert!(err.contains("claims") && err.contains("parts"), "{err}");
        assert!(admit_output("model", u32::MAX, cap, None).is_err());
        // A run configured with a higher ceiling admits proportionally more parts.
        assert!(admit_output("model", 300, 512, Some(&declared)).is_ok());
        assert!(admit_output("model", 513, 512, Some(&declared)).is_err());

        // Known declared set: only the exact declared names pass.
        assert!(admit_output("model", 1, cap, Some(&declared)).is_ok());
        assert!(admit_output("ckpts/latest", 8, cap, Some(&declared)).is_ok());
        // An undeclared but well-formed name is refused as unexpected...
        let err = admit_output("secret", 1, cap, Some(&declared)).unwrap_err();
        assert!(err.contains("not a declared output"), "{err}");
        // ...and a traversal name is refused by the shape gate first (before the allow-list).
        let err = admit_output("../../evil", 1, cap, Some(&declared)).unwrap_err();
        assert!(err.contains("must be a plain relative path"), "{err}");

        // A run that declared nothing accepts nothing — an empty allow-list is "expect zero
        // artifacts", distinct from `None` ("declared set unknown").
        let none_declared: &[String] = &[];
        assert!(admit_output("model", 1, cap, Some(none_declared)).is_err());
    }
}
