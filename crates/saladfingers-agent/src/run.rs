// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `sf-agent run` — one-shot batch job supervisor.
//!
//! Boot → fetch [`JobSpec`] → record attempt → idempotent-resume short-circuit →
//! probe checkpoint metadata → bandwidth gate → download inputs → restore checkpoint →
//! exec (with the checkpoint watcher alongside) → upload outputs → write the
//! [`ResultEnvelope`] commit record. The checkpoint is *probed* before the gate and the
//! inputs but *extracted* after them: an unusable checkpoint must fail before the run
//! spends a full input download on a boot that cannot proceed, while the extraction has
//! to come after the inputs so a restored checkpoint wins any overlapping paths (an
//! input may carry initial weights; the checkpoint carries later ones).
//!
//! Agent-assigned exit codes, distinct from the job's own: 3 = no usable job spec (no
//! envelope — there is nowhere to write one); 4 = input download, or exec could not be
//! spawned/waited on; 5 = output upload failed, or the envelope PUT itself failed (the
//! one 5 that leaves nothing in storage); 6 = checkpoint probe/restore; 7 = the
//! `--max-duration` timeout; 143 = interrupted by a platform stop. Every path except 3
//! and the failed-envelope 5 writes an envelope first, so the CLI reports the reason
//! rather than an unexplained restart.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::Args;
use reqwest::header::CONTENT_TYPE;
use saladfingers_protocol::{
    AttemptRecord, Attempts, GpuVendor, JobSpec, JobStatus, NodeInfo, PROTOCOL_VERSION,
    ResultEnvelope, Timings, UploadReport, transfer,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;

use crate::imds::ImdsClient;
use crate::probe;

/// Default attempt cap when the spec does not set one: how many times the work may run
/// in full before an existing `Failed`/`AgentError` envelope short-circuits the boot.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Args)]
pub struct RunArgs {}

/// Run one batch job to completion, then exit with the mapped code.
pub async fn run(_args: RunArgs) -> Result<()> {
    let agent_start = Utc::now();
    let http = transfer::transfer_client().context("building transfer HTTP client")?;

    let spec = match load_spec(&http).await {
        Ok(spec) => spec,
        Err(e) => {
            tracing::error!("spec fetch failed: {e:#}");
            std::process::exit(3);
        }
    };
    tracing::info!(run_id = %spec.run_id, shard = spec.shard_index, "sf-agent run booted");

    // Attempts ledger: increment on every boot; carries the gate-reallocation count.
    let mut attempts = load_attempts(&http, &spec).await;
    attempts.attempts.push(AttemptRecord {
        machine_id: env_opt("SALAD_MACHINE_ID").unwrap_or_else(|| "unknown".into()),
        boot_at: agent_start,
    });
    put_attempts(&http, &spec.urls.attempts_put, &attempts).await;

    // Idempotent resume: if a terminal envelope already exists, we are done. The envelope
    // is the commit record, so transient transport errors here are retried rather than
    // silently treated as "no envelope" (which would re-execute completed work).
    if let Some(prev) = fetch_json_retry::<ResultEnvelope>(&http, &spec.urls.result_get, 3).await {
        if prev.status.is_terminal_for_resume() {
            tracing::info!(status = ?prev.status, "run already terminal; exiting 0");
            std::process::exit(0);
        }
        // The platform relaunches the container on every exit (empirical E1/E2), so an
        // uncapped deterministic failure would re-download inputs and re-execute in full
        // on every cycle until something deletes the group. Once the ledger shows the cap
        // is spent and the last outcome was a completed failure, stop doing work and exit
        // cheaply. Interrupted envelopes are exempt: retrying them IS the reallocation /
        // checkpoint-resume story.
        let cap = spec.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS) as usize;
        if matches!(prev.status, JobStatus::Failed | JobStatus::AgentError)
            && attempts.attempts.len() > cap
        {
            tracing::error!(
                attempts = attempts.attempts.len(),
                cap,
                status = ?prev.status,
                "attempt cap spent with a failed envelope; refusing to re-execute"
            );
            std::process::exit(0);
        }
    }

    let mut timings = Timings {
        agent_start,
        gate_done: None,
        inputs_done: None,
        exec_start: None,
        exec_end: None,
        outputs_done: None,
    };
    let mut node = collect_node_info();

    // Probe the checkpoint metadata BEFORE the gate and the inputs. A failure here is
    // fatal, not a fresh start: without the metadata the ring does not know which slot
    // is live, so the first upload can land on the committed one — and even when it
    // rotates correctly, the commit that follows *reclaims* the other slot. Either path
    // destroys the last good checkpoint, the exact loss the ring exists to prevent.
    // Failing costs one relaunch cycle, bounded by the attempt cap above; each doomed
    // relaunch stops HERE, at one small GET, rather than after re-downloading the full
    // input set — the boot is in the billed `running` state the whole time, so what a
    // permanently unreadable checkpoint burns per relaunch should be seconds, not an
    // input transfer.
    let probed = match crate::checkpoint::probe(&http, &spec).await {
        Ok(probed) => probed,
        Err(e) => {
            tracing::error!("checkpoint probe failed: {e:#}");
            let env = agent_error_envelope(
                &spec,
                &node,
                &timings,
                attempts.attempts.len(),
                0,
                format!("checkpoint restore: {e:#}"),
            );
            let _ = put_envelope(&http, &spec.urls.result_put, &env).await;
            std::process::exit(6);
        }
    };

    // Bandwidth gate (may reallocate this instance and never return).
    let gate = run_gate(&http, &spec, &mut attempts).await;
    node.measured_down_mbps = gate.down_mbps;
    node.measured_up_mbps = gate.up_mbps;
    timings.gate_done = Some(Utc::now());

    // Inputs.
    if let Err(e) = download_inputs(&http, &spec).await {
        tracing::error!("input download failed: {e:#}");
        let env = agent_error_envelope(
            &spec,
            &node,
            &timings,
            attempts.attempts.len(),
            gate.gate_reallocs,
            format!("input download: {e:#}"),
        );
        let _ = put_envelope(&http, &spec.urls.result_put, &env).await;
        std::process::exit(4);
    }
    timings.inputs_done = Some(Utc::now());

    // Extract the probed checkpoint (resume path), then run the watcher alongside exec.
    // After the inputs on purpose: a restored checkpoint must win overlapping paths.
    let restored = match crate::checkpoint::restore(&http, &spec, probed).await {
        Ok(state) => state,
        Err(e) => {
            tracing::error!("checkpoint restore failed: {e:#}");
            let env = agent_error_envelope(
                &spec,
                &node,
                &timings,
                attempts.attempts.len(),
                gate.gate_reallocs,
                format!("checkpoint restore: {e:#}"),
            );
            let _ = put_envelope(&http, &spec.urls.result_put, &env).await;
            std::process::exit(6);
        }
    };
    let ckpt_stop = std::sync::Arc::new(tokio::sync::Notify::new());
    // Set when the child had to be SIGKILLed mid-stop: its final checkpoint writes are
    // then suspect, and the watcher's final upload must not commit a torn directory.
    let ckpt_dirty = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ckpt_watcher = spec.checkpoint.is_some().then(|| {
        crate::checkpoint::spawn_watcher(
            http.clone(),
            spec.clone(),
            restored,
            ckpt_stop.clone(),
            ckpt_dirty.clone(),
        )
    });

    // Exec.
    let outcome = exec(&spec, &ckpt_dirty).await;
    timings.exec_start = Some(outcome.exec_start);
    timings.exec_end = Some(outcome.exec_end);

    // Stop the watcher; it does one final upload of the now-settled checkpoint.
    if let Some(handle) = ckpt_watcher {
        ckpt_stop.notify_one();
        let _ = handle.await;
    }

    // Outputs (only on success).
    let mut uploads = Vec::new();
    let mut agent_exit = outcome.agent_exit_code;
    let mut status = outcome.status;
    let mut error = outcome.error.clone();
    if matches!(status, JobStatus::Succeeded) {
        match upload_outputs(&http, &spec).await {
            Ok(reports) => {
                uploads = reports;
                timings.outputs_done = Some(Utc::now());
            }
            Err(e) => {
                tracing::error!("output upload failed: {e:#}");
                status = JobStatus::AgentError;
                error = Some(format!("output upload: {e:#}"));
                agent_exit = 5;
            }
        }
    }

    // Ship the captured output before the envelope, and whatever the status: a failed run's
    // output is the most valuable thing it produced, and the envelope's arrival is what ends
    // the run as far as the CLI is concerned. A failure here is never fatal — the run's real
    // result is already computed, and the output also went to container stdout on the way.
    if let Some(path) = &outcome.log_path {
        match upload_log(&http, &spec.urls.log_put, path).await {
            Ok(()) => tracing::info!("uploaded captured run output"),
            Err(e) => tracing::warn!("run log upload failed: {e:#}"),
        }
    }

    let envelope = ResultEnvelope {
        v: PROTOCOL_VERSION,
        run_id: spec.run_id.clone(),
        shard_index: spec.shard_index,
        status,
        exit_code: outcome.exit_code,
        error,
        timings,
        node,
        uploads,
        attempts: u32::try_from(attempts.attempts.len()).unwrap_or(u32::MAX),
        gate_reallocations: gate.gate_reallocs,
    };
    if let Err(e) = put_envelope(&http, &spec.urls.result_put, &envelope).await {
        tracing::error!("envelope upload failed: {e:#}");
        std::process::exit(5);
    }
    tracing::info!(status = ?status, code = agent_exit, "run complete");
    std::process::exit(agent_exit);
}

// ---- exec (M3 core) -------------------------------------------------------

struct Outcome {
    status: JobStatus,
    exit_code: Option<i32>,
    error: Option<String>,
    exec_start: DateTime<Utc>,
    exec_end: DateTime<Utc>,
    agent_exit_code: i32,
    /// The captured child output, ready to upload (see [`Capture`]).
    log_path: Option<PathBuf>,
}

enum ExecEnd {
    Exited(std::io::Result<std::process::ExitStatus>),
    Interrupted,
    TimedOut,
}

/// How long to keep draining the child's pipes after it exits. Normally both hit EOF the
/// instant the child dies, but a grandchild that inherited the pipe keeps the write end open
/// for as long as it lives — without a bound, a job that leaves a daemon behind would hang
/// the agent here forever instead of committing its envelope.
const DRAIN_GRACE: Duration = Duration::from_secs(10);

async fn exec(spec: &JobSpec, ckpt_dirty: &std::sync::atomic::AtomicBool) -> Outcome {
    let exec_start = Utc::now();
    let Some((program, args)) = spec.command.split_first() else {
        return agent_outcome("empty command", exec_start);
    };

    let mut cmd = Command::new(program);
    cmd.args(args)
        .current_dir(workdir(spec))
        .stdin(Stdio::null())
        // Piped rather than inherited so the agent can keep a complete copy of the run's
        // output and upload it alongside the envelope. Both streams are teed straight back
        // to the agent's own fds, so container stdout still carries everything live and
        // `saladfingers logs --follow` is unaffected.
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in &spec.env {
        cmd.env(key, value);
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => return agent_outcome(&format!("spawn {program}: {e}"), exec_start),
    };
    let pid = child.id();
    let stop_signal = parse_signal(spec.stop_signal.as_deref());
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let max_duration = spec.max_duration_secs.map(Duration::from_secs);

    let capture = Capture::create().await.map(|c| Arc::new(Mutex::new(c)));
    // The pumps run whether or not the capture file opened. Now that the streams are piped,
    // *something* must read them: a child that outruns the pipe buffer (64 KiB by default)
    // blocks in write(2) until someone drains it, so an unread pipe would hang the job.
    let pumps = (
        tokio::spawn(tee(
            child.stdout.take(),
            tokio::io::stdout(),
            capture.clone(),
        )),
        tokio::spawn(tee(
            child.stderr.take(),
            tokio::io::stderr(),
            capture.clone(),
        )),
    );

    let end = tokio::select! {
        result = child.wait() => ExecEnd::Exited(result),
        _ = sigterm.recv() => {
            tracing::warn!("SIGTERM received; forwarding to child");
            if forward_and_reap(&mut child, pid, stop_signal).await {
                ckpt_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            ExecEnd::Interrupted
        }
        () = sleep_opt(max_duration) => {
            tracing::warn!("max duration exceeded; stopping child");
            if forward_and_reap(&mut child, pid, stop_signal).await {
                ckpt_dirty.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            ExecEnd::TimedOut
        }
    };
    let exec_end = Utc::now();

    // Drain to EOF before finalizing: the child's last writes are still in flight in the
    // pipe when `wait()` returns, and they are exactly the lines a reader wants most.
    let log_path = finish_capture(capture, pumps).await;

    let (status, exit_code, error, agent_exit_code) = match end {
        ExecEnd::Exited(Ok(s)) if s.success() => (JobStatus::Succeeded, Some(0), None, 0),
        ExecEnd::Exited(Ok(s)) => {
            let code = s.code().unwrap_or(1);
            (JobStatus::Failed, Some(code), None, code.clamp(1, 255))
        }
        ExecEnd::Exited(Err(e)) => (
            JobStatus::AgentError,
            None,
            Some(format!("wait failed: {e}")),
            4,
        ),
        ExecEnd::Interrupted => (JobStatus::Interrupted, None, None, 143),
        ExecEnd::TimedOut => (JobStatus::TimedOut, None, None, 7),
    };
    Outcome {
        status,
        exit_code,
        error,
        exec_start,
        exec_end,
        agent_exit_code,
        // The same capture whatever the status: a failed or timed-out run's output is
        // the most valuable thing it produced.
        log_path,
    }
}

fn agent_outcome(error: &str, exec_start: DateTime<Utc>) -> Outcome {
    Outcome {
        status: JobStatus::AgentError,
        exit_code: None,
        error: Some(error.to_string()),
        exec_start,
        exec_end: Utc::now(),
        agent_exit_code: 4,
        log_path: None,
    }
}

// ---- child output capture -------------------------------------------------

/// Bytes of the child's output kept from the start of the run.
const CAPTURE_HEAD_BYTES: u64 = 8 * 1024 * 1024;

/// Bytes kept from the end of the run once the head budget is spent. The *tail* is the half
/// worth protecting: a run's results and its dying error are both at the end, and the tail
/// is precisely what the org log query was losing.
const CAPTURE_TAIL_BYTES: usize = 8 * 1024 * 1024;

/// The child's merged stdout/stderr, captured to a file for upload.
///
/// Container stdout is only best-effort: it is queryable through SaladCloud's org log API
/// for ~90 days, but that endpoint answers a bounded page at a time — the CLI has to
/// bisect the time window to read a long run back — and stamps entries with the *node's*
/// clock, so ordering across a skewed node is approximate. The run's inputs, outputs, and
/// result envelope all travel through object storage, where nothing is capped or
/// reordered; its output is the one artifact that did not. This makes it one too.
///
/// Bounded head + tail so a chatty job cannot fill the node's disk or push a multi-gigabyte
/// upload: the first [`CAPTURE_HEAD_BYTES`] go straight to the file, and once that budget is
/// spent the most recent [`CAPTURE_TAIL_BYTES`] are held in a ring and appended at the end,
/// with a marker naming how much was dropped between the two.
struct Capture {
    path: PathBuf,
    file: tokio::fs::File,
    head_left: u64,
    tail: std::collections::VecDeque<u8>,
    tail_cap: usize,
    dropped: u64,
    /// Why capturing stopped, once a write to the file has failed.
    ///
    /// Load-bearing: a failed head write leaves `head_left` untouched, so without this
    /// every later push re-enters the same failing branch, the tail never fills, and
    /// [`Self::finish`] reports success on a log that silently ends mid-run.
    failed: Option<String>,
}

impl Capture {
    async fn create() -> Option<Self> {
        // Deliberately not under the workdir: an output glob as ordinary as `*` would
        // otherwise sweep the agent's own capture into the user's uploaded outputs.
        let path = std::env::temp_dir().join(format!("sf-run-{}.log", std::process::id()));
        match Self::open(path.clone(), CAPTURE_HEAD_BYTES, CAPTURE_TAIL_BYTES).await {
            Ok(capture) => Some(capture),
            Err(e) => {
                // Not fatal: the run's output still reaches container stdout as before.
                tracing::warn!("cannot capture run output to {}: {e}", path.display());
                None
            }
        }
    }

    async fn open(path: PathBuf, head: u64, tail_cap: usize) -> std::io::Result<Self> {
        let file = tokio::fs::File::create(&path).await?;
        Ok(Self {
            path,
            file,
            head_left: head,
            tail: std::collections::VecDeque::new(),
            tail_cap,
            dropped: 0,
            failed: None,
        })
    }

    async fn push(&mut self, mut bytes: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        if self.head_left > 0 {
            let n = bytes
                .len()
                .min(usize::try_from(self.head_left).unwrap_or(usize::MAX));
            if let Err(e) = self.file.write_all(&bytes[..n]).await {
                // A full disk is the likely cause and it will not clear itself, so stop
                // rather than retry every 8 KiB — but say so, here and in the file.
                tracing::warn!(
                    "capturing run output to {} failed: {e}",
                    self.path.display()
                );
                self.failed = Some(e.to_string());
                return;
            }
            self.head_left -= n as u64;
            bytes = &bytes[n..];
        }
        self.tail.extend(bytes);
        while self.tail.len() > self.tail_cap {
            self.tail.pop_front();
            self.dropped += 1;
        }
    }

    /// Append the retained tail and get the bytes onto disk.
    async fn finish(&mut self) -> std::io::Result<()> {
        if let Some(reason) = self.failed.clone() {
            // Upload what there is, but never let it read as a run that simply went quiet.
            // Best-effort: the write that reports the failure can fail the same way.
            let marker = format!("\n[sf-agent] output capture stopped early: {reason}\n");
            let _ = self.file.write_all(marker.as_bytes()).await;
            return self.file.flush().await;
        }
        if !self.tail.is_empty() {
            // Only when something was actually lost: under the cap the tail continues the
            // head byte-for-byte, and a marker there would invent a gap that is not present.
            if self.dropped > 0 {
                let marker = format!(
                    "\n[sf-agent] {} bytes of output dropped from the middle of this log\n",
                    self.dropped
                );
                self.file.write_all(marker.as_bytes()).await?;
            }
            let (front, back) = self.tail.as_slices();
            self.file.write_all(front).await?;
            self.file.write_all(back).await?;
        }
        // `flush` is the load-bearing one — tokio buffers writes and the upload reads this
        // file back by path. `sync_all` is cheap insurance for the same reason the envelope
        // is retried: a truncated log is worth less than no log at all.
        self.file.flush().await?;
        self.file.sync_all().await
    }
}

/// Tee one of the child's streams: mirror it to the agent's matching fd (so it still reaches
/// container stdout, which is what `saladfingers logs` reads) and copy it into the capture.
async fn tee<R, W>(reader: Option<R>, mut sink: W, capture: Option<Arc<Mutex<Capture>>>)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let Some(mut reader) = reader else { return };
    let mut buf = vec![0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                // Mirrored unbuffered: the platform's log shipper should see a line at the
                // same moment it would have with an inherited fd.
                let _ = sink.write_all(&buf[..n]).await;
                let _ = sink.flush().await;
                if let Some(capture) = &capture {
                    capture.lock().await.push(&buf[..n]).await;
                }
            }
        }
    }
}

type Pumps = (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>);

/// Drain both pipes to EOF (bounded by [`DRAIN_GRACE`]), then finalize the capture file.
///
/// The drain happens even with no capture file, because the pumps are also what mirror the
/// child's output to container stdout: returning before they finish would drop whatever the
/// child wrote last — the exact loss this whole path exists to prevent.
async fn finish_capture(capture: Option<Arc<Mutex<Capture>>>, pumps: Pumps) -> Option<PathBuf> {
    let (out, err) = pumps;
    // Kept because dropping a `JoinHandle` only detaches the task; on timeout the pumps have
    // to be stopped explicitly or they would keep appending past the tail marker below.
    let (out_abort, err_abort) = (out.abort_handle(), err.abort_handle());
    let drained = tokio::time::timeout(DRAIN_GRACE, async {
        let _ = out.await;
        let _ = err.await;
    })
    .await;
    if drained.is_err() {
        // Something still holds the write end open — typically a grandchild that inherited
        // the pipe. Take what arrived rather than hanging the run.
        out_abort.abort();
        err_abort.abort();
        tracing::warn!(
            "child output still open {DRAIN_GRACE:?} after exit; capturing what arrived"
        );
    }

    let capture = capture?;
    let mut guard = capture.lock().await;
    if let Err(e) = guard.finish().await {
        tracing::warn!("finalizing captured run output failed: {e}");
        return None;
    }
    Some(guard.path.clone())
}

/// Upload the captured output to the run's log slot.
///
/// `without_url`: like every other transfer-path request, the presigned signature must never
/// reach error text — this one's failure is logged to container stdout, which is retained and
/// queryable for ~90 days.
///
/// Rides `transfer_client` with **no** per-request deadline, deliberately. The rule this
/// follows is the one `CONTROL_TIMEOUT` states: a fixed-size control document gets a
/// deadline, a payload whose size is not known in advance does not, because any cap on how
/// long a transfer may take is a cap on how large one may be. A capture is up to
/// [`CAPTURE_HEAD_BYTES`] + [`CAPTURE_TAIL_BYTES`] of a chatty job's output, which is a
/// payload, not a document. A storage endpoint that accepts the connection and never
/// answers is caught instead by the connect timeout and TCP keepalive.
///
/// One attempt, unlike `put_envelope`'s three. The envelope is the run's contract and a
/// missing one loses the work; the log is the best copy of something container stdout
/// already has, and a retry loop over a 16 MiB body bills the node for every second of it.
/// Failure here is warned about and ignored.
async fn upload_log(http: &reqwest::Client, url: &str, path: &Path) -> Result<()> {
    let body = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading captured output {}", path.display()))?;
    http.put(url)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    Ok(())
}

/// Forward the stop signal and wait out the grace period; SIGKILL if it expires.
/// Returns whether the child had to be force-killed (its final writes are then suspect).
async fn forward_and_reap(child: &mut Child, pid: Option<u32>, sig: i32) -> bool {
    if let Some(pid) = pid {
        // SAFETY: kill(2) with a valid pid and signal number; failure is ignored.
        unsafe {
            libc::kill(pid as libc::pid_t, sig);
        }
    }
    if tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        return true;
    }
    false
}

async fn sleep_opt(d: Option<Duration>) {
    match d {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

fn parse_signal(name: Option<&str>) -> i32 {
    match name.map(str::to_ascii_uppercase).as_deref() {
        Some("INT") => libc::SIGINT,
        Some("KILL") => libc::SIGKILL,
        _ => libc::SIGTERM,
    }
}

// ---- data plane (M4) ------------------------------------------------------

struct GateResult {
    gate_reallocs: u32,
    down_mbps: Option<f64>,
    up_mbps: Option<f64>,
}

async fn run_gate(http: &reqwest::Client, spec: &JobSpec, attempts: &mut Attempts) -> GateResult {
    let Some(gate) = &spec.bandwidth_gate else {
        return GateResult {
            gate_reallocs: attempts.gate_reallocs,
            down_mbps: None,
            up_mbps: None,
        };
    };

    // Upload probe first: it PUTs a known `sample_bytes`-sized object, which then serves
    // as the download probe's target (`gate_get_url`). Probing the first input instead is
    // only a fallback — a first input smaller than the sample yields a latency-dominated
    // reading (a few KB reads as ~1 Mbps of pure RTT) that would reallocate every node.
    let up = measure_upload(http, &gate.gate_put_url, gate.sample_bytes)
        .await
        .ok();
    let mut down = match (&gate.gate_get_url, up.is_some()) {
        (Some(url), true) => measure_download(http, url, gate.sample_bytes).await.ok(),
        _ => None,
    };
    if down.is_none() {
        down = match spec.inputs.first().and_then(|i| i.urls.first()) {
            Some(url) => measure_download(http, url, gate.sample_bytes).await.ok(),
            None => None,
        };
    }

    let below_down = below_threshold(gate.min_download_mbps, down);
    let below_up = below_threshold(gate.min_upload_mbps, up);

    if (below_down || below_up) && attempts.gate_reallocs < gate.max_reallocations {
        attempts.gate_reallocs += 1;
        put_attempts(http, &spec.urls.attempts_put, attempts).await;
        let reason = format!(
            "bandwidth gate: down={down:?} up={up:?} (thresholds down={:?} up={:?})",
            gate.min_download_mbps, gate.min_upload_mbps
        );
        tracing::warn!("{reason}; reallocating");
        if let Ok(imds) = ImdsClient::new() {
            let _ = imds.reallocate(&reason).await;
        }
        // Await the platform killing this instance; if it doesn't, proceed.
        tokio::time::sleep(Duration::from_secs(180)).await;
    }
    GateResult {
        gate_reallocs: attempts.gate_reallocs,
        down_mbps: down,
        up_mbps: up,
    }
}

/// Whether a measured throughput is genuinely below a configured minimum. An absent
/// measurement (`None`) is NOT below-threshold — we only reallocate on a real reading.
fn below_threshold(min: Option<f64>, measured: Option<f64>) -> bool {
    matches!((min, measured), (Some(m), Some(v)) if v < m)
}

/// Below this many transferred bytes a throughput sample is latency-dominated noise, not
/// a bandwidth measurement. Too-small samples are discarded (→ `None`, which never gates)
/// instead of poisoning the gate and the envelope with an RTT reading.
const MIN_GATE_SAMPLE_BYTES: u64 = 1024 * 1024;

async fn measure_download(http: &reqwest::Client, url: &str, sample_bytes: u64) -> Result<f64> {
    let start = std::time::Instant::now();
    let mut resp = http
        .get(url)
        .header(
            "Range",
            format!("bytes=0-{}", sample_bytes.saturating_sub(1)),
        )
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    let mut total: u64 = 0;
    while let Some(chunk) = resp.chunk().await.map_err(reqwest::Error::without_url)? {
        total += chunk.len() as u64;
        if total >= sample_bytes {
            break;
        }
    }
    if total < MIN_GATE_SAMPLE_BYTES.min(sample_bytes) {
        bail!("sample too small to measure bandwidth ({total} bytes)");
    }
    Ok(mbps(total, start.elapsed()))
}

async fn measure_upload(http: &reqwest::Client, url: &str, sample_bytes: u64) -> Result<f64> {
    let body = vec![0u8; usize::try_from(sample_bytes).unwrap_or(8 * 1024 * 1024)];
    let start = std::time::Instant::now();
    // `without_url`: the gate PUT URL is presigned; like every other transfer-path
    // request, its signature must never survive into error text or logs.
    http.put(url)
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    Ok(mbps(sample_bytes, start.elapsed()))
}

fn mbps(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64().max(0.001);
    (bytes as f64 * 8.0) / secs / 1e6
}

async fn download_inputs(http: &reqwest::Client, spec: &JobSpec) -> Result<()> {
    for input in &spec.inputs {
        transfer::download_artifact(
            http,
            &input.urls,
            Path::new(&input.dest),
            input.archive,
            None,
        )
        .await
        .with_context(|| format!("downloading input to {}", input.dest))?;
    }
    Ok(())
}

async fn upload_outputs(http: &reqwest::Client, spec: &JobSpec) -> Result<Vec<UploadReport>> {
    let workdir = workdir(spec);
    let mut reports = Vec::new();
    for out in &spec.outputs {
        let matches = glob_outputs(&workdir, &out.src_glob)?;
        if matches.is_empty() {
            // A declared output that matches nothing is almost always a typo'd pattern or a
            // job that never produced its result. Fail loudly rather than silently report
            // success with no upload: on success the CLI deletes the group, destroying the
            // only copy of the work.
            bail!(
                "output '{}' pattern '{}' matched no files under {}",
                out.name,
                out.src_glob,
                workdir.display()
            );
        }
        let report = if matches.len() == 1 {
            // Single match: archive the path directly (a dir flattens to the archive root, a
            // file to its basename) — preserves the layout for the common one-path output.
            let src = workdir.join(&matches[0]);
            transfer::upload_artifact(http, &src, out.archive, &out.put_urls, &out.name).await
        } else if out.archive {
            // A fan-out glob: archive every match, preserving paths relative to the workdir.
            let rels: Vec<String> = matches
                .iter()
                .map(|m| m.to_string_lossy().into_owned())
                .collect();
            transfer::upload_archive(http, &workdir, &rels, &out.put_urls, &out.name).await
        } else {
            bail!(
                "output '{}' is not archived but pattern '{}' matched {} files; \
                 archive it or narrow the pattern",
                out.name,
                out.src_glob,
                matches.len()
            );
        };
        reports.push(report.with_context(|| format!("uploading output '{}'", out.name))?);
    }
    Ok(reports)
}

/// Resolve an output's `src_glob` against the job workdir into de-duplicated paths (relative
/// to `workdir`). A trailing `**` resolves to the matching directories themselves, which the
/// tar walk then takes recursively; entries nested under an earlier match are dropped so each
/// path is archived exactly once. An empty result is a real failure the caller must surface,
/// never a silent skip.
fn glob_outputs(workdir: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
    // A TRAILING `**` does not mean what everyone writing it assumes. The `glob`
    // crate resolves `dir/**` to the directories BELOW `dir` — never `dir` itself,
    // and never the files sitting at `dir`'s own root.
    //
    // That cost a real checkpoint. A 10k-step run with `--output "ckpt/**:ckpt"`
    // shipped `step_00008000/` and `step_00010000/` and silently left behind
    // `ckpt/LATEST.json` — the marker tooling reads to find the newest
    // checkpoint. Every layer reported success.
    //
    // So a trailing `**` resolves to the matching DIRECTORIES themselves and the
    // tar walk recurses from each: what was meant, and strictly more complete.
    // The rule is applied by rewriting the pattern to its base and globbing THAT,
    // rather than by testing a base path on the filesystem — which is what makes
    // it hold for a wildcard base as well. `*/**` and `a*/**` have exactly the
    // same defect, and `workdir.join("a*").is_dir()` can never be true.
    //
    // An EMPTY matching directory is taken like any other, because every other way
    // of naming the same directory already does: `ckpt` and `ckpt/` both resolve to
    // it and upload an empty archive, and `*` lists it alongside its siblings. A
    // guard here would make `ckpt/**` the one spelling that calls an empty `ckpt` an
    // error, and it could not even mean "holds data" — a directory whose only
    // content is another empty directory has a dirent and would pass. The caller's
    // "pattern matched no files" bail still covers what it was written for, a
    // pattern naming nothing at all: a missing path, or a typo.
    let trimmed = pattern.trim_end_matches('/');
    // A bare `**` is the same pattern rooted at the workdir. No glob expresses
    // that as a relative match, so it is spelled out: the empty path.
    if trimmed == "**" {
        return Ok(vec![PathBuf::new()]);
    }
    if let Some(base) = trimmed.strip_suffix("/**") {
        // Directories only: a trailing `/**` asks for a tree, so a plain file that
        // happens to match the base is not one, and says so by matching nothing.
        return Ok(glob_paths(workdir, base)?
            .into_iter()
            .filter(|p| workdir.join(p).is_dir())
            .collect());
    }
    glob_paths(workdir, pattern)
}

/// Glob `pattern` under `workdir`, drop entries nested under an earlier match so each
/// path is archived exactly once, and relativize the result against `workdir`.
fn glob_paths(workdir: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
    let joined = workdir.join(pattern);
    let joined = joined
        .to_str()
        .with_context(|| format!("output pattern '{pattern}' is not valid UTF-8"))?;
    let mut matches: Vec<PathBuf> = glob::glob(joined)
        .with_context(|| format!("invalid output pattern '{pattern}'"))?
        .filter_map(Result::ok)
        .collect();
    matches.sort();
    let mut top: Vec<PathBuf> = Vec::new();
    for m in matches {
        if !top.iter().any(|t| m.starts_with(t)) {
            top.push(m);
        }
    }
    Ok(top
        .into_iter()
        .filter_map(|m| m.strip_prefix(workdir).ok().map(Path::to_path_buf))
        .collect())
}

// ---- control-plane IO -----------------------------------------------------

fn job_url() -> Result<String> {
    let mut url = std::env::var("SF_JOB_URL").context("SF_JOB_URL is not set")?;
    for i in 1..=9 {
        match std::env::var(format!("SF_JOB_URL_{i}")) {
            Ok(part) => url.push_str(&part),
            Err(_) => break,
        }
    }
    Ok(url)
}

async fn load_spec(http: &reqwest::Client) -> Result<JobSpec> {
    let url = job_url()?;
    // `without_url`: SF_JOB_URL is presigned, and the spec it fetches carries every OTHER
    // presigned URL for this run — so it is the run's master capability and its signature
    // must never reach error text. `run` logs this failure to container stdout, which is
    // retained ~90 days and stays queryable after the group is deleted. `bytes()` re-attaches
    // the URL on a body-read failure, so it needs stripping too, not just `send()`.
    let bytes = http
        .get(&url)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?
        .bytes()
        .await
        .map_err(reqwest::Error::without_url)?;
    if let Ok(expected) = std::env::var("SF_JOB_SHA256")
        && !expected.trim().is_empty()
    {
        let actual = transfer::sha256_hex(&bytes);
        if actual != expected.trim() {
            bail!("job spec sha256 mismatch (expected {expected}, got {actual})");
        }
    }
    let spec: JobSpec = serde_json::from_slice(&bytes).context("parsing JobSpec")?;
    // The version gate the protocol doc promises. Field-level serde only catches a skew
    // whose shapes differ — a v1 spec with no checkpoint block is byte-identical to a v2
    // one and would run to completion on silently mismatched semantics. Checking `v`
    // makes every skew loud, at boot, with a message that says which side to change.
    anyhow::ensure!(
        spec.v == PROTOCOL_VERSION,
        "job spec is protocol v{} but this agent speaks v{PROTOCOL_VERSION}; \
         rebuild the image (or use the CLI that matches it)",
        spec.v
    );
    Ok(spec)
}

async fn load_attempts(http: &reqwest::Client, spec: &JobSpec) -> Attempts {
    // Retried like the resume check: a transient transport blip here would otherwise
    // come back as a fresh, empty ledger — and the next `put_attempts` overwrites the
    // real one, silently resetting the attempt count the re-execution cap depends on.
    fetch_json_retry::<Attempts>(http, &spec.urls.attempts_get, 3)
        .await
        .unwrap_or(Attempts {
            v: PROTOCOL_VERSION,
            attempts: Vec::new(),
            gate_reallocs: 0,
        })
}

async fn put_attempts(http: &reqwest::Client, url: &str, attempts: &Attempts) {
    if let Ok(body) = serde_json::to_vec(attempts) {
        let _ = http
            .put(url)
            .timeout(transfer::CONTROL_TIMEOUT)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;
    }
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
) -> Result<Option<T>> {
    // `without_url`: the URL is presigned; its signature must not reach logs.
    let resp = http
        .get(url)
        .timeout(transfer::CONTROL_TIMEOUT)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    match resp.json::<T>().await {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            tracing::debug!("body was not the expected JSON: {e}");
            Ok(None)
        }
    }
}

/// [`fetch_json`] with bounded retries on transport errors. An error HTTP status is an
/// authoritative "absent" (`None`, not retried); a transport failure is retried, and if
/// it persists the caller proceeds as if absent — the availability-preserving choice.
async fn fetch_json_retry<T: serde::de::DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
    max_attempts: u32,
) -> Option<T> {
    for attempt in 1..=max_attempts {
        match fetch_json::<T>(http, url).await {
            Ok(v) => return v,
            Err(e) if attempt < max_attempts => {
                tracing::warn!("fetch attempt {attempt} failed: {e:#}; retrying");
                tokio::time::sleep(Duration::from_millis(500 << attempt.min(4))).await;
            }
            Err(e) => tracing::warn!("fetch failed after {max_attempts} attempts: {e:#}"),
        }
    }
    None
}

/// PUT the result envelope — the run's commit record — retrying transient failures.
/// A dropped envelope makes a finished run look unfinished (re-execution on the next
/// relaunch, or a false "group failed" at the CLI), so it gets more than one shot.
async fn put_envelope(http: &reqwest::Client, url: &str, env: &ResultEnvelope) -> Result<()> {
    let body = serde_json::to_vec(env)?;
    let mut last: Option<anyhow::Error> = None;
    for attempt in 1u32..=3 {
        let result = http
            .put(url)
            .timeout(transfer::CONTROL_TIMEOUT)
            .header(CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .and_then(|resp| resp.error_for_status().map_err(reqwest::Error::without_url));
        match result {
            Ok(_) => return Ok(()),
            Err(e) => {
                if attempt < 3 {
                    tracing::warn!("envelope PUT attempt {attempt} failed: {e}; retrying");
                    tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
                }
                last = Some(e.into());
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("envelope PUT failed")))
}

fn agent_error_envelope(
    spec: &JobSpec,
    node: &NodeInfo,
    timings: &Timings,
    attempts: usize,
    gate_reallocs: u32,
    error: String,
) -> ResultEnvelope {
    ResultEnvelope {
        v: PROTOCOL_VERSION,
        run_id: spec.run_id.clone(),
        shard_index: spec.shard_index,
        status: JobStatus::AgentError,
        exit_code: None,
        error: Some(error),
        timings: timings.clone(),
        node: node.clone(),
        uploads: Vec::new(),
        attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
        gate_reallocations: gate_reallocs,
    }
}

pub(crate) fn workdir(spec: &JobSpec) -> PathBuf {
    PathBuf::from(spec.workdir.as_deref().unwrap_or("/work"))
}

fn collect_node_info() -> NodeInfo {
    let report = probe::collect();
    NodeInfo {
        machine_id: env_opt("SALAD_MACHINE_ID"),
        container_group: env_opt("SALAD_CONTAINER_GROUP_NAME"),
        gpu_vendor: Some(vendor_str(report.gpu_vendor).to_string()),
        gpu_name: report.gpu_name,
        driver_version: report.driver_version,
        vram_mb: report.vram_mb,
        measured_down_mbps: None,
        measured_up_mbps: None,
    }
}

fn vendor_str(v: GpuVendor) -> &'static str {
    match v {
        GpuVendor::Nvidia => "nvidia",
        GpuVendor::Amd => "amd",
        GpuVendor::None => "none",
    }
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_threshold_matches_only_a_real_reading_below_the_minimum() {
        // No threshold configured → never below, whatever we measured.
        assert!(!below_threshold(None, None));
        assert!(!below_threshold(None, Some(50.0)));

        // Threshold set and a real reading below it → below.
        assert!(below_threshold(Some(100.0), Some(50.0)));

        // Threshold set and a real reading at or above it → not below.
        assert!(!below_threshold(Some(100.0), Some(150.0)));
        assert!(!below_threshold(Some(100.0), Some(100.0)));
    }

    #[test]
    fn absent_measurement_is_never_below_threshold() {
        // The footgun: a threshold is set but no measurement was taken (e.g. a
        // download threshold with no inputs). An absent reading must NOT count as
        // below-threshold, so it can never trigger a reallocation.
        assert!(!below_threshold(Some(100.0), None));
    }

    /// `dir/**` must ship the FILES at `dir`'s root, not only its subdirs.
    ///
    /// Found by a real 10k-step training run: `--output "ckpt/**:ckpt"`
    /// delivered `step_00008000/` and `step_00010000/` and silently left behind
    /// `ckpt/LATEST.json` — the marker tooling reads to find the newest
    /// checkpoint without listing and sorting directories. Every layer reported
    /// success, so the loss is invisible until something looks for the file.
    #[test]
    fn a_recursive_glob_includes_files_at_the_directory_root() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        std::fs::create_dir_all(w.join("ckpt").join("step_00010000")).unwrap();
        std::fs::write(w.join("ckpt").join("LATEST.json"), b"{}").unwrap();
        std::fs::write(
            w.join("ckpt")
                .join("step_00010000")
                .join("model.safetensors"),
            b"m",
        )
        .unwrap();

        // Exactly one match, and it is the directory: `upload_outputs` then takes
        // the single-path route, where the directory flattens to the archive root.
        // A fan-out over the subdirectories both drops the marker (the bug) and
        // doubles the path component on extraction, so the count matters as much
        // as the coverage. Unfixed, this returns ["ckpt/step_00010000"].
        assert_eq!(
            glob_outputs(w, "ckpt/**").unwrap(),
            vec![PathBuf::from("ckpt")]
        );
    }

    /// A bare `**` is `dir/**` rooted at the workdir, and drops root-level files
    /// the same way if it is not given the same treatment. `--output "**:all"` is
    /// how one naturally spells "everything", so it must not be the one spelling
    /// that silently loses files.
    #[test]
    fn a_bare_recursive_glob_covers_the_whole_workdir() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        std::fs::create_dir_all(w.join("sub")).unwrap();
        std::fs::write(w.join("sub").join("a.pt"), b"a").unwrap();
        std::fs::write(w.join("root.json"), b"{}").unwrap();

        // The workdir itself, as the empty relative path. Unfixed this is ["sub"],
        // and root.json never ships.
        let m = glob_outputs(w, "**").unwrap();
        assert_eq!(m, vec![PathBuf::from("")]);
        // What `upload_outputs` then does with it must still be a directory to
        // archive — `workdir.join("")` is the workdir.
        assert!(w.join(&m[0]).is_dir());

        // A trailing slash is the same pattern.
        assert_eq!(glob_outputs(w, "**/").unwrap(), vec![PathBuf::from("")]);
        // Matching `**` by equality rather than by stripping it cannot capture a
        // pattern like `s**` — and nothing is lost by that, because the glob crate
        // rejects one outright: "recursive wildcards must form a single path
        // component". So the two spellings above are the whole of `**`.
        assert!(glob_outputs(w, "s**").is_err());
    }

    /// Every spelling of the same directory must agree about it, empty or not, and
    /// only a pattern naming NOTHING may reach the caller's "matched no files" bail.
    ///
    /// The trap here is a guard that drops empty directories: it reads as prudence
    /// (a job that produced nothing must not report success, since the CLI deletes
    /// the group on success) but it makes `ckpt/**` the single spelling that calls
    /// an empty `ckpt` an error while `ckpt`, `ckpt/` and `*` all take it happily.
    /// It cannot even mean "holds data" — `hollow` below has a dirent and no bytes.
    #[test]
    fn a_recursive_glob_agrees_with_every_other_spelling_about_empty_dirs() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        std::fs::create_dir_all(w.join("empty")).unwrap();
        std::fs::create_dir_all(w.join("hollow").join("inner")).unwrap();
        std::fs::create_dir_all(w.join("full")).unwrap();
        std::fs::write(w.join("full").join("f.bin"), b"f").unwrap();
        std::fs::write(w.join("afile"), b"f").unwrap();

        let one = |p| vec![PathBuf::from(p)];
        // An empty directory: the same answer however it is named.
        assert_eq!(glob_outputs(w, "empty").unwrap(), one("empty"));
        assert_eq!(glob_outputs(w, "empty/").unwrap(), one("empty"));
        assert_eq!(glob_outputs(w, "empty/**").unwrap(), one("empty"));
        // ...including as one of several matches.
        let mut all = glob_outputs(w, "*/**").unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![
                PathBuf::from("empty"),
                PathBuf::from("full"),
                PathBuf::from("hollow"),
            ],
            "`*/**` must list the same directories `*` does"
        );
        // Only a directory, though: a trailing `/**` asks for a tree.
        assert!(glob_outputs(w, "afile/**").unwrap().is_empty());
        // And a pattern naming nothing still reaches the caller's bail.
        assert!(glob_outputs(w, "missing/**").unwrap().is_empty());
        assert!(glob_outputs(w, "missing").unwrap().is_empty());
    }

    /// A trailing `**` on a WILDCARD base has the identical defect, and a fix that
    /// tests the base on the filesystem cannot reach it: `workdir.join("a*")` is
    /// never a directory. Unfixed, `*/**` returned only the grandchild directories —
    /// dropping whole top-level matches, not merely the files beside them.
    #[test]
    fn a_recursive_glob_works_on_a_wildcard_base() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        std::fs::create_dir_all(w.join("adir")).unwrap();
        std::fs::write(w.join("adir").join("x.txt"), b"x").unwrap();
        std::fs::create_dir_all(w.join("ckpt").join("step_1")).unwrap();
        std::fs::write(w.join("ckpt").join("LATEST.json"), b"{}").unwrap();

        // Unfixed: ["ckpt/step_1"] — `adir` gone entirely, `LATEST.json` with it.
        let mut m = glob_outputs(w, "*/**").unwrap();
        m.sort();
        assert_eq!(m, vec![PathBuf::from("adir"), PathBuf::from("ckpt")]);

        // Unfixed: [] — a loud failure, but still the wrong answer.
        assert_eq!(
            glob_outputs(w, "a*/**").unwrap(),
            vec![PathBuf::from("adir")]
        );
    }

    #[test]
    fn glob_outputs_expands_dedups_and_relativizes() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        std::fs::create_dir_all(w.join("ckpts").join("latest").join("sub")).unwrap();
        std::fs::write(w.join("ckpts").join("latest").join("a.pt"), b"a").unwrap();
        std::fs::write(
            w.join("ckpts").join("latest").join("sub").join("b.pt"),
            b"b",
        )
        .unwrap();
        std::fs::write(w.join("model.safetensors"), b"m").unwrap();
        std::fs::write(w.join("x.safetensors"), b"x").unwrap();

        // A fan-out pattern yields each match, relative to the workdir.
        let mut m = glob_outputs(w, "*.safetensors").unwrap();
        m.sort();
        assert_eq!(
            m,
            vec![
                PathBuf::from("model.safetensors"),
                PathBuf::from("x.safetensors"),
            ]
        );

        // A literal existing path resolves to itself.
        assert_eq!(
            glob_outputs(w, "model.safetensors").unwrap(),
            vec![PathBuf::from("model.safetensors")]
        );

        // A `**` pattern matches a directory and its contents; dedup guarantees no returned
        // entry is nested under another, so the archive never double-counts a path.
        let deep = glob_outputs(w, "ckpts/latest/**").unwrap();
        assert!(!deep.is_empty());
        for a in &deep {
            for b in &deep {
                if a != b {
                    assert!(!a.starts_with(b), "{a:?} is nested under {b:?}");
                }
            }
        }

        // No match → empty; the caller turns this into a hard failure (never a silent skip).
        assert!(glob_outputs(w, "does/not/exist").unwrap().is_empty());
        assert!(glob_outputs(w, "nope-*.bin").unwrap().is_empty());
    }

    #[tokio::test]
    async fn capture_keeps_the_head_and_the_tail_and_names_the_gap() {
        let dir = tempfile::tempdir().unwrap();

        // Over the budget: the head is kept, the tail is kept, and the bytes lost between
        // them are stated. The tail is the half that matters — it holds the run's results
        // and, on a failure, the error that ended it.
        let over = dir.path().join("over.log");
        let mut cap = Capture::open(over.clone(), 4, 4).await.unwrap();
        cap.push(b"HEADmiddle-droppedTAIL").await;
        cap.finish().await.unwrap();
        let got = std::fs::read_to_string(&over).unwrap();
        assert!(got.starts_with("HEAD"), "{got:?}");
        assert!(got.ends_with("TAIL"), "{got:?}");
        assert!(got.contains("14 bytes of output dropped"), "{got:?}");

        // Under the budget: byte-for-byte, with no marker inventing a gap that never
        // happened — the overwhelmingly common case, and it must be an exact transcript.
        let under = dir.path().join("under.log");
        let mut cap = Capture::open(under.clone(), 4, 64).await.unwrap();
        cap.push(b"HEAD").await;
        cap.push(b"and the rest").await;
        cap.finish().await.unwrap();
        assert_eq!(std::fs::read_to_string(&under).unwrap(), "HEADand the rest");
    }

    #[tokio::test]
    async fn measure_download_rejects_latency_dominated_samples() {
        // A few-hundred-byte object measures RTT, not bandwidth — the reading must be
        // discarded (Err → the gate sees None, which never reallocates), while a
        // sample past the floor produces a real reading.
        let app = axum::Router::new()
            .route("/tiny", axum::routing::get(|| async { vec![0u8; 512] }))
            .route(
                "/big",
                axum::routing::get(|| async { vec![0u8; 2 * 1024 * 1024] }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let http = reqwest::Client::new();
        let sample = 8 * 1024 * 1024;
        assert!(
            measure_download(&http, &format!("{base}/tiny"), sample)
                .await
                .is_err(),
            "a 512-byte sample must be rejected"
        );
        let reading = measure_download(&http, &format!("{base}/big"), sample)
            .await
            .expect("a 2 MiB sample is a real measurement");
        assert!(reading > 0.0);
    }
}
