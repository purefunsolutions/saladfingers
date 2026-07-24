// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `sf-agent serve` — interactive session HTTP server.
//!
//! Binds `[::]` (IPv6 is mandatory for the SaladCloud gateway), supervises exec
//! sessions and chunked file transfers, and self-exits on the deadman / max-duration
//! timers. NOTE: self-exit alone does NOT stop billing — the platform relaunches the
//! container on every exit regardless of restart policy (empirical E1/E2/E4); only
//! CLI-side group deletion (the session reaper, `session rm`, `gc`) stops it. The
//! timers bound useful work and give the reaper a fresh `boot_id` to detect. Every
//! route except `/v1/healthz` requires `Authorization: Bearer <SF_AGENT_TOKEN>`; the
//! gateway fronts it with `auth=true` as a second layer. The design lives within the
//! gateway's 100 s / 1 GB limits: output is long-polled (≤30 s waits) and files move
//! in bounded chunks.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, middleware};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use clap::Args;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, Semaphore};

use saladfingers_protocol::PROTOCOL_VERSION;
use saladfingers_protocol::agent_api::{
    ExecCreated, ExecRequest, ExecStatus, FileStat, Health, MAX_OUTPUT_WAIT_MS, OutputPage,
    ShutdownRequest, SignalRequest, Stream, UploadInit, UploadInitResponse, UploadStatus, route,
};

use crate::ring::OutputRing;

/// Per-exec output ring size (matches the plan's 8 MiB budget).
const OUTPUT_RING_BYTES: usize = 8 * 1024 * 1024;
/// Maximum concurrent exec sessions.
const MAX_EXECS: usize = 4;
/// Chunk read size when pumping child stdout/stderr into the ring.
const PUMP_BUF: usize = 64 * 1024;
/// Body limit for file-chunk PUTs (32 MiB chunk + headroom).
const BODY_LIMIT: usize = 64 * 1024 * 1024;
/// How long an exited exec (and its up-to-8 MiB ring) is retained for late output
/// reads before being pruned. Unpruned, a long dev-box session accumulates rings
/// without bound → OOM-kill → platform relaunch → all session state lost.
const EXEC_RETAIN: Duration = Duration::from_secs(15 * 60);
/// Retained-exited-exec count cap (belt to the TTL's suspenders).
const MAX_RETAINED_EXECS: usize = 16;
/// An upload with no chunk activity for this long is abandoned: its map entry and its
/// preallocated `.sf/uploads/<id>.part` file (sized `size` up front) are swept.
const UPLOAD_STALE: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Reverse-proxy (inference) mode: supervise the app and proxy gateway traffic to
    /// it, exposing `/sf/v1/ready` and `/sf/v1/idle`. Without this, session mode runs.
    #[arg(long)]
    pub proxy: bool,
    /// The app's loopback listen port inside the container (proxy mode).
    #[arg(long)]
    pub app_port: Option<u16>,
    /// The app command to supervise, given after `--` (proxy mode).
    #[arg(last = true)]
    pub app_command: Vec<String>,
    /// Serve the exec/file API without a bearer token. Without this flag, an unset
    /// `SF_AGENT_TOKEN` is a startup error (fail-closed), not an open server.
    #[arg(long)]
    pub allow_unauthenticated: bool,
}

/// Serve the interactive session API — or, with `--proxy`, the inference reverse proxy —
/// until the deadman/max-duration fires or SIGTERM.
///
/// # Errors
/// Returns an error only if the listener fails to bind or serve.
pub async fn serve(args: ServeArgs) -> Result<()> {
    if args.proxy {
        return crate::proxy::serve_proxy(args).await;
    }
    let state = AppState::from_env();
    if state.inner.token.is_none() {
        // Fail closed: this API executes arbitrary commands and reads/writes files. The
        // session CLI always sets the token; a bespoke deployment that genuinely wants an
        // open server (e.g. gateway auth=true as the only layer) must say so explicitly.
        anyhow::ensure!(
            args.allow_unauthenticated,
            "SF_AGENT_TOKEN is not set; refusing to serve the exec/file API unauthenticated \
             (set the token, or pass --allow-unauthenticated to accept the risk)"
        );
        tracing::warn!(
            "SF_AGENT_TOKEN is unset — the session API is UNAUTHENTICATED (explicitly allowed)"
        );
    }
    let port: u16 = env_nonempty("SF_PORT")
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);

    let timers = state.clone();
    tokio::spawn(async move { timers.run_timers().await });

    let app = app(state.clone());
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding [::]:{port}"))?;
    tracing::info!("session API serving on [::]:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_future(state.clone()))
        .await?;
    // Best-effort: signal any children still running so we don't leak processes.
    state.terminate_children().await;
    Ok(())
}

/// Build the router. Split out so tests can serve it on an ephemeral port.
pub fn app(state: AppState) -> Router {
    let protected = Router::new()
        .route(route::EXEC, post(exec_create))
        .route("/v1/exec/{id}", get(exec_status))
        .route("/v1/exec/{id}/output", get(exec_output))
        .route("/v1/exec/{id}/signal", post(exec_signal))
        .route(route::FILES_UPLOAD, post(upload_init))
        .route("/v1/files/upload/{id}", get(upload_status))
        .route("/v1/files/upload/{id}/{index}", put(upload_chunk))
        .route("/v1/files/upload/{id}/complete", post(upload_complete))
        .route(route::FILES_DOWNLOAD, get(download))
        .route(route::FILES_STAT, get(stat))
        .route(route::SHUTDOWN, post(shutdown_handler))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn_with_state(state.clone(), auth_mw));
    Router::new()
        .route(route::HEALTHZ, get(healthz))
        .merge(protected)
        .with_state(state)
}

/// Shared server state (cheap to clone; all real state lives behind the `Arc`).
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    run_id: String,
    boot_id: String,
    started: Instant,
    token: Option<String>,
    workdir: PathBuf,
    deadman: Option<Duration>,
    max_duration: Option<Duration>,
    execs: Mutex<HashMap<String, Arc<Exec>>>,
    sem: Arc<Semaphore>,
    last_contact: Mutex<Instant>,
    uploads: Mutex<HashMap<String, Upload>>,
    shutdown: Notify,
}

/// A single exec session. The supervising task owns the `Child` and semaphore permit;
/// handlers reach the child only by `pid` (via `libc::kill`) so status/output reads
/// never contend with the process wait.
struct Exec {
    started_at: DateTime<Utc>,
    pid: u32,
    ring: Mutex<OutputRing>,
    notify: Notify,
    state: Mutex<ExecState>,
}

#[derive(Clone)]
enum ExecState {
    Running,
    Exited {
        code: Option<i32>,
        signal: Option<String>,
        /// When the exec finished — drives retention pruning.
        at: Instant,
    },
}

struct Upload {
    path: PathBuf,
    temp: PathBuf,
    size: u64,
    sha256: String,
    chunk_bytes: u64,
    received: std::collections::BTreeSet<u32>,
    /// Last chunk (or init) time — an idle upload past [`UPLOAD_STALE`] is swept.
    last_activity: Instant,
}

impl AppState {
    /// Construct server state from the agent's bootstrap environment.
    #[must_use]
    pub fn from_env() -> Self {
        let run_id = env_nonempty("SF_RUN_ID")
            .or_else(|| env_nonempty("SALAD_CONTAINER_GROUP_NAME"))
            .unwrap_or_else(|| "session".to_string());
        let workdir =
            env_nonempty("SF_WORKDIR").map_or_else(|| PathBuf::from("/work"), PathBuf::from);
        let deadman = env_nonempty("SF_DEADMAN_SECS")
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs);
        let max_duration = env_nonempty("SF_MAX_DURATION_SECS")
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs);
        Self::new(
            run_id,
            env_nonempty("SF_AGENT_TOKEN"),
            workdir,
            deadman.or(Some(Duration::from_secs(900))),
            max_duration,
        )
    }

    /// Build state directly (used by tests).
    #[must_use]
    pub fn new(
        run_id: String,
        token: Option<String>,
        workdir: PathBuf,
        deadman: Option<Duration>,
        max_duration: Option<Duration>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                run_id,
                boot_id: rand_id(),
                started: Instant::now(),
                token,
                workdir,
                deadman,
                max_duration,
                execs: Mutex::new(HashMap::new()),
                sem: Arc::new(Semaphore::new(MAX_EXECS)),
                last_contact: Mutex::new(Instant::now()),
                uploads: Mutex::new(HashMap::new()),
                shutdown: Notify::new(),
            }),
        }
    }

    fn execs_running(&self) -> u32 {
        (MAX_EXECS - self.inner.sem.available_permits()) as u32
    }

    /// Prune exited execs past their retention (each pins an up-to-8 MiB ring) and
    /// abandoned uploads (map entry + preallocated `.part` file). In-flight readers
    /// holding an `Arc<Exec>` are unaffected — pruning only drops the map's reference.
    async fn prune(&self, exec_retain: Duration, upload_stale: Duration) {
        let mut exited: Vec<(String, Instant)> = Vec::new();
        {
            let execs = self.inner.execs.lock().await;
            for (id, exec) in execs.iter() {
                if let ExecState::Exited { at, .. } = &*exec.state.lock().await {
                    exited.push((id.clone(), *at));
                }
            }
        }
        // Oldest first; drop everything past the TTL, plus overflow beyond the count cap.
        exited.sort_by_key(|(_, at)| *at);
        let overflow = exited.len().saturating_sub(MAX_RETAINED_EXECS);
        let drop_ids: Vec<String> = exited
            .iter()
            .enumerate()
            .filter(|(i, (_, at))| *i < overflow || at.elapsed() >= exec_retain)
            .map(|(_, (id, _))| id.clone())
            .collect();
        if !drop_ids.is_empty() {
            let mut execs = self.inner.execs.lock().await;
            for id in &drop_ids {
                execs.remove(id);
            }
            tracing::debug!(count = drop_ids.len(), "pruned exited exec sessions");
        }

        let stale: Vec<(String, PathBuf)> = {
            let uploads = self.inner.uploads.lock().await;
            uploads
                .iter()
                .filter(|(_, up)| up.last_activity.elapsed() >= upload_stale)
                .map(|(id, up)| (id.clone(), up.temp.clone()))
                .collect()
        };
        for (id, temp) in stale {
            self.inner.uploads.lock().await.remove(&id);
            tokio::fs::remove_file(&temp).await.ok();
            tracing::warn!(upload = %id, "swept abandoned upload and its temp file");
        }
    }

    /// Deadman + max-duration loop: self-exit when overdue or idle with no execs.
    async fn run_timers(&self) {
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
                () = self.inner.shutdown.notified() => return,
            }
            self.prune(EXEC_RETAIN, UPLOAD_STALE).await;
            if let Some(max) = self.inner.max_duration
                && self.inner.started.elapsed() >= max
            {
                tracing::info!("max-duration reached; shutting down");
                self.inner.shutdown.notify_waiters();
                return;
            }
            if let Some(deadman) = self.inner.deadman {
                let idle = self.inner.last_contact.lock().await.elapsed();
                if idle >= deadman && self.execs_running() == 0 {
                    tracing::info!("deadman ({deadman:?} idle, no execs); shutting down");
                    self.inner.shutdown.notify_waiters();
                    return;
                }
            }
        }
    }

    async fn terminate_children(&self) {
        let execs = self.inner.execs.lock().await;
        for exec in execs.values() {
            if matches!(&*exec.state.lock().await, ExecState::Running) {
                send_signal(exec.pid, libc::SIGTERM);
            }
        }
    }
}

/// Await whichever comes first: a timer-triggered shutdown or SIGTERM/Ctrl-C.
async fn shutdown_future(state: AppState) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        () = state.inner.shutdown.notified() => {}
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Bearer-auth guard + deadman keepalive for every route except `/v1/healthz`.
async fn auth_mw(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    if let Some(expected) = &state.inner.token {
        let presented = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        if presented.is_none_or(|t| !ct_eq(t.as_bytes(), expected.as_bytes())) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    *state.inner.last_contact.lock().await = Instant::now();
    next.run(req).await
}

async fn healthz(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        v: PROTOCOL_VERSION,
        run_id: state.inner.run_id.clone(),
        boot_id: state.inner.boot_id.clone(),
        uptime_secs: state.inner.started.elapsed().as_secs(),
        execs_running: state.execs_running(),
    })
}

async fn exec_create(
    State(state): State<AppState>,
    Json(req): Json<ExecRequest>,
) -> Result<(StatusCode, Json<ExecCreated>), Response> {
    let Some(program) = req.argv.first().cloned() else {
        return Err((StatusCode::BAD_REQUEST, "argv must be non-empty").into_response());
    };
    let permit = state
        .inner
        .sem
        .clone()
        .try_acquire_owned()
        .map_err(|_| (StatusCode::CONFLICT, "max concurrent execs reached").into_response())?;

    let mut cmd = Command::new(&program);
    cmd.args(&req.argv[1..]);
    let workdir = req
        .workdir
        .clone()
        .unwrap_or_else(|| state.inner.workdir.to_string_lossy().into_owned());
    cmd.current_dir(&workdir);
    if let Some(env) = &req.env {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("spawn {program}: {e}"),
        )
            .into_response()
    })?;
    let pid = child.id().unwrap_or(0);
    let exec_id = rand_id();
    let exec = Arc::new(Exec {
        started_at: Utc::now(),
        pid,
        ring: Mutex::new(OutputRing::new(OUTPUT_RING_BYTES)),
        notify: Notify::new(),
        state: Mutex::new(ExecState::Running),
    });
    state
        .inner
        .execs
        .lock()
        .await
        .insert(exec_id.clone(), exec.clone());

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    tokio::spawn(async move {
        let o = tokio::spawn(pump(stdout, Stream::Stdout, exec.clone()));
        let e = tokio::spawn(pump(stderr, Stream::Stderr, exec.clone()));
        let status = child.wait().await;
        let _ = o.await;
        let _ = e.await;
        let (code, signal) = match status {
            Ok(s) => (s.code(), s.signal().map(signal_name)),
            Err(_) => (None, None),
        };
        *exec.state.lock().await = ExecState::Exited {
            code,
            signal,
            at: Instant::now(),
        };
        exec.notify.notify_waiters();
        drop(permit);
    });

    Ok((StatusCode::CREATED, Json(ExecCreated { exec_id })))
}

/// Read one stream to EOF, appending to the exec's ring and waking pollers.
async fn pump<R: AsyncReadExt + Unpin>(reader: Option<R>, stream: Stream, exec: Arc<Exec>) {
    let Some(mut reader) = reader else { return };
    let mut buf = vec![0u8; PUMP_BUF];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                exec.ring.lock().await.push(stream, &buf[..n]);
                exec.notify.notify_waiters();
            }
        }
    }
}

async fn exec_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ExecStatus>, Response> {
    let exec = get_exec(&state, &id).await?;
    let st = exec.state.lock().await.clone();
    let (running, exit_code, signal) = match st {
        ExecState::Running => (true, None, None),
        ExecState::Exited { code, signal, .. } => (false, code, signal),
    };
    Ok(Json(ExecStatus {
        running,
        exit_code,
        started_at: exec.started_at,
        signal,
    }))
}

#[derive(Debug, Deserialize)]
struct OutputQuery {
    #[serde(default)]
    cursor: u64,
    #[serde(default)]
    wait_ms: u64,
}

async fn exec_output(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<OutputQuery>,
) -> Result<Json<OutputPage>, Response> {
    let exec = get_exec(&state, &id).await?;
    // Register interest (via `enable`) before the first read so a write that lands
    // between the read and the wait still wakes us.
    let notified = exec.notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    let page = read_page(q.cursor, &exec).await;
    if !page.chunks.is_empty() || page.exited || q.wait_ms == 0 {
        return Ok(Json(page));
    }
    let wait = Duration::from_millis(q.wait_ms.min(MAX_OUTPUT_WAIT_MS));
    tokio::select! {
        () = notified => {}
        () = tokio::time::sleep(wait) => {}
    }
    Ok(Json(read_page(q.cursor, &exec).await))
}

/// Read the ring from `cursor` and fold in the exec's exit state. Reading all bytes
/// past the cursor each time makes a missed wakeup a latency issue, never data loss.
///
/// Order matters: snapshot the exit state FIRST, then read the ring. The supervisor
/// sets `Exited` only after both pumps have drained, so state-then-ring guarantees that
/// `exited: true` implies the ring holds the complete output. The reverse order could
/// observe a final push + `Exited` landing between the two locks and report
/// `exited: true` with the tail missing — clients stop polling at `exited`, so that
/// tail (typically the result line the user ran the command for) would be lost.
async fn read_page(cursor: u64, exec: &Exec) -> OutputPage {
    let (exited, exit_code) = match &*exec.state.lock().await {
        ExecState::Running => (false, None),
        ExecState::Exited { code, .. } => (true, *code),
    };
    let (chunks, next_cursor, truncated) = exec.ring.lock().await.read_from(cursor);
    OutputPage {
        chunks,
        next_cursor,
        exited,
        exit_code,
        truncated,
    }
}

async fn exec_signal(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SignalRequest>,
) -> Result<StatusCode, Response> {
    let exec = get_exec(&state, &id).await?;
    let sig = match req.signal.to_ascii_uppercase().as_str() {
        "TERM" => libc::SIGTERM,
        "INT" => libc::SIGINT,
        "KILL" => libc::SIGKILL,
        other => {
            return Err(
                (StatusCode::BAD_REQUEST, format!("unknown signal {other}")).into_response()
            );
        }
    };
    if matches!(&*exec.state.lock().await, ExecState::Running) {
        send_signal(exec.pid, sig);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn get_exec(state: &AppState, id: &str) -> Result<Arc<Exec>, Response> {
    state
        .inner
        .execs
        .lock()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such exec").into_response())
}

// ---- file transfer ----

async fn upload_init(
    State(state): State<AppState>,
    Json(req): Json<UploadInit>,
) -> Result<Json<UploadInitResponse>, Response> {
    let upload_id = rand_id();
    let dir = state.inner.workdir.join(".sf/uploads");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")).into_response())?;
    let temp = dir.join(format!("{upload_id}.part"));
    // Preallocate so chunk PUTs can seek-write in any order.
    let file = tokio::fs::File::create(&temp)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("create: {e}")).into_response())?;
    file.set_len(req.size).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("truncate: {e}")).into_response()
    })?;
    state.inner.uploads.lock().await.insert(
        upload_id.clone(),
        Upload {
            path: PathBuf::from(&req.path),
            temp,
            size: req.size,
            sha256: req.sha256.to_ascii_lowercase(),
            chunk_bytes: req.chunk_bytes.max(1),
            received: std::collections::BTreeSet::new(),
            last_activity: Instant::now(),
        },
    );
    Ok(Json(UploadInitResponse { upload_id }))
}

async fn upload_chunk(
    State(state): State<AppState>,
    Path((id, index)): Path<(String, u32)>,
    body: Bytes,
) -> Result<StatusCode, Response> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    let (temp, offset) = {
        let uploads = state.inner.uploads.lock().await;
        let up = uploads
            .get(&id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, "no such upload").into_response())?;
        // Checked math + a size bound: `index * chunk_bytes` are client-controlled and
        // would wrap in release (no overflow-checks); an out-of-bounds chunk must be a
        // 400, not a silent seek-write to a wild offset extending the file.
        let offset = u64::from(index)
            .checked_mul(up.chunk_bytes)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "chunk offset overflows").into_response())?;
        let end = offset
            .checked_add(body.len() as u64)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "chunk end overflows").into_response())?;
        if end > up.size {
            return Err((
                StatusCode::BAD_REQUEST,
                "chunk extends past the declared upload size",
            )
                .into_response());
        }
        (up.temp.clone(), offset)
    };
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&temp)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("open: {e}")).into_response())?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("seek: {e}")).into_response())?;
    file.write_all(&body)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")).into_response())?;
    file.flush().await.ok();
    let mut uploads = state.inner.uploads.lock().await;
    let up = uploads
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such upload").into_response())?;
    up.received.insert(index);
    up.last_activity = Instant::now();
    Ok(StatusCode::NO_CONTENT)
}

async fn upload_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<UploadStatus>, Response> {
    let uploads = state.inner.uploads.lock().await;
    let up = uploads
        .get(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such upload").into_response())?;
    Ok(Json(UploadStatus {
        received: up.received.iter().copied().collect(),
    }))
}

async fn upload_complete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<FileStat>, Response> {
    let up = state
        .inner
        .uploads
        .lock()
        .await
        .remove(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such upload").into_response())?;
    let digest = sha256_file(&up.temp)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("hash: {e}")).into_response())?;
    if digest != up.sha256 {
        tokio::fs::remove_file(&up.temp).await.ok();
        return Err((StatusCode::BAD_REQUEST, "sha256 mismatch").into_response());
    }
    if let Some(parent) = up.path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    tokio::fs::rename(&up.temp, &up.path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("rename: {e}")).into_response())?;
    Ok(Json(FileStat {
        size: up.size,
        mtime: Utc::now(),
    }))
}

#[derive(Debug, Deserialize)]
struct DownloadQuery {
    path: String,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    len: Option<u64>,
}

async fn download(
    State(_state): State<AppState>,
    Query(q): Query<DownloadQuery>,
) -> Result<Response, Response> {
    use tokio::io::AsyncSeekExt;
    let mut file = tokio::fs::File::open(&q.path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("open {}: {e}", q.path)).into_response())?;
    file.seek(std::io::SeekFrom::Start(q.offset))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("seek: {e}")).into_response())?;
    let want = q.len.unwrap_or(u64::MAX).min(BODY_LIMIT as u64) as usize;
    let mut buf = Vec::new();
    file.take(want as u64)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read: {e}")).into_response())?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], buf).into_response())
}

async fn stat(
    State(_state): State<AppState>,
    Query(q): Query<StatQuery>,
) -> Result<Json<FileStat>, Response> {
    let meta = tokio::fs::metadata(&q.path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("stat {}: {e}", q.path)).into_response())?;
    let mtime = meta
        .modified()
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(|_| Utc::now());
    Ok(Json(FileStat {
        size: meta.len(),
        mtime,
    }))
}

#[derive(Debug, Deserialize)]
struct StatQuery {
    path: String,
}

async fn shutdown_handler(
    State(state): State<AppState>,
    Json(_req): Json<ShutdownRequest>,
) -> StatusCode {
    state.inner.shutdown.notify_waiters();
    StatusCode::ACCEPTED
}

// ---- helpers ----

pub(crate) fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

/// 128-bit random id as lowercase hex.
fn rand_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("os rng");
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn signal_name(sig: i32) -> String {
    match sig {
        libc::SIGTERM => "TERM".to_string(),
        libc::SIGINT => "INT".to_string(),
        libc::SIGKILL => "KILL".to_string(),
        other => other.to_string(),
    }
}

fn send_signal(pid: u32, sig: i32) {
    if pid != 0 {
        // SAFETY: kill(2) with a plain pid and signal number; failure is ignored.
        unsafe {
            libc::kill(pid as libc::pid_t, sig);
        }
    }
}

/// Constant-time byte comparison for the bearer token.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn sha256_file(path: &std::path::Path) -> std::io::Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exited_exec() -> Arc<Exec> {
        Arc::new(Exec {
            started_at: Utc::now(),
            pid: 0,
            ring: Mutex::new(OutputRing::new(1024)),
            notify: Notify::new(),
            state: Mutex::new(ExecState::Exited {
                code: Some(0),
                signal: None,
                at: Instant::now(),
            }),
        })
    }

    #[tokio::test]
    async fn prune_sweeps_exited_execs_and_stale_uploads_but_keeps_live_state() {
        let state = AppState::new(
            "t".into(),
            Some("tok".into()),
            std::env::temp_dir(),
            None,
            None,
        );
        // One exited exec, one running exec, one upload (with a real temp file).
        let running = Arc::new(Exec {
            started_at: Utc::now(),
            pid: 0,
            ring: Mutex::new(OutputRing::new(1024)),
            notify: Notify::new(),
            state: Mutex::new(ExecState::Running),
        });
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join("u1.part");
        std::fs::write(&temp, b"partial").unwrap();
        {
            state
                .inner
                .execs
                .lock()
                .await
                .insert("done".into(), exited_exec());
            state
                .inner
                .execs
                .lock()
                .await
                .insert("live".into(), running);
            state.inner.uploads.lock().await.insert(
                "u1".into(),
                Upload {
                    path: dir.path().join("dest"),
                    temp: temp.clone(),
                    size: 7,
                    sha256: String::new(),
                    chunk_bytes: 1,
                    received: std::collections::BTreeSet::new(),
                    last_activity: Instant::now(),
                },
            );
        }

        // Generous TTLs: nothing is pruned.
        state
            .prune(Duration::from_secs(3600), Duration::from_secs(3600))
            .await;
        assert_eq!(state.inner.execs.lock().await.len(), 2);
        assert_eq!(state.inner.uploads.lock().await.len(), 1);

        // Zero TTLs: the exited exec and the stale upload (incl. its temp file) go;
        // the running exec must never be pruned.
        state.prune(Duration::ZERO, Duration::ZERO).await;
        let execs = state.inner.execs.lock().await;
        assert!(execs.contains_key("live"), "running exec must be kept");
        assert!(!execs.contains_key("done"), "exited exec must be pruned");
        assert!(state.inner.uploads.lock().await.is_empty());
        assert!(!temp.exists(), "abandoned upload temp file must be removed");
    }
}
