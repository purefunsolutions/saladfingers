// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `sf-agent serve --proxy` — inference reverse proxy (M6).
//!
//! Supervises the app child (e.g. `infurer qwen36-serve --addr 127.0.0.1:8080`) and
//! reverse-proxies all non-`/sf/*` gateway traffic to it on loopback, streaming
//! responses so SSE/token streams pass through. The gateway fronts this with
//! `auth=false` — end users reach the app without a Salad key, and the app enforces its
//! own auth. Control endpoints: `/sf/v1/ready` (unauth, for readiness probes) and
//! `/sf/v1/idle` (bearer-guarded, for the `serve autostop` watchdog). Exits on SIGTERM,
//! the max-duration timer, or the app process dying.

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::process::Command;

use saladfingers_protocol::PROTOCOL_VERSION;

use crate::serve::{ServeArgs, ct_eq, env_nonempty};

/// Hop-by-hop headers that must not be forwarded across the proxy.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];
/// Max buffered request body. Inference requests are small; responses stream unbuffered.
const MAX_REQUEST_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
struct ProxyState {
    inner: Arc<ProxyInner>,
}

struct ProxyInner {
    /// `http://127.0.0.1:<app_port>`.
    target: String,
    app_port: u16,
    http: reqwest::Client,
    token: Option<String>,
    started: Instant,
    /// Stamped at request start AND on every streamed response chunk — a std (not
    /// tokio) mutex so the sync stream `inspect` closure can stamp it. Stamping only
    /// at request start made an hour-long SSE stream look idle, so the autostop
    /// watchdog would stop a box mid-generation.
    last_proxied: std::sync::Mutex<Instant>,
}

impl ProxyInner {
    fn touch(&self) {
        if let Ok(mut guard) = self.last_proxied.lock() {
            *guard = Instant::now();
        }
    }
}

/// Run the reverse proxy until SIGTERM, the max-duration timer, or the app exits.
///
/// # Errors
/// Returns an error if the app cannot be spawned or the listener fails to bind/serve.
pub async fn serve_proxy(args: ServeArgs) -> Result<()> {
    let app_port = args
        .app_port
        .or_else(|| env_port("SF_PROXY_TARGET"))
        .context("proxy mode needs --app-port (or SF_PROXY_TARGET)")?;
    let listen_port: u16 = env_nonempty("SF_PORT")
        .and_then(|s| s.parse().ok())
        .unwrap_or(8888);
    let max_duration = env_nonempty("SF_MAX_DURATION_SECS")
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs);

    // Supervise the app; its stdout/stderr flow to the container log.
    let mut child = if let Some((program, rest)) = args.app_command.split_first() {
        let mut cmd = Command::new(program);
        cmd.args(rest).stdin(Stdio::null());
        Some(
            cmd.spawn()
                .with_context(|| format!("spawning app {program}"))?,
        )
    } else {
        tracing::warn!("no app command; proxying to a pre-existing process on :{app_port}");
        None
    };
    let app_pid = child.as_ref().and_then(tokio::process::Child::id);

    let state = ProxyState {
        inner: Arc::new(ProxyInner {
            target: format!("http://127.0.0.1:{app_port}"),
            app_port,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                // A reverse proxy passes an upstream 3xx straight back to the client; it must
                // never chase the redirect itself. Following one server-side would hide it
                // from the client and — worse — let an app-level open redirect pivot this
                // (otherwise loopback-only) client into fetching an attacker-chosen URL from
                // inside the container (SSRF), since `serve --proxy` fronts untrusted traffic.
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            token: env_nonempty("SF_AGENT_TOKEN"),
            started: Instant::now(),
            last_proxied: std::sync::Mutex::new(Instant::now()),
        }),
    };

    // A single Notify drives graceful shutdown from any trigger. Triggers use `notify_one`,
    // not `notify_waiters`: the app can die before axum has polled the graceful-shutdown
    // future to its `notified()` await point, and only `notify_one` stores a permit for that
    // case. A lost signal would leave the proxy — and the GPU billing — running until the
    // max-duration timer or SIGTERM, long after the app it fronts has exited.
    let shutdown = Arc::new(tokio::sync::Notify::new());

    if let Some(max) = max_duration {
        let s = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(max).await;
            tracing::info!("max-duration reached; shutting down");
            s.notify_one();
        });
    }
    // Watch the app: if it dies, tear the proxy down too (so billing stops).
    if let Some(mut c) = child.take() {
        let s = shutdown.clone();
        tokio::spawn(async move {
            let status = c.wait().await;
            tracing::warn!("app process exited ({status:?}); shutting down");
            s.notify_one();
        });
    }

    let router = Router::new()
        .route("/sf/v1/healthz", get(healthz))
        .route("/sf/v1/ready", get(ready))
        .route("/sf/v1/idle", get(idle))
        .fallback(proxy_handler)
        .with_state(state.clone());

    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), listen_port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding [::]:{listen_port}"))?;
    tracing::info!(
        "inference proxy serving on [::]:{listen_port} → {}",
        state.inner.target
    );

    let sd = shutdown.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { proxy_shutdown(sd).await })
        .await?;

    // Best-effort: stop the app on the way out.
    if let Some(pid) = app_pid {
        // SAFETY: kill(2) with a plain pid; failure ignored.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
    }
    Ok(())
}

async fn proxy_shutdown(shutdown: Arc<tokio::sync::Notify>) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        () = shutdown.notified() => {}
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

async fn healthz() -> axum::Json<Value> {
    axum::Json(json!({ "v": PROTOCOL_VERSION, "role": "proxy" }))
}

/// Readiness: can we open a TCP connection to the app? Returns 200 when yes, else 503.
async fn ready(State(s): State<ProxyState>) -> Response {
    let up = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(("127.0.0.1", s.inner.app_port)),
    )
    .await
    .is_ok_and(|r| r.is_ok());
    if up {
        (StatusCode::OK, axum::Json(json!({ "ready": true }))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "ready": false })),
        )
            .into_response()
    }
}

/// Idle report for the autostop watchdog. Bearer-guarded when a token is configured.
async fn idle(State(s): State<ProxyState>, headers: HeaderMap) -> Response {
    if let Some(expected) = &s.inner.token {
        let ok = headers
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            .is_some_and(|t| ct_eq(t.as_bytes(), expected.as_bytes()));
        if !ok {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    let idle_secs = s
        .inner
        .last_proxied
        .lock()
        .map(|g| g.elapsed().as_secs())
        .unwrap_or_default();
    axum::Json(json!({
        "idle_secs": idle_secs,
        "uptime_secs": s.inner.started.elapsed().as_secs(),
    }))
    .into_response()
}

/// Reverse-proxy a request to the app, streaming the response back unbuffered.
async fn proxy_handler(State(s): State<ProxyState>, req: Request) -> Response {
    s.inner.touch();
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_REQUEST_BYTES).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response(),
    };
    let path_q = parts.uri.path_and_query().map_or("/", |pq| pq.as_str());
    let url = format!("{}{path_q}", s.inner.target);

    // `Bytes` into the body directly — no `.to_vec()` copy of a possibly-large payload.
    let mut out = s.inner.http.request(parts.method, &url).body(bytes);
    for (name, value) in &parts.headers {
        if !HOP_BY_HOP.contains(&name.as_str()) {
            out = out.header(name, value);
        }
    }
    match out.send().await {
        Ok(resp) => {
            let mut builder = Response::builder().status(resp.status());
            for (name, value) in resp.headers() {
                if !HOP_BY_HOP.contains(&name.as_str()) {
                    builder = builder.header(name, value);
                }
            }
            // Stamp activity on every streamed chunk so a long-lived SSE/token stream
            // counts as active the whole way, not just at request start.
            let inner = s.inner.clone();
            let stream = resp.bytes_stream().inspect(move |_| inner.touch());
            builder
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response(),
    }
}

/// Parse a port from `SF_PROXY_TARGET` (`host:port` or a bare port).
fn env_port(name: &str) -> Option<u16> {
    let v = env_nonempty(name)?;
    v.rsplit(':').next().and_then(|p| p.parse().ok())
}
