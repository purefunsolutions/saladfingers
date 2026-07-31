// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers tunnel RUN_ID` — a loopback proxy onto a run's gateway port.
//!
//! `run --expose-port` publishes the port with the gateway set to `auth=true`,
//! so nothing on the public internet can reach it: the Cloudflare edge rejects
//! any request without `Salad-Api-Key` before it reaches the container. That
//! also locks out a browser, which cannot attach a header to a navigation.
//!
//! This bridges the gap. It listens on loopback, forwards every request to the
//! run's `https://<dns>` gateway with the key attached, and streams the
//! response back. The browser talks to `http://127.0.0.1:PORT`; the API key
//! never leaves this host.
//!
//! There is no other route in — SaladCloud's only ingress is the gateway (see
//! `docs/salad-facts.md`), and there is no SSH anywhere in this system. So this
//! *is* the tunnel, not a workaround for a missing one.
//!
//! **Responses stream.** The dashboard is Server-Sent Events, and buffering a
//! response body here would hold every event until the gateway cut the request
//! at its 100 s cap (`docs/salad-facts.md`) — an entire minute and a half of a
//! live dashboard arriving at once, then a reconnect. `sf-agent`'s in-container
//! proxy streams for exactly the same reason; this is its caller-side mirror.
//! Requests do not stream: they are buffered to a bounded size, because a
//! request body is a form post or a JSON document, not an open-ended stream.
//!
//! **WebSockets do not work through this.** The gateway carries them only with
//! `auth=false` (`docs/salad-facts.md`), and `--expose-port` is `auth=true` by
//! construction — the same trade that put `session` and `serve` on long-poll.
//! `connection` and `upgrade` are hop-by-hop and stripped here too, so an
//! upgrade could not survive even if the edge allowed it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;

use crate::cli::TunnelArgs;
use crate::config::Config;
use crate::state;

/// Headers that are meaningful only for one hop and must not be forwarded.
/// Mirrors the list in `sf-agent`'s proxy — the same rules apply in reverse.
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
];

/// How long to wait for the TLS connection to the gateway.
///
/// The client carries no *total* deadline (see [`tunnel`]), so this and the OS's own
/// keepalives are what distinguish "the run ended and the DNS name is dead" from "the
/// dashboard has nothing to say right now".
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest request body forwarded upstream.
///
/// Requests are buffered, so this is memory on the operator's own machine. The gateway
/// caps a request at 1 GB and nothing a dashboard sends is anywhere near either number;
/// refusing at 32 MiB turns a runaway upload into a 413 instead of an allocation.
const MAX_REQUEST_BODY: usize = 32 * 1024 * 1024;

struct Tunnel {
    upstream: String,
    api_key: String,
    http: reqwest::Client,
}

/// # Errors
/// Returns an error if the run has no local state, no gateway, or the local
/// port is already bound.
pub async fn tunnel(cfg: Config, args: TunnelArgs) -> Result<()> {
    let run = state::load_run(&args.run_id)
        .with_context(|| format!("reading local state for run {}", args.run_id))?
        .with_context(|| format!("no local state for run {}", args.run_id))?;
    let client = cfg.client()?;

    // Resolve the gateway DNS from the first shard that has one. A run with
    // several shards exposes one gateway each (per-instance routing is
    // impossible on SaladCloud), so --shard picks between them.
    let want = args.shard;
    let group_name = run
        .groups
        .iter()
        .find(|g| g.shard == want)
        .map(|g| g.name.clone())
        .with_context(|| format!("run {} has no shard {want}", args.run_id))?;

    let group = client
        .get_container_group(&group_name)
        .await
        .with_context(|| format!("looking up container group {group_name}"))?;
    let Some(upstream) = group.gateway_url() else {
        bail!(
            "run {} shard {want} has no gateway — it was not started with --expose-port, \
             or the DNS name is not published yet",
            args.run_id
        );
    };

    let state = Arc::new(Tunnel {
        upstream,
        api_key: cfg.api_key.expose().to_string(),
        // No *total* timeout: SSE responses are long-lived by design, and the gateway's
        // own 100 s cap already bounds them. A connect timeout is a different thing and
        // is kept, matching every other client in this workspace.
        http: reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context("building the tunnel's HTTP client")?,
    });

    let listener = bind_loopback(args.local_port).await?;

    eprintln!(
        "tunnel: {} -> {} (shard {want})\n  \
         the gateway is authenticated; this process holds the key. Ctrl-C to stop.",
        browser_url(args.local_port),
        state.upstream
    );

    let app = axum::Router::new()
        .fallback(forward)
        .with_state(Arc::clone(&state));
    axum::serve(listener, app)
        .await
        .context("tunnel server failed")
}

/// Bind the local end of the tunnel.
///
/// Loopback only, and not configurable. Binding this anywhere else would re-publish the
/// port this command exists to keep private — now with the API key helpfully attached to
/// every forwarded request, so every host on the LAN would hold the operator's gateway
/// capability. Someone who wants it elsewhere can forward it themselves (`ssh -L`) and own
/// that decision.
async fn bind_loopback(local_port: u16) -> Result<tokio::net::TcpListener> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_port);
    tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr} (is another tunnel already running?)"))
}

/// Forward one request upstream with the key attached, streaming the response.
async fn forward(State(st): State<Arc<Tunnel>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    let url = format!("{}{path_and_query}", st.upstream);

    let bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY).await {
        Ok(b) => b,
        Err(e) => {
            // Bounded, unlike the response: this one is held in memory here.
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body over {MAX_REQUEST_BODY} bytes, or unreadable: {e}"),
            )
                .into_response();
        }
    };

    let mut headers = strip_hop_by_hop(&parts.headers);
    // The whole point of this process.
    headers.insert(
        "Salad-Api-Key",
        match st.api_key.parse() {
            Ok(v) => v,
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "malformed API key").into_response();
            }
        },
    );

    let upstream = st
        .http
        .request(parts.method, &url)
        .headers(headers)
        .body(bytes)
        .send()
        .await;

    let resp = match upstream {
        Ok(r) => r,
        // 502 rather than a panic: the run can end, or the node can cycle,
        // while a browser tab is still open and polling.
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("upstream: {e}")).into_response(),
    };

    let status = resp.status();
    let mut out = Response::builder().status(status);
    for (k, v) in resp.headers() {
        if !HOP_BY_HOP.contains(&k.as_str().to_ascii_lowercase().as_str()) {
            out = out.header(k, v);
        }
    }
    // Stream, do not collect: an SSE body never ends on its own.
    let stream = resp
        .bytes_stream()
        .map(|c| c.map_err(std::io::Error::other));
    out.body(Body::from_stream(stream))
        .unwrap_or_else(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())
}

fn strip_hop_by_hop(src: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(src.len());
    for (k, v) in src {
        if !HOP_BY_HOP.contains(&k.as_str().to_ascii_lowercase().as_str()) {
            out.insert(k, v.clone());
        }
    }
    out
}

/// Where a browser should be pointed, given the local port.
#[must_use]
pub fn browser_url(local_port: u16) -> String {
    format!("http://127.0.0.1:{local_port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_by_hop_headers_are_dropped_case_insensitively() {
        let mut h = HeaderMap::new();
        h.insert("Connection", "keep-alive".parse().unwrap());
        h.insert("TE", "trailers".parse().unwrap());
        h.insert("accept", "text/event-stream".parse().unwrap());
        let out = strip_hop_by_hop(&h);
        assert!(
            out.get("connection").is_none(),
            "Connection must not cross the proxy"
        );
        assert!(out.get("te").is_none(), "TE must not cross the proxy");
        assert_eq!(
            out.get("accept").unwrap(),
            "text/event-stream",
            "end-to-end headers must survive, or SSE negotiation breaks"
        );
    }

    #[test]
    fn browser_url_is_loopback() {
        // If this ever renders anything but a loopback host, the command has
        // started publishing the port it exists to keep private.
        assert_eq!(
            browser_url(crate::cli::DEFAULT_TUNNEL_PORT),
            "http://127.0.0.1:7777"
        );
    }

    /// The listener itself, not the string that describes it: a `--bind` flag, or an
    /// `Ipv4Addr::UNSPECIFIED` typo, would publish the run's gateway to the whole LAN
    /// with the operator's API key attached to every forwarded request.
    #[tokio::test]
    async fn the_listener_is_bound_to_loopback() {
        let listener = bind_loopback(0).await.expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        assert!(
            addr.ip().is_loopback(),
            "bound {addr}, which is not loopback"
        );
    }

    /// A fake gateway that echoes what it received, so the proxy's own behaviour is
    /// visible: the key it attaches, the method and path it preserves, the headers it
    /// drops.
    async fn echo(req: Request) -> Response {
        let (parts, body) = req.into_parts();
        let seen: std::collections::BTreeMap<String, String> = parts
            .headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let body = axum::body::to_bytes(body, 1 << 20)
            .await
            .unwrap_or_default();
        axum::Json(serde_json::json!({
            "method": parts.method.as_str(),
            "uri": parts.uri.to_string(),
            "headers": seen,
            "body": String::from_utf8_lossy(&body),
        }))
        .into_response()
    }

    async fn upstream_echo() -> String {
        let app = axum::Router::new().fallback(echo);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    async fn serve_tunnel(upstream: String, api_key: &str) -> String {
        let state = Arc::new(Tunnel {
            upstream,
            api_key: api_key.to_string(),
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .unwrap(),
        });
        let listener = bind_loopback(0).await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(forward).with_state(state);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    /// The one thing this command exists to do: attach the key the caller cannot attach,
    /// and otherwise get out of the way.
    #[tokio::test]
    async fn a_forwarded_request_carries_the_key_and_keeps_its_method_path_and_query() {
        let upstream = upstream_echo().await;
        let local = serve_tunnel(upstream, "sk-secret").await;

        let resp = reqwest::Client::new()
            .post(format!("{local}/metrics?scale=log"))
            .header("Connection", "keep-alive")
            .header("accept", "text/event-stream")
            .body("ping")
            .send()
            .await
            .expect("through the tunnel");
        assert_eq!(resp.status(), 200);
        let seen: serde_json::Value = resp.json().await.expect("echo");

        assert_eq!(seen["headers"]["salad-api-key"], "sk-secret");
        assert_eq!(seen["method"], "POST");
        assert_eq!(seen["uri"], "/metrics?scale=log");
        assert_eq!(seen["body"], "ping");
        assert_eq!(
            seen["headers"]["accept"], "text/event-stream",
            "end-to-end headers must survive or SSE negotiation breaks"
        );
        assert!(
            seen["headers"].get("connection").is_none(),
            "a hop-by-hop header reached the gateway: {seen}"
        );
    }

    /// Request bodies are buffered here, on the operator's machine, so an unbounded one
    /// is an allocation someone else chooses. 413 beats OOM.
    #[tokio::test]
    async fn an_oversized_request_body_is_refused_rather_than_buffered() {
        let upstream = upstream_echo().await;
        let local = serve_tunnel(upstream, "sk-secret").await;

        let resp = reqwest::Client::new()
            .post(format!("{local}/upload"))
            .body(vec![b'x'; MAX_REQUEST_BODY + 1])
            .send()
            .await
            .expect("the tunnel answers rather than dying");
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
