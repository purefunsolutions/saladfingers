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
//!
//! **Redirects are never followed, and the browser is kept on the tunnel.** Two
//! halves of one rule. Following a 3xx here would carry `Salad-Api-Key` to
//! whatever host it names — reqwest strips only the standard sensitive headers
//! on a cross-host redirect, never a custom one — so the key this command exists
//! to keep local would land on an arbitrary server. But simply handing the 3xx
//! back is not enough either: the app sees the *gateway* as its `Host` and so
//! builds absolute redirects naming it, and a browser sent there leaves the
//! tunnel and meets the edge's 403, since a navigation cannot carry the key.
//! So an upstream URL pointing at the gateway is rewritten back onto this
//! process's own origin, and anything naming another host is passed through
//! untouched — not ours to serve.

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
    /// This process's own origin (`http://127.0.0.1:<bound port>`), taken from the
    /// listener rather than the flag so an ephemeral `:0` bind resolves to the real port.
    /// An upstream URL naming the gateway is rewritten onto this — see
    /// [`rewrite_upstream_url`].
    local_base: String,
    api_key: String,
    http: reqwest::Client,
}

/// The tunnel's HTTP client.
///
/// One constructor, deliberately shared with the tests: a test that builds its own client
/// pins the *test's* policy, so the redirect guard below could be deleted from the shipped
/// path with the suite still green. Everything security-relevant about this client has to
/// live where both callers get it.
///
/// No *total* timeout: SSE responses are long-lived by design, and the gateway's own 100 s
/// cap already bounds them. A connect timeout is a different thing and is kept, matching
/// every other client in this workspace.
///
/// **Never follow a redirect.** `Salad-Api-Key` is a custom header, and reqwest strips only
/// the standard sensitive ones (`AUTHORIZATION`, `COOKIE`, `PROXY_AUTHORIZATION`, …) when a
/// redirect crosses hosts — a custom header travels verbatim. With reqwest's default policy
/// (follow up to 10), one 3xx from an app behind the gateway, or an app-level open redirect,
/// would hand the operator's account-wide SaladCloud key to whatever host it named. Same
/// rule, for the same reason, as `sf-agent`'s in-container proxy.
///
/// # Errors
/// Returns an error if the client cannot be built.
fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building the tunnel's HTTP client")
}

/// Rewrite an absolute URL that names the gateway so it names this tunnel instead.
///
/// The browser's `host` is stripped on the way up, so reqwest sets the gateway's — which
/// means the app behind it builds absolute URLs from the gateway's name. A `Location`
/// carrying one, handed to a browser unchanged, sends that browser off the tunnel and
/// straight at the edge, which answers **403** because a navigation cannot carry the key.
/// That is precisely the failure this command exists to remove, so the redirect is aimed
/// back here instead.
///
/// Returns `None` — meaning "pass through untouched" — for any URL that is not the
/// gateway's. A URL on another host is not ours to serve, and pointing it at loopback
/// would silently fetch some third party's path from the gateway.
///
/// The match is anchored at a component boundary: `https://gw.example.com` must not match
/// `https://gw.example.com.attacker.test/`, which a bare `starts_with` would accept.
fn rewrite_upstream_url(value: &str, upstream: &str, local_base: &str) -> Option<String> {
    let rest = value.strip_prefix(upstream)?;
    (rest.is_empty() || rest.starts_with(['/', '?', '#'])).then(|| format!("{local_base}{rest}"))
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

    // Bind before building the state: the tunnel has to know its own origin to rewrite
    // the gateway out of upstream redirects, and only the listener knows the real port
    // once an ephemeral bind is in play.
    let listener = bind_loopback(args.local_port).await?;
    let local_base = browser_url(
        listener
            .local_addr()
            .context("reading the tunnel's own bound address")?
            .port(),
    );

    let state = Arc::new(Tunnel {
        upstream,
        api_key: cfg.api_key.expose().to_string(),
        http: http_client()?,
        local_base,
    });

    eprintln!(
        "tunnel: {} -> {} (shard {want})\n  \
         the gateway is authenticated; this process holds the key. Ctrl-C to stop.",
        state.local_base, state.upstream
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
        if HOP_BY_HOP.contains(&k.as_str().to_ascii_lowercase().as_str()) {
            continue;
        }
        // The two headers a browser will *navigate* to. Any other value passes through
        // byte-for-byte — this proxy rewrites destinations, not content.
        if matches!(k.as_str(), "location" | "content-location")
            && let Ok(text) = v.to_str()
            && let Some(rewritten) = rewrite_upstream_url(text, &st.upstream, &st.local_base)
        {
            out = out.header(k, rewritten);
            continue;
        }
        out = out.header(k, v);
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
        // Bind first, exactly as production does: the tunnel needs its own origin before
        // it can rewrite the gateway out of an upstream redirect.
        let listener = bind_loopback(0).await.unwrap();
        let base = browser_url(listener.local_addr().unwrap().port());
        let state = Arc::new(Tunnel {
            upstream,
            api_key: api_key.to_string(),
            // The production constructor, NOT a copy of its settings. A copy pins this
            // helper's policy, which would let the redirect guard be deleted from
            // `http_client` with every test here still green.
            http: http_client().unwrap(),
            local_base: base.clone(),
        });
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

    /// A server that counts what reaches it, so "the key never got there" is observable
    /// rather than inferred. Returns `(base_url, hit_counter)`.
    async fn counting_echo() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let route_hits = Arc::clone(&hits);
        let app = axum::Router::new().fallback(move |req: Request| {
            let hits = Arc::clone(&route_hits);
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                echo(req).await
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, hits)
    }

    /// A gateway that redirects `/go` to `location` and echoes everything else.
    async fn redirecting_gateway(location: String) -> String {
        let app = axum::Router::new().fallback(move |req: Request| {
            // Per-call clone keeps the handler `Fn`: an `async move` would consume the
            // captured `String` on the first request and make the closure `FnOnce`.
            let location = location.clone();
            async move {
                if req.uri().path() == "/go" {
                    // 302, not 307: the status a login flow actually emits, and the one
                    // where a followed redirect would also rewrite the method to GET.
                    return (
                        StatusCode::FOUND,
                        [(axum::http::header::LOCATION, location)],
                    )
                        .into_response();
                }
                echo(req).await
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    /// A browser that reports a 3xx instead of resolving it, so the tunnel's own answer
    /// is what the assertions see.
    fn non_following_browser() -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }

    /// The security property: an upstream 3xx is never chased, so `Salad-Api-Key` — a
    /// custom header reqwest does NOT strip across hosts — cannot reach the host the
    /// redirect names. Proven by the redirect target recording zero requests, which also
    /// catches the case where the tunnel forwards the key *and* returns the 3xx.
    #[tokio::test]
    async fn an_upstream_redirect_to_another_host_is_never_followed() {
        let (elsewhere, elsewhere_hits) = counting_echo().await;
        let gateway = redirecting_gateway(format!("{elsewhere}/landed")).await;
        let local = serve_tunnel(gateway, "sk-secret").await;

        let resp = non_following_browser()
            .get(format!("{local}/go"))
            .send()
            .await
            .expect("through the tunnel");

        assert_eq!(
            resp.status(),
            StatusCode::FOUND,
            "the tunnel must hand the 3xx back, not resolve it to the target's 200"
        );
        assert_eq!(
            elsewhere_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the tunnel followed the redirect — the operator's API key reached another host"
        );
        // A foreign host is not ours to serve: the Location is passed through as-is, so
        // the browser decides, and nothing is silently fetched from the gateway instead.
        assert_eq!(
            resp.headers().get("location").and_then(|v| v.to_str().ok()),
            Some(format!("{elsewhere}/landed").as_str()),
            "a Location on another host must pass through unrewritten"
        );
    }

    /// The usability half of the same rule. The app sees the gateway as its `Host`, so it
    /// emits absolute redirects naming the gateway; handed to a browser unchanged, those
    /// walk it off the tunnel and into the edge's 403 — the exact failure this command
    /// exists to remove. They come back pointing at the tunnel instead.
    #[tokio::test]
    async fn a_redirect_naming_the_gateway_comes_back_pointing_at_the_tunnel() {
        // One server that redirects to its OWN origin — what a framework does when it
        // builds an absolute URL from the `Host` it was given.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let self_url = upstream.clone();
        let app = axum::Router::new().fallback(move |req: Request| {
            let self_url = self_url.clone();
            async move {
                if req.uri().path() == "/go" {
                    return (
                        StatusCode::FOUND,
                        [(
                            axum::http::header::LOCATION,
                            format!("{self_url}/dashboard?tab=1"),
                        )],
                    )
                        .into_response();
                }
                echo(req).await
            }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let local = serve_tunnel(upstream, "sk-secret").await;
        let resp = non_following_browser()
            .get(format!("{local}/go"))
            .send()
            .await
            .expect("through the tunnel");

        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(
            resp.headers().get("location").and_then(|v| v.to_str().ok()),
            Some(format!("{local}/dashboard?tab=1").as_str()),
            "a Location naming the gateway must come back on the tunnel's own origin, \
             query intact — otherwise the browser leaves the tunnel and gets a 403"
        );
    }

    /// The rewrite is anchored at a component boundary. A gateway DNS name is a prefix of
    /// infinitely many other hostnames, and a bare `starts_with` would aim the browser at
    /// the tunnel for a URL belonging to someone else entirely.
    #[test]
    fn only_the_gateways_own_origin_is_rewritten() {
        let up = "https://gw.example.com";
        let local = "http://127.0.0.1:7777";
        assert_eq!(
            rewrite_upstream_url("https://gw.example.com/a?b#c", up, local).as_deref(),
            Some("http://127.0.0.1:7777/a?b#c")
        );
        assert_eq!(
            rewrite_upstream_url("https://gw.example.com", up, local).as_deref(),
            Some("http://127.0.0.1:7777")
        );
        // Not the gateway: a look-alike host, a different scheme, and a relative URL the
        // browser already resolves against the tunnel on its own.
        for foreign in [
            "https://gw.example.com.attacker.test/steal",
            "https://gw.example.comX/y",
            "http://gw.example.com/a",
            "/already-relative",
        ] {
            assert_eq!(
                rewrite_upstream_url(foreign, up, local),
                None,
                "{foreign} must pass through untouched"
            );
        }
    }
}
