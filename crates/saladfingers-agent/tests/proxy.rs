// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration test for `sf-agent serve --proxy` (M6 inference reverse proxy), local —
//! no Salad. A dummy app runs in-process; the real agent binary proxies to it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::routing::{get, post};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Start a tiny loopback app and return its port.
async fn dummy_app() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new()
        .route("/hello", get(|| async { "app-says-hello" }))
        .route("/echo", post(|body: String| async move { body }));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    port
}

/// Reserve an ephemeral IPv6-loopback port for the agent (it binds `[::]`).
fn free_agent_port() -> u16 {
    let l = std::net::TcpListener::bind("[::1]:0").unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test]
async fn proxy_forwards_requests_and_reports_ready_and_idle() {
    let app_port = dummy_app().await;
    let agent_port = free_agent_port();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sf-agent"))
        .arg("serve")
        .arg("--proxy")
        .arg("--app-port")
        .arg(app_port.to_string())
        .env("SF_PORT", agent_port.to_string())
        .env("SF_AGENT_TOKEN", "tok")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let base = format!("http://[::1]:{agent_port}");

    // Wait for the proxy to come up and see the app.
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(r) = client.get(format!("{base}/sf/v1/ready")).send().await
            && r.status().is_success()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "proxy never became ready");

    // Proxied GET reaches the app.
    let hello = client
        .get(format!("{base}/hello"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(hello, "app-says-hello");

    // Proxied POST forwards the body.
    let echo = client
        .post(format!("{base}/echo"))
        .body("ping-pong")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(echo, "ping-pong");

    // /sf/v1/idle is bearer-guarded.
    let unauth = client
        .get(format!("{base}/sf/v1/idle"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    let idle: serde_json::Value = client
        .get(format!("{base}/sf/v1/idle"))
        .bearer_auth("tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(idle["idle_secs"].is_number(), "idle report: {idle}");

    let _ = child.start_kill();
}

/// When the supervised app exits, the proxy tears itself down (billing stops).
#[tokio::test]
async fn proxy_exits_when_the_app_dies() {
    let agent_port = free_agent_port();
    // App command exits immediately.
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sf-agent"))
        .arg("serve")
        .arg("--proxy")
        .arg("--app-port")
        .arg("9")
        .env("SF_PORT", agent_port.to_string())
        .arg("--")
        .arg("true")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("proxy did not exit after the app died")
        .unwrap();
    assert!(status.success(), "expected clean exit, got {status:?}");
}

// ---------------------------------------------------------------------------
// Adversarial: request-target host injection (SSRF) against the reverse proxy.
//
// `proxy_handler` builds the upstream URL as `format!("{target}{path_q}")` where
// `target == "http://127.0.0.1:<app_port>"` and `path_q` comes from
// `parts.uri.path_and_query()`. If an untrusted client could make `path_q` start
// with `@evil:port/…`, `//evil:port/…`, or an absolute-form/authority that the
// handler doesn't strip, the formatted string could parse with a DIFFERENT host —
// e.g. `http://127.0.0.1:8080@evil:9999/` parses as host `evil:9999`, userinfo
// `127.0.0.1:8080` — turning the loopback-only client into an arbitrary-host
// fetch (SSRF) reachable from inside the container.
//
// We prove/refute this empirically: a legit APP on loopback (the proxy target),
// a separate ATTACKER listener on another loopback port that flips a hit-counter
// on ANY inbound TCP connection, and RAW HTTP/1.1 request bytes written straight
// to the proxy socket (reqwest/hyper would normalize the very targets under test).
// Security property: after every vector, the attacker counter is 0 and no proxy
// response body contained "ATTACKER".
// ---------------------------------------------------------------------------

/// Legit app: the proxy's fixed loopback target. Answers every path with "APP".
async fn app_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new().fallback(|| async { "APP" });
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    port
}

/// Attacker: a bare loopback listener that must NEVER be contacted by the proxy.
/// Bumps `hits` on every accepted TCP connection (the primary, most-sensitive
/// signal — it fires even if the upstream request never completes) and replies
/// with a body containing "ATTACKER" (the secondary signal, visible if the proxy
/// streams the response back). Returns its `127.0.0.1` port.
async fn attacker_server(hits: Arc<AtomicUsize>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                // Best-effort drain of the inbound request so the response is
                // delivered cleanly; the hit is already recorded regardless.
                let mut buf = [0u8; 1024];
                let _ = tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf)).await;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nATTACKER",
                    )
                    .await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

/// Write raw HTTP/1.1 bytes to the proxy over a fresh TCP connection and return
/// whatever it sends back (empty on connect failure / timeout / immediate close).
/// The proxy binds `[::]`, so we reach it over IPv6 loopback like the other tests.
async fn raw_request(agent_port: u16, raw: &str) -> String {
    let connect = tokio::net::TcpStream::connect(("::1", agent_port));
    let mut stream = match tokio::time::timeout(Duration::from_secs(3), connect).await {
        Ok(Ok(s)) => s,
        _ => return String::new(),
    };
    if tokio::time::timeout(Duration::from_secs(3), stream.write_all(raw.as_bytes()))
        .await
        .is_err()
    {
        return String::new();
    }
    let mut buf = Vec::new();
    // `Connection: close` makes the proxy close after the response, so read_to_end
    // returns promptly; the timeout is the backstop against a hung connection.
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// A malicious client cannot redirect the proxy's upstream fetch to any host other
/// than its fixed `http://127.0.0.1:<app_port>` target via a crafted request-target.
#[tokio::test]
async fn proxy_rejects_request_target_host_injection() {
    let app_port = app_server().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let attacker_port = attacker_server(hits.clone()).await;
    let agent_port = free_agent_port();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sf-agent"))
        .arg("serve")
        .arg("--proxy")
        .arg("--app-port")
        .arg(app_port.to_string())
        .env("SF_PORT", agent_port.to_string())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Wait for the proxy to come up and see the app.
    let client = reqwest::Client::new();
    let base = format!("http://[::1]:{agent_port}");
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(r) = client.get(format!("{base}/sf/v1/ready")).send().await
            && r.status().is_success()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "proxy never became ready");

    // Sanity: the raw-client → proxy → APP path actually works, so a later
    // zero attacker-hit result cannot be a false negative from "nothing worked".
    let sane = raw_request(
        agent_port,
        "GET /hello HTTP/1.1\r\nHost: proxy.local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        sane.contains("APP"),
        "sanity check failed — legit request never reached APP: {sane:?}"
    );

    let attacker = format!("127.0.0.1:{attacker_port}");
    // Each vector attempts to smuggle the attacker's host:port into the position
    // that would make the formatted upstream URL parse with a different authority.
    let vectors: Vec<(&str, String)> = vec![
        // Leading `@` (no slash): would form `http://127.0.0.1:<app>@127.0.0.1:<atk>/`
        // → host `127.0.0.1:<atk>`, userinfo `127.0.0.1:<app>`. THE critical vector.
        (
            "leading-at",
            format!("GET @{attacker}/ HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
        // Network-path reference `//host/path`.
        (
            "double-slash",
            format!("GET //{attacker}/path HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
        // Absolute-form request-target (legal for proxies; host must be dropped).
        (
            "absolute-form",
            format!("GET http://{attacker}/ HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
        // `@` after a leading slash (must stay in the path, not become userinfo).
        (
            "slash-at",
            format!("GET /@{attacker}/ HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
        // Backslash trick (url crate treats `\` as `/` for special schemes).
        (
            "slash-backslash-at",
            format!("GET /\\@{attacker}/ HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
        // Percent-encoded `@`.
        (
            "encoded-at",
            format!("GET /%40{attacker}/ HTTP/1.1\r\nHost: p\r\nConnection: close\r\n\r\n"),
        ),
    ];

    for (name, raw) in &vectors {
        let resp = raw_request(agent_port, raw).await;
        let first_line = resp.lines().next().unwrap_or("<no response / closed>");
        let verdict = if resp.contains("ATTACKER") {
            "REACHED-ATTACKER"
        } else if resp.contains("APP") {
            "routed-to-APP"
        } else if resp.is_empty() {
            "connection-closed / no-body"
        } else {
            "rejected-by-proxy (no APP/ATTACKER body)"
        };
        eprintln!("vector {name:<20} -> {verdict:<40} | status: {first_line}");
        assert!(
            !resp.contains("ATTACKER"),
            "vector `{name}` streamed the ATTACKER body back — host injection! response: {resp:?}"
        );
    }

    // The single authoritative security assertion: the attacker was never contacted.
    let count = hits.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "host-injection SSRF: the proxy opened {count} connection(s) to the attacker at 127.0.0.1:{attacker_port}"
    );

    let _ = child.start_kill();
}
