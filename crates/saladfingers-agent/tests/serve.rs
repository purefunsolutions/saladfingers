// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration tests for `sf-agent serve` (M5 session mode), all local — no Salad.
//!
//! The HTTP API is exercised in-process against `serve::app` on an ephemeral port; the
//! deadman self-exit is exercised against the real binary since it drives process exit.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use saladfingers_agent::serve::{AppState, app};
use saladfingers_protocol::agent_api::{
    ExecCreated, ExecRequest, ExecStatus, Health, OutputPage, UploadInit, UploadInitResponse,
    UploadStatus,
};
use sha2::{Digest, Sha256};

/// Serve `app` on a loopback ephemeral port; return its address and a client.
async fn start(token: Option<String>, workdir: PathBuf) -> (SocketAddr, reqwest::Client) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new("test-run".to_string(), token, workdir, None, None);
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    (addr, reqwest::Client::new())
}

fn exec(argv: &[&str]) -> ExecRequest {
    ExecRequest {
        argv: argv.iter().map(|s| (*s).to_string()).collect(),
        workdir: None,
        env: None,
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn healthz_is_open_but_exec_needs_the_bearer_token() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, client) = start(Some("s3cr3t".to_string()), dir.path().to_path_buf()).await;

    // healthz needs no auth.
    let health: Health = client
        .get(format!("http://{addr}/v1/healthz"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health.run_id, "test-run");
    assert_eq!(health.execs_running, 0);

    // exec without a token → 401.
    let unauth = client
        .post(format!("http://{addr}/v1/exec"))
        .json(&exec(&["true"]))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    // exec with the wrong token → 401.
    let wrong = client
        .post(format!("http://{addr}/v1/exec"))
        .bearer_auth("nope")
        .json(&exec(&["true"]))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);

    // exec with the right token → 201.
    let ok = client
        .post(format!("http://{addr}/v1/exec"))
        .bearer_auth("s3cr3t")
        .json(&exec(&["true"]))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 201);
}

#[tokio::test]
async fn exec_streams_merged_output_and_propagates_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, client) = start(None, dir.path().to_path_buf()).await;

    let created: ExecCreated = client
        .post(format!("http://{addr}/v1/exec"))
        .json(&exec(&["sh", "-c", "echo out; echo err 1>&2; exit 7"]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let mut cursor = 0u64;
    let mut merged = Vec::new();
    let mut exit_code = None;
    for _ in 0..40 {
        let page: OutputPage = client
            .get(format!(
                "http://{addr}/v1/exec/{}/output?cursor={cursor}&wait_ms=500",
                created.exec_id
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        for chunk in &page.chunks {
            merged.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(&chunk.data_b64)
                    .unwrap(),
            );
        }
        cursor = page.next_cursor;
        if page.exited {
            exit_code = page.exit_code;
            break;
        }
    }
    let text = String::from_utf8_lossy(&merged);
    assert!(text.contains("out"), "stdout captured: {text:?}");
    assert!(text.contains("err"), "stderr captured: {text:?}");
    assert_eq!(exit_code, Some(7));

    // Final status agrees.
    let status: ExecStatus = client
        .get(format!("http://{addr}/v1/exec/{}", created.exec_id))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!status.running);
    assert_eq!(status.exit_code, Some(7));
}

#[tokio::test]
async fn concurrent_exec_limit_returns_409() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, client) = start(None, dir.path().to_path_buf()).await;
    // Fill all four slots with long sleepers.
    for _ in 0..4 {
        let r = client
            .post(format!("http://{addr}/v1/exec"))
            .json(&exec(&["sleep", "30"]))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
    }
    let fifth = client
        .post(format!("http://{addr}/v1/exec"))
        .json(&exec(&["sleep", "30"]))
        .send()
        .await
        .unwrap();
    assert_eq!(fifth.status(), 409);
}

#[tokio::test]
async fn chunked_upload_resumes_and_verifies_then_downloads() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, client) = start(None, dir.path().to_path_buf()).await;

    // 100 KB of pseudo-varied data over 32 KiB chunks.
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    let chunk_bytes = 32 * 1024u64;
    let dest = dir.path().join("sub/out.bin");
    let init: UploadInitResponse = client
        .post(format!("http://{addr}/v1/files/upload"))
        .json(&UploadInit {
            path: dest.to_string_lossy().into_owned(),
            size: data.len() as u64,
            sha256: sha256_hex(&data),
            chunk_bytes,
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = init.upload_id;

    let chunks: Vec<&[u8]> = data.chunks(chunk_bytes as usize).collect();

    // Upload chunk 0 first, then confirm the server reports it (resume support).
    client
        .put(format!("http://{addr}/v1/files/upload/{id}/0"))
        .body(chunks[0].to_vec())
        .send()
        .await
        .unwrap();
    let status: UploadStatus = client
        .get(format!("http://{addr}/v1/files/upload/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status.received, vec![0]);

    // Upload the rest, out of order.
    for index in (1..chunks.len()).rev() {
        client
            .put(format!("http://{addr}/v1/files/upload/{id}/{index}"))
            .body(chunks[index].to_vec())
            .send()
            .await
            .unwrap();
    }

    // Complete verifies the sha256 and atomically renames into place.
    let complete = client
        .post(format!("http://{addr}/v1/files/upload/{id}/complete"))
        .send()
        .await
        .unwrap();
    assert_eq!(complete.status(), 200);
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // Ranged download of the middle 1000 bytes matches.
    let body = client
        .get(format!(
            "http://{addr}/v1/files/download?path={}&offset=1000&len=1000",
            urlencoding(&dest.to_string_lossy())
        ))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&body[..], &data[1000..2000]);
}

#[tokio::test]
async fn upload_complete_rejects_a_bad_sha256() {
    let dir = tempfile::tempdir().unwrap();
    let (addr, client) = start(None, dir.path().to_path_buf()).await;
    let dest = dir.path().join("bad.bin");
    let init: UploadInitResponse = client
        .post(format!("http://{addr}/v1/files/upload"))
        .json(&UploadInit {
            path: dest.to_string_lossy().into_owned(),
            size: 4,
            sha256: "00".repeat(32),
            chunk_bytes: 32 * 1024,
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    client
        .put(format!(
            "http://{addr}/v1/files/upload/{}/0",
            init.upload_id
        ))
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();
    let complete = client
        .post(format!(
            "http://{addr}/v1/files/upload/{}/complete",
            init.upload_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(complete.status(), 400);
    assert!(!dest.exists());
}

/// The deadman must self-exit an idle box. Exercises the real binary end to end.
#[tokio::test]
async fn deadman_self_exits_the_idle_agent() {
    // Reserve an ephemeral port, then hand it to the child.
    let port = {
        let l = std::net::TcpListener::bind("[::1]:0")
            .or_else(|_| std::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        l.local_addr().unwrap().port()
    };
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sf-agent"))
        .arg("serve")
        .env("SF_PORT", port.to_string())
        .env("SF_AGENT_TOKEN", "x")
        .env("SF_DEADMAN_SECS", "1")
        .env("SF_WORKDIR", "/tmp")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Idle: no authenticated requests → deadman fires within a few timer ticks.
    let status = tokio::time::timeout(Duration::from_secs(12), child.wait())
        .await
        .expect("agent did not self-exit before the timeout")
        .unwrap();
    assert!(status.success(), "expected clean exit, got {status:?}");
}

/// Minimal percent-encoding for a filesystem path in a query string.
fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
