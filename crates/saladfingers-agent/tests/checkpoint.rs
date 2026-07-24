// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration test for the `sf-agent` checkpoint watcher + restore (M4 hardening),
//! local — no Salad. A tiny in-process HTTP store stands in for presigned S3 URLs: PUT
//! saves the body at its path, GET serves it back (404 when absent).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use saladfingers_agent::checkpoint;
use saladfingers_protocol::job::{CheckpointSpec, ControlUrls, JobSpec};
use tokio::sync::Notify;

type Store = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Start a loopback PUT/GET object store; return its base URL and the backing map.
async fn storage() -> (String, Store) {
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/{*path}", any(handle))
        .with_state(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), store)
}

async fn handle(
    State(store): State<Store>,
    Path(path): Path<String>,
    method: axum::http::Method,
    body: Bytes,
) -> axum::response::Response {
    match method {
        axum::http::Method::PUT => {
            store.lock().unwrap().insert(path, body.to_vec());
            StatusCode::OK.into_response()
        }
        axum::http::Method::GET => match store.lock().unwrap().get(&path) {
            Some(bytes) => (StatusCode::OK, bytes.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

/// A JobSpec whose only meaningful field is a checkpoint spec pointing at `base`, with
/// the checkpoint directory `dir`.
fn spec_with_checkpoint(base: &str, dir: &str) -> JobSpec {
    let dummy = format!("{base}/unused");
    JobSpec {
        v: 1,
        run_id: "test".into(),
        shard_index: 0,
        shard_count: 1,
        command: vec![],
        workdir: None,
        env: Default::default(),
        stop_signal: None,
        max_duration_secs: None,
        max_attempts: None,
        inputs: vec![],
        outputs: vec![],
        checkpoint: Some(CheckpointSpec {
            glob: dir.to_string(),
            interval_secs: 1,
            quiesce_secs: 0, // always "settled" for the test
            put_urls: vec![
                format!("{base}/ckpt/data.000"),
                format!("{base}/ckpt/data.001"),
            ],
            meta_put_url: format!("{base}/ckpt/meta.json"),
            meta_get_url: format!("{base}/ckpt/meta.json"),
            get_urls: vec![
                format!("{base}/ckpt/data.000"),
                format!("{base}/ckpt/data.001"),
            ],
        }),
        bandwidth_gate: None,
        urls: ControlUrls {
            result_put: dummy.clone(),
            result_get: dummy.clone(),
            attempts_put: dummy.clone(),
            attempts_get: dummy.clone(),
            log_put: dummy,
        },
    }
}

#[tokio::test]
async fn checkpoint_watcher_uploads_and_restore_recovers_it() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    // A source checkpoint dir with content (simulating a job's saved state).
    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src.path().join("nested")).unwrap();
    std::fs::write(src.path().join("step.txt"), b"step=42").unwrap();
    std::fs::write(src.path().join("nested/w.bin"), vec![7u8; 4096]).unwrap();

    // Restore first with an empty store → no-op (nothing uploaded yet).
    let fresh = tempfile::tempdir().unwrap();
    checkpoint::restore(
        &http,
        &spec_with_checkpoint(&base, &fresh.path().to_string_lossy()),
    )
    .await
    .expect("restore with no checkpoint is a no-op");
    assert!(!fresh.path().join("step.txt").exists());

    // Run the watcher over the source dir; it should upload within an interval.
    let spec = spec_with_checkpoint(&base, &src.path().to_string_lossy());
    let stop = Arc::new(Notify::new());
    let dirty = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handle = checkpoint::spawn_watcher(http.clone(), spec, stop.clone(), dirty);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop.notify_one();
    handle.await.unwrap();

    // The metadata + at least one data part were stored.
    {
        let s = store.lock().unwrap();
        assert!(s.contains_key("ckpt/meta.json"), "meta uploaded");
        assert!(s.contains_key("ckpt/data.000"), "part uploaded");
    }

    // Restore into a fresh directory and confirm the content round-trips exactly.
    let dst = tempfile::tempdir().unwrap();
    checkpoint::restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("restore recovers the checkpoint");
    assert_eq!(
        std::fs::read(dst.path().join("step.txt")).unwrap(),
        b"step=42"
    );
    assert_eq!(
        std::fs::read(dst.path().join("nested/w.bin")).unwrap(),
        vec![7u8; 4096]
    );
}
