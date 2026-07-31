// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration tests for `saladfingers checkpoint show|fetch` against a loopback object
//! store. No Salad, no credentials — an `S3Backend` presigns against a local axum server
//! that stores what it is given.
//!
//! What makes these worth having: the CLI re-derives the agent's storage keys from
//! scratch, hours or days after the run that wrote them, from a metadata object written
//! by an untrusted node. The keys have to match a producer in another crate, the version
//! has to be checked before the decode, and the part count has to be bounded before it
//! reaches URL generation. Each of those is invisible to a unit test of either side.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as AxPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use saladfingers_cli::checkpoint::{fetch_into, resolve};
use saladfingers_cli::presign::S3Backend;
use saladfingers_cli::spec;
use saladfingers_protocol::job::CheckpointMeta;
use saladfingers_protocol::{PROTOCOL_VERSION, transfer};

const RUN: &str = "sf-x7k2mq";
const EXPIRY: Duration = Duration::from_secs(300);

type Store = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// A loopback store addressed exactly as the CLI addresses the real one: path-style, so a
/// presigned URL for key `runs/…` lands at `/b/runs/…`. The bucket segment is stripped
/// here so the map is keyed by storage key.
async fn storage() -> (String, Store) {
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/{*path}", any(handle))
        .with_state(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), store)
}

async fn handle(
    State(store): State<Store>,
    AxPath(path): AxPath<String>,
    method: axum::http::Method,
    body: Bytes,
) -> axum::response::Response {
    let key = path.strip_prefix("b/").unwrap_or(&path).to_string();
    match method {
        axum::http::Method::PUT => {
            store.lock().unwrap().insert(key, body.to_vec());
            StatusCode::OK.into_response()
        }
        axum::http::Method::GET => match store.lock().unwrap().get(&key) {
            Some(bytes) => (StatusCode::OK, bytes.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn backend(base: &str) -> S3Backend {
    S3Backend::new(base, "auto", "b", true, "AKID", "SECRET").expect("backend")
}

/// A checkpoint directory in the trainer's layout.
fn ckpt_dir(step: u64, payload: &[u8]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let inner = dir.path().join(format!("step_{step:08}"));
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("weights.bin"), payload).unwrap();
    dir
}

/// Upload `dir` into `slot` the way the agent does — through presigned PUTs, over the
/// shared key helper — and return the metadata that commits it.
async fn upload_slot(
    http: &reqwest::Client,
    backend: &S3Backend,
    shard: u32,
    slot: u32,
    dir: &Path,
    step: u64,
) -> CheckpointMeta {
    let stem = spec::ckpt_slot_stem(&spec::shard_prefix(RUN, shard), slot);
    let put_urls: Vec<String> = (0..4)
        .map(|k| backend.presign_put(&transfer::part_key(&stem, k), EXPIRY))
        .collect();
    let report = transfer::upload_artifact(http, dir, true, &put_urls, "checkpoint")
        .await
        .expect("upload");
    CheckpointMeta {
        v: PROTOCOL_VERSION,
        slot,
        parts: report.parts,
        bytes: report.bytes,
        sha256: report.sha256,
        step: Some(step),
        uploaded_at: chrono::Utc::now(),
    }
}

/// Commit `meta` as the shard's checkpoint metadata.
fn commit(store: &Store, shard: u32, meta: &serde_json::Value) {
    let key = spec::ckpt_meta_key(&spec::shard_prefix(RUN, shard));
    store
        .lock()
        .unwrap()
        .insert(key, serde_json::to_vec(meta).unwrap());
}

#[tokio::test]
async fn resolve_returns_the_metadata_the_agent_committed() {
    let (base, store) = storage().await;
    let backend = backend(&base);
    let http = reqwest::Client::new();

    let src = ckpt_dir(21_000, &[4u8; 8192]);
    let meta = upload_slot(&http, &backend, 0, 1, src.path(), 21_000).await;
    commit(&store, 0, &serde_json::to_value(&meta).unwrap());

    let got = resolve(&http, &backend, RUN, 0).await.expect("resolve");
    assert_eq!(got.step, Some(21_000));
    assert_eq!(got.slot, 1);
    assert_eq!(got.sha256, meta.sha256);
    assert_eq!(got.parts, meta.parts);
}

/// The failure an operator actually hits — a run that never checkpointed, or a `gc` that
/// already reaped it — must read as "there is no checkpoint", not as a bare HTTP status
/// from a URL they never saw.
#[tokio::test]
async fn a_missing_checkpoint_is_named_rather_than_rendered_as_a_status_line() {
    let (base, _store) = storage().await;
    let http = reqwest::Client::new();

    let err = resolve(&http, &backend(&base), RUN, 3)
        .await
        .expect_err("an empty store holds no checkpoint");
    let text = format!("{err:#}");
    assert!(
        text.contains("no checkpoint for run 'sf-x7k2mq' shard 3"),
        "unexpected error: {text}"
    );
    assert!(
        !text.contains("X-Amz-Signature"),
        "a presigned URL leaked into the error: {text}"
    );
}

/// A v1 object has no `slot` field, so decoding it directly reports `missing field
/// 'slot'` — which reads like corruption and sends the operator to the wrong problem.
/// The probe exists to say which side is out of date instead.
#[tokio::test]
async fn v1_metadata_reports_a_version_mismatch_not_a_missing_field() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    commit(
        &store,
        0,
        &serde_json::json!({
            "v": 1, "parts": 1, "bytes": 8192,
            "sha256": "0".repeat(64), "uploaded_at": "2026-07-01T00:00:00Z",
        }),
    );

    let err = resolve(&http, &backend(&base), RUN, 0)
        .await
        .expect_err("a v1 checkpoint is not readable by this CLI");
    let text = format!("{err:#}");
    assert!(
        text.contains("protocol v1") && text.contains("v2"),
        "the error must name both versions: {text}"
    );
    assert!(
        !text.contains("missing field"),
        "the version probe should have caught this first: {text}"
    );
}

/// The whole reason `checkpoint fetch` exists: after a rotation the live slot is not
/// guessable, so the metadata is the only thing that knows where the bytes are. Both
/// slots hold a complete, *different* checkpoint here — reading the wrong one returns
/// plausible data from an older step rather than an error.
#[tokio::test]
async fn fetch_downloads_the_slot_the_metadata_names_byte_for_byte() {
    let (base, store) = storage().await;
    let backend = backend(&base);
    let http = reqwest::Client::new();

    let old = ckpt_dir(10_000, &[1u8; 8192]);
    upload_slot(&http, &backend, 0, 0, old.path(), 10_000).await;
    let new = ckpt_dir(21_000, &[2u8; 8192]);
    let meta = upload_slot(&http, &backend, 0, 1, new.path(), 21_000).await;
    commit(&store, 0, &serde_json::to_value(&meta).unwrap());

    let dest = tempfile::tempdir().unwrap();
    let got = fetch_into(&http, &backend, RUN, 0, dest.path())
        .await
        .expect("fetch");
    assert_eq!(got.slot, 1);
    assert_eq!(
        std::fs::read(dest.path().join("step_00021000/weights.bin")).unwrap(),
        vec![2u8; 8192],
        "fetched the bytes of a different slot"
    );
    assert!(
        !dest.path().join("step_00010000").exists(),
        "the superseded slot must not be what fetch returns"
    );
}

/// The metadata and the parts are written by a node that may have died between them. A
/// mismatch has to stop before extraction, or `fetch` quietly produces a directory that
/// looks like a checkpoint and is not one.
#[tokio::test]
async fn fetch_fails_the_checksum_before_extracting_anything() {
    let (base, store) = storage().await;
    let backend = backend(&base);
    let http = reqwest::Client::new();

    let src = ckpt_dir(21_000, &[5u8; 8192]);
    let mut meta = upload_slot(&http, &backend, 0, 0, src.path(), 21_000).await;
    meta.sha256 = "1".repeat(64);
    commit(&store, 0, &serde_json::to_value(&meta).unwrap());

    let dest = tempfile::tempdir().unwrap();
    let err = fetch_into(&http, &backend, RUN, 0, dest.path())
        .await
        .expect_err("a checksum mismatch must fail the fetch");
    assert!(
        format!("{err:#}").contains("integrity"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        std::fs::read_dir(dest.path()).unwrap().count(),
        0,
        "a failed fetch left a half-written checkpoint behind"
    );
}

/// `parts` arrives from the node, which is untrusted, and drives `(0..parts)` presigned-URL
/// generation. The envelope path already bounds its own part count for exactly this
/// reason; without the same bound here, a metadata object claiming billions of parts makes
/// the CLI sign billions of URLs — gigabytes of strings — before it issues one request.
#[tokio::test]
async fn a_parts_count_past_the_protocol_cap_is_refused_before_any_url_is_signed() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    commit(
        &store,
        0,
        &serde_json::json!({
            "v": PROTOCOL_VERSION,
            "slot": 0,
            "parts": spec::MAX_ARTIFACT_PARTS_LIMIT + 1,
            "bytes": 8192,
            "sha256": "0".repeat(64),
            "uploaded_at": "2026-07-01T00:00:00Z",
        }),
    );

    let dest = tempfile::tempdir().unwrap();
    let err = fetch_into(&http, &backend(&base), RUN, 0, dest.path())
        .await
        .expect_err("an absurd part count must be refused");
    let text = format!("{err:#}");
    assert!(
        text.contains("malformed") && text.contains(&spec::MAX_ARTIFACT_PARTS_LIMIT.to_string()),
        "the error should name the cap it exceeded: {text}"
    );

    // `show` is deliberately not bounded the same way: reading a broken checkpoint's own
    // account of itself is how an operator diagnoses it, and it signs nothing per part.
    resolve(&http, &backend(&base), RUN, 0)
        .await
        .expect("show must still display a checkpoint that fetch refuses");
}

/// `slot` picks the key stem, and an out-of-ring slot can only 404 — which reads as "the
/// checkpoint is gone", the same misdiagnosis the version probe exists to prevent. The
/// malformed field is named instead. And a sha256 that is not 64 hex characters can only
/// ever fail the integrity check, which reads as data corruption; that too is named at
/// the metadata, where the problem actually is.
#[tokio::test]
async fn malformed_slot_or_checksum_is_named_at_the_metadata_not_downstream() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();
    let dest = tempfile::tempdir().unwrap();

    commit(
        &store,
        0,
        &serde_json::json!({
            "v": PROTOCOL_VERSION, "slot": 7, "parts": 1, "bytes": 8192,
            "sha256": "0".repeat(64), "uploaded_at": "2026-07-01T00:00:00Z",
        }),
    );
    let err = fetch_into(&http, &backend(&base), RUN, 0, dest.path())
        .await
        .expect_err("an out-of-ring slot must be refused");
    assert!(
        format!("{err:#}").contains("slot 7"),
        "the error should name the slot: {err:#}"
    );

    commit(
        &store,
        0,
        &serde_json::json!({
            "v": PROTOCOL_VERSION, "slot": 0, "parts": 1, "bytes": 8192,
            "sha256": "not-a-checksum", "uploaded_at": "2026-07-01T00:00:00Z",
        }),
    );
    let err = fetch_into(&http, &backend(&base), RUN, 0, dest.path())
        .await
        .expect_err("a malformed checksum must be refused");
    assert!(
        format!("{err:#}").contains("malformed sha256"),
        "the error should blame the metadata, not the data: {err:#}"
    );
}
