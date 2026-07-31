// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration test for the `sf-agent` checkpoint watcher + restore (M4 hardening),
//! local — no Salad. A tiny in-process HTTP store stands in for presigned S3 URLs: PUT
//! saves the body at its path, GET serves it back (404 when absent), DELETE drops it.
//! The store can be told to reject metadata PUTs, which is how the tests reproduce a
//! node dying in the commit window.
//!
//! Two properties of the real thing the store deliberately reproduces:
//!
//! - **A presigned URL authorizes one verb.** Every URL in the spec carries a `kind`
//!   marker and the store refuses a mismatch, so code that reaches for the wrong list
//!   (restoring from `delete_urls`, sweeping `put_urls`) fails here. With one URL set
//!   shared by all three lists — as this harness began — every such bug is invisible.
//! - **Requests are recorded, not just their effects.** Deleting an absent key is a
//!   no-op, so "slot 1 was never swept" cannot be shown by looking at what the store
//!   holds afterwards; it needs the request log.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use saladfingers_agent::checkpoint::{self, CheckpointMeta, RestoredState};
use saladfingers_protocol::PROTOCOL_VERSION;
use saladfingers_protocol::job::{CheckpointSlot, CheckpointSpec, ControlUrls, JobSpec};
use tokio::sync::Notify;

/// Parts presigned per slot. Two is enough to exercise multi-part bookkeeping while
/// keeping the assertions readable; real runs use `DEFAULT_MAX_PARTS`.
const PARTS: u32 = 2;

#[derive(Clone)]
struct Store {
    map: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Every request that arrived, as `(method, path, query)` — including the ones that
    /// changed nothing. The query carries the `kind=` marker, so assertions can tell a
    /// DELETE issued through `delete_urls` from one aimed (and 403'd) at the wrong list.
    log: Arc<Mutex<Vec<(String, String, String)>>>,
    /// When set, metadata PUTs are rejected — the commit window a dying node falls into.
    fail_meta: Arc<AtomicBool>,
    /// Remaining number of metadata PUTs to STORE and then answer 500 — commits whose
    /// ACK was lost. The write happens; the writer cannot know it. Counted, so a test
    /// can outlast the commit retries: an idempotent re-PUT that succeeds RESOLVES a
    /// lost ACK, which is the point of the retry — reaching the uncertainty machinery
    /// takes losing every attempt.
    ack_lost_meta: Arc<std::sync::atomic::AtomicU32>,
    /// Remaining number of metadata GETs to answer 429 — a throttled storage endpoint.
    throttle_meta_gets: Arc<std::sync::atomic::AtomicU32>,
    /// When set, metadata GETs answer 403 with this body — an expired URL, a bucket
    /// policy, or a backend that answers 403 for absent keys, depending on the text.
    forbid_meta_gets_with: Arc<Mutex<Option<String>>>,
}

impl Store {
    fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.map.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    fn meta(&self) -> CheckpointMeta {
        let raw = self.map.lock().unwrap()["ckpt/meta.json"].clone();
        serde_json::from_slice(&raw).expect("metadata decodes")
    }

    /// `(path, query)` pairs a given method was aimed at, in arrival order.
    fn requests(&self, method: &str) -> Vec<(String, String)> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _, _)| m == method)
            .map(|(_, p, q)| (p.clone(), q.clone()))
            .collect()
    }

    fn clear_log(&self) {
        self.log.lock().unwrap().clear();
    }
}

/// Start a loopback PUT/GET/DELETE object store; return its base URL and the backing map.
async fn storage() -> (String, Store) {
    let store = Store {
        map: Arc::new(Mutex::new(HashMap::new())),
        log: Arc::new(Mutex::new(Vec::new())),
        fail_meta: Arc::new(AtomicBool::new(false)),
        ack_lost_meta: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        throttle_meta_gets: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        forbid_meta_gets_with: Arc::new(Mutex::new(None)),
    };
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
    RawQuery(query): RawQuery,
    body: Bytes,
) -> axum::response::Response {
    store.log.lock().unwrap().push((
        method.to_string(),
        path.clone(),
        query.clone().unwrap_or_default(),
    ));

    // A presigned URL authorizes exactly one verb; using it with another is a 403 from
    // any real S3 endpoint. `kind` is how this store knows which URL the caller picked,
    // since all three of a slot's lists otherwise address the same keys.
    let expected = match method {
        axum::http::Method::PUT => "put",
        axum::http::Method::GET => "get",
        axum::http::Method::DELETE => "delete",
        _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
    };
    let kind_of = |q: &str| {
        q.split('&')
            .find_map(|p| p.strip_prefix("kind="))
            .map(str::to_string)
    };
    if query.as_deref().and_then(kind_of).as_deref() != Some(expected) {
        return (
            StatusCode::FORBIDDEN,
            format!("{method} on a URL not signed for it (path {path}, query {query:?})"),
        )
            .into_response();
    }

    match method {
        axum::http::Method::PUT => {
            if path.ends_with("meta.json") && store.fail_meta.load(Ordering::SeqCst) {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            if path.ends_with("meta.json")
                && store
                    .ack_lost_meta
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                // The write lands; the response is lost. From the writer's side this is
                // indistinguishable from a commit that never happened.
                store.map.lock().unwrap().insert(path, body.to_vec());
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            store.map.lock().unwrap().insert(path, body.to_vec());
            StatusCode::OK.into_response()
        }
        axum::http::Method::GET => {
            if path.ends_with("meta.json")
                && let Some(body) = store.forbid_meta_gets_with.lock().unwrap().clone()
            {
                return (StatusCode::FORBIDDEN, body).into_response();
            }
            if path.ends_with("meta.json")
                && store
                    .throttle_meta_gets
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                return StatusCode::TOO_MANY_REQUESTS.into_response();
            }
            match store.map.lock().unwrap().get(&path) {
                Some(bytes) => (StatusCode::OK, bytes.clone()).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        // S3 semantics: deleting an absent key succeeds.
        _ => {
            store.map.lock().unwrap().remove(&path);
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

/// Probe + restore in one call — what `run.rs` does across the input download, for tests
/// that do not care about the split.
async fn probe_restore(http: &reqwest::Client, spec: &JobSpec) -> anyhow::Result<RestoredState> {
    let probed = checkpoint::probe(http, spec).await?;
    checkpoint::restore(http, spec, probed).await
}

/// A JobSpec whose only meaningful field is a checkpoint spec pointing at `base`, with
/// the checkpoint directory `dir` and a two-slot ring.
fn spec_with_checkpoint(base: &str, dir: &str) -> JobSpec {
    let dummy = format!("{base}/unused");
    let slots = (0..2)
        .map(|slot| {
            let urls = |kind: &str| -> Vec<String> {
                (0..PARTS)
                    .map(|k| format!("{base}/ckpt/slot{slot}/data.{k:03}?kind={kind}"))
                    .collect()
            };
            CheckpointSlot {
                put_urls: urls("put"),
                get_urls: urls("get"),
                delete_urls: urls("delete"),
            }
        })
        .collect();
    JobSpec {
        v: PROTOCOL_VERSION,
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
            slots,
            meta_put_url: format!("{base}/ckpt/meta.json?kind=put"),
            meta_get_url: format!("{base}/ckpt/meta.json?kind=get"),
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

/// Run the watcher over `dir` for one interval starting from `restored`, then stop it.
async fn watch_once(
    http: &reqwest::Client,
    base: &str,
    dir: &std::path::Path,
    restored: RestoredState,
) {
    let spec = spec_with_checkpoint(base, &dir.to_string_lossy());
    let stop = Arc::new(Notify::new());
    let dirty = Arc::new(AtomicBool::new(false));
    let handle = checkpoint::spawn_watcher(http.clone(), spec, restored, stop.clone(), dirty);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop.notify_one();
    handle.await.unwrap();
}

/// A checkpoint directory in the trainer's layout, so the watcher can read the step off it.
fn make_ckpt_dir(step: u64, payload: &[u8]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let inner = dir.path().join(format!("step_{step:08}"));
    std::fs::create_dir_all(inner.join("nested")).unwrap();
    std::fs::write(inner.join("step.txt"), format!("step={step}")).unwrap();
    std::fs::write(inner.join("nested/w.bin"), payload).unwrap();
    dir
}

#[tokio::test]
async fn checkpoint_watcher_uploads_and_restore_recovers_it() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let src = make_ckpt_dir(42, &[7u8; 4096]);

    // Restore first with an empty store → no-op (nothing uploaded yet).
    let fresh = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &fresh.path().to_string_lossy()),
    )
    .await
    .expect("restore with no checkpoint is a no-op");
    assert!(state.live.is_none(), "nothing remote → no live slot");
    assert!(!state.had_remote);
    assert!(!fresh.path().join("step_00000042").exists());

    // Run the watcher over the source dir; it should upload within an interval.
    watch_once(
        &http,
        &base,
        src.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;

    // The metadata + the first slot's leading part were stored, and the metadata says so.
    assert_eq!(
        store.keys(),
        vec!["ckpt/meta.json", "ckpt/slot0/data.000"],
        "a fresh run fills slot 0 and writes nothing else — no stray sweep of slot 1"
    );
    let meta = store.meta();
    assert_eq!(meta.v, PROTOCOL_VERSION);
    assert_eq!(meta.slot, 0);
    assert_eq!(meta.parts, 1);
    assert_eq!(meta.step, Some(42), "step read off the checkpoint layout");

    // Restore into a fresh directory and confirm the content round-trips exactly.
    let dst = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("restore recovers the checkpoint");
    assert_eq!(state.live, Some(0));
    assert!(state.had_remote);
    assert_eq!(
        std::fs::read(dst.path().join("step_00000042/step.txt")).unwrap(),
        b"step=42"
    );
    assert_eq!(
        std::fs::read(dst.path().join("step_00000042/nested/w.bin")).unwrap(),
        vec![7u8; 4096]
    );
}

#[tokio::test]
async fn next_upload_rotates_to_the_free_slot_and_reclaims_the_old_one() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    // Slot 0 already holds the committed checkpoint.
    let first = make_ckpt_dir(10, &[1u8; 4096]);
    watch_once(
        &http,
        &base,
        first.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;
    assert_eq!(store.meta().slot, 0);

    // A later checkpoint, uploaded by a node that restored slot 0.
    let second = make_ckpt_dir(20, &[2u8; 4096]);
    watch_once(
        &http,
        &base,
        second.path(),
        RestoredState {
            live: Some(0),
            had_remote: true,
        },
    )
    .await;

    let meta = store.meta();
    assert_eq!(meta.slot, 1, "the write must avoid the live slot");
    assert_eq!(meta.step, Some(20));
    assert_eq!(
        store.keys(),
        vec!["ckpt/meta.json", "ckpt/slot1/data.000"],
        "retention is one checkpoint: slot 0's parts are reclaimed after the commit"
    );
    // The sweep went through `delete_urls` — the `kind=` marker proves the list, and the
    // request log proves the width: a resumed run cannot trust any part count it never
    // observed, so it sweeps slot 0's full presigned range, absent keys included.
    assert_eq!(
        store.requests("DELETE"),
        vec![
            ("ckpt/slot0/data.000".to_string(), "kind=delete".to_string()),
            ("ckpt/slot0/data.001".to_string(), "kind=delete".to_string()),
        ],
        "a superseded slot of unknown extent must be swept in full, via its DELETE URLs"
    );

    // And the newest checkpoint is what restores.
    let dst = tempfile::tempdir().unwrap();
    probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("restore recovers the rotated checkpoint");
    assert!(dst.path().join("step_00000020").is_dir());
    assert!(
        !dst.path().join("step_00000010").exists(),
        "the superseded checkpoint is gone, not merged in"
    );
}

/// The regression this ring exists for: a node that dies between uploading the parts and
/// committing the metadata must leave the previous checkpoint restorable. Under the old
/// fixed-key scheme the new parts had already overwritten the old ones, so the surviving
/// metadata pointed at bytes whose sha256 no longer matched and the run restarted from
/// step 0.
#[tokio::test]
async fn interrupted_upload_leaves_the_previous_checkpoint_intact() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let first = make_ckpt_dir(10, &[1u8; 4096]);
    watch_once(
        &http,
        &base,
        first.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;
    let committed = store.meta();
    assert_eq!(committed.slot, 0);

    // Now the commit fails: parts land, metadata does not.
    store.fail_meta.store(true, Ordering::SeqCst);
    let second = make_ckpt_dir(20, &[2u8; 4096]);
    watch_once(
        &http,
        &base,
        second.path(),
        RestoredState {
            live: Some(0),
            had_remote: true,
        },
    )
    .await;
    store.fail_meta.store(false, Ordering::SeqCst);

    // The half-written checkpoint is visible in slot 1 but uncommitted; slot 0 and the
    // metadata are untouched.
    assert_eq!(
        store.keys(),
        vec![
            "ckpt/meta.json",
            "ckpt/slot0/data.000",
            "ckpt/slot1/data.000"
        ],
        "the interrupted upload occupies the free slot and nothing else"
    );
    let meta = store.meta();
    assert_eq!(meta.slot, 0, "the commit never happened");
    assert_eq!(meta.sha256, committed.sha256);
    assert_eq!(meta.step, Some(10));

    // A replacement node restores the *previous* checkpoint rather than restarting.
    let dst = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("the last good checkpoint survives an interrupted upload");
    assert_eq!(state.live, Some(0));
    assert_eq!(
        std::fs::read(dst.path().join("step_00000010/step.txt")).unwrap(),
        b"step=10"
    );

    // Its next upload takes the free slot again, overwriting the debris, and reclaims
    // slot 0 only once the new checkpoint is committed.
    let third = make_ckpt_dir(30, &[3u8; 4096]);
    watch_once(&http, &base, third.path(), state).await;
    assert_eq!(store.meta().slot, 1);
    assert_eq!(store.meta().step, Some(30));
    assert_eq!(store.keys(), vec!["ckpt/meta.json", "ckpt/slot1/data.000"]);
}

/// A first life has nothing to reclaim, and must not go looking. The assertion needs the
/// request log rather than the stored keys: deleting an absent key succeeds and leaves no
/// trace, so a ring that wrongly marked every slot "unknown" on a fresh run would issue a
/// full sweep of slot 1 — `PARTS` pointless round trips per interval, growing with
/// `max_parts` — and every key-set assertion in this file would still pass.
#[tokio::test]
async fn a_first_life_reclaims_nothing_because_there_is_nothing_to_reclaim() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let src = make_ckpt_dir(42, &[7u8; 4096]);
    watch_once(
        &http,
        &base,
        src.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;

    assert_eq!(store.meta().slot, 0);
    assert!(
        store.requests("DELETE").is_empty(),
        "a fresh run swept slots it never wrote: {:?}",
        store.requests("DELETE")
    );

    // A *resumed* run is the opposite case: an earlier incarnation may have died
    // mid-upload, leaving an unknown number of parts, so the first rotation sweeps the
    // whole range of the slot it supersedes rather than trusting a count it never saw.
    store.clear_log();
    let next = make_ckpt_dir(50, &[9u8; 4096]);
    watch_once(
        &http,
        &base,
        next.path(),
        RestoredState {
            live: Some(0),
            had_remote: true,
        },
    )
    .await;
    assert_eq!(
        store.requests("DELETE").len() as u32,
        PARTS,
        "a resumed run must sweep the superseded slot's full part range"
    );
}

/// The version bump's actual promise: a v1 object has no `slot` field at all, so decoding
/// straight into [`CheckpointMeta`] reports `missing field 'slot'` — which reads like a
/// corrupt object and sends the operator hunting storage rather than versions. The probe
/// exists to name the real cause, and only a genuinely v1-shaped object tests it.
#[tokio::test]
async fn v1_metadata_reports_a_version_mismatch_not_a_missing_field() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let v1 = serde_json::json!({
        "v": 1,
        "parts": 1,
        "bytes": 4096,
        "sha256": "0".repeat(64),
        "uploaded_at": "2026-07-01T00:00:00Z",
    });
    store
        .map
        .lock()
        .unwrap()
        .insert("ckpt/meta.json".into(), serde_json::to_vec(&v1).unwrap());

    let dst = tempfile::tempdir().unwrap();
    let err = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect_err("a v1 checkpoint must fail loudly, not half-decode");
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

/// A committed checkpoint with no parts still names a slot, and the ring has to respect
/// it. Reporting "fresh" here would reset the rotation to slot 0 — which is the slot the
/// metadata names — so the next upload would overwrite the live one.
#[tokio::test]
async fn an_empty_committed_checkpoint_still_names_its_live_slot() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let empty = CheckpointMeta {
        v: PROTOCOL_VERSION,
        slot: 0,
        parts: 0,
        bytes: 0,
        sha256: "0".repeat(64),
        step: None,
        uploaded_at: chrono::Utc::now(),
    };
    store
        .map
        .lock()
        .unwrap()
        .insert("ckpt/meta.json".into(), serde_json::to_vec(&empty).unwrap());

    let dst = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("an empty checkpoint is not an error");
    assert_eq!(
        state.live,
        Some(0),
        "the committed slot must stay live even when it holds nothing"
    );
    assert!(
        state.had_remote,
        "a metadata object existed, so earlier debris may too"
    );

    // And the watcher honours it: the next upload takes the other slot.
    let src = make_ckpt_dir(7, &[3u8; 4096]);
    watch_once(&http, &base, src.path(), state).await;
    assert_eq!(store.meta().slot, 1, "the write must avoid the live slot");
}

/// A lost ACK on FEWER than all commit attempts is resolved by the retry itself: the
/// re-PUT of identical bytes either overwrites the landed commit or lands it, and the
/// 200 it returns is the one the first attempt dropped. The ring stays in sync with no
/// uncertainty detour — proven by the request log: rotation continues with no metadata
/// re-GET.
#[tokio::test]
async fn a_lost_ack_is_resolved_by_the_idempotent_commit_retry() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let dir = make_ckpt_dir(10, &[1u8; 4096]);
    let spec = spec_with_checkpoint(&base, &dir.path().to_string_lossy());
    let stop = Arc::new(Notify::new());
    let handle = checkpoint::spawn_watcher(
        http.clone(),
        spec,
        RestoredState {
            live: None,
            had_remote: false,
        },
        stop.clone(),
        Arc::new(AtomicBool::new(false)),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(store.meta().slot, 0);
    store.clear_log();

    // One lost ACK; the second attempt's 200 comes through.
    store.ack_lost_meta.store(1, Ordering::SeqCst);
    let inner = dir.path().join("step_00000020");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("step.txt"), "step=20").unwrap();
    tokio::time::sleep(Duration::from_millis(2000)).await;
    stop.notify_one();
    handle.await.unwrap();

    assert_eq!(store.meta().slot, 1, "the retried commit landed normally");
    let meta_puts = store
        .requests("PUT")
        .into_iter()
        .filter(|(p, _)| p.ends_with("meta.json"))
        .count();
    assert_eq!(meta_puts, 2, "one lost ACK costs exactly one re-PUT");
    assert!(
        !store
            .requests("GET")
            .iter()
            .any(|(p, _)| p.ends_with("meta.json")),
        "a retry-resolved commit must not need the uncertainty re-read"
    );
}

/// A 403 whose BODY says nothing useful is still fatal when the URL's own
/// `X-Amz-Date` + `X-Amz-Expires` place it past its window: the server said no, and our
/// copy of the URL corroborates why. Without this, expiry phrased any way other than
/// "expired" reads as an absent checkpoint and the run silently retrains from zero.
#[tokio::test]
async fn an_expired_url_is_fatal_even_when_the_body_does_not_say_so() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    *store.forbid_meta_gets_with.lock().unwrap() =
        Some("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>".into());
    let dst = tempfile::tempdir().unwrap();
    let mut spec = spec_with_checkpoint(&base, &dst.path().to_string_lossy());
    spec.checkpoint.as_mut().unwrap().meta_get_url =
        format!("{base}/ckpt/meta.json?kind=get&X-Amz-Date=20200101T000000Z&X-Amz-Expires=60");
    let err = probe_restore(&http, &spec)
        .await
        .expect_err("an out-of-window URL must not read as an absent checkpoint");
    assert!(
        format!("{err:#}").contains("expired"),
        "the error should name the expiry: {err:#}"
    );
}

/// `SignatureDoesNotMatch` / `InvalidAccessKeyId` can never mean "absent" — the URL is
/// broken, most plausibly by rotated storage credentials. Starting fresh over that would
/// be a guess with the checkpoint as the stake.
#[tokio::test]
async fn a_rejected_signature_is_fatal_not_an_absent_checkpoint() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    *store.forbid_meta_gets_with.lock().unwrap() = Some(
        "<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we \
         calculated does not match</Message></Error>"
            .into(),
    );
    let dst = tempfile::tempdir().unwrap();
    let err = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect_err("a broken URL must not read as an absent checkpoint");
    assert!(
        format!("{err:#}").contains("SignatureDoesNotMatch"),
        "the error should carry the code: {err:#}"
    );
}

/// Storage throttling answers 429, and the repo's transfer policy calls that transient —
/// so must the metadata probe, now that its failure fails the whole run. The likeliest
/// moment for a 429 here is precisely N shards booting together and probing in the same
/// second; one throttled boot must cost a retry, not a relaunch cycle.
#[tokio::test]
async fn a_throttled_metadata_get_is_retried_not_fatal() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let src = make_ckpt_dir(10, &[1u8; 4096]);
    watch_once(
        &http,
        &base,
        src.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;

    // Two 429s, then the store answers normally — within the probe's three attempts.
    store
        .throttle_meta_gets
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let dst = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("a throttled probe must retry through, not fail the run");
    assert_eq!(state.live, Some(0));
    assert!(dst.path().join("step_00000010").is_dir());
}

/// A 403 whose body says the URL has EXPIRED is not "no checkpoint stored": the
/// checkpoint may be sitting there, unreachable to this run only. Starting fresh over it
/// is the silent-retrain-from-zero this module exists to prevent, so expiry is fatal.
#[tokio::test]
async fn an_expired_metadata_url_is_fatal_not_a_fresh_start() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    *store.forbid_meta_gets_with.lock().unwrap() = Some(
        "<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>".into(),
    );
    let dst = tempfile::tempdir().unwrap();
    let err = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect_err("an expired URL must not read as an absent checkpoint");
    assert!(
        format!("{err:#}").contains("expired"),
        "the error should name the expiry: {err:#}"
    );
}

/// Some backends answer 403 for a key that simply does not exist. That has to stay a
/// fresh start — made fatal, a first life on such a backend could never boot.
#[tokio::test]
async fn a_plain_403_still_reads_as_no_checkpoint_stored() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    *store.forbid_meta_gets_with.lock().unwrap() =
        Some("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>".into());
    let dst = tempfile::tempdir().unwrap();
    let state = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect("a backend that 403s absent keys must still allow a first life");
    assert!(state.live.is_none());
    assert!(!state.had_remote);
}

/// One slot is not a ring: rotation degenerates to overwrite-in-place and every
/// non-destructive promise this module makes silently stops holding. The spec is wire
/// input, so the shape is refused rather than trusted.
#[tokio::test]
async fn a_one_slot_ring_is_refused_before_it_can_overwrite_in_place() {
    let (base, _store) = storage().await;
    let http = reqwest::Client::new();

    let dst = tempfile::tempdir().unwrap();
    let mut spec = spec_with_checkpoint(&base, &dst.path().to_string_lossy());
    spec.checkpoint.as_mut().unwrap().slots.truncate(1);
    let err = checkpoint::probe(&http, &spec)
        .await
        .expect_err("a one-slot ring cannot keep the committed checkpoint safe");
    assert!(
        format!("{err:#}").contains("at least 2"),
        "unexpected error: {err:#}"
    );
}

/// A commit PUT whose response is lost may still have landed. Trusting the stale local
/// notion of the live slot would aim the NEXT upload at the slot storage now considers
/// committed — so after an unacknowledged commit, the agent re-reads the metadata and
/// rotates off whatever it actually says.
#[tokio::test]
async fn an_unacknowledged_commit_is_re_resolved_before_the_next_upload() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    // First checkpoint commits normally into slot 0.
    let dir = make_ckpt_dir(10, &[1u8; 4096]);
    let spec = spec_with_checkpoint(&base, &dir.path().to_string_lossy());
    let stop = Arc::new(Notify::new());
    let dirty = Arc::new(AtomicBool::new(false));
    let handle = checkpoint::spawn_watcher(
        http.clone(),
        spec,
        RestoredState {
            live: None,
            had_remote: false,
        },
        stop.clone(),
        dirty,
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(store.meta().slot, 0, "first commit lands in slot 0");

    // Second checkpoint: the parts land in slot 1 and the commit LANDS TOO, but every
    // ACK is lost — including the retries', so even the idempotent re-PUT cannot learn
    // the truth and the watcher must fall back to uncertainty. (Losing FEWER than all
    // attempts is the case the commit retry solves by itself — its own test above.)
    store.clear_log();
    store.ack_lost_meta.store(3, Ordering::SeqCst);
    let inner = dir.path().join("step_00000020");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("step.txt"), "step=20").unwrap();
    // Budget covers the lost-ACK upload (interval + two retry backoffs) AND the next
    // interval's recovery upload — the thing under test.
    tokio::time::sleep(Duration::from_millis(6000)).await;
    stop.notify_one();
    handle.await.unwrap();

    assert_eq!(
        store.ack_lost_meta.load(Ordering::SeqCst),
        0,
        "all three commit attempts should have been made and lost"
    );
    // The recovery upload re-read the metadata (the uncertainty path's one GET), saw the
    // landed-but-unacknowledged commit naming slot 1, and rotated onto slot 0 — a watcher
    // trusting its stale `live = 0` would have written slot 1, the committed one.
    assert!(
        store
            .requests("GET")
            .iter()
            .any(|(p, _)| p.ends_with("meta.json")),
        "the recovery upload must re-read the metadata before choosing a slot"
    );
    let recovery_data_puts: Vec<String> = store
        .requests("PUT")
        .into_iter()
        .filter(|(p, _)| p.contains("/data."))
        .map(|(p, _)| p)
        .collect();
    assert!(
        recovery_data_puts
            .last()
            .is_some_and(|p| p.contains("slot0")),
        "the recovery upload must rotate off the slot the metadata actually names: \
         {recovery_data_puts:?}"
    );
    let meta = store.meta();
    assert_eq!(meta.slot, 0, "the recovery commit names slot 0");
    assert_eq!(meta.step, Some(20), "and carries the current state");
}

/// After a commit whose part count the ring observed, reclamation deletes exactly those
/// keys — not the full presigned range. The request log is the only witness: deleting an
/// absent key is a no-op the stored keys cannot see.
#[tokio::test]
async fn reclaim_deletes_exactly_the_parts_it_knows_about() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    // One watcher life, two commits: slot 0 (1 part, count observed), then slot 1. The
    // reclaim of slot 0 happens with `known = Some(1)` and must issue exactly one DELETE
    // — data.001 was never written and is not swept.
    let dir = make_ckpt_dir(10, &[1u8; 4096]);
    let spec = spec_with_checkpoint(&base, &dir.path().to_string_lossy());
    let stop = Arc::new(Notify::new());
    let dirty = Arc::new(AtomicBool::new(false));
    let handle = checkpoint::spawn_watcher(
        http.clone(),
        spec,
        RestoredState {
            live: None,
            had_remote: false,
        },
        stop.clone(),
        dirty,
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(store.meta().slot, 0);
    assert!(
        store.requests("DELETE").is_empty(),
        "nothing to reclaim yet"
    );

    let inner = dir.path().join("step_00000020");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("step.txt"), "step=20").unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop.notify_one();
    handle.await.unwrap();

    assert_eq!(store.meta().slot, 1);
    assert_eq!(
        store.requests("DELETE"),
        vec![("ckpt/slot0/data.000".to_string(), "kind=delete".to_string())],
        "an observed one-part slot must cost exactly one DELETE"
    );
}

/// Metadata written by a different protocol version cannot be read back — and must not be
/// silently ignored either, because starting fresh would retrain from step 0 and then
/// overwrite the pointer to the checkpoint that is still there.
#[tokio::test]
async fn restore_refuses_metadata_from_another_protocol_version() {
    let (base, store) = storage().await;
    let http = reqwest::Client::new();

    let src = make_ckpt_dir(10, &[1u8; 4096]);
    watch_once(
        &http,
        &base,
        src.path(),
        RestoredState {
            live: None,
            had_remote: false,
        },
    )
    .await;

    let mut meta = store.meta();
    meta.v = PROTOCOL_VERSION + 1;
    store
        .map
        .lock()
        .unwrap()
        .insert("ckpt/meta.json".into(), serde_json::to_vec(&meta).unwrap());

    let dst = tempfile::tempdir().unwrap();
    let err = probe_restore(
        &http,
        &spec_with_checkpoint(&base, &dst.path().to_string_lossy()),
    )
    .await
    .expect_err("a version we cannot read must fail loudly");
    assert!(
        format!("{err:#}").contains("protocol"),
        "unexpected error: {err:#}"
    );
}
