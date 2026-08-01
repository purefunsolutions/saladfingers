// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration tests for `sf-agent run` — spawn the real binary against a mock
//! job-spec / result server. No SaladCloud involved.

use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_result_endpoints(server: &MockServer) {
    // No prior envelope.
    Mock::given(method("GET"))
        .and(path("/result"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
    // Accept the envelope PUT.
    Mock::given(method("PUT"))
        .and(path("/result"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

fn job_spec(server_uri: &str, command: Vec<&str>) -> Value {
    json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION,
        "run_id": "sf-test01",
        "shard_index": 0,
        "shard_count": 1,
        "command": command,
        "workdir": "/tmp",
        "urls": {
            "result_put": format!("{server_uri}/result"),
            "result_get": format!("{server_uri}/result"),
            "attempts_put": format!("{server_uri}/attempts"),
            "attempts_get": format!("{server_uri}/attempts"),
            "log_put": format!("{server_uri}/log"),
        }
    })
}

async fn spawn_agent(job_url: &str) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_sf-agent"))
        .arg("run")
        .env("SF_JOB_URL", job_url)
        .env_remove("SALAD_MACHINE_ID")
        .kill_on_drop(true)
        .output()
        .await
        .expect("spawn sf-agent")
}

async fn last_envelope_put(server: &MockServer) -> Value {
    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .rev()
        .find(|r| r.method.as_str() == "PUT" && r.url.path() == "/result")
        .expect("an envelope PUT to /result was made");
    serde_json::from_slice(&put.body).expect("envelope is JSON")
}

#[tokio::test]
async fn run_succeeds_and_writes_envelope() {
    let server = MockServer::start().await;
    mount_result_endpoints(&server).await;
    Mock::given(method("GET"))
        .and(path("/job"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(job_spec(&server.uri(), vec!["sh", "-c", "exit 0"])),
        )
        .mount(&server)
        .await;

    let output = spawn_agent(&format!("{}/job", server.uri())).await;
    assert_eq!(output.status.code(), Some(0), "agent should exit 0");

    let env = last_envelope_put(&server).await;
    assert_eq!(env["status"], "succeeded");
    assert_eq!(env["exit_code"], 0);
    assert_eq!(env["run_id"], "sf-test01");
    assert_eq!(env["v"], saladfingers_protocol::PROTOCOL_VERSION);
}

#[tokio::test]
async fn run_propagates_failure_and_reports_failed() {
    let server = MockServer::start().await;
    mount_result_endpoints(&server).await;
    Mock::given(method("GET"))
        .and(path("/job"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(job_spec(&server.uri(), vec!["sh", "-c", "exit 3"])),
        )
        .mount(&server)
        .await;

    let output = spawn_agent(&format!("{}/job", server.uri())).await;
    assert_eq!(
        output.status.code(),
        Some(3),
        "agent should propagate the child's exit code"
    );

    let env = last_envelope_put(&server).await;
    assert_eq!(env["status"], "failed");
    assert_eq!(env["exit_code"], 3);
}

// A tiny in-memory object store: PUT stores, GET returns.
type Store = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>;

async fn storage_server() -> (String, Store) {
    use axum::body::Bytes;
    use axum::extract::{Path as AxPath, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::put;

    let store: Store = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let app = axum::Router::new()
        .route(
            "/{*key}",
            put(
                |AxPath(key): AxPath<String>, State(s): State<Store>, body: Bytes| async move {
                    s.lock().unwrap().insert(key, body.to_vec());
                    StatusCode::OK
                },
            )
            .get(
                |AxPath(key): AxPath<String>, State(s): State<Store>| async move {
                    match s.lock().unwrap().get(&key) {
                        Some(v) => (StatusCode::OK, v.clone()).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                },
            ),
        )
        .with_state(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, store)
}

#[tokio::test]
async fn run_downloads_input_and_uploads_output() {
    use saladfingers_protocol::transfer;

    let (base, store) = storage_server().await;
    let http = reqwest::Client::new();

    // Pre-upload an input artifact (single file).
    let src = tempfile::tempdir().unwrap();
    let in_file = src.path().join("hello.txt");
    std::fs::write(&in_file, b"input-data").unwrap();
    let in_urls = vec![format!("{base}/in/input0.tzst.000")];
    transfer::upload_artifact(&http, &in_file, false, &in_urls, "in/input0")
        .await
        .unwrap();

    // The agent runs in this workdir and produces out.txt from the input.
    let work = tempfile::tempdir().unwrap();
    let job = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION, "run_id": "sf-io0001", "shard_index": 0, "shard_count": 1,
        "command": ["sh", "-c", "cp data.txt out.txt"],
        "workdir": work.path().to_str().unwrap(),
        "inputs": [{
            "name": "in/input0", "urls": in_urls,
            "dest": work.path().join("data.txt").to_str().unwrap(), "archive": false
        }],
        "outputs": [{
            "name": "result", "src_glob": "out.txt", "archive": false,
            "put_urls": [format!("{base}/out/result.tzst.000"), format!("{base}/out/result.tzst.001")]
        }],
        "urls": {
            "result_put": format!("{base}/result.json"), "result_get": format!("{base}/result.json"),
            "attempts_put": format!("{base}/attempts.json"), "attempts_get": format!("{base}/attempts.json"),
            "log_put": format!("{base}/log.txt")
        }
    });
    http.put(format!("{base}/job.json"))
        .body(serde_json::to_vec(&job).unwrap())
        .send()
        .await
        .unwrap();

    let output = spawn_agent(&format!("{base}/job.json")).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Envelope reports success and the uploaded output.
    let env_bytes = store
        .lock()
        .unwrap()
        .get("result.json")
        .cloned()
        .expect("envelope stored");
    let env: Value = serde_json::from_slice(&env_bytes).unwrap();
    assert_eq!(env["status"], "succeeded");
    assert_eq!(env["uploads"][0]["name"], "result");
    assert_eq!(env["attempts"], 1);

    // Downloading the output yields the input's bytes (cp'd through).
    let dl = tempfile::tempdir().unwrap();
    let dl_file = dl.path().join("got.txt");
    transfer::download_artifact(
        &http,
        &[format!("{base}/out/result.tzst.000")],
        &dl_file,
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(&dl_file).unwrap(), b"input-data");
}

#[tokio::test]
async fn run_short_circuits_when_already_succeeded() {
    let server = MockServer::start().await;
    // A prior terminal envelope exists → the agent must NOT run the command.
    let prior = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION, "run_id": "sf-test01", "shard_index": 0,
        "status": "succeeded", "exit_code": 0,
        "timings": {"agent_start": "2026-07-17T12:00:00Z"},
        "node": {}, "uploads": [], "attempts": 1, "gate_reallocations": 0
    });
    Mock::given(method("GET"))
        .and(path("/result"))
        .respond_with(ResponseTemplate::new(200).set_body_json(prior))
        .mount(&server)
        .await;
    // If the agent wrongly ran, this marker command would create a file; it must not.
    let marker = std::env::temp_dir().join("sf-agent-shortcircuit-marker");
    let _ = std::fs::remove_file(&marker);
    let cmd = format!("touch {}", marker.display());
    Mock::given(method("GET"))
        .and(path("/job"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(job_spec(&server.uri(), vec!["sh", "-c", &cmd])),
        )
        .mount(&server)
        .await;

    let output = spawn_agent(&format!("{}/job", server.uri())).await;
    assert_eq!(output.status.code(), Some(0));
    assert!(
        !marker.exists(),
        "command must not run when a terminal envelope exists"
    );
}

#[tokio::test]
async fn run_stops_reexecuting_a_failing_job_once_the_attempt_cap_is_spent() {
    // The platform relaunches the container on every exit (E1/E2), so a job that fails
    // deterministically re-runs forever unless the agent caps it. Simulate the relaunch
    // loop by booting the agent repeatedly against a STATEFUL store (its own envelope
    // and attempts-ledger PUTs persist): with max_attempts=2 the job must execute
    // exactly twice, and the third boot must short-circuit to exit 0 without running.
    let (base, store) = storage_server().await;
    let work = tempfile::tempdir().unwrap();
    let count_file = work.path().join("count");
    let spec = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION,
        "run_id": "sf-cap01",
        "shard_index": 0,
        "shard_count": 1,
        "command": ["sh", "-c", format!("echo x >> {}; exit 3", count_file.display())],
        "workdir": "/tmp",
        "max_attempts": 2,
        "urls": {
            "result_put": format!("{base}/result"),
            "result_get": format!("{base}/result"),
            "attempts_put": format!("{base}/attempts"),
            "attempts_get": format!("{base}/attempts"),
            "log_put": format!("{base}/log"),
        }
    });
    store
        .lock()
        .unwrap()
        .insert("job".into(), serde_json::to_vec(&spec).unwrap());

    let runs = |path: &std::path::Path| -> usize {
        std::fs::read_to_string(path).map_or(0, |s| s.lines().count())
    };

    // Boots 1 and 2: the cap allows them; the job runs and fails each time.
    for boot in 1..=2u32 {
        let output = spawn_agent(&format!("{base}/job")).await;
        assert_eq!(
            output.status.code(),
            Some(3),
            "boot {boot} propagates the failure"
        );
        assert_eq!(
            runs(&count_file),
            boot as usize,
            "boot {boot} executed the job"
        );
    }

    // Boot 3: ledger shows the cap is spent and the envelope says Failed → the agent
    // must exit 0 cheaply WITHOUT re-downloading/re-executing anything.
    let output = spawn_agent(&format!("{base}/job")).await;
    assert_eq!(output.status.code(), Some(0), "capped boot exits 0");
    assert_eq!(
        runs(&count_file),
        2,
        "capped boot must not re-execute the job"
    );

    // The commit record still reports the true outcome for the CLI.
    let env_bytes = store.lock().unwrap().get("result").cloned().unwrap();
    let env: Value = serde_json::from_slice(&env_bytes).unwrap();
    assert_eq!(env["status"], "failed");
    assert_eq!(env["exit_code"], 3);
}

/// A run's output must survive as an artifact, not only as best-effort container stdout —
/// and it must still reach stdout, because that is what `saladfingers logs` reads.
///
/// The script logs more lines than one log-entries page holds, which is the shape that lost
/// its tail on sf-vf278i / sf-i1903a: the org log query is capped and node-clock stamped, so
/// the uploaded copy is the one that can be trusted to be complete.
#[tokio::test]
async fn run_uploads_the_childs_complete_output_and_still_mirrors_it() {
    let (base, store) = storage_server().await;
    let http = reqwest::Client::new();
    let work = tempfile::tempdir().unwrap();

    let script = "i=0; while [ $i -lt 250 ]; do echo \"line $i\"; i=$((i+1)); done; \
                  echo 'on-stderr' >&2; echo 'FINAL LINE'";
    let job = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION, "run_id": "sf-log001", "shard_index": 0, "shard_count": 1,
        "command": ["sh", "-c", script],
        "workdir": work.path().to_str().unwrap(),
        "urls": {
            "result_put": format!("{base}/result.json"), "result_get": format!("{base}/result.json"),
            "attempts_put": format!("{base}/attempts.json"), "attempts_get": format!("{base}/attempts.json"),
            "log_put": format!("{base}/log.txt")
        }
    });
    http.put(format!("{base}/job.json"))
        .body(serde_json::to_vec(&job).unwrap())
        .send()
        .await
        .unwrap();

    let output = spawn_agent(&format!("{base}/job.json")).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let uploaded = store
        .lock()
        .unwrap()
        .get("log.txt")
        .cloned()
        .expect("the agent uploaded the run's output");
    let uploaded = String::from_utf8(uploaded).expect("utf-8 log");
    assert!(uploaded.contains("line 0"), "the head is present");
    assert!(uploaded.contains("line 249"), "past one page is present");
    assert!(
        uploaded.contains("FINAL LINE"),
        "the tail is the whole point: {}",
        uploaded
            .lines()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .join(" | ")
    );
    assert!(uploaded.contains("on-stderr"), "stderr is captured too");

    // Unchanged for the platform's log shipper: piping the child must not stop its output
    // reaching the agent's own stdout, or `saladfingers logs --follow` goes dark.
    let mirrored = String::from_utf8_lossy(&output.stdout);
    assert!(mirrored.contains("line 0"));
    assert!(mirrored.contains("FINAL LINE"));
}

/// The output of a run that *failed* is the most valuable thing it produced, so the upload
/// must not be conditional on success the way the output artifacts are.
#[tokio::test]
async fn a_failed_run_still_uploads_its_output() {
    let (base, store) = storage_server().await;
    let http = reqwest::Client::new();
    let work = tempfile::tempdir().unwrap();

    let job = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION, "run_id": "sf-log002", "shard_index": 0, "shard_count": 1,
        "command": ["sh", "-c", "echo 'boom happened' >&2; exit 3"],
        "workdir": work.path().to_str().unwrap(),
        "urls": {
            "result_put": format!("{base}/result.json"), "result_get": format!("{base}/result.json"),
            "attempts_put": format!("{base}/attempts.json"), "attempts_get": format!("{base}/attempts.json"),
            "log_put": format!("{base}/log.txt")
        }
    });
    http.put(format!("{base}/job.json"))
        .body(serde_json::to_vec(&job).unwrap())
        .send()
        .await
        .unwrap();

    let output = spawn_agent(&format!("{base}/job.json")).await;
    assert_eq!(output.status.code(), Some(3));

    let uploaded = store
        .lock()
        .unwrap()
        .get("log.txt")
        .cloned()
        .expect("a failed run still uploads its output");
    assert!(String::from_utf8_lossy(&uploaded).contains("boom happened"));
}

/// A checkpoint that exists but cannot be restored must stop the run, not start it over.
///
/// Continuing is what the old code did, and it destroys the thing it was protecting: the
/// ring no longer knows which slot is live, so the next upload can land on the committed
/// slot — and even when it rotates correctly, the commit that follows reclaims the other
/// one. Either way the checkpoint the operator still had is gone, and the job silently
/// retrains from step 0 while the metadata says otherwise.
///
/// The unreadable checkpoint here is a v1 metadata object, which is what an older agent
/// leaves behind. The job's command would create a marker file; it must never run. The
/// spec also carries an input whose URL 404s: the probe must fail the boot BEFORE the
/// input download (exit 6, not the input path's exit 4) — a permanently bad checkpoint
/// must not re-pay the full input transfer on every relaunch of a doomed run. And the
/// metadata URL carries a fake signature, so the no-leak assertion can actually fail.
#[tokio::test]
async fn an_unreadable_checkpoint_stops_the_run_instead_of_retraining_from_zero() {
    let (base, store) = storage_server().await;
    let http = reqwest::Client::new();
    let work = tempfile::tempdir().unwrap();
    let marker = work.path().join("the-job-ran");
    let input_dest = work.path().join("never-downloaded.txt");

    let slot_urls = |slot: u32| {
        json!({
            "put_urls": [format!("{base}/ckpt/slot{slot}/data.000")],
            "get_urls": [format!("{base}/ckpt/slot{slot}/data.000")],
            "delete_urls": [format!("{base}/ckpt/slot{slot}/data.000")],
        })
    };
    let sig = "?X-Amz-Signature=deadbeefcafef00d&X-Amz-Credential=AKID%2Ftest";
    let job = json!({
        "v": saladfingers_protocol::PROTOCOL_VERSION, "run_id": "sf-ckpt01", "shard_index": 0, "shard_count": 1,
        "command": ["sh", "-c", format!("touch {}", marker.display())],
        "workdir": work.path().to_str().unwrap(),
        "inputs": [{
            "name": "in/input0",
            "urls": [format!("{base}/in/absent.tzst.000")],
            "dest": input_dest.to_str().unwrap(),
            "archive": false
        }],
        "checkpoint": {
            "glob": "ckpt",
            "interval_secs": 1,
            "quiesce_secs": 0,
            "slots": [slot_urls(0), slot_urls(1)],
            "meta_put_url": format!("{base}/ckpt/meta.json{sig}"),
            "meta_get_url": format!("{base}/ckpt/meta.json{sig}"),
        },
        "urls": {
            "result_put": format!("{base}/result.json"), "result_get": format!("{base}/result.json"),
            "attempts_put": format!("{base}/attempts.json"), "attempts_get": format!("{base}/attempts.json"),
            "log_put": format!("{base}/log.txt")
        }
    });
    // A checkpoint written by an agent one protocol version back: readable enough to know
    // it is there, not readable enough to resume from.
    let v1_meta = json!({
        "v": 1, "parts": 1, "bytes": 4096,
        "sha256": "0".repeat(64), "uploaded_at": "2026-07-01T00:00:00Z",
    });
    for (key, value) in [("job.json", &job), ("ckpt/meta.json", &v1_meta)] {
        http.put(format!("{base}/{key}"))
            .body(serde_json::to_vec(value).unwrap())
            .send()
            .await
            .unwrap();
    }

    let output = spawn_agent(&format!("{base}/job.json")).await;
    assert_eq!(
        output.status.code(),
        Some(6),
        "exit 6 (checkpoint), not 4 (inputs): the probe must run before the download; \
         stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !marker.exists(),
        "the job ran anyway — it would have retrained from step 0 over a live checkpoint"
    );
    assert!(
        !input_dest.exists(),
        "the input was downloaded before the checkpoint was probed — a doomed relaunch \
         re-pays the whole input transfer"
    );

    let raw = store
        .lock()
        .unwrap()
        .get("result.json")
        .cloned()
        .expect("the reason must reach the CLI, not just the node's logs");
    let env: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(env["status"], "agent_error");
    let error = env["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("checkpoint restore") && error.contains("protocol v1"),
        "the envelope should name the cause: {error}"
    );
    assert!(
        !error.contains("X-Amz-Signature") && !error.contains("deadbeef"),
        "a presigned URL leaked into the stored envelope: {error}"
    );
}

/// The version field exists to make a mismatched CLI/agent pair fail loudly at boot.
/// Field-level serde only catches a skew whose shapes differ — a spec with no checkpoint
/// block is byte-identical across v1 and v2 — so the check has to be explicit, and this
/// pins that it is: same spec, wrong `v`, exit 3 without running anything.
#[tokio::test]
async fn a_job_spec_from_another_protocol_version_is_refused_at_boot() {
    let server = MockServer::start().await;
    mount_result_endpoints(&server).await;
    let mut spec = job_spec(&server.uri(), vec!["sh", "-c", "exit 0"]);
    spec["v"] = json!(saladfingers_protocol::PROTOCOL_VERSION - 1);
    Mock::given(method("GET"))
        .and(path("/job"))
        .respond_with(ResponseTemplate::new(200).set_body_json(spec))
        .mount(&server)
        .await;

    let output = spawn_agent(&format!("{}/job", server.uri())).await;
    assert_eq!(
        output.status.code(),
        Some(3),
        "a version skew is a spec problem: exit 3, before any work"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("protocol v1") && stderr.contains("v2"),
        "the log should name both versions: {stderr}"
    );
}
