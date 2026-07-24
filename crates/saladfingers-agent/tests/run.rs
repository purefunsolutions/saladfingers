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
        "v": 1,
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
    assert_eq!(env["v"], 1);
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
        "v": 1, "run_id": "sf-io0001", "shard_index": 0, "shard_count": 1,
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
        "v": 1, "run_id": "sf-test01", "shard_index": 0,
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
        "v": 1,
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
