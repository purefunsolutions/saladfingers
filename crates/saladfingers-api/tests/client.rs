// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration tests for the SaladCloud client against a wiremock server.
//!
//! Fixtures are transcribed from the vendored OpenAPI examples and a live API
//! snapshot (GPU-class UUIDs are the real global identifiers; org/project are the
//! placeholder `my-org` / `my-proj`).

use std::collections::BTreeMap;
use std::time::Duration;

use saladfingers_api::models::{
    ContainerPriority, CreateContainer, CreateContainerGroup, GroupStatus, InstanceState,
    Resources, RestartPolicy, UpdateContainerGroup,
};
use saladfingers_api::{
    ApiError, RetryPolicy, S4Auth, S4Client, SaladClient, SaladClientConfig, Secret,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ORG: &str = "my-org";
const PROJ: &str = "my-proj";

fn fast_client(base: String) -> SaladClient {
    let mut cfg =
        SaladClientConfig::new(Secret::new("test-key-abc123"), ORG, PROJ).with_base_url(base);
    // Tiny backoff and a huge rate so retry/pacing never slow the tests down.
    cfg.retry = RetryPolicy {
        max_attempts: 4,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    };
    cfg.rate_limit_per_min = 1_000_000;
    SaladClient::new(cfg).expect("client builds")
}

fn json_response(status: u16, body: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_raw(body.to_owned(), "application/json")
}

fn containers_path() -> String {
    format!("/organizations/{ORG}/projects/{PROJ}/containers")
}

const GPU_CLASSES_JSON: &str = r#"{
  "items": [
    {
      "id": "f51baccc-dc95-40fb-a5d1-6d0ee0db31d2",
      "name": "RTX 3060 (12 GB)",
      "gpu_class_type": "community",
      "is_high_demand": false,
      "prices": [
        {"priority": "high", "price": "0.08"},
        {"priority": "medium", "price": "0.067"},
        {"priority": "low", "price": "0.053"},
        {"priority": "batch", "price": "0.04"}
      ]
    },
    {
      "id": "ed563892-aacd-40f5-80b7-90c9be6c759b",
      "name": "RTX 4090 (24 GB)",
      "gpu_class_type": "community",
      "prices": [
        {"priority": "high", "price": "0.30"},
        {"priority": "batch", "price": "0.16"}
      ]
    }
  ]
}"#;

const QUOTAS_JSON: &str = r#"{
  "container_groups_quotas": {
    "container_replicas_quota": 10,
    "container_replicas_used": 3,
    "max_container_group_reallocations_per_minute": 10,
    "max_container_group_recreates_per_minute": 10,
    "max_container_group_restarts_per_minute": 10
  },
  "create_time": "2026-07-16T23:22:32.7454522+00:00",
  "update_time": "2026-07-16T23:22:32.7735296+00:00"
}"#;

const GROUP_CREATED_JSON: &str = r#"{
  "id": "cg-123",
  "name": "sf-x7k2mq-0",
  "replicas": 1,
  "current_state": {"status": "pending"},
  "create_time": "2026-07-17T12:00:00Z"
}"#;

const GROUP_RUNNING_JSON: &str = r#"{
  "id": "cg-123",
  "name": "sf-x7k2mq-0",
  "replicas": 1,
  "current_state": {
    "status": "running",
    "instance_status_counts": {"allocating_count": 0, "creating_count": 0, "running_count": 1, "stopping_count": 0}
  }
}"#;

const GROUP_WEIRD_STATE_JSON: &str =
    r#"{"name": "g", "current_state": {"status": "quantum-limbo"}}"#;

const INSTANCES_DOWNLOADING_JSON: &str = r#"{
  "instances": [
    {"instance_id": "i-1", "machine_id": "mach-a", "state": "downloading", "pulling_progress": 41.5, "started": false, "ready": false}
  ]
}"#;

const PROBLEM_400_JSON: &str = r#"{
  "type": "https://docs.salad.com/errors/validation",
  "title": "Bad Request",
  "status": 400,
  "detail": "name must match ^[a-z]...",
  "instance": "/req/abc"
}"#;

const CLOUDFLARE_HTML: &str = "<!DOCTYPE html><html><head><title>503</title></head><body><h1>Service Unavailable</h1></body></html>";

const S4_TOKEN_JSON: &str = r#"{"url": "https://storage-api.salad.com/signed?token=xyz"}"#;

#[tokio::test]
async fn gpu_classes_parse_with_decimal_prices() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/gpu-classes")))
        .respond_with(json_response(200, GPU_CLASSES_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let classes = client.list_gpu_classes().await.expect("ok");
    assert_eq!(classes.len(), 2);
    let rtx3060 = &classes[0];
    assert_eq!(rtx3060.name, "RTX 3060 (12 GB)");
    let batch = rtx3060
        .price(ContainerPriority::Batch)
        .expect("has batch price");
    assert_eq!(batch.to_string(), "0.04");
    assert_eq!(
        classes[1]
            .price(ContainerPriority::High)
            .unwrap()
            .to_string(),
        "0.30"
    );
}

#[tokio::test]
async fn create_sends_the_expected_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(containers_path()))
        .respond_with(json_response(201, GROUP_CREATED_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let mut env = BTreeMap::new();
    env.insert("SF_RUN_ID".to_string(), "sf-x7k2mq".to_string());
    let req = CreateContainerGroup {
        name: "sf-x7k2mq-0".into(),
        display_name: None,
        autostart_policy: true,
        replicas: 1,
        restart_policy: RestartPolicy::OnFailure,
        container: CreateContainer {
            image: "reg.example/gpu-probe@sha256:abc".into(),
            resources: Resources::gpu(
                4,
                8192,
                vec!["f51baccc-dc95-40fb-a5d1-6d0ee0db31d2".into()],
                20,
            ),
            command: Some(vec!["/bin/sf-agent".into(), "run".into()]),
            environment_variables: env,
            priority: ContainerPriority::Batch,
            image_caching: Some(true),
            registry_authentication: None,
        },
        networking: None,
        country_codes: None,
    };
    let group = client.create_container_group(&req).await.expect("created");
    assert_eq!(group.name, "sf-x7k2mq-0");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["restart_policy"], "on_failure");
    assert_eq!(body["autostart_policy"], true);
    assert_eq!(body["container"]["priority"], "batch");
    assert_eq!(body["container"]["image_caching"], true);
    assert_eq!(
        body["container"]["resources"]["storage_amount"],
        20u64 * 1024 * 1024 * 1024
    );
    assert_eq!(
        body["container"]["environment_variables"]["SF_RUN_ID"],
        "sf-x7k2mq"
    );
    // Absent optionals must be omitted.
    assert!(body.get("display_name").is_none());
    assert!(body.get("networking").is_none());
    // The API key must be sent as a header, never in the body.
    let key_header = requests[0].headers.get("salad-api-key").unwrap();
    assert_eq!(key_header.to_str().unwrap(), "test-key-abc123");
    let body_str = String::from_utf8_lossy(&requests[0].body);
    assert!(
        !body_str.contains("test-key-abc123"),
        "key must not be in the body"
    );
}

#[tokio::test]
async fn group_status_parses_known_and_unknown_states() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{}/running", containers_path())))
        .respond_with(json_response(200, GROUP_RUNNING_JSON))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{}/weird", containers_path())))
        .respond_with(json_response(200, GROUP_WEIRD_STATE_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let running = client.get_container_group("running").await.unwrap();
    let state = running.current_state.unwrap();
    assert_eq!(state.status, GroupStatus::Running);
    assert_eq!(state.instance_status_counts.unwrap().running_count, 1);

    // An unrecognized status must map to Unknown, never panic.
    let weird = client.get_container_group("weird").await.unwrap();
    assert_eq!(weird.current_state.unwrap().status, GroupStatus::Unknown);
}

#[tokio::test]
async fn a_gateway_group_yields_a_public_https_url() {
    // `run --expose-port` reports this URL to the user, so the shape of the
    // response the CLI reads from is worth pinning: `networking.dns` is a bare
    // hostname and the caller must get a scheme in front of it.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{}/exposed", containers_path())))
        .respond_with(json_response(
            200,
            r#"{
              "id": "cg-123",
              "name": "exposed",
              "current_state": {"status": "running"},
              "networking": {"dns": "curious-salad-a1b2c3.salad.cloud"}
            }"#,
        ))
        .mount(&server)
        .await;
    // No networking block at all — every run without --expose-port.
    Mock::given(method("GET"))
        .and(path(format!("{}/plain", containers_path())))
        .respond_with(json_response(200, GROUP_RUNNING_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let exposed = client.get_container_group("exposed").await.unwrap();
    assert_eq!(
        exposed.gateway_url().as_deref(),
        Some("https://curious-salad-a1b2c3.salad.cloud")
    );

    let plain = client.get_container_group("plain").await.unwrap();
    assert!(plain.gateway_url().is_none());
}

#[tokio::test]
async fn instances_parse_pulling_progress() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{}/g/instances", containers_path())))
        .respond_with(json_response(200, INSTANCES_DOWNLOADING_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let instances = client.list_instances("g").await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].state, Some(InstanceState::Downloading));
    assert_eq!(instances[0].pulling_progress, Some(41.5));
    assert_eq!(instances[0].action_id(), Some("mach-a"));
}

#[tokio::test]
async fn rate_limited_then_succeeds_after_retry() {
    let server = MockServer::start().await;
    // wiremock uses the first-mounted non-exhausted mock: mount the transient 429
    // (one hit) first, then the success fallback.
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/gpu-classes")))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/gpu-classes")))
        .respond_with(json_response(200, GPU_CLASSES_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let classes = client
        .list_gpu_classes()
        .await
        .expect("succeeds after retry");
    assert_eq!(classes.len(), 2);
}

#[tokio::test]
async fn rate_limited_exhausted_maps_to_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/quotas")))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let err = client.get_quotas().await.unwrap_err();
    assert!(matches!(err, ApiError::RateLimited { .. }), "got {err:?}");
}

#[tokio::test]
async fn cloudflare_html_is_html_error_not_decode() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/quotas")))
        .respond_with(ResponseTemplate::new(503).set_body_raw(CLOUDFLARE_HTML, "text/html"))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let err = client.get_quotas().await.unwrap_err();
    match err {
        ApiError::Html { status, snippet } => {
            assert_eq!(status, 503);
            assert!(
                snippet.contains("Service Unavailable"),
                "snippet: {snippet}"
            );
        }
        other => panic!("expected Html error, got {other:?}"),
    }
}

#[tokio::test]
async fn get_retries_on_503_then_succeeds() {
    let server = MockServer::start().await;
    // Transient 503s (two hits) first, then the success fallback.
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/quotas")))
        .respond_with(ResponseTemplate::new(503).set_body_raw("<html>503</html>", "text/html"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/organizations/{ORG}/quotas")))
        .respond_with(json_response(200, QUOTAS_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let quotas = client.get_quotas().await.expect("succeeds after 503s");
    assert_eq!(quotas.replicas_available(), 7);
}

#[tokio::test]
async fn create_409_conflict_adopts_existing_group() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(containers_path()))
        .respond_with(json_response(
            409,
            r#"{"title":"Conflict","status":409,"detail":"name in use"}"#,
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{}/sf-x7k2mq-0", containers_path())))
        .respond_with(json_response(200, GROUP_RUNNING_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let req = CreateContainerGroup {
        name: "sf-x7k2mq-0".into(),
        display_name: None,
        autostart_policy: true,
        replicas: 1,
        restart_policy: RestartPolicy::OnFailure,
        container: CreateContainer {
            image: "img".into(),
            resources: Resources::gpu(2, 4096, vec!["uuid".into()], 10),
            command: None,
            environment_variables: BTreeMap::new(),
            priority: ContainerPriority::default(),
            image_caching: None,
            registry_authentication: None,
        },
        networking: None,
        country_codes: None,
    };
    let group = client.create_container_group(&req).await.expect("adopted");
    assert_eq!(group.name, "sf-x7k2mq-0");
}

#[tokio::test]
async fn delete_404_is_ok() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(format!("{}/gone", containers_path())))
        .respond_with(json_response(404, r#"{"title":"Not Found","status":404}"#))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    client
        .delete_container_group("gone")
        .await
        .expect("404 delete is Ok");
}

#[tokio::test]
async fn get_404_is_not_found_variant() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{}/gone", containers_path())))
        .respond_with(json_response(404, r#"{"title":"Not Found","status":404}"#))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let err = client.get_container_group("gone").await.unwrap_err();
    assert!(err.is_not_found(), "got {err:?}");
}

#[tokio::test]
async fn problem_400_surfaces_title_and_detail() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(containers_path()))
        .respond_with(json_response(400, PROBLEM_400_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let req = CreateContainerGroup {
        name: "bad".into(),
        display_name: None,
        autostart_policy: true,
        replicas: 1,
        restart_policy: RestartPolicy::Never,
        container: CreateContainer {
            image: "img".into(),
            resources: Resources::gpu(1, 1024, vec![], 1),
            command: None,
            environment_variables: BTreeMap::new(),
            priority: ContainerPriority::default(),
            image_caching: None,
            registry_authentication: None,
        },
        networking: None,
        country_codes: None,
    };
    let err = client.create_container_group(&req).await.unwrap_err();
    match err {
        ApiError::Problem {
            status,
            title,
            detail,
            ..
        } => {
            assert_eq!(status, 400);
            assert_eq!(title, "Bad Request");
            assert!(detail.unwrap().contains("name must match"));
        }
        other => panic!("expected Problem, got {other:?}"),
    }
}

#[tokio::test]
async fn api_key_absent_from_debug_and_errors() {
    let cfg = SaladClientConfig::new(Secret::new("super-secret-key"), ORG, PROJ);
    let debug = format!("{cfg:?}");
    assert!(
        !debug.contains("super-secret-key"),
        "config Debug leaked the key: {debug}"
    );
    assert!(debug.contains("Secret(***)"));
}

#[tokio::test]
async fn update_container_group_patches_replicas() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path(format!("{}/g", containers_path())))
        .respond_with(json_response(200, GROUP_RUNNING_JSON))
        .mount(&server)
        .await;

    let client = fast_client(server.uri());
    let patch = UpdateContainerGroup { replicas: Some(0) };
    client
        .update_container_group("g", &patch)
        .await
        .expect("patched");
    let requests = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["replicas"], 0);
}

#[tokio::test]
async fn s4_sign_get_posts_method_and_exp() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/organizations/{ORG}/file_tokens/runs/sf-x/job.json"
        )))
        .respond_with(json_response(200, S4_TOKEN_JSON))
        .mount(&server)
        .await;

    let s4 = S4Client::new(server.uri(), ORG, S4Auth::ApiKey(Secret::new("k"))).unwrap();
    let url = s4
        .sign_get("runs/sf-x/job.json", 3600)
        .await
        .expect("signed");
    assert!(url.contains("token=xyz"));

    let requests = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["method"], "GET");
    assert_eq!(body["exp"], "3600");
}
