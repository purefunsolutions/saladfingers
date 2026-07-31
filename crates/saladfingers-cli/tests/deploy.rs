// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration test for the deploy poll loop against a mock SaladCloud API.

use std::time::Duration;

use saladfingers_api::{GroupStatus, RetryPolicy, SaladClient, SaladClientConfig, Secret};
use saladfingers_cli::deploy::{PollOptions, poll_until_running, resolve_gpu_uuids};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(base: String) -> SaladClient {
    let mut cfg = SaladClientConfig::new(Secret::new("k"), "my-org", "my-proj").with_base_url(base);
    cfg.retry = RetryPolicy {
        max_attempts: 2,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    };
    cfg.rate_limit_per_min = 1_000_000;
    SaladClient::new(cfg).unwrap()
}

#[tokio::test]
async fn poll_reaches_running_through_transitions() {
    let server = MockServer::start().await;
    let base = "/organizations/my-org/projects/my-proj/containers";

    // wiremock uses the first-mounted matching mock that is not exhausted: mount
    // `deploying` (limited to two hits) first, then `running` as the fallback.
    Mock::given(method("GET"))
        .and(path(format!("{base}/g")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "g", "current_state": {"status": "deploying"}
        })))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{base}/g")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "g", "current_state": {"status": "running"}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{base}/g/instances")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "instances": [] })),
        )
        .mount(&server)
        .await;

    let opts = PollOptions {
        timeout: Duration::from_secs(30),
        interval: Duration::from_millis(5),
        quiet: true,
    };
    let result = poll_until_running(&client(server.uri()), "g", &opts)
        .await
        .unwrap();

    assert_eq!(result.status, GroupStatus::Running);
    // At least deploying → running was observed.
    assert!(
        result.transitions.len() >= 2,
        "transitions: {:?}",
        result.transitions
    );
}

/// A `--cpu-only` run names no GPU class, so it must not need the class list either.
/// The server here answers 500 to everything: reaching it at all would fail a run that
/// asked nothing about GPUs, which is what a cold cache plus a control-plane outage
/// looks like from the operator's side.
#[tokio::test]
async fn a_cpu_only_run_never_asks_for_the_gpu_class_list() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    // `refresh: true`, so this stays red-capable on a developer machine: with
    // `false`, a fresh on-disk gpu-classes cache (which any recent real run
    // leaves behind) would satisfy the un-guarded code without a request, and
    // the test would guard nothing outside the sandbox. The early return this
    // pins comes before `refresh` is even consulted.
    let uuids = resolve_gpu_uuids(&client(server.uri()), &[], true)
        .await
        .expect("no classes means no lookup");
    assert!(uuids.is_empty());
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        0,
        "resolving zero classes must not touch the API"
    );
}
