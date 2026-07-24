// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration test for the deploy poll loop against a mock SaladCloud API.

use std::time::Duration;

use saladfingers_api::{GroupStatus, RetryPolicy, SaladClient, SaladClientConfig, Secret};
use saladfingers_cli::deploy::{PollOptions, poll_until_running};
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
