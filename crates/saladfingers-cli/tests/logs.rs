// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Integration tests for `saladfingers logs` paging against a mock log-entries API.
//!
//! The endpoint caps `page_size` at 100, so a run that logs more than that can only be read
//! back by issuing more than one request. These tests pin the property that broke on
//! `sf-vf278i` / `sf-i1903a`: a ~120-line run whose *last* sections never appeared.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use saladfingers_api::{RetryPolicy, SaladClient, SaladClientConfig, Secret};
use saladfingers_cli::logs::fetch_entries;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

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

/// Which end of an over-full window the API hands back.
#[derive(Clone, Copy, PartialEq)]
enum Keeps {
    /// The page is filled from the oldest matching entry — the behaviour that loses a run's
    /// final output, and that `sort_order: desc` was (wrongly) assumed to rule out.
    Oldest,
    /// The page is filled from the newest matching entry.
    Newest,
}

/// A fake log store: `count` entries `step_ms` apart, answering a time-window query with at
/// most `page_size` of them.
struct Corpus {
    t0: DateTime<Utc>,
    count: usize,
    keeps: Keeps,
    step_ms: i64,
}

impl Corpus {
    fn line(&self, i: usize) -> (DateTime<Utc>, String) {
        (
            self.t0 + chrono::Duration::milliseconds(self.step_ms * i as i64),
            format!("line {i}"),
        )
    }
}

impl Respond for Corpus {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&request.body).expect("json body");
        let parse = |k: &str| -> DateTime<Utc> {
            body[k]
                .as_str()
                .unwrap_or_else(|| panic!("{k} is a string"))
                .parse()
                .expect("rfc3339")
        };
        let (start, end) = (parse("start_time"), parse("end_time"));
        let page_size = body["page_size"].as_u64().unwrap_or(100) as usize;
        assert!(page_size <= 100, "the API rejects page_size above 100");

        // Entries inside the requested window, oldest first.
        let mut hits: Vec<(DateTime<Utc>, String)> = (0..self.count)
            .map(|i| self.line(i))
            .filter(|(t, _)| *t >= start && *t <= end)
            .collect();
        if self.keeps == Keeps::Newest {
            hits.reverse();
        }
        hits.truncate(page_size);

        let items: Vec<serde_json::Value> = hits
            .iter()
            .map(|(t, text)| {
                serde_json::json!({
                    "time": t.to_rfc3339(),
                    "text_log": text,
                    "severity": "default",
                    "resource": { "type": "container", "labels": {} },
                })
            })
            .collect();
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": items,
            "organization_name": "my-org",
            "page_min_time": start.to_rfc3339(),
            "page_max_time": end.to_rfc3339(),
        }))
    }
}

async fn mock_store(count: usize, keeps: Keeps) -> (MockServer, DateTime<Utc>) {
    mock_store_stepped(count, keeps, 1000).await
}

async fn mock_store_stepped(
    count: usize,
    keeps: Keeps,
    step_ms: i64,
) -> (MockServer, DateTime<Utc>) {
    let server = MockServer::start().await;
    let t0 = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();
    Mock::given(method("POST"))
        .and(path("/organizations/my-org/log-entries"))
        .respond_with(Corpus {
            t0,
            count,
            keeps,
            step_ms,
        })
        .mount(&server)
        .await;
    (server, t0)
}

fn texts(entries: &[saladfingers_api::LogEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|e| e.text_log.clone().unwrap_or_default())
        .collect()
}

/// The regression: a run logging more than one page must come back whole — including the
/// last line, which is where a benchmark's final sections and a failure's error live.
#[tokio::test]
async fn paging_recovers_output_past_the_hundred_entry_page_cap() {
    for keeps in [Keeps::Oldest, Keeps::Newest] {
        let (server, t0) = mock_store(250, keeps).await;
        let (entries, truncated) = fetch_entries(
            &client(server.uri()),
            "g",
            t0 - chrono::Duration::hours(1),
            t0 + chrono::Duration::hours(1),
            10_000,
        )
        .await
        .expect("fetch");

        assert_eq!(entries.len(), 250, "every entry must survive paging");
        assert!(!truncated, "the whole window was covered");
        // Oldest first, and crucially the *tail* is present: the single-request version
        // stopped at 100 entries and called it "the most recent 100 lines".
        assert_eq!(texts(&entries).first().unwrap(), "line 0");
        assert_eq!(texts(&entries).last().unwrap(), "line 249");

        // Bisection must stay cheap; the client's own bucket sustains 180 requests/min.
        let requests = server.received_requests().await.unwrap().len();
        assert!(requests < 200, "took {requests} requests");
    }
}

/// When the caller's cap binds, the entries kept must be the newest ones — "most recent N"
/// has to actually mean most recent, which is exactly the claim the old trailer got wrong.
#[tokio::test]
async fn a_binding_limit_keeps_the_newest_entries() {
    let (server, t0) = mock_store(250, Keeps::Oldest).await;
    let (entries, truncated) = fetch_entries(
        &client(server.uri()),
        "g",
        t0 - chrono::Duration::hours(1),
        t0 + chrono::Duration::hours(1),
        50,
    )
    .await
    .expect("fetch");

    assert!(truncated, "a bound fetch must report that it cut output");
    assert_eq!(entries.len(), 50);
    assert_eq!(
        texts(&entries),
        (200..250).map(|i| format!("line {i}")).collect::<Vec<_>>()
    );
}

/// An empty result that stopped on the entry cap must still say it was cut — the caller
/// tells "no entries exist" apart from "none were kept" by exactly this flag, and a
/// degenerate `--limit 0` answered with "no log entries" would read as a run that never
/// printed anything.
#[tokio::test]
async fn a_zero_limit_reports_truncation_not_absence() {
    let (server, t0) = mock_store(12, Keeps::Oldest).await;
    let (entries, truncated) = fetch_entries(
        &client(server.uri()),
        "g",
        t0 - chrono::Duration::hours(1),
        t0 + chrono::Duration::hours(1),
        0,
    )
    .await
    .expect("fetch");

    assert!(entries.is_empty());
    assert!(truncated, "an entry cap that kept nothing still cut output");
}

/// A window that fits in one page must cost exactly one request — bisection is only for
/// windows the API could not answer completely.
#[tokio::test]
async fn a_window_that_fits_in_one_page_is_not_split() {
    let (server, t0) = mock_store(12, Keeps::Oldest).await;
    let (entries, truncated) = fetch_entries(
        &client(server.uri()),
        "g",
        t0 - chrono::Duration::hours(1),
        t0 + chrono::Duration::hours(1),
        10_000,
    )
    .await
    .expect("fetch");

    assert_eq!(entries.len(), 12);
    assert!(!truncated);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

/// A burst of lines sharing one timestamp must not print backwards.
///
/// The final sort is stable, so whatever order a page arrives in survives ties — and we
/// ask for `sort_order: desc`, the same assumption the "cannot split further" rule already
/// depends on. Without reversing each page, every burst inside one millisecond (which
/// `MIN_WINDOW_MS` exists because of) comes out newest-first in the middle of an
/// otherwise chronological log.
#[tokio::test]
async fn a_burst_sharing_one_timestamp_keeps_its_order() {
    let (server, t0) = mock_store_stepped(12, Keeps::Newest, 0).await;
    let (entries, truncated) = fetch_entries(
        &client(server.uri()),
        "g",
        t0 - chrono::Duration::hours(1),
        t0 + chrono::Duration::hours(1),
        10_000,
    )
    .await
    .expect("fetch");

    assert!(!truncated);
    assert_eq!(
        texts(&entries),
        (0..12).map(|i| format!("line {i}")).collect::<Vec<_>>(),
        "twelve lines in one millisecond must stay in the order the job wrote them"
    );
}

/// `--follow` tails a window of its own, so the window flags are refused at parse rather
/// than accepted and ignored — but their defaults must not trip the conflict, or plain
/// `logs --follow` would stop working.
#[test]
fn follow_refuses_the_window_flags_without_tripping_on_their_defaults() {
    use clap::Parser as _;

    let ok =
        saladfingers_cli::cli::Cli::try_parse_from(["saladfingers", "logs", "sf-x", "--follow"]);
    assert!(ok.is_ok(), "plain --follow must still parse");

    for extra in [vec!["--all"], vec!["--limit", "10"], vec!["--since", "2h"]] {
        let mut argv = vec!["saladfingers", "logs", "sf-x", "--follow"];
        argv.extend_from_slice(&extra);
        assert!(
            saladfingers_cli::cli::Cli::try_parse_from(&argv).is_err(),
            "{extra:?} with --follow must be refused, not silently ignored"
        );
    }
}
