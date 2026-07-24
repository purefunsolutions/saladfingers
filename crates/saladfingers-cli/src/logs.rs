// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers logs RUN_ID` — query a run's container logs via the org log-entries
//! endpoint (Axiom-backed, ~90-day retention; works even after the group is deleted).
//!
//! The API requires a time range + a non-empty query in SaladCloud's log query
//! language. We filter server-side by container group name
//! (`resource.labels.container_group_name = "<name>"`), one query per group.

use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use saladfingers_api::{LogEntriesQuery, LogEntry, SaladClient};

use crate::cli::LogsArgs;
use crate::config::Config;
use crate::state;

/// Server-side filter for one container group in SaladCloud's log query language.
fn group_filter(name: &str) -> String {
    format!("resource.labels.container_group_name = \"{name}\"")
}

/// `saladfingers logs RUN_ID [--follow]`
pub async fn logs(cfg: Config, args: LogsArgs) -> Result<()> {
    let client = cfg.client()?;
    let names = match state::load_run(&args.run_id)? {
        Some(run) => run.group_names(),
        None => vec![args.run_id.clone()],
    };
    if args.follow {
        return follow(&client, &names).await;
    }

    let end = Utc::now() + Duration::minutes(5);
    let start = end - Duration::hours(24);
    for name in &names {
        let query = LogEntriesQuery {
            start_time: start,
            end_time: end,
            query: group_filter(name),
            // The API caps page_size at 100. Fetch newest-first so a truncated snapshot
            // keeps the tail (where a failed run's error is), then reverse for display.
            page_size: Some(100),
            sort_order: Some("desc".to_string()),
        };
        match client.query_log_entries(&query).await {
            Ok(entries) if entries.is_empty() => {
                eprintln!("no log entries for {name} (last 24 h)");
            }
            Ok(mut entries) => {
                let truncated = entries.len() >= 100;
                entries.reverse();
                for entry in &entries {
                    print_entry(name, entry);
                }
                if truncated {
                    eprintln!("… most recent 100 lines for {name} (use --follow to tail)");
                }
            }
            Err(e) => eprintln!("log query for {name} failed: {e}"),
        }
    }
    Ok(())
}

/// Tail the groups' logs: poll a rolling window and print entries not seen before.
///
/// A lookback window plus content-keyed dedup tolerates the per-node clock skew we found
/// empirically (E6) — container-stdout timestamps can arrive out of order, so a plain
/// "newer than watermark" filter would drop lines.
async fn follow(client: &SaladClient, names: &[String]) -> Result<()> {
    const LOOKBACK: i64 = 150; // seconds of history each poll
    const POLL_SECS: u64 = 4;
    const SEEN_CAP: usize = 5000;

    let mut seen: HashSet<String> = HashSet::new();
    let mut order: VecDeque<String> = VecDeque::new();
    loop {
        let end = Utc::now() + Duration::seconds(30);
        let start = end - Duration::seconds(LOOKBACK + 30);
        for name in names {
            let query = LogEntriesQuery {
                start_time: start,
                end_time: end,
                query: group_filter(name),
                page_size: Some(100),
                sort_order: Some("asc".to_string()),
            };
            match client.query_log_entries(&query).await {
                Ok(entries) => {
                    for entry in &entries {
                        let key = entry_key(name, entry);
                        if seen.insert(key.clone()) {
                            order.push_back(key);
                            print_entry(name, entry);
                        }
                    }
                }
                Err(e) => eprintln!("log query for {name} failed (will retry): {e}"),
            }
        }
        while order.len() > SEEN_CAP {
            if let Some(k) = order.pop_front() {
                seen.remove(&k);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)).await;
    }
}

fn print_entry(name: &str, entry: &LogEntry) {
    if let Some(text) = line_text(entry) {
        let ts = entry
            .time
            .map(|t: DateTime<Utc>| t.to_rfc3339())
            .unwrap_or_default();
        println!("{ts} [{name}] {text}");
    }
}

fn entry_key(name: &str, entry: &LogEntry) -> String {
    format!(
        "{name}|{}|{}",
        entry.time.map(|t| t.to_rfc3339()).unwrap_or_default(),
        line_text(entry).unwrap_or_default()
    )
}

/// Render one log entry: container stdout/stderr verbatim, or a platform lifecycle
/// event (`json_log.message`) prefixed with `·`. Returns `None` for empty entries.
fn line_text(entry: &LogEntry) -> Option<String> {
    if let Some(text) = entry.text_log.as_deref()
        && !text.is_empty()
    {
        return Some(text.to_string());
    }
    let message = entry.json_log.as_ref()?.get("message")?.as_str()?;
    Some(format!("· {message}"))
}
