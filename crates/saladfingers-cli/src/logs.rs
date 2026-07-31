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

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use saladfingers_api::{LogEntriesQuery, LogEntry, SaladClient};
use saladfingers_protocol::transfer;

use crate::cli::LogsArgs;
use crate::config::Config;
use crate::presign::S3Backend;
use crate::spec;
use crate::state;

/// Entries one request can return. The endpoint validates `page_size` to `1..=100`
/// (`PageSize` in the OpenAPI spec), so this is a hard ceiling per request, not a tunable —
/// reaching more than 100 entries means issuing more than one request.
const PAGE_SIZE: usize = 100;

/// Stop bisecting once a window is this narrow. Below a couple of milliseconds a split no
/// longer separates entries (a burst of lines shares one timestamp at this resolution), so
/// further recursion would spin without making progress.
const MIN_WINDOW_MS: i64 = 2;

/// Ceiling on requests per group, so a pathological window cannot bisect indefinitely.
///
/// This bounds *wall time*, not rate-limit pressure: the client's own token bucket is
/// `DEFAULT_RATE_PER_MIN = 180` (`saladfingers-api`), so 400 queries already exceeds one
/// minute's budget and the last of them wait on the bucket. What it buys is a `logs` that
/// gives up after a couple of throttled minutes instead of running until the operator does.
/// `follow` re-enters this budget on every poll, so the aggregate over a long tail is
/// bounded only by its own `FOLLOW_LIMIT` stopping each poll early.
const MAX_QUERIES: usize = 400;

/// Slack added to the end of the query window. Container-stdout timestamps are *node*
/// assigned and node clocks skew by unpredictable amounts (E6: one node ran ~73 s off the
/// control plane), so a node whose clock runs fast stamps its final lines in the future.
/// Too little slack here excludes exactly the newest lines — the tail an operator opened
/// the logs to read. Over-wide costs nothing but a few empty windows.
const END_SLACK_MINUTES: i64 = 60;

/// Server-side filter for one container group in SaladCloud's log query language.
fn group_filter(name: &str) -> String {
    format!("resource.labels.container_group_name = \"{name}\"")
}

fn page_query(name: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> LogEntriesQuery {
    LogEntriesQuery {
        start_time: start,
        end_time: end,
        query: group_filter(name),
        page_size: Some(PAGE_SIZE as u32),
        // Only load-bearing for a window that comes back full and can no longer be split:
        // newest-first then keeps the tail rather than the head.
        sort_order: Some("desc".to_string()),
    }
}

/// Every log entry for one group within `[start, end]`, oldest first, plus whether the
/// fetch stopped short of covering the whole window.
///
/// Pages by *window bisection*. A full page proves the window held at least a page worth of
/// entries, but the API does not specify *which* of them it returns, so a full page is
/// evidence to split on, not a trustworthy sample. A window that comes back short is
/// unambiguous: it contained exactly what it returned. Splitting until every window is short
/// therefore reconstructs the whole stream no matter which end the API truncates from.
///
/// This is the bug: the previous implementation issued exactly one 100-entry request per
/// group and printed the result as "the most recent 100 lines", trusting `sort_order: desc`
/// to mean the *newest* hundred. Nothing in the API contract promises that, and two RTX 5090
/// benchmark runs (`sf-vf278i`, `sf-i1903a`) came back with their early sections intact and
/// their final sections missing, with total output just past the 100-entry cap. Bisection
/// drops the assumption instead of betting on it, and makes output past 100 entries
/// reachable at all.
///
/// Windows are visited newest-first, so a binding `limit` keeps the *tail*.
///
/// # Errors
/// Returns an error if a log query fails.
pub async fn fetch_entries(
    client: &SaladClient,
    name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: usize,
) -> Result<(Vec<LogEntry>, bool)> {
    let mut stack = vec![(start, end)];
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<LogEntry> = Vec::new();
    let mut queries = 0usize;
    let mut truncated = false;

    while let Some((ws, we)) = stack.pop() {
        if out.len() >= limit || queries >= MAX_QUERIES {
            truncated = true;
            break;
        }
        queries += 1;
        let entries = client.query_log_entries(&page_query(name, ws, we)).await?;
        let full = entries.len() >= PAGE_SIZE;
        if full && (we - ws) > Duration::milliseconds(MIN_WINDOW_MS) {
            let mid = ws + (we - ws) / 2;
            // Older half pushed first so the newer half pops first.
            stack.push((ws, mid));
            stack.push((mid + Duration::milliseconds(1), we));
            continue;
        }
        // Either short (so complete) or too narrow to split further — keep what came back
        // and admit the loss rather than presenting a partial window as the whole story.
        truncated |= full;
        // Reversed, because the page arrives newest-first and the final sort is *stable*:
        // entries sharing a timestamp keep the order they were pushed in. MIN_WINDOW_MS
        // exists precisely because a burst of lines shares one millisecond, so keeping the
        // page's own order would print every such burst backwards.
        for entry in entries.into_iter().rev() {
            if seen.insert(entry_key(name, &entry)) {
                out.push(entry);
            }
        }
    }

    out.sort_by_key(|e| e.time);
    if out.len() > limit {
        out.drain(..out.len() - limit);
        truncated = true;
    }
    Ok((out, truncated))
}

/// How long the presigned GET for an uploaded log stays valid — long enough to read a
/// 16 MiB object over a slow link, short enough to remain a bounded credential. It is
/// consumed on the next line, so it need not outlive its use by an hour.
const UPLOADED_EXPIRY: std::time::Duration = std::time::Duration::from_secs(300);

/// Print the complete copy the agent uploaded to storage.
///
/// The platform's log service is best-effort by construction: it answers a bounded page at
/// a time (which is why the query above bisects) and stamps entries with the *node's*
/// clock. The agent's capture travels the same path as the run's inputs, outputs, and
/// result envelope, where nothing is capped or reordered — so when the two disagree, this
/// is the one to believe.
async fn uploaded(cfg: &Config, args: &LogsArgs) -> Result<()> {
    let storage = cfg
        .storage
        .as_ref()
        .context("`logs --uploaded` needs an S3-compatible [storage] backend")?;
    let backend = S3Backend::from_config(storage)?;
    let body = fetch_uploaded(&backend, &args.run_id, args.shard).await?;
    // The child wrote these bytes, so they need not be valid UTF-8 (a job that emits a
    // progress bar or a binary blob still deserves a readable log). Pass them through.
    std::io::Write::write_all(&mut std::io::stdout(), &body)?;
    Ok(())
}

/// The agent's uploaded output for one shard, verbatim.
///
/// Split from `uploaded` so the storage key is reachable from a test: `S3Backend`
/// reads its credentials from the environment, which a test cannot set safely, but it
/// can be built directly.
///
/// # Errors
/// Returns an error if the object is missing or the fetch fails.
pub async fn fetch_uploaded(backend: &S3Backend, run_id: &str, shard: u32) -> Result<Vec<u8>> {
    let key = format!("{}/log.txt", spec::shard_prefix(run_id, shard));
    let http = transfer::transfer_client()?;
    let resp = http
        .get(backend.presign_get(&key, UPLOADED_EXPIRY))
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("fetching the run's uploaded output")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "no uploaded output for {run_id} shard {shard} ({}). The agent uploads it just \
         before the result envelope, so a run killed before it committed has only its \
         container stdout — try `saladfingers logs {run_id}` without --uploaded.",
        resp.status(),
    );
    let body = resp
        .bytes()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading the run's uploaded output")?;
    Ok(body.to_vec())
}

/// `saladfingers logs RUN_ID [--follow] [--limit N] [--all] [--since DUR] [--uploaded]`
pub async fn logs(cfg: Config, args: LogsArgs) -> Result<()> {
    if args.uploaded {
        return uploaded(&cfg, &args).await;
    }
    let client = cfg.client()?;
    let names = match state::load_run(&args.run_id)? {
        Some(run) => run.group_names(),
        None => vec![args.run_id.clone()],
    };
    if args.follow {
        return follow(&client, &names).await;
    }

    let lookback = humantime::parse_duration(&args.since)
        .with_context(|| format!("invalid --since '{}'", args.since))?;
    let lookback = Duration::from_std(lookback).context("--since out of range")?;
    let limit = if args.all { usize::MAX } else { args.limit };

    let now = Utc::now();
    let end = now + Duration::minutes(END_SLACK_MINUTES);
    let start = now - lookback;
    for name in &names {
        match fetch_entries(&client, name, start, end, limit).await {
            // Only a fetch that actually covered the window may say "no entries" —
            // an empty result that stopped on the entry cap or the query budget is a
            // truncation, and the trailer's advice is the right message for it.
            Ok((entries, truncated)) if entries.is_empty() && !truncated => {
                eprintln!("no log entries for {name} (last {})", args.since);
            }
            Ok((entries, truncated)) => {
                for entry in &entries {
                    print_entry(name, entry);
                }
                if truncated {
                    // `--all` already lifted the entry cap, so advising it again would be
                    // advice that does nothing: what stopped this fetch was the query
                    // budget or a window too narrow to split.
                    let remedy = if args.all {
                        "narrow --since — this window needed more queries than one \
                         invocation may spend"
                    } else {
                        "raise --limit, pass --all, or narrow --since"
                    };
                    eprintln!(
                        "… newest {} entries for {name}; older output was cut ({remedy})",
                        entries.len()
                    );
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
    /// Per-poll cap. A job that emits more than this in one lookback window is louder than
    /// a human tail can follow anyway; the bound just keeps one poll from monopolizing the
    /// rate limit.
    const FOLLOW_LIMIT: usize = 2000;

    let mut seen: HashSet<String> = HashSet::new();
    let mut order: VecDeque<String> = VecDeque::new();
    loop {
        let end = Utc::now() + Duration::seconds(30);
        let start = end - Duration::seconds(LOOKBACK + 30);
        for name in names {
            // Paged like the one-shot path: a job logging faster than 100 lines per poll
            // window would otherwise have the overflow silently dropped from the tail.
            match fetch_entries(client, name, start, end, FOLLOW_LIMIT).await {
                Ok((entries, _)) => {
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
