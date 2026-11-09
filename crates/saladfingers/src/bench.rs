// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers bench startup` — measure cold-start time-to-running.
//!
//! Creates N single-replica groups, times each until it reaches `running`, deletes
//! them, and reports percentiles. Requires a pushed image (`--image` or
//! `SALADFINGERS_PROBE_IMAGE`).

use std::time::Instant;

use anyhow::{Context, Result};
use saladfingers_api::{GroupStatus, RestartPolicy};

use crate::cli::BenchStartupArgs;
use crate::config::Config;
use crate::deploy::{self, GroupParams, PollOptions};
use crate::names;
use crate::output::{print_table, table};
use crate::probecmd;

/// `saladfingers bench startup`
pub async fn bench_startup(cfg: Config, args: BenchStartupArgs) -> Result<()> {
    let image = probecmd::probe_image(args.image.as_deref())?;
    let client = cfg.client()?;
    let uuids =
        deploy::resolve_gpu_uuids(&client, std::slice::from_ref(&args.gpu_class), false).await?;
    let priority = deploy::parse_priority(&args.priority)?;

    let mut samples = Vec::new();
    for i in 0..args.count {
        let name = names::generate_run_id();
        let request = deploy::build_request(GroupParams {
            name: name.clone(),
            image: image.clone(),
            gpu_uuids: uuids.clone(),
            priority,
            cpu: 2,
            memory_mb: 4096,
            disk_gib: 20,
            // Keep the container alive so it stays `running` until we delete it.
            // Salad `command` replaces ENTRYPOINT+CMD; absolute path (busybox applet).
            command: Some(vec!["/bin/sleep".into(), "600".into()]),
            env: std::collections::BTreeMap::new(),
            gateway_port: None,
            gateway_auth: false,
            registry_auth: deploy::registry_auth(&cfg),
            restart_policy: RestartPolicy::Never,
            country_codes: vec![],
            shm_mb: None,
        });

        eprintln!("[{}/{}] creating {name}...", i + 1, args.count);
        let created = Instant::now();
        client
            .create_container_group(&request)
            .await
            .context("creating bench group")?;
        let poll = deploy::poll_until_running(
            &client,
            &name,
            &PollOptions {
                quiet: true,
                ..PollOptions::default()
            },
        )
        .await;
        let elapsed = created.elapsed().as_secs_f64();
        // Delete at first running (or on error) to minimize billed time.
        if let Err(e) = deploy::delete_group(&client, &name).await {
            eprintln!("  warning: failed to delete {name}: {e}");
        }
        match poll {
            Ok(r) if r.status == GroupStatus::Running => {
                eprintln!("  running in {elapsed:.0}s");
                samples.push(elapsed);
            }
            Ok(r) => eprintln!("  reached {:?}, skipping sample", r.status),
            Err(e) => eprintln!("  poll error: {e}"),
        }
    }

    report_percentiles(&samples);
    Ok(())
}

fn report_percentiles(samples: &[f64]) {
    if samples.is_empty() {
        eprintln!("no successful samples");
        return;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| {
        let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    };
    let mut t = table(&["metric", "seconds"]);
    t.add_row(vec!["samples".to_string(), sorted.len().to_string()]);
    t.add_row(vec!["min".to_string(), format!("{:.0}", sorted[0])]);
    t.add_row(vec!["p50".to_string(), format!("{:.0}", pct(50.0))]);
    t.add_row(vec!["p80".to_string(), format!("{:.0}", pct(80.0))]);
    t.add_row(vec!["p90".to_string(), format!("{:.0}", pct(90.0))]);
    t.add_row(vec![
        "max".to_string(),
        format!("{:.0}", sorted[sorted.len() - 1]),
    ]);
    print_table(&t);
}
