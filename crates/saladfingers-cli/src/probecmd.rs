// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers gpu-probe` — run the node environment probe on a rented GPU.
//!
//! Creates a single-replica group with the probe image behind the gateway, polls
//! until it is running, fetches the `ProbeReport` over the gateway, and deletes the
//! group. Live execution requires a pushed probe image (via `--image` or
//! `SALADFINGERS_PROBE_IMAGE`); the group is always cleaned up.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use saladfingers_api::{GroupStatus, RestartPolicy, SaladClient};
use saladfingers_protocol::{GpuVendor, ProbeReport};

use crate::cli::GpuProbeArgs;
use crate::config::Config;
use crate::deploy::{self, GroupParams, PollOptions};
use crate::names;
use crate::output::{OutputFormat, print_json, print_table, table};

/// Resolve the probe image: `explicit` > `SALADFINGERS_PROBE_IMAGE`.
///
/// # Errors
/// Returns an error naming how to set it if neither is present.
pub fn probe_image(explicit: Option<&str>) -> Result<String> {
    let requested = explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var("SALADFINGERS_PROBE_IMAGE")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .context(
            "no probe image (pass --image or set SALADFINGERS_PROBE_IMAGE to a pushed gpu-probe image ref)",
        )?;
    // Pin bare names exactly as the deploy commands do. The fallback chain stays
    // env-based (these are not profile-driven commands), only the resolution is shared.
    Ok(crate::image::resolve_image_ref(&requested))
}

/// Create a probe group, poll, fetch the report, and delete the group.
///
/// # Errors
/// Returns an error on API failure, timeout, or if no report is returned.
pub async fn run_probe(
    cfg: &Config,
    gpu_class: &str,
    image: &str,
    priority: &str,
) -> Result<ProbeReport> {
    let client = cfg.client()?;
    let uuids = deploy::resolve_gpu_uuids(&client, &[gpu_class.to_string()], false).await?;
    let priority = deploy::parse_priority(priority)?;
    let name = names::generate_run_id();

    let request = deploy::build_request(GroupParams {
        name: name.clone(),
        image: image.to_string(),
        gpu_uuids: uuids,
        priority,
        cpu: 2,
        memory_mb: 8192,
        disk_gib: 20,
        // Salad `command` REPLACES the image ENTRYPOINT+CMD — always send full argv.
        command: Some(vec![
            "/bin/sf-agent".into(),
            "probe".into(),
            "--emit".into(),
            "http".into(),
        ]),
        env: std::collections::BTreeMap::new(),
        gateway_port: Some(8000),
        gateway_auth: true,
        registry_auth: deploy::registry_auth(cfg),
        restart_policy: RestartPolicy::Never,
        country_codes: vec![],
        shm_mb: None,
    });

    eprintln!("creating probe group {name} on '{gpu_class}' (priority {priority:?})...");
    client
        .create_container_group(&request)
        .await
        .context("creating probe group")?;

    // Always delete the group, even if fetching the report fails.
    let result = probe_and_fetch(cfg, &client, &name).await;
    eprintln!("deleting probe group {name}...");
    if let Err(e) = deploy::delete_group(&client, &name).await {
        eprintln!("warning: failed to delete probe group {name}: {e}");
    }
    result
}

async fn probe_and_fetch(cfg: &Config, client: &SaladClient, name: &str) -> Result<ProbeReport> {
    let poll = deploy::poll_until_running(client, name, &PollOptions::default()).await?;
    if poll.status != GroupStatus::Running {
        bail!("probe group reached {:?}, not running", poll.status);
    }
    let group = client.get_container_group(name).await?;
    let gateway = group
        .gateway_url()
        .context("probe group exposed no gateway URL")?;
    fetch_report(cfg, &gateway).await
}

async fn fetch_report(cfg: &Config, gateway: &str) -> Result<ProbeReport> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    // The app may take a moment to bind after `running`; poll the gateway briefly.
    for _ in 0..20 {
        if let Ok(resp) = http
            .get(gateway)
            .header("Salad-Api-Key", cfg.api_key.expose())
            .send()
            .await
            && resp.status().is_success()
            && let Ok(report) = resp.json::<ProbeReport>().await
        {
            return Ok(report);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    bail!("gateway did not return a probe report in time")
}

/// `saladfingers gpu-probe`
pub async fn gpu_probe(cfg: Config, args: GpuProbeArgs) -> Result<()> {
    let image = probe_image(args.image.as_deref())?;
    let report = run_probe(&cfg, &args.gpu_class, &image, "batch").await?;
    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&report)?,
        OutputFormat::Table => render(&report),
    }
    Ok(())
}

/// A one-line summary of a probe report, used by `doctor --live`.
#[must_use]
pub fn summary(report: &ProbeReport) -> String {
    format!(
        "gpu={} driver={} imds={} s4-jwt={}",
        report.gpu_name.as_deref().unwrap_or("?"),
        report.driver_version.as_deref().unwrap_or("?"),
        opt_bool(report.imds_reachable),
        opt_bool(report.s4_jwt_upload_ok),
    )
}

fn render(report: &ProbeReport) {
    let mut t = table(&["field", "value"]);
    let vendor = match report.gpu_vendor {
        GpuVendor::Nvidia => "nvidia",
        GpuVendor::Amd => "amd",
        GpuVendor::None => "none",
    };
    let rows = [
        ("gpu vendor", vendor.to_string()),
        (
            "gpu name",
            report.gpu_name.clone().unwrap_or_else(|| "-".into()),
        ),
        (
            "driver",
            report.driver_version.clone().unwrap_or_else(|| "-".into()),
        ),
        (
            "vram (MiB)",
            report.vram_mb.map_or_else(|| "-".into(), |v| v.to_string()),
        ),
        (
            "down mbps",
            report
                .measured_down_mbps
                .map_or_else(|| "-".into(), |v| format!("{v:.1}")),
        ),
        ("imds reachable", opt_bool(report.imds_reachable)),
        ("s4 jwt upload", opt_bool(report.s4_jwt_upload_ok)),
        ("gpu libraries", report.library_paths.len().to_string()),
        (
            "tools",
            report.tools.keys().cloned().collect::<Vec<_>>().join(", "),
        ),
    ];
    for (k, v) in rows {
        t.add_row(vec![k.to_string(), v]);
    }
    print_table(&t);
    for note in &report.notes {
        eprintln!("note: {note}");
    }
}

fn opt_bool(v: Option<bool>) -> String {
    match v {
        Some(true) => "yes".into(),
        Some(false) => "no".into(),
        None => "-".into(),
    }
}
