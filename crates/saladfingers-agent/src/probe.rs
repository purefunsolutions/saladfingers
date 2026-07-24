// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `sf-agent probe` — report GPU/driver/environment facts about a rented node.
//!
//! Synchronous, dependency-free detection (env, PATH tools, well-known library
//! paths, `nvidia-smi` query) plus the async probes: a bandwidth measurement, IMDS
//! reachability, an S4 upload using the IMDS workload JWT, and HTTP serving of the
//! report over the gateway on `[::]:8000`.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use clap::Args;
use reqwest::header::CONTENT_TYPE;
use saladfingers_api::{S4Auth, S4Client, Secret};
use saladfingers_protocol::{GpuVendor, ProbeReport};

use crate::imds::ImdsClient;

/// Standard directories where the NVIDIA/AMD container runtimes inject libraries.
const LIB_DIRS: &[&str] = &[
    "/usr/lib/x86_64-linux-gnu",
    "/usr/lib64",
    "/usr/local/nvidia/lib",
    "/usr/local/nvidia/lib64",
    "/usr/local/cuda/lib64",
    "/opt/rocm/lib",
];

/// GPU libraries to look for.
const LIB_NAMES: &[&str] = &[
    "libcuda.so.1",
    "libnvidia-ml.so.1",
    "libnvidia-ptxjitcompiler.so.1",
    "libamdhip64.so",
    "libhsa-runtime64.so.1",
    "librocm_smi64.so",
];

/// Vendor tools to resolve on `PATH`.
const TOOLS: &[&str] = &["nvidia-smi", "rocminfo", "amd-smi", "rocm-smi"];

#[derive(Debug, Args)]
pub struct ProbeArgs {
    /// Where to emit the report: `stdout` or `http` (serve over the gateway).
    #[arg(long, default_value = "stdout")]
    pub emit: String,
    /// Bandwidth probe size in MB (0 disables). Downloads from `SF_BANDWIDTH_URL`
    /// or, if unset, a public speed-test endpoint.
    #[arg(long, env = "SF_PROBE_BANDWIDTH_MB", default_value_t = 32)]
    pub bandwidth_mb: u64,
}

/// Run the probe: collect a report and emit it to stdout or over HTTP.
///
/// # Errors
/// Returns an error only if HTTP serving fails to bind or serve.
pub async fn run(args: ProbeArgs) -> Result<()> {
    let report = collect_extended(&args).await;
    if args.emit == "http" {
        serve_http(report).await
    } else {
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
}

/// Collect the synchronous report, then add the async probes.
async fn collect_extended(args: &ProbeArgs) -> ProbeReport {
    let mut report = collect();

    if args.bandwidth_mb > 0 {
        let wanted = args.bandwidth_mb * 1024 * 1024;
        // Default to a public speed-test endpoint so the probe always measures the
        // node's downlink. Override SF_BANDWIDTH_URL with a presigned storage GET to
        // measure the node→storage path the data plane actually uses.
        let url = std::env::var("SF_BANDWIDTH_URL")
            .unwrap_or_else(|_| format!("https://speed.cloudflare.com/__down?bytes={wanted}"));
        match measure_download(&url, args.bandwidth_mb).await {
            Ok(mbps) => report.measured_down_mbps = Some(mbps),
            Err(e) => report.notes.push(format!("bandwidth probe failed: {e}")),
        }
    }

    probe_imds_and_s4(&mut report).await;
    report
}

/// Check IMDS reachability and attempt an S4 upload with the workload JWT (E9).
async fn probe_imds_and_s4(report: &mut ProbeReport) {
    let Ok(imds) = ImdsClient::new() else {
        return;
    };
    if imds.status().await.is_err() {
        report.imds_reachable = Some(false);
        return;
    }
    report.imds_reachable = Some(true);

    let (Ok(token), Ok(org)) = (imds.token().await, std::env::var("SALAD_ORGANIZATION_NAME"))
    else {
        return;
    };
    match try_s4_upload(&token, &org).await {
        Ok(()) => report.s4_jwt_upload_ok = Some(true),
        Err(e) => {
            report.s4_jwt_upload_ok = Some(false);
            report.notes.push(format!("S4 JWT upload failed: {e}"));
        }
    }
}

async fn try_s4_upload(token: &str, org: &str) -> Result<()> {
    let client = S4Client::production(org, S4Auth::Bearer(Secret::new(token)))?;
    let machine = std::env::var("SALAD_MACHINE_ID").unwrap_or_else(|_| "unknown".to_string());
    let body = bytes::Bytes::from_static(b"{\"probe\":true}");
    client
        .upload(&format!("probe/{machine}.json"), body, "application/json")
        .await?;
    Ok(())
}

async fn measure_download(url: &str, mb: u64) -> Result<f64> {
    let wanted = mb * 1024 * 1024;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    let start = Instant::now();
    let mut resp = client
        .get(url)
        .header("Range", format!("bytes=0-{}", wanted.saturating_sub(1)))
        .send()
        .await?
        .error_for_status()?;
    let mut total: u64 = 0;
    while let Some(chunk) = resp.chunk().await? {
        total += chunk.len() as u64;
        if total >= wanted {
            break;
        }
    }
    let secs = start.elapsed().as_secs_f64().max(0.001);
    Ok((total as f64 * 8.0) / secs / 1e6)
}

/// Serve the report over the gateway on `[::]:8000` until SIGTERM. Optionally pushes
/// it to `SF_CALLBACK_URL` first.
async fn serve_http(report: ProbeReport) -> Result<()> {
    let port: u16 = std::env::var("SF_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8000);

    if let Ok(callback) = std::env::var("SF_CALLBACK_URL")
        && let Err(e) = reqwest::Client::new()
            .post(&callback)
            .json(&report)
            .send()
            .await
    {
        tracing::warn!("callback push failed: {e}");
    }

    let json = Arc::new(serde_json::to_string_pretty(&report)?);
    let app = Router::new().fallback(serve_report).with_state(json);
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding [::]:{port}"))?;
    tracing::info!("probe report served on [::]:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn serve_report(State(json): State<Arc<String>>) -> impl IntoResponse {
    ([(CONTENT_TYPE, "application/json")], (*json).clone())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Substrings that mark an environment variable *name* as secret-bearing. Matched
/// case-insensitively against the whole name.
const SECRET_NAME_MARKERS: &[&str] = &[
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASS",
    "CREDENTIAL",
    "JWT",
    "SIGNATURE",
    "AUTH",
    "KEY",
];

/// Replacement for a redacted value.
const REDACTED: &str = "***";

/// The `SALAD_*` variables to put in the report, with secret-shaped values redacted.
///
/// Every `SALAD_*` variable is kept, name included — the probe exists to *discover*
/// what the platform injects, so this is deliberately not an allow-list and nothing is
/// dropped. Only the value is masked, and only when the name matches
/// [`SECRET_NAME_MARKERS`].
///
/// This is future-proofing rather than a fix for a live leak. Today SaladCloud injects
/// only benign identifiers (`SALAD_MACHINE_ID`, `SALAD_CONTAINER_GROUP_NAME`,
/// `SALAD_ORGANIZATION_NAME`, `SALAD_PROJECT_NAME`, `SALAD_REPLICA_ID`), and our own
/// secrets — `SF_AGENT_TOKEN` and `SF_JOB_URL`, a live presigned capability — are
/// `SF_`-prefixed and so never collected. But the report is a wide disclosure surface:
/// it is printed to stdout (container logs, ~90-day retention), served unauthenticated
/// over the gateway, and optionally POSTed to `SF_CALLBACK_URL`. A `SALAD_`-prefixed
/// secret in some future platform release must not land there verbatim.
fn collect_salad_env(vars: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, String> {
    vars.into_iter()
        .filter(|(key, _)| key.starts_with("SALAD_"))
        .map(|(key, value)| {
            if is_secret_name(&key) {
                (key, REDACTED.to_string())
            } else {
                (key, value)
            }
        })
        .collect()
}

/// Whether an environment variable name looks like it carries a secret.
fn is_secret_name(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    SECRET_NAME_MARKERS
        .iter()
        .any(|marker| upper.contains(marker))
}

/// Collect a node environment report.
#[must_use]
pub fn collect() -> ProbeReport {
    let mut report = ProbeReport::empty();

    report.salad_env = collect_salad_env(std::env::vars());

    for tool in TOOLS {
        if let Some(path) = which(tool) {
            report.tools.insert((*tool).to_string(), path);
        }
    }

    for dir in LIB_DIRS {
        for name in LIB_NAMES {
            let path = format!("{dir}/{name}");
            if Path::new(&path).exists() {
                report.library_paths.push(path);
            }
        }
    }

    collect_wsl_and_devices(&mut report);
    let drm = drm_vendors();

    let has_nvidia = Path::new("/proc/driver/nvidia/version").exists()
        || report.tools.contains_key("nvidia-smi")
        || report.library_paths.iter().any(|p| p.contains("libcuda"))
        || drm.iter().any(|v| v == PCI_VENDOR_NVIDIA);
    // AMD detection must not rely on `/dev/kfd`: Salad runs AMD nodes under WSL2, where
    // the GPU arrives via `/dev/dxg` and the native `/dev/kfd` is absent (E13). Fall back
    // to the PCI vendor id under `/sys/class/drm`.
    let has_amd = Path::new("/dev/kfd").exists()
        || report.tools.contains_key("rocminfo")
        || report.library_paths.iter().any(|p| p.contains("libamdhip"))
        || drm.iter().any(|v| v == PCI_VENDOR_AMD);

    if has_nvidia {
        report.gpu_vendor = GpuVendor::Nvidia;
        collect_nvidia(&mut report);
    } else if has_amd {
        report.gpu_vendor = GpuVendor::Amd;
        collect_amd(&mut report);
    } else {
        report
            .notes
            .push("no GPU detected (expected for the base gpu-probe image build)".to_string());
    }

    report
}

/// AMD / NVIDIA PCI vendor ids as they appear in `/sys/class/drm/*/device/vendor`.
const PCI_VENDOR_AMD: &str = "0x1002";
const PCI_VENDOR_NVIDIA: &str = "0x10de";

/// Record WSL2 and GPU device-node facts. Salad runs containers under WSL2, so the GPU
/// is exposed via `/dev/dxg` (DirectX paravirt) rather than the native `/dev/kfd` (AMD)
/// or `/dev/nvidia*` (NVIDIA). This dumps what is actually present so the AMD layout can
/// be pinned down (E13).
fn collect_wsl_and_devices(report: &mut ProbeReport) {
    if let Ok(version) = std::fs::read_to_string("/proc/version") {
        let lower = version.to_ascii_lowercase();
        if lower.contains("microsoft") || lower.contains("wsl") {
            report
                .notes
                .push("WSL2 kernel (Salad Enterprise Linux)".to_string());
        }
    }
    for dev in ["/dev/dxg", "/dev/kfd"] {
        if Path::new(dev).exists() {
            report.notes.push(format!("device present: {dev}"));
        }
    }
    if let Ok(entries) = std::fs::read_dir("/dev/dri") {
        let mut names: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        if !names.is_empty() {
            report.notes.push(format!("/dev/dri: {}", names.join(", ")));
        }
    }
    for vendor in drm_vendors() {
        let label = match vendor.as_str() {
            PCI_VENDOR_AMD => "AMD",
            PCI_VENDOR_NVIDIA => "NVIDIA",
            _ => "unknown",
        };
        report
            .notes
            .push(format!("/sys/class/drm GPU vendor {vendor} ({label})"));
    }
    if let Ok(entries) = std::fs::read_dir("/usr/lib/wsl/lib") {
        let mut names: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        if !names.is_empty() {
            report.notes.push(format!(
                "/usr/lib/wsl/lib ({} files): {}",
                names.len(),
                names.join(", ")
            ));
        }
    }
}

/// GPU PCI vendor ids found under `/sys/class/drm/*/device/vendor` (deduplicated).
fn drm_vendors() -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(cards) = std::fs::read_dir("/sys/class/drm") {
        for card in cards.flatten() {
            if let Ok(id) = std::fs::read_to_string(card.path().join("device/vendor")) {
                let id = id.trim().to_string();
                if !id.is_empty() && !out.contains(&id) {
                    out.push(id);
                }
            }
        }
    }
    out
}

/// Query AMD/ROCm details. Runs `rocminfo` when it is present (i.e. the `rocm-runtime`
/// image flavor baked it in). ROCm 7.x has WSL support, so on Salad's WSL2 AMD nodes it
/// enumerates the GPU over `/dev/dxg` even though `/dev/kfd` is absent (E13).
fn collect_amd(report: &mut ProbeReport) {
    let Some(rocminfo) = report.tools.get("rocminfo").cloned() else {
        report.notes.push(
            "AMD GPU present but no ROCm userspace — bake the rocm-runtime image flavor"
                .to_string(),
        );
        return;
    };
    match output_with_timeout(Command::new(&rocminfo), TOOL_TIMEOUT) {
        Some(Ok(out)) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            report.gpu_name = gpu_marketing_name(&text);
            report.smi_output = Some(text.trim().to_string());
        }
        Some(Ok(out)) => {
            let err = String::from_utf8_lossy(&out.stderr);
            report.notes.push(format!(
                "rocminfo exited {:?}: {}",
                out.status.code(),
                err.trim()
            ));
        }
        Some(Err(e)) => report.notes.push(format!("rocminfo failed to run: {e}")),
        None => report
            .notes
            .push(format!("rocminfo hung; killed after {TOOL_TIMEOUT:?}")),
    }
}

/// The GPU agent's marketing name from `rocminfo` output. rocminfo lists the CPU agent
/// first, so we return the `Marketing Name:` of the agent whose `Device Type:` is `GPU`.
fn gpu_marketing_name(rocminfo: &str) -> Option<String> {
    let mut last_marketing: Option<&str> = None;
    for line in rocminfo.lines() {
        let t = line.trim();
        if let Some(name) = t.strip_prefix("Marketing Name:") {
            last_marketing = Some(name.trim());
        } else if t.starts_with("Device Type:")
            && t.contains("GPU")
            && let Some(name) = last_marketing
        {
            return Some(name.to_string());
        }
    }
    None
}

fn collect_nvidia(report: &mut ProbeReport) {
    if let Ok(version) = std::fs::read_to_string("/proc/driver/nvidia/version")
        && let Some(line) = version.lines().next()
    {
        report.notes.push(line.trim().to_string());
    }
    let Some(smi) = report.tools.get("nvidia-smi").cloned() else {
        return;
    };
    let mut cmd = Command::new(&smi);
    cmd.args([
        "--query-gpu=name,memory.total,driver_version",
        "--format=csv,noheader,nounits",
    ]);
    let output = output_with_timeout(cmd, TOOL_TIMEOUT);
    if output.is_none() {
        report
            .notes
            .push(format!("nvidia-smi hung; killed after {TOOL_TIMEOUT:?}"));
    }
    if let Some(Ok(out)) = output
        && out.status.success()
    {
        let text = String::from_utf8_lossy(&out.stdout);
        report.smi_output = Some(text.trim().to_string());
        if let Some(first) = text.lines().next() {
            let fields: Vec<&str> = first.split(',').map(str::trim).collect();
            if let Some(name) = fields.first() {
                report.gpu_name = Some((*name).to_string());
            }
            if let Some(mem) = fields.get(1).and_then(|m| m.parse::<u64>().ok()) {
                report.vram_mb = Some(mem);
            }
            if let Some(driver) = fields.get(2) {
                report.driver_version = Some((*driver).to_string());
            }
        }
    }
}

/// Hard cap on a vendor tool's runtime. `collect_node_info` runs at boot, BEFORE the
/// gate/inputs/exec and before any envelope exists — a wedged `nvidia-smi` on a broken
/// consumer/WSL2 driver would otherwise hang the whole run silently while it bills, up
/// to the reap hard cap. Better to degrade to `gpu_name: None` after a bounded wait.
const TOOL_TIMEOUT: Duration = Duration::from_secs(10);

/// `Command::output()` with a hard timeout: the child is killed at the deadline and
/// `None` is returned (also on spawn failure). Reader threads drain the pipes so a
/// chatty child (e.g. `nvidia-smi -q -x` > 64 KiB) can't deadlock on a full pipe.
fn output_with_timeout(
    mut cmd: Command,
    timeout: Duration,
) -> Option<std::io::Result<std::process::Output>> {
    use std::io::Read;
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return Some(Err(e)),
    };
    let mut stdout = child.stdout.take()?;
    let mut stderr = child.stderr.take()?;
    let out_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = stdout.read_to_end(&mut v);
        v
    });
    let err_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = stderr.read_to_end(&mut v);
        v
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => {
                let _ = child.kill();
                return Some(Err(e));
            }
        }
    };
    Some(Ok(std::process::Output {
        status,
        stdout: out_reader.join().unwrap_or_default(),
        stderr: err_reader.join().unwrap_or_default(),
    }))
}

/// Minimal `PATH` search for an executable.
fn which(name: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_runs_and_reports_no_gpu_on_ci() {
        let report = collect();
        // On a CI runner there is no GPU; the probe must not panic and must produce
        // a well-formed report.
        assert_eq!(report.v, saladfingers_protocol::PROTOCOL_VERSION);
        let _ = serde_json::to_string(&report).expect("serializable");
    }

    /// Build the `(String, String)` pairs `collect_salad_env` consumes.
    fn pairs(vars: &[(&str, &str)]) -> Vec<(String, String)> {
        vars.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn salad_env_redacts_secret_shaped_names_and_keeps_the_rest() {
        let env = collect_salad_env(pairs(&[
            ("SALAD_MACHINE_ID", "abc"),
            ("SALAD_ORGANIZATION_NAME", "acme"),
            ("SALAD_API_KEY", "supersecret"),
            ("SALAD_SESSION_TOKEN", "tok"),
            ("SF_AGENT_TOKEN", "nope"),
        ]));

        // Benign values pass through verbatim.
        assert_eq!(env.get("SALAD_MACHINE_ID").map(String::as_str), Some("abc"));
        assert_eq!(
            env.get("SALAD_ORGANIZATION_NAME").map(String::as_str),
            Some("acme")
        );

        // Secret-shaped names keep the name but lose the value.
        assert_eq!(env.get("SALAD_API_KEY").map(String::as_str), Some(REDACTED));
        assert_eq!(
            env.get("SALAD_SESSION_TOKEN").map(String::as_str),
            Some(REDACTED)
        );

        // Nothing outside the SALAD_ prefix is collected at all.
        assert!(!env.contains_key("SF_AGENT_TOKEN"));
        assert_eq!(env.len(), 4, "every SALAD_ var is kept: {env:?}");

        // No raw secret survives anywhere in the map — not as a key, not as a value.
        let flat = format!("{env:?}");
        assert!(
            !flat.contains("supersecret"),
            "leaked SALAD_API_KEY: {flat}"
        );
        assert!(!flat.contains("tok"), "leaked SALAD_SESSION_TOKEN: {flat}");
        assert!(!flat.contains("nope"), "leaked SF_AGENT_TOKEN: {flat}");
    }

    #[test]
    fn salad_env_does_not_over_redact_the_variables_salad_actually_injects() {
        // The five variables SaladCloud injects today are all benign identifiers; the
        // probe's whole purpose is to report them, so none may be masked.
        let real = [
            ("SALAD_MACHINE_ID", "m-1"),
            ("SALAD_CONTAINER_GROUP_NAME", "cg"),
            ("SALAD_ORGANIZATION_NAME", "org"),
            ("SALAD_PROJECT_NAME", "proj"),
            ("SALAD_REPLICA_ID", "r-1"),
        ];
        let env = collect_salad_env(pairs(&real));
        for (key, value) in real {
            assert_eq!(
                env.get(key).map(String::as_str),
                Some(value),
                "{key} must not be redacted"
            );
        }
    }

    #[test]
    fn gpu_marketing_name_picks_the_gpu_agent_not_the_cpu() {
        // Real `rocminfo` layout from a Salad RX 7800 XT (WSL2): CPU agent first, GPU
        // second. The parser must skip the CPU and return the GPU's marketing name.
        let rocminfo = "\
HSA Agents
*******
Agent 1
*******
  Name:                    12th Gen Intel(R) Core(TM) i7-12700KF
  Marketing Name:          12th Gen Intel(R) Core(TM) i7-12700KF
  Device Type:             CPU
*******
Agent 2
*******
  Name:                    gfx1101
  Marketing Name:          AMD Radeon RX 7800 XT
  Device Type:             GPU
";
        assert_eq!(
            gpu_marketing_name(rocminfo).as_deref(),
            Some("AMD Radeon RX 7800 XT")
        );
        assert_eq!(gpu_marketing_name("no agents here"), None);
    }

    #[test]
    fn output_with_timeout_kills_a_hung_tool_and_passes_a_quick_one() {
        // A wedged vendor tool must be killed at the deadline, not hang the boot.
        let start = Instant::now();
        let hung = output_with_timeout(
            {
                let mut c = Command::new("sleep");
                c.arg("30");
                c
            },
            Duration::from_millis(300),
        );
        assert!(hung.is_none(), "hung tool must yield None");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "must not wait out the child"
        );

        // A quick tool passes through with its output intact.
        let ok = output_with_timeout(
            {
                let mut c = Command::new("echo");
                c.arg("hello");
                c
            },
            Duration::from_secs(10),
        );
        let out = ok.expect("completed").expect("spawned");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hello");
    }
}
