// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers doctor` — validate configuration and connectivity.

use anyhow::{Result, bail};

use crate::cli::DoctorArgs;
use crate::config::{Config, RegistryConfig};
use crate::image::{PUSH_PASS_ENV, PUSH_USER_ENV};
use crate::output::{OutputFormat, print_json, print_table, table};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Ok,
    Warn,
    Fail,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Status::Ok => "OK",
            Status::Warn => "WARN",
            Status::Fail => "FAIL",
        }
    }
}

struct Check {
    name: String,
    status: Status,
    detail: String,
}

impl Check {
    fn ok(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Ok,
            detail: detail.into(),
        }
    }
    fn warn(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Warn,
            detail: detail.into(),
        }
    }
    fn fail(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Fail,
            detail: detail.into(),
        }
    }
}

/// `saladfingers doctor`
pub async fn doctor(cfg: Config, args: DoctorArgs) -> Result<()> {
    let mut checks = vec![
        Check::ok(
            "config",
            format!("org={} project={}", cfg.organization, cfg.project),
        ),
        Check::ok("api key", "resolved"),
    ];

    validate_profiles(&cfg, &mut checks);
    validate_storage(&cfg, &mut checks);
    validate_registry(&cfg, &mut checks);
    validate_build_env(&cfg, &mut checks);

    // Online checks (proves auth + connectivity).
    let client = cfg.client()?;
    match client.get_quotas().await {
        Ok(q) => checks.push(Check::ok(
            "quotas (online)",
            format!(
                "{} of {} replicas available",
                q.replicas_available(),
                q.container_groups_quotas.container_replicas_quota
            ),
        )),
        Err(e) => checks.push(Check::fail("quotas (online)", e.to_string())),
    }
    match client.list_gpu_classes().await {
        Ok(cs) => checks.push(Check::ok(
            "gpu-classes (online)",
            format!("{} classes available", cs.len()),
        )),
        Err(e) => checks.push(Check::fail("gpu-classes (online)", e.to_string())),
    }

    if args.live {
        match live_probe(&cfg).await {
            Ok(report) => checks.push(Check::ok("live probe", crate::probecmd::summary(&report))),
            Err(e) => checks.push(Check::warn("live probe", e.to_string())),
        }
    }

    render(&checks, args.json);

    if checks.iter().any(|c| c.status == Status::Fail) {
        bail!("doctor found problems (see FAIL rows above)");
    }
    Ok(())
}

async fn live_probe(cfg: &Config) -> anyhow::Result<saladfingers_protocol::ProbeReport> {
    let image = crate::probecmd::probe_image(None)?;
    crate::probecmd::run_probe(cfg, crate::cli::DEFAULT_PROBE_GPU_CLASS, &image, "batch").await
}

fn validate_profiles(cfg: &Config, checks: &mut Vec<Check>) {
    if cfg.profiles.is_empty() {
        checks.push(Check::warn(
            "profiles",
            "none defined (add [profiles.<name>] to run with --profile)",
        ));
        return;
    }
    for (name, profile) in &cfg.profiles {
        let mut issues = Vec::new();
        if let Some(priority) = &profile.priority
            && !is_valid_priority(priority)
        {
            issues.push(format!("invalid priority '{priority}'"));
        }
        if profile.gpu_classes.is_empty() {
            issues.push("no gpu_classes".to_string());
        }
        if profile.image.is_none() {
            issues.push("no image".to_string());
        }
        let label = format!("profile '{name}'");
        if issues.is_empty() {
            checks.push(Check::ok(label, "valid"));
        } else {
            checks.push(Check::warn(label, issues.join("; ")));
        }
    }
}

fn validate_storage(cfg: &Config, checks: &mut Vec<Check>) {
    let Some(storage) = &cfg.storage else {
        checks.push(Check::warn(
            "storage",
            "not configured (bulk artifacts unavailable; small objects use S4)",
        ));
        return;
    };
    let mut missing = Vec::new();
    check_env_ref(
        storage.access_key_env.as_deref(),
        "access_key_env",
        &mut missing,
    );
    check_env_ref(
        storage.secret_key_env.as_deref(),
        "secret_key_env",
        &mut missing,
    );
    if missing.is_empty() {
        checks.push(Check::ok(
            "storage",
            format!("{} @ {}", storage.bucket, storage.endpoint),
        ));
    } else {
        checks.push(Check::warn(
            "storage",
            format!("credentials not in environment: {}", missing.join(", ")),
        ));
    }
}

fn validate_registry(cfg: &Config, checks: &mut Vec<Check>) {
    let Some(registry) = &cfg.registry else {
        checks.push(Check::warn(
            "registry",
            "not configured (set [registry] before `image push`)",
        ));
        return;
    };
    let mut missing = Vec::new();
    check_env_ref(
        registry.username_env.as_deref(),
        "username_env",
        &mut missing,
    );
    check_env_ref(
        registry.password_env.as_deref(),
        "password_env",
        &mut missing,
    );
    if missing.is_empty() {
        checks.push(Check::ok("registry", registry.base.clone()));
    } else {
        checks.push(Check::warn(
            "registry",
            format!("credentials not in environment: {}", missing.join(", ")),
        ));
    }
    validate_registry_push(registry, checks);
}

/// Report push-credential resolution separately from the pull pair.
///
/// The two are different roles: the pull credential is handed to SaladCloud nodes
/// so they can fetch the image at deploy time, and is routinely read-only; the
/// push credential is the operator's. `image push` falls back from the second to
/// the first, so a machine holding only pull credentials looked perfectly
/// configured here and then failed at the first layer upload — the check said
/// "registry OK" because, for the role it was checking, it was.
fn validate_registry_push(registry: &RegistryConfig, checks: &mut Vec<Check>) {
    let resolved = |named: Option<&str>, convention: &str| -> bool {
        let set = |var: &str| {
            std::env::var(var)
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
        };
        named.is_some_and(set) || set(convention)
    };
    let user = resolved(registry.push_username_env.as_deref(), PUSH_USER_ENV);
    let pass = resolved(registry.push_password_env.as_deref(), PUSH_PASS_ENV);
    if user && pass {
        checks.push(Check::ok("registry push", "push credentials resolved"));
    } else {
        checks.push(Check::warn(
            "registry push",
            format!(
                "no push credentials — `image push` will fall back to the read-only pull \
                 credential and fail at the first layer upload. Set {PUSH_USER_ENV} / \
                 {PUSH_PASS_ENV}, or point `[registry] push_username_env` / \
                 `push_password_env` at the env vars holding them"
            ),
        ));
    }
}

/// The toolchain `image push` needs. Everything here is a **warning** at worst: a config
/// that can never push images is still perfectly good for `run`, `session`, and `serve`,
/// which shell out to nothing. These checks exist because the failure they predict —
/// building a `.copyTo` for a system this machine cannot execute, or reaching for a skopeo
/// that is not installed — otherwise only surfaces as an opaque error deep into a push.
fn validate_build_env(cfg: &Config, checks: &mut Vec<Check>) {
    for (tool, hint) in [
        ("nix", "required by `image push`"),
        (
            "skopeo",
            "required by `image push` (provided by `nix develop`)",
        ),
    ] {
        match tool_version(tool) {
            Some(v) => checks.push(Check::ok(tool, v)),
            None => checks.push(Check::warn(tool, format!("not on PATH — {hint}"))),
        }
    }

    let system = crate::image::effective_image_system(cfg);
    if let Some(host) = crate::image::configured_build_host(cfg) {
        checks.push(Check::ok(
            "image build",
            format!("on {host} (system {system}) — `[build] host`"),
        ));
        return;
    }

    // A system this machine cannot execute needs a builder; nix only tells us about one
    // via `builders` / `extra-platforms`.
    if crate::image::is_locally_runnable(&system) {
        checks.push(Check::ok("image build", format!("local (system {system})")));
    } else if nix_config_mentions(&system) {
        checks.push(Check::ok(
            "image build",
            format!("system {system} via a configured remote builder"),
        ));
    } else {
        checks.push(Check::warn(
            "image build",
            format!(
                "system {system} is not runnable here and no builder for it appears in \
                 `nix config show` — configure one, use `--on <ssh-host>`, or let the \
                 default native system be used (see docs/macos.md)"
            ),
        ));
    }
}

/// `<tool> --version`'s first line, or `None` if it cannot be run at all.
fn tool_version(tool: &str) -> Option<String> {
    let out = std::process::Command::new(tool)
        .arg("--version")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Some(text.lines().next()?.trim().to_string())
}

/// Whether `nix config show` names `system` among the builders or extra platforms.
/// Deliberately a substring test: the goal is a helpful hint, not an exact model of
/// nix's builder-selection rules.
fn nix_config_mentions(system: &str) -> bool {
    let Ok(out) = std::process::Command::new("nix")
        .args(["config", "show"])
        .output()
    else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.starts_with("builders") || l.starts_with("extra-platforms"))
        .any(|l| l.contains(system))
}

fn check_env_ref(env_name: Option<&str>, field: &str, missing: &mut Vec<String>) {
    match env_name {
        None => missing.push(format!("{field} unset")),
        Some(var)
            if std::env::var(var)
                .map(|v| v.trim().is_empty())
                .unwrap_or(true) =>
        {
            missing.push(format!("${var}"));
        }
        Some(_) => {}
    }
}

fn is_valid_priority(p: &str) -> bool {
    matches!(p, "high" | "medium" | "low" | "batch")
}

fn render(checks: &[Check], json: bool) {
    match OutputFormat::from_json_flag(json) {
        OutputFormat::Json => {
            let rows: Vec<_> = checks
                .iter()
                .map(|c| {
                    serde_json::json!({"check": c.name, "status": c.status.label(), "detail": c.detail})
                })
                .collect();
            let _ = print_json(&rows);
        }
        OutputFormat::Table => {
            let mut t = table(&["check", "status", "detail"]);
            for c in checks {
                t.add_row(vec![
                    c.name.clone(),
                    c.status.label().to_string(),
                    c.detail.clone(),
                ]);
            }
            print_table(&t);
        }
    }
}
