// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers doctor` — validate configuration and connectivity.

use anyhow::{Result, bail};

use crate::cli::DoctorArgs;
use crate::config::Config;
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
