// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers serve …` — inference services backed by `sf-agent serve --proxy`.
//!
//! `up` deploys a single-replica group whose agent reverse-proxies the app behind an
//! `auth=false` gateway (end users reach the app with no Salad key; the app enforces its
//! own auth). `autostop` is a foreground watchdog that polls the agent's `/sf/v1/idle`
//! and stops the group once it has been idle past the timeout.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use saladfingers_api::{RestartPolicy, SaladClient};

use crate::cli::{ServeAutostopArgs, ServeUpArgs, SessionNameArgs};
use crate::config::Config;
use crate::deploy::{self, GroupParams, PollOptions};
use crate::{names, state};

/// The port `sf-agent serve --proxy` binds for the gateway.
const AGENT_PORT: u16 = 8888;
/// How long `up` waits for the app to answer `/sf/v1/ready` before returning a note.
const READY_TIMEOUT: Duration = Duration::from_secs(300);

/// `saladfingers serve up`
pub async fn up(cfg: Config, args: ServeUpArgs) -> Result<()> {
    let client = cfg.client()?;
    let profile = match &args.profile {
        Some(p) => Some(cfg.profile(p)?.clone()),
        None => None,
    };
    let image = crate::image::resolve_deploy_image(
        args.image.as_deref(),
        profile.as_ref().and_then(|p| p.image.as_deref()),
    )?;
    // Before any group is created: a service that cannot pull its own image should
    // cost nothing — and it bills until something stops it.
    deploy::check_registry_auth(&cfg, &image)?;
    let mut gpu_classes = args.gpu_classes.clone();
    if gpu_classes.is_empty() {
        gpu_classes = profile
            .as_ref()
            .map(|p| p.gpu_classes.clone())
            .unwrap_or_default();
    }
    if gpu_classes.is_empty() {
        bail!("no GPU class (pass --gpu-class or set gpu_classes in the profile)");
    }
    let priority_str = args
        .priority
        .clone()
        .or_else(|| profile.as_ref().and_then(|p| p.priority.clone()))
        .unwrap_or_else(|| "batch".to_string());
    let priority = deploy::parse_priority(&priority_str)?;

    let name = args.name.clone().unwrap_or_else(names::generate_run_id);
    let token = random_token();
    let max_secs = humantime::parse_duration(&args.max_duration)
        .with_context(|| format!("invalid --max-duration {:?}", args.max_duration))?
        .as_secs();

    let uuids = deploy::resolve_gpu_uuids(&client, &gpu_classes, false).await?;
    let mut env = profile.as_ref().map(|p| p.env.clone()).unwrap_or_default();
    env.insert("SF_PORT".into(), AGENT_PORT.to_string());
    env.insert("SF_MAX_DURATION_SECS".into(), max_secs.to_string());
    env.insert("SF_AGENT_TOKEN".into(), token.clone());

    // command = sf-agent serve --proxy --app-port N -- <app argv>
    let mut command = vec![
        "/bin/sf-agent".to_string(),
        "serve".to_string(),
        "--proxy".to_string(),
        "--app-port".to_string(),
        args.app_port.to_string(),
        "--".to_string(),
    ];
    command.extend(args.command.clone());

    let request = deploy::build_request(GroupParams {
        name: name.clone(),
        image: image.clone(),
        gpu_uuids: uuids,
        priority,
        cpu: 4,
        memory_mb: 16384,
        disk_gib: 30,
        command: Some(command),
        env,
        gateway_port: Some(AGENT_PORT),
        gateway_auth: false, // end users need no Salad key; the app enforces its own auth
        registry_auth: deploy::registry_auth(&cfg),
        restart_policy: RestartPolicy::Never,
        country_codes: vec![],
        shm_mb: None,
    });

    eprintln!("bringing up service {name} on {gpu_classes:?} (priority {priority:?})...");
    client.create_container_group(&request).await?;

    let run = state::RunState {
        v: 1,
        run_id: name.clone(),
        kind: "serve".into(),
        created_at: chrono::Utc::now(),
        org: cfg.organization.clone(),
        project: cfg.project.clone(),
        profile: args.profile.clone(),
        image: Some(image),
        gpu_classes: gpu_classes.clone(),
        gpu_observed: None,
        priority: Some(priority_str),
        command: args.command.clone(),
        output_names: None,
        max_parts: None,
        checkpoint_prefix: None,
        groups: vec![state::GroupRef {
            name: name.clone(),
            shard: 0,
            last_state: Some("creating".into()),
            machine_history: vec![],
            running_spans: vec![],
        }],
        status: "creating".into(),
        agent_token: Some(token),
        max_duration_secs: Some(max_secs),
        result: None,
    };
    state::save_run(&run)?;

    // A failure between create and ready must not leak the group — it is live and
    // billing (or about to be) with nothing to show for it.
    let gateway = match async {
        deploy::poll_until_running(&client, &name, &PollOptions::default()).await?;
        gateway_of(&client, &name).await
    }
    .await
    {
        Ok(g) => g,
        Err(e) => {
            eprintln!("service {name} failed to come up; deleting the group to stop billing");
            let _ = deploy::delete_group(&client, &name).await;
            let mut run = run;
            run.status = "failed".into();
            let _ = state::save_run(&run);
            return Err(e);
        }
    };

    // Poll readiness; the app may still be loading (e.g. weights).
    let ready = wait_ready(&gateway, READY_TIMEOUT).await;
    let mut run = run;
    run.status = if ready {
        "running".into()
    } else {
        "starting".into()
    };
    if let Some(g) = run.groups.first_mut() {
        g.last_state = Some(run.status.clone());
    }
    state::save_run(&run)?;

    if ready {
        println!("service {name} ready at {gateway}");
    } else {
        println!(
            "service {name} deployed at {gateway} (app still starting — check `serve status {name}`; \
             if the gateway keeps returning 503, the app is probably not accepting connections \
             on 127.0.0.1:{} yet)",
            args.app_port
        );
    }
    println!("  autostop: saladfingers serve autostop {name} --idle-timeout 30m");
    println!("  down:     saladfingers serve down {name}");
    Ok(())
}

/// `saladfingers serve status NAME`
pub async fn status(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    let group = client.get_container_group(&args.name).await?;
    let gateway = group.gateway_url();
    let st = group
        .status()
        .map_or_else(|| "unknown".to_string(), |s| format!("{s:?}"));
    println!("service {}", args.name);
    println!("  state:   {st}");
    if let Some(url) = &gateway {
        println!("  gateway: {url}");
        // The idle poll below presents the service's agent token, so the
        // credential-safe builder applies (the ready probe sends nothing).
        let http = saladfingers_protocol::transfer::credentialed_client_builder().build()?;
        let ready = matches!(
            http.get(format!("{url}/sf/v1/ready")).send().await,
            Ok(r) if r.status().is_success()
        );
        println!("  ready:   {ready}");
        if let Some(token) = state::load_run(&args.name)?.and_then(|r| r.agent_token)
            && let Ok(resp) = http
                .get(format!("{url}/sf/v1/idle"))
                .bearer_auth(&token)
                .send()
                .await
            && let Ok(v) = resp.json::<serde_json::Value>().await
            && let Some(idle) = v.get("idle_secs").and_then(serde_json::Value::as_u64)
        {
            println!("  idle:    {idle}s");
        }
    }
    Ok(())
}

/// `saladfingers serve autostop NAME --idle-timeout 30m` — foreground idle watchdog.
pub async fn autostop(cfg: Config, args: ServeAutostopArgs) -> Result<()> {
    let client = cfg.client()?;
    let run = state::load_run(&args.name)?
        .filter(|r| r.kind == "serve")
        .with_context(|| format!("no such service {}", args.name))?;
    let token = run
        .agent_token
        .context("service has no agent token in local state")?;
    let timeout = humantime::parse_duration(&args.idle_timeout)
        .with_context(|| format!("invalid --idle-timeout {:?}", args.idle_timeout))?
        .as_secs();
    // A per-request timeout is load-bearing here, not a nicety: for `serve` (which, unlike
    // `session`, spawns no reaper) this foreground watchdog is a primary way billing stops.
    // A bare `Client::new()` has no timeout, so one stalled poll — a half-open connection to
    // the gateway that never answers — would block the loop forever and the idle box would
    // bill on. 30 s is well under the gateway's own 100 s cap; a stalled poll just retries.
    // Credential-safe builder because every poll presents the agent token.
    let http = saladfingers_protocol::transfer::credentialed_client_builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    eprintln!(
        "watching {} — will stop after {} idle",
        args.name, args.idle_timeout
    );
    loop {
        let gateway = match gateway_of(&client, &args.name).await {
            Ok(g) => g,
            Err(_) => {
                eprintln!("service {} is gone; watchdog exiting", args.name);
                return Ok(());
            }
        };
        match http
            .get(format!("{gateway}/sf/v1/idle"))
            .bearer_auth(&token)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(resp) => {
                let idle = resp
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("idle_secs").and_then(serde_json::Value::as_u64))
                    .unwrap_or(0);
                if idle >= timeout {
                    eprintln!("idle {idle}s ≥ {timeout}s — stopping {}", args.name);
                    client.stop_container_group(&args.name).await?;
                    if let Some(mut r) = state::load_run(&args.name)? {
                        r.status = "stopped".into();
                        state::save_run(&r)?;
                    }
                    return Ok(());
                }
            }
            Err(e) => eprintln!("idle check failed (will retry): {e}"),
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// `saladfingers serve down NAME` — stop the group (billing ends).
pub async fn down(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    client.stop_container_group(&args.name).await?;
    update_status(&args.name, "stopped")?;
    println!("stopped service {}", args.name);
    Ok(())
}

/// `saladfingers serve resume NAME` — start a stopped group (new node, fresh).
pub async fn resume(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    client.start_container_group(&args.name).await?;
    update_status(&args.name, "creating")?;
    println!("resuming service {} (new node; app reloads)", args.name);
    Ok(())
}

/// `saladfingers serve rm NAME` — delete the group and forget the service.
pub async fn rm(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    deploy::delete_group(&client, &args.name).await?;
    state::delete_run(&args.name)?;
    println!("removed service {}", args.name);
    Ok(())
}

// ---- helpers ----

async fn gateway_of(client: &SaladClient, name: &str) -> Result<String> {
    client
        .get_container_group(name)
        .await?
        .gateway_url()
        .with_context(|| format!("service {name} exposes no gateway yet"))
}

/// Poll `/sf/v1/ready` (no auth — the serve gateway is `auth=false`) until 200 or timeout.
async fn wait_ready(gateway: &str, timeout: Duration) -> bool {
    // Per-request timeout so the `deadline` below is actually honored: a bare
    // `Client::new()` never times out, so a single stalled poll would hang here past the
    // overall timeout instead of falling through to the "still starting" message.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(r) = http.get(format!("{gateway}/sf/v1/ready")).send().await
            && r.status().is_success()
        {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn update_status(name: &str, status: &str) -> Result<()> {
    if let Some(mut run) = state::load_run(name)? {
        run.status = status.to_string();
        state::save_run(&run)?;
    }
    Ok(())
}

/// 256-bit random bearer token as lowercase hex.
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("os rng");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
