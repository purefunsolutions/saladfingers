// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Container-group deployment lifecycle: resolve GPU classes, create, poll to
//! running, and delete. Shared by `gpu-probe`, `bench`, and (in M4) `run`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use saladfingers_api::{
    BasicAuth, ContainerPriority, CreateContainer, CreateContainerGroup, GroupStatus, Instance,
    InstanceState, Networking, RegistryAuthentication, Resources, RestartPolicy, SaladClient,
};

use crate::commands;
use crate::config::Config;
use crate::state;

/// GPU-class cache TTL (hours).
const CACHE_TTL_HOURS: i64 = 24;

/// Resolve GPU-class names or UUIDs to UUIDs via the cached class list.
///
/// # Errors
/// Returns an error if the class list cannot be fetched or a name does not match.
pub async fn resolve_gpu_uuids(
    client: &SaladClient,
    classes: &[String],
    refresh: bool,
) -> Result<Vec<String>> {
    let available = state::cached_gpu_classes(client, refresh, CACHE_TTL_HOURS).await?;
    let mut uuids = Vec::with_capacity(classes.len());
    for name in classes {
        let class = commands::resolve_gpu_class(&available, name)
            .map_err(|e| anyhow::anyhow!("GPU class '{name}': {e}"))?;
        uuids.push(class.id.clone());
    }
    Ok(uuids)
}

/// Best-effort hourly USD price for `class_name` at `priority`, from the cached
/// GPU-class list. Returns `None` (rather than erroring) if the class or its price
/// can't be resolved — cost estimation is advisory, never a hard failure.
pub async fn gpu_hourly_price(
    client: &SaladClient,
    class_name: &str,
    priority: ContainerPriority,
) -> Option<rust_decimal::Decimal> {
    let available = state::cached_gpu_classes(client, false, CACHE_TTL_HOURS)
        .await
        .ok()?;
    let class = commands::resolve_gpu_class(&available, class_name).ok()?;
    class.price(priority)
}

/// Parse a priority string into a [`ContainerPriority`].
///
/// # Errors
/// Returns an error for an unrecognized priority.
pub fn parse_priority(s: &str) -> Result<ContainerPriority> {
    match s.to_ascii_lowercase().as_str() {
        "high" => Ok(ContainerPriority::High),
        "medium" => Ok(ContainerPriority::Medium),
        "low" => Ok(ContainerPriority::Low),
        "batch" => Ok(ContainerPriority::Batch),
        other => bail!("invalid priority '{other}' (expected high|medium|low|batch)"),
    }
}

/// Build `registry_authentication` from the config's `[registry]` section, if set.
#[must_use]
pub fn registry_auth(cfg: &Config) -> Option<RegistryAuthentication> {
    let registry = cfg.registry.as_ref()?;
    let user = registry
        .username_env
        .as_ref()
        .and_then(|e| std::env::var(e).ok())?;
    let password = registry
        .password_env
        .as_ref()
        .and_then(|e| std::env::var(e).ok())?;
    Some(RegistryAuthentication {
        basic: Some(BasicAuth {
            username: user,
            password,
        }),
        docker_hub: None,
    })
}

/// Parameters for a probe/session-style single-replica group.
pub struct GroupParams {
    /// Group name.
    pub name: String,
    /// Image reference.
    pub image: String,
    /// GPU class UUIDs.
    pub gpu_uuids: Vec<String>,
    /// Priority.
    pub priority: ContainerPriority,
    /// vCPU count.
    pub cpu: u32,
    /// RAM in MB.
    pub memory_mb: u32,
    /// Disk in GiB.
    pub disk_gib: u64,
    /// Command override (argv), or `None` to use the image default.
    pub command: Option<Vec<String>>,
    /// Extra environment.
    pub env: BTreeMap<String, String>,
    /// Gateway port to expose, or `None` for no gateway.
    pub gateway_port: Option<u16>,
    /// Whether the gateway requires the Salad key (`auth=true`). Sessions/probes use
    /// `true`; inference `serve` uses `false` so end users need no Salad key.
    pub gateway_auth: bool,
    /// Registry auth.
    pub registry_auth: Option<RegistryAuthentication>,
    /// Restart policy.
    pub restart_policy: RestartPolicy,
    /// Country allow-list (ISO alpha-2, lowercase); empty = anywhere.
    pub country_codes: Vec<String>,
    /// `/dev/shm` size in MB, when the workload needs one (PyTorch DataLoader workers
    /// SIGBUS on the default tiny shm).
    pub shm_mb: Option<u32>,
}

/// Build a create-container-group request for a single-replica group.
#[must_use]
pub fn build_request(params: GroupParams) -> CreateContainerGroup {
    let networking = params.gateway_port.map(|port| Networking {
        auth: params.gateway_auth,
        port,
        protocol: "http".to_string(),
        load_balancer: None,
        single_connection_limit: None,
        client_request_timeout: None,
        server_response_timeout: None,
    });
    let mut resources = Resources::gpu(
        params.cpu,
        params.memory_mb,
        params.gpu_uuids,
        params.disk_gib,
    );
    resources.shm_size = params.shm_mb;
    CreateContainerGroup {
        name: params.name,
        display_name: None,
        autostart_policy: true,
        replicas: 1,
        restart_policy: params.restart_policy,
        container: CreateContainer {
            image: params.image,
            resources,
            command: params.command,
            environment_variables: params.env,
            priority: params.priority,
            image_caching: Some(true),
            registry_authentication: params.registry_auth,
        },
        networking,
        country_codes: (!params.country_codes.is_empty()).then_some(params.country_codes),
    }
}

/// How to poll a group toward running.
pub struct PollOptions {
    /// Give up after this long.
    pub timeout: Duration,
    /// Interval between polls.
    pub interval: Duration,
    /// Suppress per-transition stderr logging.
    pub quiet: bool,
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(20 * 60),
            interval: Duration::from_secs(5),
            quiet: false,
        }
    }
}

/// The result of polling a group.
pub struct PollResult {
    /// Terminal (or running) status reached.
    pub status: GroupStatus,
    /// State-transition timestamps observed.
    pub transitions: Vec<(String, DateTime<Utc>)>,
}

/// Poll a group until it reaches `running` (success) or a terminal state.
///
/// # Errors
/// Returns an error on API failure or on timeout.
pub async fn poll_until_running(
    client: &SaladClient,
    name: &str,
    opts: &PollOptions,
) -> Result<PollResult> {
    let start = Instant::now();
    let mut transitions = Vec::new();
    let mut last_key = String::new();

    loop {
        let group = client.get_container_group(name).await?;
        let status = group.status().unwrap_or(GroupStatus::Unknown);
        let instances = client.list_instances(name).await.unwrap_or_default();
        let detail = describe(status, &instances);
        let key = format!("{status:?}|{detail}");
        if key != last_key {
            let now = Utc::now();
            transitions.push((detail.clone(), now));
            if !opts.quiet {
                eprintln!("  {}  {detail}", now.format("%H:%M:%S"));
            }
            last_key = key;
        }

        match status {
            GroupStatus::Running => {
                return Ok(PollResult {
                    status,
                    transitions,
                });
            }
            GroupStatus::Failed | GroupStatus::Stopped | GroupStatus::Succeeded => {
                return Ok(PollResult {
                    status,
                    transitions,
                });
            }
            _ => {}
        }
        if start.elapsed() > opts.timeout {
            bail!(
                "timed out after {:?} waiting for '{name}' to reach running",
                opts.timeout
            );
        }
        tokio::time::sleep(opts.interval).await;
    }
}

/// Delete a group (idempotent).
///
/// # Errors
/// Returns an error on API failure.
pub async fn delete_group(client: &SaladClient, name: &str) -> Result<()> {
    client
        .delete_container_group(name)
        .await
        .map_err(Into::into)
}

/// A one-line description of a group's progress: the instance state plus pulling
/// progress while downloading (e.g. `downloading 42%`), else the group status.
pub(crate) fn describe(status: GroupStatus, instances: &[Instance]) -> String {
    if let Some(inst) = instances.first() {
        let state = inst.state.map(instance_state_label).unwrap_or("");
        if let Some(progress) = inst.pulling_progress {
            return format!("{state} {progress:.0}%");
        }
        if !state.is_empty() {
            return state.to_string();
        }
    }
    format!("{status:?}").to_lowercase()
}

fn instance_state_label(s: InstanceState) -> &'static str {
    match s {
        InstanceState::Allocating => "allocating",
        InstanceState::Downloading => "downloading",
        InstanceState::Creating => "creating",
        InstanceState::Running => "running",
        InstanceState::Stopping => "stopping",
        InstanceState::Unknown => "unknown",
    }
}
