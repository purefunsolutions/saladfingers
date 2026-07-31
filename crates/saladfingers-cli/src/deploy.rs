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
    BasicAuth, ContainerPriority, CreateContainer, CreateContainerGroup, GpuClass, GroupStatus,
    Instance, InstanceState, Networking, RegistryAuthentication, Resources, RestartPolicy,
    SaladClient,
};

use crate::commands;
use crate::config::Config;
use crate::state;

/// GPU-class cache TTL (hours).
const CACHE_TTL_HOURS: i64 = 24;

/// Resolve GPU-class names or UUIDs to the full classes via the cached class list.
///
/// The canonical `name` on each result is what a raw-UUID request cannot supply itself —
/// callers that want to reason about *what* was asked for (e.g. which vendor's query
/// tool the node will answer to) reason about these, never the caller-typed strings.
///
/// # Errors
/// Returns an error if the class list cannot be fetched or a name does not match.
pub async fn resolve_gpu_classes(
    client: &SaladClient,
    classes: &[String],
    refresh: bool,
) -> Result<Vec<GpuClass>> {
    // A CPU-only request names no class, so it must not depend on the class list being
    // fetchable: with a cold cache and the API down, `?` below would fail a run that
    // asked nothing about GPUs.
    if classes.is_empty() {
        return Ok(Vec::new());
    }
    let available = state::cached_gpu_classes(client, refresh, CACHE_TTL_HOURS).await?;
    let mut resolved = Vec::with_capacity(classes.len());
    for name in classes {
        let class = commands::resolve_gpu_class(&available, name)
            .map_err(|e| anyhow::anyhow!("GPU class '{name}': {e}"))?;
        resolved.push(class.clone());
    }
    Ok(resolved)
}

/// Resolve GPU-class names or UUIDs to UUIDs via the cached class list.
///
/// # Errors
/// Returns an error if the class list cannot be fetched or a name does not match.
pub async fn resolve_gpu_uuids(
    client: &SaladClient,
    classes: &[String],
    refresh: bool,
) -> Result<Vec<String>> {
    Ok(resolve_gpu_classes(client, classes, refresh)
        .await?
        .into_iter()
        .map(|c| c.id)
        .collect())
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

/// Fail now if `image` comes from the configured private registry and the
/// credentials to pull it are missing.
///
/// [`registry_auth`] answers `None` when the credentials are unavailable, which creates
/// the group with no authentication at all. The node then fails to pull and SaladCloud
/// reports "Access Denied, Check Permissions" half a minute later, through a
/// channel that carries no log entries and no result envelope. Everything needed
/// to prevent that is local: the config names the env vars, and the environment
/// either holds them or does not.
///
/// This asks exactly the question [`registry_auth`] answers — an unnamed
/// `username_env` counts as missing, not as "nothing to check" — because a check
/// that is satisfied where the builder gives up guards nothing.
///
/// Note the asymmetry this corrects — `image push` already refuses eagerly when
/// its credentials are absent ("no registry push username — set …"). Pull was the
/// silent one.
///
/// # Errors
/// Returns an error naming the env var that is missing, or the config key that
/// never named one.
pub fn check_registry_auth(cfg: &Config, image: &str) -> anyhow::Result<()> {
    check_registry_auth_with(cfg, image, |name| std::env::var(name).ok())
}

fn check_registry_auth_with(
    cfg: &Config,
    image: &str,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<()> {
    let Some(registry) = cfg.registry.as_ref() else {
        return Ok(());
    };
    // Only images from the configured registry need its credentials; a public
    // image pulled from anywhere else must not be blocked by them.
    if !image_is_from(&registry.base, image) {
        return Ok(());
    }
    for (which, var) in [
        ("username", registry.username_env.as_deref()),
        ("password", registry.password_env.as_deref()),
    ] {
        let Some(var) = var else {
            anyhow::bail!(
                "[registry].{which}_env names no environment variable, but the image comes \
                 from the private registry {}. The group would be created with no pull \
                 credentials at all and the node would fail with \"Access Denied, Check \
                 Permissions\"",
                registry.base
            );
        };
        let present = env(var).is_some_and(|v| !v.trim().is_empty());
        anyhow::ensure!(
            present,
            "registry {which} env var ${var} is empty or unset, but the image comes from \
             the private registry {}. The node would fail to pull it with \"Access Denied, \
             Check Permissions\" only after the group had been created",
            registry.base
        );
    }
    Ok(())
}

/// Whether `image` names a repository under `base`.
///
/// A bare `starts_with` is a substring test, not a boundary test: with
/// `base = registry.example.com/org/imgs` it also matches the neighbouring
/// `…/imgs-public/tool`, gating a genuinely public image on private credentials.
fn image_is_from(base: &str, image: &str) -> bool {
    // A trailing slash in the config must not un-gate the registry: with it left in,
    // the stripped remainder starts with the repository name instead of `/` and every
    // image under the base stops matching.
    let base = base.trim_end_matches('/');
    let Some(rest) = image.strip_prefix(base) else {
        return false;
    };
    // The base repository itself (`base`, `base:tag`, `base@digest`) or one below it.
    rest.is_empty() || rest.starts_with(['/', ':', '@'])
}

/// Build `registry_authentication` from the config's `[registry]` section, if set.
#[must_use]
pub fn registry_auth(cfg: &Config) -> Option<RegistryAuthentication> {
    let registry = cfg.registry.as_ref()?;
    let user = env_credential(registry.username_env.as_deref())?;
    let password = env_credential(registry.password_env.as_deref())?;
    Some(RegistryAuthentication {
        basic: Some(BasicAuth {
            username: user,
            password,
        }),
        docker_hub: None,
    })
}

/// A trimmed, non-empty credential from the named variable, or `None`.
///
/// The emptiness rule has to match [`check_registry_auth`]'s: with a bare
/// `std::env::var(e).ok()` an `export REG_USER=` reaches SaladCloud as a basic-auth
/// username of `""`, which the check has already called missing.
fn env_credential(var: Option<&str>) -> Option<String> {
    env_credential_from(var, |name| std::env::var(name).ok())
}

fn env_credential_from(var: Option<&str>, env: impl Fn(&str) -> Option<String>) -> Option<String> {
    env(var?)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
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
    let mut resources = if params.gpu_uuids.is_empty() {
        // `--cpu-only`: say so at the call site rather than leaving a bare `vec![]`
        // for a reader to interpret.
        Resources::cpu_only(params.cpu, params.memory_mb, params.disk_gib)
    } else {
        Resources::gpu(
            params.cpu,
            params.memory_mb,
            params.gpu_uuids,
            params.disk_gib,
        )
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RegistryConfig;

    fn cfg_with_registry() -> Config {
        Config {
            organization: "o".into(),
            project: "p".into(),
            api_key: saladfingers_api::Secret::new("k"),
            storage: None,
            registry: Some(RegistryConfig {
                base: "registry.example.com/org/imgs".into(),
                auth_kind: Some("basic".into()),
                username_env: Some("REG_USER".into()),
                password_env: Some("REG_PASS".into()),
                push_username_env: None,
                push_password_env: None,
            }),
            build: crate::config::BuildConfig::default(),
            defaults: Default::default(),
            profiles: Default::default(),
        }
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |name: &str| {
            owned
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    const PRIVATE: &str = "registry.example.com/org/imgs/kernel-test@sha256:abc";

    #[test]
    fn a_private_image_without_pull_credentials_is_refused_before_submitting() {
        let err = check_registry_auth_with(&cfg_with_registry(), PRIVATE, env_of(&[]))
            .expect_err("must not create a group that cannot pull its image");
        let msg = format!("{err}");
        assert!(
            msg.contains("REG_USER"),
            "should name the missing var: {msg}"
        );
    }

    #[test]
    fn an_empty_credential_counts_as_missing() {
        // `export REG_PASS=` is a real way to get here, and it fails exactly the
        // same way on the node as leaving it unset.
        let env = env_of(&[("REG_USER", "u"), ("REG_PASS", "   ")]);
        let err = check_registry_auth_with(&cfg_with_registry(), PRIVATE, env).unwrap_err();
        assert!(format!("{err}").contains("REG_PASS"));
    }

    #[test]
    fn credentials_present_means_go() {
        let env = env_of(&[("REG_USER", "u"), ("REG_PASS", "p")]);
        assert!(check_registry_auth_with(&cfg_with_registry(), PRIVATE, env).is_ok());
    }

    #[test]
    fn a_public_image_is_not_blocked_by_private_registry_credentials() {
        // The credentials belong to one registry; an image from anywhere else
        // neither needs nor should be gated by them.
        let ok = check_registry_auth_with(
            &cfg_with_registry(),
            "docker.io/library/ubuntu:24.04",
            env_of(&[]),
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn a_neighbouring_repository_is_not_gated_by_the_private_registrys_credentials() {
        // `imgs-public` shares a prefix with `imgs` and nothing else. Gating it
        // would refuse a run that would have pulled perfectly well.
        let ok = check_registry_auth_with(
            &cfg_with_registry(),
            "registry.example.com/org/imgs-public/tool:1",
            env_of(&[]),
        );
        assert!(ok.is_ok(), "{:?}", ok.unwrap_err());

        // The base repository itself, tagged, is still inside the boundary.
        let err = check_registry_auth_with(
            &cfg_with_registry(),
            "registry.example.com/org/imgs:latest",
            env_of(&[]),
        );
        assert!(err.is_err(), "the registry's own repository must be gated");
    }

    #[test]
    fn a_trailing_slash_in_the_config_base_still_gates_the_registry() {
        // `base = "…/imgs/"` is a config shape, not an error, and stripping it as-is
        // leaves a remainder that starts with the repository name instead of `/` —
        // silently turning the guard off for every image it exists to gate.
        let mut cfg = cfg_with_registry();
        cfg.registry.as_mut().unwrap().base = "registry.example.com/org/imgs/".into();
        let err = check_registry_auth_with(&cfg, PRIVATE, env_of(&[]));
        assert!(err.is_err(), "a trailing slash must not un-gate the base");
    }

    #[test]
    fn a_credential_var_that_was_never_named_is_refused_like_a_missing_one() {
        // `registry_auth` gives up on an unnamed var and creates the group with no
        // authentication at all — the exact outcome this check exists to prevent, so
        // it cannot be the one case that passes.
        let mut cfg = cfg_with_registry();
        cfg.registry.as_mut().unwrap().username_env = None;
        let err = check_registry_auth_with(&cfg, PRIVATE, env_of(&[("REG_PASS", "p")]))
            .expect_err("an unnamed username_env is a missing credential");
        let msg = format!("{err}");
        assert!(msg.contains("username_env"), "should name the key: {msg}");
    }

    #[test]
    fn an_exported_but_empty_credential_is_never_sent_as_a_username() {
        // Both halves of the pair have to agree on "present": `export REG_USER=`
        // must not reach SaladCloud as a basic-auth username of "".
        assert_eq!(
            env_credential_from(Some("REG_USER"), env_of(&[("REG_USER", "   ")])),
            None
        );
        assert_eq!(env_credential_from(Some("REG_USER"), env_of(&[])), None);
        assert_eq!(
            env_credential_from(None, env_of(&[("REG_USER", "u")])),
            None
        );
        assert_eq!(
            env_credential_from(Some("REG_USER"), env_of(&[("REG_USER", " tok\n")])),
            Some("tok".to_string()),
        );
    }

    fn group_params(gateway_port: Option<u16>) -> GroupParams {
        GroupParams {
            name: "sf-abc-0".into(),
            image: "docker.io/library/ubuntu:24.04".into(),
            gpu_uuids: vec!["uuid-1".into()],
            priority: ContainerPriority::Batch,
            cpu: 8,
            memory_mb: 16384,
            disk_gib: 25,
            command: None,
            env: BTreeMap::new(),
            gateway_port,
            gateway_auth: false,
            registry_auth: None,
            restart_policy: RestartPolicy::OnFailure,
            country_codes: Vec::new(),
            shm_mb: None,
        }
    }

    #[test]
    fn no_gateway_port_means_no_networking_block_at_all() {
        // The pre-`--expose-port` behaviour of every batch run: the field must
        // be absent from the JSON, not present-and-null.
        let req = build_request(group_params(None));
        assert!(req.networking.is_none());
        let body = serde_json::to_value(&req).unwrap();
        assert!(
            body.get("networking").is_none(),
            "an unexposed run must not send a networking block: {body}"
        );
    }

    #[test]
    fn no_gpu_uuids_serializes_as_an_empty_class_list_not_a_missing_field() {
        // CPU-only (`run --cpu-only`). `gpu_classes` is required by the API,
        // so it must appear as `[]` — dropping the field would be a different
        // request, and one that fails at the far end rather than here.
        let mut p = group_params(None);
        p.gpu_uuids = Vec::new();
        let req = build_request(p);
        let body = serde_json::to_value(&req).unwrap();
        let classes = &body["container"]["resources"]["gpu_classes"];
        assert!(classes.is_array(), "gpu_classes must be present: {body}");
        assert_eq!(classes.as_array().unwrap().len(), 0);
        // The rest of the resource request is unchanged by dropping the GPU.
        assert_eq!(body["container"]["resources"]["cpu"], 8);
    }

    #[test]
    fn an_exposed_port_becomes_an_http_gateway_carrying_the_requested_auth() {
        // `auth` is a pass-through, and BOTH polarities are load-bearing:
        // `serve` needs false (its end users hold no Salad key), while `run`
        // and `session` need true (nothing reaches the container without one).
        // Asserting only one lets a change flip the other silently — and for
        // `run` that change publishes a live training dashboard to the
        // internet, which is not a failure any test would otherwise catch.
        for auth in [false, true] {
            let mut p = group_params(Some(7777));
            p.gateway_auth = auth;
            let req = build_request(p);
            let net = req.networking.as_ref().expect("networking block");
            assert_eq!(net.port, 7777);
            // Only "http" is accepted by the API.
            assert_eq!(net.protocol, "http");
            assert_eq!(net.auth, auth);
            let body = serde_json::to_value(&req).unwrap();
            assert_eq!(body["networking"]["port"], 7777);
            assert_eq!(body["networking"]["auth"], auth);
        }
    }
}
