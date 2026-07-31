// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Container-group request and response models.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One gibibyte in bytes (`storage_amount` is in BYTES per the spec).
pub const GIB: u64 = 1024 * 1024 * 1024;

/// Scheduling priority. Lower priority is cheaper; `batch` is cheapest.
///
/// **`batch` is the default** — never let an unspecified priority fall through to
/// SaladCloud's own default, which is `high` (the most expensive tier).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerPriority {
    /// Not preempted by other workloads.
    High,
    /// Medium priority.
    Medium,
    /// Low priority.
    Low,
    /// Cheapest; most preemptible. The default.
    #[default]
    Batch,
    /// An unrecognized tier. Forward-compat: the alpha API adding one price tier must
    /// not brick `gpu-classes` parsing. Never constructed by this client for requests.
    #[serde(other)]
    Unknown,
}

/// What to do when the container's main process exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    /// Always restart.
    Always,
    /// Restart only on non-zero exit.
    OnFailure,
    /// Never restart (one-shot).
    Never,
}

/// Container-group lifecycle status. `Unknown` future-proofs the alpha API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupStatus {
    /// Being deployed.
    Pending,
    /// At least one instance running.
    Running,
    /// Stopped by the user.
    Stopped,
    /// Ran to completion.
    Succeeded,
    /// Deploying.
    Deploying,
    /// Failed to deploy.
    Failed,
    /// An unrecognized status (never panic on new states).
    #[serde(other)]
    Unknown,
}

/// Gateway load-balancing strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancer {
    /// Round robin.
    RoundRobin,
    /// Send to the instance with the fewest open connections.
    LeastNumberOfConnections,
}

/// A container group as returned by the API (read model; unknown fields ignored).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerGroup {
    /// Server-assigned id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Group name (unique per project).
    pub name: String,
    /// Human-friendly display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Desired replica count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    /// Current lifecycle state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_state: Option<ContainerGroupState>,
    /// Creation time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time: Option<DateTime<Utc>>,
    /// Last update time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_time: Option<DateTime<Utc>>,
    /// Gateway networking info (present when the group exposes a gateway).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub networking: Option<NetworkingInfo>,
}

impl ContainerGroup {
    /// The container-group lifecycle status, if known.
    #[must_use]
    pub fn status(&self) -> Option<GroupStatus> {
        self.current_state.as_ref().map(|s| s.status)
    }

    /// The gateway base URL (`https://<dns>`), if the group exposes one.
    #[must_use]
    pub fn gateway_url(&self) -> Option<String> {
        self.networking
            .as_ref()
            .and_then(|n| n.dns.as_ref())
            .map(|dns| format!("https://{dns}"))
    }
}

/// Read-model gateway networking info. The exact field name for the generated DNS
/// is confirmed empirically at the first live milestone; `dns` covers the common
/// spelling with an alias fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkingInfo {
    /// The group's public gateway DNS name.
    #[serde(default, alias = "dns_name", alias = "hostname")]
    pub dns: Option<String>,
}

/// The `current_state` block of a container group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerGroupState {
    /// Lifecycle status.
    pub status: GroupStatus,
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// When the group entered its current state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,
    /// Per-instance-state counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_status_counts: Option<InstanceStatusCounts>,
}

/// Counts of instances in each state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceStatusCounts {
    /// Instances allocating.
    #[serde(default)]
    pub allocating_count: u32,
    /// Instances creating.
    #[serde(default)]
    pub creating_count: u32,
    /// Instances running (the only billed state).
    #[serde(default)]
    pub running_count: u32,
    /// Instances stopping.
    #[serde(default)]
    pub stopping_count: u32,
}

/// Create-container-group request body.
#[derive(Debug, Clone, Serialize)]
pub struct CreateContainerGroup {
    /// Group name; `^[a-z][a-z0-9-]{0,61}[a-z0-9]$`, 2–63 chars, unique per project.
    pub name: String,
    /// Optional display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Whether to start on create.
    pub autostart_policy: bool,
    /// Replica count (0–500).
    pub replicas: u32,
    /// Restart policy.
    pub restart_policy: RestartPolicy,
    /// Container spec.
    pub container: CreateContainer,
    /// Gateway networking (present only when inbound access is needed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub networking: Option<Networking>,
    /// Country allow-list (ISO alpha-2, lowercase).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_codes: Option<Vec<String>>,
}

/// The `container` block of a create request.
#[derive(Debug, Clone, Serialize)]
pub struct CreateContainer {
    /// Image reference (≤ 2048 chars). Prefer `ref@sha256:…` for reproducibility.
    pub image: String,
    /// Resource requirements.
    pub resources: Resources,
    /// Command override (replaces the image ENTRYPOINT+CMD).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    /// Environment variables (values ≤ 1000 chars each).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub environment_variables: BTreeMap<String, String>,
    /// Scheduling priority. Non-optional and defaults to `batch`: an omitted priority
    /// would fall through to SaladCloud's `high` default (most expensive), so we always
    /// serialize one.
    #[serde(default)]
    pub priority: ContainerPriority,
    /// Enable node-level image layer caching.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_caching: Option<bool>,
    /// Private-registry credentials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry_authentication: Option<RegistryAuthentication>,
}

/// Resource requirements. Note `storage_amount` is in **bytes**.
#[derive(Debug, Clone, Serialize)]
pub struct Resources {
    /// vCPU count (1–16 practical).
    pub cpu: u32,
    /// RAM in MB (≤ 61440 practical).
    pub memory: u32,
    /// GPU class UUIDs (multiple = first-available wins).
    ///
    /// **Empty means CPU-only**: the group is placed on whatever host can
    /// supply the vCPU and RAM, with no GPU attached. Serialized as `[]`
    /// rather than omitted — the field is required by the API.
    pub gpu_classes: Vec<String>,
    /// Minimum free disk in **bytes** (≥ 1 GiB).
    pub storage_amount: u64,
    /// `/dev/shm` size in MB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shm_size: Option<u32>,
}

impl Resources {
    /// Build a GPU resource request, taking disk as GiB (converted to bytes).
    ///
    /// An empty `gpu_classes` is a CPU-only request; see [`Self::cpu_only`],
    /// which says so at the call site instead of leaving a bare `vec![]` for
    /// a reader to interpret.
    #[must_use]
    pub fn gpu(cpu: u32, memory_mb: u32, gpu_classes: Vec<String>, disk_gib: u64) -> Self {
        Self {
            cpu,
            memory: memory_mb,
            gpu_classes,
            storage_amount: disk_gib.max(1) * GIB,
            shm_size: None,
        }
    }

    /// Build a CPU-only resource request — no GPU class, so placement is on
    /// vCPU/RAM/disk alone.
    #[must_use]
    pub fn cpu_only(cpu: u32, memory_mb: u32, disk_gib: u64) -> Self {
        Self::gpu(cpu, memory_mb, Vec::new(), disk_gib)
    }
}

/// Gateway networking for inbound access.
#[derive(Debug, Clone, Serialize)]
pub struct Networking {
    /// When true, callers must send the `Salad-Api-Key` header.
    pub auth: bool,
    /// Container listen port (the app must bind IPv6 `[::]`).
    pub port: u16,
    /// Protocol; only `"http"` is accepted.
    pub protocol: String,
    /// Load-balancing strategy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer: Option<LoadBalancer>,
    /// Serialize requests one-at-a-time per instance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_connection_limit: Option<bool>,
    /// Queue-wait timeout in ms (≤ 100000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_timeout: Option<u32>,
    /// Response-wait timeout in ms (≤ 100000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_response_timeout: Option<u32>,
}

/// Private-registry credentials. Include exactly one variant.
#[derive(Clone, Serialize, Default)]
pub struct RegistryAuthentication {
    /// Generic basic auth (GHCR, GitLab, Quay, self-hosted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basic: Option<BasicAuth>,
    /// Docker Hub username + PAT.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docker_hub: Option<DockerHubAuth>,
}

impl fmt::Debug for RegistryAuthentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never render credentials.
        f.debug_struct("RegistryAuthentication")
            .field("basic", &self.basic.as_ref().map(|_| "***"))
            .field("docker_hub", &self.docker_hub.as_ref().map(|_| "***"))
            .finish()
    }
}

/// Basic-auth registry credentials.
#[derive(Clone, Serialize)]
pub struct BasicAuth {
    /// Registry username.
    pub username: String,
    /// Registry password / token.
    pub password: String,
}

/// Docker Hub registry credentials.
#[derive(Clone, Serialize)]
pub struct DockerHubAuth {
    /// Docker Hub username.
    pub username: String,
    /// Personal access token.
    pub personal_access_token: String,
}

/// Patch-container-group request body (v1 only touches `replicas`).
#[derive(Debug, Clone, Default, Serialize)]
pub struct UpdateContainerGroup {
    /// New replica count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_body_serializes_expected_shape() {
        let req = CreateContainerGroup {
            name: "sf-x7k2mq-0".into(),
            display_name: None,
            autostart_policy: true,
            replicas: 1,
            restart_policy: RestartPolicy::OnFailure,
            container: CreateContainer {
                image: "reg.example/gpu-probe@sha256:abc".into(),
                resources: Resources::gpu(4, 8192, vec!["uuid-1".into()], 20),
                command: Some(vec!["/bin/sf-agent".into(), "run".into()]),
                environment_variables: BTreeMap::new(),
                priority: ContainerPriority::Batch,
                image_caching: Some(true),
                registry_authentication: None,
            },
            networking: None,
            country_codes: None,
        };
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v["restart_policy"], "on_failure");
        assert_eq!(v["container"]["priority"], "batch");
        assert_eq!(
            v["container"]["resources"]["storage_amount"],
            20 * 1024 * 1024 * 1024_u64
        );
        assert!(v["container"]["resources"]["gpu_classes"].is_array());
        // Absent optionals are omitted.
        assert!(v.get("display_name").is_none());
        assert!(v["container"].get("registry_authentication").is_none());
        // priority is NEVER omitted (omission → SaladCloud's `high` default).
        assert_eq!(v["container"]["priority"], "batch");
    }

    #[test]
    fn priority_defaults_to_batch() {
        assert_eq!(ContainerPriority::default(), ContainerPriority::Batch);
    }

    #[test]
    fn registry_auth_debug_is_redacted() {
        let auth = RegistryAuthentication {
            basic: Some(BasicAuth {
                username: "u".into(),
                password: "supersecret".into(),
            }),
            docker_hub: None,
        };
        assert!(!format!("{auth:?}").contains("supersecret"));
    }
}
