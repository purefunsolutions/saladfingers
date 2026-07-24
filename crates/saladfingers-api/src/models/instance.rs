// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Container-group instance models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Instance lifecycle state. Only `Running` is billed. `Unknown` future-proofs the
/// alpha API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// Waiting for a node.
    Allocating,
    /// Pulling the image (free).
    Downloading,
    /// Creating the container (free).
    Creating,
    /// Running (billed).
    Running,
    /// Stopping.
    Stopping,
    /// An unrecognized state (never panic on new states).
    #[serde(other)]
    Unknown,
}

/// One instance of a container group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    /// Instance id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// Machine id — the path parameter for reallocate/recreate/restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,
    /// Current state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<InstanceState>,
    /// Image pull progress while downloading (0–100 or 0–1 depending on the API).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulling_progress: Option<f64>,
    /// Whether the container has started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,
    /// Whether the readiness probe passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
    /// Deployment version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// Last update time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_time: Option<DateTime<Utc>>,
}

impl Instance {
    /// The id used for instance action endpoints, preferring `machine_id`.
    #[must_use]
    pub fn action_id(&self) -> Option<&str> {
        self.machine_id.as_deref().or(self.instance_id.as_deref())
    }
}

/// Instances list envelope (the key varies in the alpha API).
#[derive(Debug, Clone, Deserialize)]
pub struct InstanceList {
    /// The instances.
    #[serde(alias = "items", default)]
    pub instances: Vec<Instance>,
}
