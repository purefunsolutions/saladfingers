// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! [`ResultEnvelope`] — the agent's commit record for a batch run.
//!
//! The envelope is uploaded **last**, as a single small object, after all outputs.
//! The CLI trusts only the artifacts listed in [`ResultEnvelope::uploads`]; part
//! objects left over from an interrupted attempt are ignored. This is the entire
//! reallocated-mid-upload idempotency story.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terminal outcome of a batch run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// The command exited 0 and outputs uploaded.
    Succeeded,
    /// The command exited non-zero.
    Failed,
    /// The wall-clock budget was exceeded; the child was stopped.
    TimedOut,
    /// The instance was interrupted (SIGTERM / node loss).
    Interrupted,
    /// The agent itself failed (bad spec, input fetch, etc.).
    AgentError,
}

impl JobStatus {
    /// Whether an agent booting and finding this status should short-circuit to
    /// success (the work is done and must not be repeated).
    #[must_use]
    pub fn is_terminal_for_resume(self) -> bool {
        matches!(self, JobStatus::Succeeded | JobStatus::TimedOut)
    }
}

/// The commit record for one shard of one run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultEnvelope {
    /// Protocol version; equals [`crate::PROTOCOL_VERSION`].
    pub v: u32,
    /// Run identifier.
    pub run_id: String,
    /// Shard index this envelope belongs to.
    pub shard_index: u32,
    /// Terminal status.
    pub status: JobStatus,
    /// Child process exit code, when the child ran to completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Human-readable error detail (for `AgentError`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Phase timestamps for cost/latency accounting.
    pub timings: Timings,
    /// Facts about the node the run landed on.
    pub node: NodeInfo,
    /// Artifacts the CLI may safely download. The CLI trusts ONLY these.
    #[serde(default)]
    pub uploads: Vec<UploadReport>,
    /// How many attempts this run took (across reallocations).
    pub attempts: u32,
    /// How many times the bandwidth gate reallocated.
    pub gate_reallocations: u32,
}

/// Phase timestamps recorded by the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timings {
    /// When the agent process started.
    pub agent_start: DateTime<Utc>,
    /// When the bandwidth gate finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_done: Option<DateTime<Utc>>,
    /// When input downloads finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_done: Option<DateTime<Utc>>,
    /// When the child command started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_start: Option<DateTime<Utc>>,
    /// When the child command ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_end: Option<DateTime<Utc>>,
    /// When output uploads finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs_done: Option<DateTime<Utc>>,
}

/// Facts about the rented node, echoed into the envelope for accounting/debugging.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// `SALAD_MACHINE_ID`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,
    /// `SALAD_CONTAINER_GROUP_NAME`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_group: Option<String>,
    /// GPU vendor: `nvidia`, `amd`, or `none`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_vendor: Option<String>,
    /// GPU model name as reported by the vendor tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_name: Option<String>,
    /// Driver version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_version: Option<String>,
    /// GPU memory in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_mb: Option<u64>,
    /// Measured download throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_down_mbps: Option<f64>,
    /// Measured upload throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_up_mbps: Option<f64>,
}

/// A record of one uploaded artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadReport {
    /// Logical name (matches the [`crate::TransferOut::name`]).
    pub name: String,
    /// Number of parts in the series.
    pub parts: u32,
    /// Total bytes uploaded.
    pub bytes: u64,
    /// SHA-256 of the reassembled stream, lowercase hex.
    pub sha256: String,
}

/// The attempts ledger, tracked separately so it survives envelope rewrites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempts {
    /// Protocol version.
    pub v: u32,
    /// One record per boot.
    pub attempts: Vec<AttemptRecord>,
    /// Total bandwidth-gate reallocations across attempts.
    pub gate_reallocs: u32,
}

/// One boot of the agent for a given run/shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    /// The node this attempt ran on.
    pub machine_id: String,
    /// When the attempt booted.
    pub boot_at: DateTime<Utc>,
}
