// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! [`JobSpec`] — everything a batch (`sf-agent run`) agent needs to do its job.
//!
//! The CLI uploads a `JobSpec` as a small JSON object to object storage and passes
//! the agent a single presigned URL to fetch it (`SF_JOB_URL`). The agent never sees
//! storage credentials or the Salad API key — only presigned URLs carried in here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A single unit of work for one shard on one rented node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    /// Protocol version; equals [`crate::PROTOCOL_VERSION`].
    pub v: u32,
    /// Run identifier, e.g. `sf-x7k2mq`.
    pub run_id: String,
    /// Zero-based shard index for multi-node runs.
    pub shard_index: u32,
    /// Total number of shards in this run.
    pub shard_count: u32,
    /// The command to execute, argv-style (executed directly, no shell).
    pub command: Vec<String>,
    /// Working directory for the command. Defaults to `/work`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Extra environment for the command, merged over the container environment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Signal to send on graceful stop. `TERM` (default) or `INT` (e.g. infurer-train).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
    /// Hard wall-clock budget; the agent stops the child and reports `TimedOut`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_secs: Option<u64>,
    /// Attempt cap: once the ledger shows this many attempts and the last envelope is a
    /// completed failure, the agent stops re-executing (the platform relaunches the
    /// container on every exit, so an uncapped deterministic failure re-runs forever).
    /// `None` = the agent default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// Inputs to download before running.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<TransferIn>,
    /// Outputs to upload after a successful run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<TransferOut>,
    /// Optional kelpie-style checkpoint watcher configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointSpec>,
    /// Optional startup bandwidth gate (reallocate slow residential nodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bandwidth_gate: Option<BandwidthGate>,
    /// Presigned URLs for control-plane objects (envelope, attempts, logs).
    pub urls: ControlUrls,
}

/// One input artifact, fetched from an ordered series of presigned GET URLs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferIn {
    /// Logical name (also the storage key stem).
    pub name: String,
    /// Ordered presigned GET URLs for the part series.
    pub urls: Vec<String>,
    /// Destination path inside the container.
    pub dest: String,
    /// Whether the artifact is a `tar|zstd` archive to extract (vs. a single file).
    #[serde(default)]
    pub archive: bool,
}

/// One output artifact, uploaded to an ordered series of presigned PUT URLs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferOut {
    /// Logical name (also the storage key stem).
    pub name: String,
    /// Glob of files to collect, relative to the working directory.
    pub src_glob: String,
    /// Presigned PUT URLs, one per 4 GiB part (bounded, e.g. ≤ 32).
    pub put_urls: Vec<String>,
    /// Whether to archive the collected files as `tar|zstd` (vs. a single file).
    #[serde(default)]
    pub archive: bool,
}

/// Checkpoint watcher: periodically uploads quiescent checkpoint directories so an
/// interrupted run can resume on a new node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSpec {
    /// Glob matching checkpoint directories to watch.
    pub glob: String,
    /// How often to scan for new checkpoints, in seconds.
    pub interval_secs: u64,
    /// A checkpoint is uploaded once no member file changed within this window.
    #[serde(default = "default_quiesce_secs")]
    pub quiesce_secs: u64,
    /// Presigned PUT URLs for the latest checkpoint's part series (overwrite = keep-latest).
    pub put_urls: Vec<String>,
    /// Presigned PUT URL for the checkpoint metadata (written last, atomically).
    pub meta_put_url: String,
    /// Presigned GET URL for the checkpoint metadata (resume path).
    pub meta_get_url: String,
    /// Presigned GET URLs for the latest checkpoint's part series (resume path).
    #[serde(default)]
    pub get_urls: Vec<String>,
}

fn default_quiesce_secs() -> u64 {
    15
}

/// Startup bandwidth gate. The agent measures up/down throughput and, if below the
/// configured floor, asks IMDS to reallocate to a faster node (bounded retries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BandwidthGate {
    /// Minimum acceptable download throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_download_mbps: Option<f64>,
    /// Minimum acceptable upload throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_upload_mbps: Option<f64>,
    /// Probe size in bytes (default 8 MiB).
    #[serde(default = "default_sample_bytes")]
    pub sample_bytes: u64,
    /// Maximum reallocations before proceeding anyway.
    #[serde(default = "default_max_reallocations")]
    pub max_reallocations: u32,
    /// Presigned PUT URL used as the upload probe target.
    pub gate_put_url: String,
    /// Presigned GET URL for the object the upload probe wrote. When present, the
    /// download probe range-reads this known-size object instead of the first input —
    /// a first input smaller than the sample yields a latency-dominated reading that
    /// would spuriously reallocate every node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_get_url: Option<String>,
}

fn default_sample_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_max_reallocations() -> u32 {
    5
}

/// Presigned URLs for the control-plane objects an agent reads/writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlUrls {
    /// PUT: the result envelope (commit record; written last).
    pub result_put: String,
    /// GET: the result envelope (idempotent-resume short-circuit).
    pub result_get: String,
    /// PUT: the attempts ledger.
    pub attempts_put: String,
    /// GET: the attempts ledger.
    pub attempts_get: String,
    /// PUT: reserved for a rolling log tail; the agent does not write it yet.
    /// Container stdout is queryable via SaladCloud's org log storage for ~90 days
    /// (including after group deletion), which `saladfingers logs` uses instead.
    pub log_put: String,
}
