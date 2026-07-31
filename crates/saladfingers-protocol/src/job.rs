// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! [`JobSpec`] — everything a batch (`sf-agent run`) agent needs to do its job.
//!
//! The CLI uploads a `JobSpec` as a small JSON object to object storage and passes
//! the agent a single presigned URL to fetch it (`SF_JOB_URL`). The agent never sees
//! storage credentials or the Salad API key — only presigned URLs carried in here.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
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
    /// The slot ring. A checkpoint is written to a slot that is NOT the live one,
    /// so an interrupted upload cannot damage the checkpoint currently referenced
    /// by the metadata. See [`CheckpointSlot`].
    pub slots: Vec<CheckpointSlot>,
    /// Presigned PUT URL for the checkpoint metadata (written last, atomically).
    pub meta_put_url: String,
    /// Presigned GET URL for the checkpoint metadata (resume path).
    pub meta_get_url: String,
}

/// One slot of the checkpoint ring: a complete, independently addressable part
/// series.
///
/// The ring exists because the previous design overwrote a single fixed key set
/// every interval. Writing the data parts first and the metadata last made a
/// torn write *detectable* — restore verifies `sha256` before extracting — but
/// it could not make the previous checkpoint *survivable*: once the new data
/// PUT landed, the old bytes were gone, so a node dying in the window before
/// the metadata commit left `data(N+1)` under `meta(N)`, a checksum mismatch,
/// and a run that restarts from step 0. On a 30k-step job that is days of work.
///
/// With a ring, the commit is not merely atomic but non-destructive: until the
/// metadata names the new slot, the old slot is still complete and still
/// referenced.
///
/// The agent holds no storage credentials — it only ever receives presigned
/// URLs — so it cannot mint keys for a step number that was unknown at submit
/// time. The CLI therefore presigns a fixed, small ring up front and the agent
/// rotates through it, recording the step in the checkpoint metadata rather than
/// in the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSlot {
    /// Presigned PUT URLs for this slot's part series.
    pub put_urls: Vec<String>,
    /// Presigned GET URLs for the same parts (restore path).
    ///
    /// Required, like the other two: v2 has no v1 senders to tolerate, and a
    /// defaulted-empty list would make restore report "artifact has no parts"
    /// for a checkpoint that is sitting right there.
    pub get_urls: Vec<String>,
    /// Presigned DELETE URLs, so a superseded slot can be reclaimed without
    /// giving the node credentials. Retention is one complete checkpoint: after
    /// a successful commit the agent deletes every slot the new metadata does
    /// not reference.
    ///
    /// Required for the same reason, and one worse: an empty list makes
    /// reclamation *silently* succeed — sweeping zero keys, recording the slot
    /// as empty, and logging a reclamation that never happened.
    pub delete_urls: Vec<String>,
}

/// Metadata written last (atomically) after a checkpoint's parts are uploaded — the
/// commit. Its presence signals a complete checkpoint; `slot` says which slot of the ring
/// holds it and `parts` how many of that slot's (fixed-count) part URLs actually hold
/// data.
///
/// A wire message in both directions: the agent writes it, the agent's own restore path
/// reads it back on the next node, and the CLI reads it to fetch a finished run's
/// checkpoint (the key is no longer guessable by hand — it depends on the rotation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMeta {
    /// Protocol version; equals [`crate::PROTOCOL_VERSION`].
    pub v: u32,
    /// Index into [`CheckpointSpec::slots`] holding this checkpoint's parts.
    pub slot: u32,
    /// Number of parts that hold data.
    pub parts: u32,
    /// Compressed byte count.
    pub bytes: u64,
    /// SHA-256 of the compressed stream.
    pub sha256: String,
    /// Training step this checkpoint represents, when the directory layout reveals it
    /// (`step_<digits>` *directories*, as the infurer trainer writes — a job that names
    /// its checkpoints as files reports no step). Informational: it lets
    /// an operator see how far the remote checkpoint got without downloading it. The step
    /// cannot live in the key — the agent holds no credentials and can only use URLs
    /// presigned before the run started, when no step number was known yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<u64>,
    /// When the checkpoint was uploaded.
    pub uploaded_at: DateTime<Utc>,
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
    /// PUT: the run's captured stdout/stderr, written just before the envelope.
    /// Container stdout is also queryable via SaladCloud's org log storage for ~90 days
    /// (including after group deletion), which `saladfingers logs` reads — but that path
    /// pages 100 entries at a time and stamps entries with the node's clock, so it is
    /// best-effort. This object is the complete copy, on the same storage as the run's
    /// inputs, outputs, and result.
    pub log_put: String,
}
