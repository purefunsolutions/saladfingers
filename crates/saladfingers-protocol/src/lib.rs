// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Wire contract shared by the `saladfingers` CLI and the in-container `sf-agent`.
//!
//! Three surfaces:
//! - [`job`] — the [`JobSpec`] the CLI hands a batch agent (via a presigned URL).
//! - [`envelope`] — the [`ResultEnvelope`] an agent writes back as its commit record.
//! - [`agent_api`] — request/response types for the interactive session HTTP API.
//!
//! Every top-level message carries a `v` field equal to [`PROTOCOL_VERSION`]. Bump the
//! version on any breaking change. The agent refuses a job spec whose `v` differs, at
//! boot — field-level serde alone only catches a skew whose shapes differ, and a spec
//! without the changed block is byte-identical across versions. Readers that must
//! survive a mismatched *peer's data* check `v` through [`VersionProbe`] before the
//! full decode — today the checkpoint metadata on both the agent and the CLI side —
//! so the mismatch is named instead of surfacing as a missing-field decode error.

pub mod agent_api;
pub mod envelope;
pub mod job;
pub mod probe;

#[cfg(feature = "transfer")]
pub mod transfer;

/// Wire-format version stamped into every top-level message.
///
/// v2: checkpoints moved from one fixed key set to a slot ring
/// ([`job::CheckpointSlot`]), so an interrupted upload can no longer destroy the
/// last good checkpoint. The change is deliberately incompatible in both
/// directions — the replaced `CheckpointSpec` fields make a mismatched CLI/agent
/// pair fail loudly at job-spec decode rather than quietly at the first
/// checkpoint, hours into a run.
pub const PROTOCOL_VERSION: u32 = 2;

/// The `v` field alone, so a message can be version-checked before it is decoded in full.
///
/// Decoding straight into the real type reports a version mismatch as whichever field the
/// other version happened to add — `missing field 'slot'` — which reads like a corrupt or
/// truncated object and sends the reader hunting the wrong problem. Probing first names
/// the actual cause, and says which side to change.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
pub struct VersionProbe {
    /// The message's protocol version.
    pub v: u32,
}

pub use envelope::{
    AttemptRecord, Attempts, JobStatus, NodeInfo, ResultEnvelope, Timings, UploadReport,
};
pub use job::{
    BandwidthGate, CheckpointMeta, CheckpointSlot, CheckpointSpec, ControlUrls, JobSpec,
    TransferIn, TransferOut,
};
pub use probe::{GpuVendor, ProbeReport};
