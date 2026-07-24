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
//! Every top-level message carries a `v` field equal to [`PROTOCOL_VERSION`]; the
//! CLI warns on mismatch. Bump the version on any breaking change.

pub mod agent_api;
pub mod envelope;
pub mod job;
pub mod probe;

#[cfg(feature = "transfer")]
pub mod transfer;

/// Wire-format version stamped into every top-level message.
pub const PROTOCOL_VERSION: u32 = 1;

pub use envelope::{
    AttemptRecord, Attempts, JobStatus, NodeInfo, ResultEnvelope, Timings, UploadReport,
};
pub use job::{BandwidthGate, CheckpointSpec, ControlUrls, JobSpec, TransferIn, TransferOut};
pub use probe::{GpuVendor, ProbeReport};
