// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Interactive session HTTP API served by `sf-agent serve` and consumed by the CLI.
//!
//! Everything is designed around the two hard SaladCloud gateway limits: a **100 s**
//! max request duration and a **1 GB** max body. Output is streamed via long-poll
//! (`wait_ms` capped at 30 s) and files move in bounded chunks — never one giant
//! request.
//!
//! All routes require `Authorization: Bearer <SF_AGENT_TOKEN>` except
//! [`route::HEALTHZ`]. The gateway additionally fronts these with `auth=true`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Route paths for the session API (all under `/v1/`).
pub mod route {
    /// `GET` — liveness/readiness; unauthenticated.
    pub const HEALTHZ: &str = "/v1/healthz";
    /// `POST` — start an exec session.
    pub const EXEC: &str = "/v1/exec";
    /// `POST` — request graceful agent shutdown.
    pub const SHUTDOWN: &str = "/v1/shutdown";
    /// `POST` — begin a chunked, resumable file upload into the container.
    pub const FILES_UPLOAD: &str = "/v1/files/upload";
    /// `GET` — ranged file download (`?path=&offset=&len=`).
    pub const FILES_DOWNLOAD: &str = "/v1/files/download";
    /// `GET` — file metadata (`?path=`).
    pub const FILES_STAT: &str = "/v1/files/stat";

    /// `GET /v1/files/upload/{id}` — which chunk indices are already stored.
    #[must_use]
    pub fn upload_status(id: &str) -> String {
        format!("/v1/files/upload/{id}")
    }

    /// `PUT /v1/files/upload/{id}/{index}` — store one chunk (raw body).
    #[must_use]
    pub fn upload_chunk(id: &str, index: u32) -> String {
        format!("/v1/files/upload/{id}/{index}")
    }

    /// `POST /v1/files/upload/{id}/complete` — finalize, verify sha256, atomic rename.
    #[must_use]
    pub fn upload_complete(id: &str) -> String {
        format!("/v1/files/upload/{id}/complete")
    }

    /// `GET /v1/exec/{id}` — exec status.
    #[must_use]
    pub fn exec(id: &str) -> String {
        format!("/v1/exec/{id}")
    }

    /// `GET /v1/exec/{id}/output` — long-poll merged output.
    #[must_use]
    pub fn exec_output(id: &str) -> String {
        format!("/v1/exec/{id}/output")
    }

    /// `POST /v1/exec/{id}/signal` — send a signal to the child.
    #[must_use]
    pub fn exec_signal(id: &str) -> String {
        format!("/v1/exec/{id}/signal")
    }
}

/// Maximum `wait_ms` the agent honours on an output long-poll (well under the 100 s
/// gateway cap).
pub const MAX_OUTPUT_WAIT_MS: u64 = 30_000;

/// Default file-transfer chunk size (32 MiB fits comfortably in the 100 s / 1 GB window).
pub const DEFAULT_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

/// `GET /v1/healthz` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    /// Protocol version.
    pub v: u32,
    /// Run identifier the agent was booted with.
    pub run_id: String,
    /// Random per-process id; a change means the node was replaced.
    pub boot_id: String,
    /// Seconds since the agent started.
    pub uptime_secs: u64,
    /// Number of currently running exec sessions.
    pub execs_running: u32,
}

/// `POST /v1/exec` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRequest {
    /// Command to run, argv-style.
    pub argv: Vec<String>,
    /// Working directory (defaults to the agent's workdir).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Extra environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
}

/// `POST /v1/exec` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecCreated {
    /// Opaque exec session id.
    pub exec_id: String,
}

/// `GET /v1/exec/{id}` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecStatus {
    /// Whether the child is still running.
    pub running: bool,
    /// Exit code, once the child has exited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// When the child started.
    pub started_at: DateTime<Utc>,
    /// Terminating signal name, if the child was signalled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
}

/// Which standard stream a chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// One chunk of merged exec output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputChunk {
    /// Which stream produced the bytes.
    pub stream: Stream,
    /// Byte offset of this chunk within the merged per-exec sequence.
    pub offset: u64,
    /// Base64-encoded bytes.
    pub data_b64: String,
}

/// `GET /v1/exec/{id}/output` response (long-poll page).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputPage {
    /// Chunks past the requested cursor.
    pub chunks: Vec<OutputChunk>,
    /// Cursor to pass on the next poll.
    pub next_cursor: u64,
    /// Whether the child has exited.
    pub exited: bool,
    /// Exit code, if exited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// True if the output ring evicted bytes past the requested cursor.
    pub truncated: bool,
}

/// `POST /v1/exec/{id}/signal` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalRequest {
    /// Signal name: `TERM`, `INT`, or `KILL`.
    pub signal: String,
}

/// `POST /v1/files/upload` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInit {
    /// Destination path inside the container.
    pub path: String,
    /// Total size in bytes.
    pub size: u64,
    /// Expected SHA-256, lowercase hex.
    pub sha256: String,
    /// Chunk size the client will use.
    pub chunk_bytes: u64,
}

/// `POST /v1/files/upload` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInitResponse {
    /// Opaque upload id for subsequent chunk PUTs.
    pub upload_id: String,
}

/// `GET /v1/files/upload/{id}` response — which chunk indices are already stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadStatus {
    /// Received chunk indices.
    pub received: Vec<u32>,
}

/// `GET /v1/files/stat` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStat {
    /// Size in bytes.
    pub size: u64,
    /// Last-modified time.
    pub mtime: DateTime<Utc>,
}

/// `POST /v1/shutdown` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownRequest {
    /// Shutdown mode; currently only `graceful`.
    pub mode: String,
}
