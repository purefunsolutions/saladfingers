// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Checkpoint watcher + restore for `sf-agent run`.
//!
//! When a job declares a [`CheckpointSpec`], the agent:
//! - **restores** the latest remote checkpoint into the checkpoint directory *before*
//!   exec (resume path — the job reads it back and continues), and
//! - runs a **watcher** *during* exec that, on a fixed interval, uploads the checkpoint
//!   directory once it has quiesced (no member changed within `quiesce_secs`) and its
//!   contents changed since the last upload. Uploads overwrite fixed keys
//!   (keep-latest-remote); the metadata object is written last, so a reader only ever
//!   sees a complete checkpoint. On exec end the watcher does one final upload (the
//!   writer is gone, so the directory is settled) and exits.
//!
//! The transfer itself reuses the same `tar|zstd` part-series engine as inputs/outputs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use saladfingers_protocol::PROTOCOL_VERSION;
use saladfingers_protocol::job::{CheckpointSpec, JobSpec};
use saladfingers_protocol::transfer;

/// Metadata written last (atomically) after a checkpoint's parts are uploaded. Its
/// presence signals a complete checkpoint; `parts` tells the restore path how many of
/// the (fixed-count) part URLs actually hold data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMeta {
    /// Protocol version.
    pub v: u32,
    /// Number of parts that hold data.
    pub parts: u32,
    /// Compressed byte count.
    pub bytes: u64,
    /// SHA-256 of the compressed stream.
    pub sha256: String,
    /// When the checkpoint was uploaded.
    pub uploaded_at: DateTime<Utc>,
}

/// Restore the latest remote checkpoint into the checkpoint directory, if one exists.
/// A missing metadata object (no checkpoint yet) is not an error.
///
/// # Errors
/// Returns an error only if a present checkpoint fails to download or extract.
pub async fn restore(http: &reqwest::Client, spec: &JobSpec) -> Result<()> {
    let Some(ckpt) = spec.checkpoint.as_ref() else {
        return Ok(());
    };
    // Create the directory up front, not just on the restore path: a checkpointed job
    // may assume the dir exists the way it assumes the workdir exists. When it was
    // created only while extracting a remote checkpoint, a FIRST life (nothing remote
    // yet, 404 below) started without it — a job writing `dir/step` then failed every
    // write and the watcher scanned a nonexistent dir, so no checkpoint was ever born
    // and every interruption restarted from zero (caught live: an IMDS-reallocate test
    // job looped from step 0 indefinitely instead of resuming).
    let dir = ckpt_dir(spec, ckpt);
    tokio::fs::create_dir_all(&dir).await.ok();
    let resp = http
        .get(&ckpt.meta_get_url)
        .timeout(transfer::CONTROL_TIMEOUT)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("fetching checkpoint metadata")?;
    // 404/403 (or any storage "absent" answer) → no checkpoint to restore.
    if matches!(resp.status(), StatusCode::NOT_FOUND | StatusCode::FORBIDDEN) {
        return Ok(());
    }
    let meta: CheckpointMeta = resp
        .error_for_status()
        .context("checkpoint metadata status")?
        .json()
        .await
        .context("decoding checkpoint metadata")?;
    if meta.parts == 0 {
        return Ok(());
    }
    let n = (meta.parts as usize).min(ckpt.get_urls.len());
    transfer::download_artifact(http, &ckpt.get_urls[..n], &dir, true, Some(&meta.sha256))
        .await
        .context("restoring checkpoint")?;
    tracing::info!(parts = meta.parts, dir = %dir.display(), "checkpoint restored");
    Ok(())
}

/// The checkpoint directory, resolved against the job workdir. The child runs in the
/// workdir, so a relative `--checkpoint ckpts` means `<workdir>/ckpts` — resolving
/// against the agent's own CWD (typically `/`) would watch a directory the trainer
/// never writes, leaving checkpointing silently inert. An absolute path wins as-is.
fn ckpt_dir(spec: &JobSpec, ckpt: &CheckpointSpec) -> PathBuf {
    crate::run::workdir(spec).join(&ckpt.glob)
}

/// If the child was force-killed with writes this fresh, the tail of the checkpoint is
/// likely torn; skip the final upload rather than clobber the last good remote one.
const DIRTY_STOP_FRESH_WRITE: Duration = Duration::from_secs(2);

/// Spawn the checkpoint watcher. It runs until `stop` is notified (exec ended), then does
/// a final upload and exits. `dirty` is set by the supervisor when the child had to be
/// SIGKILLed — its freshest writes are then suspect. Returns a no-op handle when the job
/// has no checkpoint spec.
#[must_use]
pub fn spawn_watcher(
    http: reqwest::Client,
    spec: JobSpec,
    stop: Arc<Notify>,
    dirty: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(ckpt) = spec.checkpoint.clone() else {
            return;
        };
        let dir = ckpt_dir(&spec, &ckpt);
        watch_loop(&http, &ckpt, &dir, &stop, &dirty).await;
    })
}

async fn watch_loop(
    http: &reqwest::Client,
    ckpt: &CheckpointSpec,
    dir: &Path,
    stop: &Notify,
    dirty: &std::sync::atomic::AtomicBool,
) {
    let interval = Duration::from_secs(ckpt.interval_secs.max(1));
    let mut last_uploaded: Option<SystemTime> = None;

    loop {
        let stopping = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.notified() => true,
        };

        if let Some(mtime) = latest_mtime(dir) {
            let changed = last_uploaded.is_none_or(|prev| mtime > prev);
            // Quiescent = nothing written recently. On stop the writer (exec) is already
            // gone, so the directory is settled — upload regardless. Exception: a
            // force-killed child with writes fresher than the kill window likely died
            // MID-write; uploading that torn tail would overwrite the last good remote
            // checkpoint (parts overwrite fixed keys), so keep the previous one instead.
            let torn = stopping
                && dirty.load(std::sync::atomic::Ordering::SeqCst)
                && SystemTime::now()
                    .duration_since(mtime)
                    .is_ok_and(|idle| idle < DIRTY_STOP_FRESH_WRITE);
            let quiescent = stopping
                || SystemTime::now()
                    .duration_since(mtime)
                    .is_ok_and(|idle| idle.as_secs() >= ckpt.quiesce_secs);
            if torn {
                tracing::warn!(
                    "skipping final checkpoint upload: child was force-killed mid-write; \
                     keeping the previous remote checkpoint"
                );
            } else if changed && quiescent {
                match upload(http, ckpt, dir).await {
                    Ok(parts) => {
                        last_uploaded = Some(mtime);
                        tracing::info!(parts, "checkpoint uploaded");
                    }
                    Err(e) => tracing::warn!("checkpoint upload failed: {e:#}"),
                }
            }
        }

        if stopping {
            return;
        }
    }
}

/// Upload the checkpoint directory as a `tar|zstd` part series, then write the metadata
/// object last (atomic commit). Returns the number of parts written.
async fn upload(http: &reqwest::Client, ckpt: &CheckpointSpec, dir: &Path) -> Result<u32> {
    let report = transfer::upload_artifact(http, dir, true, &ckpt.put_urls, "checkpoint").await?;
    let meta = CheckpointMeta {
        v: PROTOCOL_VERSION,
        parts: report.parts,
        bytes: report.bytes,
        sha256: report.sha256,
        uploaded_at: Utc::now(),
    };
    // `without_url`: the meta URL is presigned; keep its signature out of warn logs.
    http.put(&ckpt.meta_put_url)
        .timeout(transfer::CONTROL_TIMEOUT)
        .json(&meta)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("uploading checkpoint metadata")?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context("checkpoint metadata PUT status")?;
    Ok(report.parts)
}

/// The most recent modification time of any file under `dir`, or `None` if the directory
/// is absent or empty. Used both to detect change and to gauge quiescence.
fn latest_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest: Option<SystemTime> = None;
    scan(dir, &mut newest);
    newest
}

fn scan(dir: &Path, newest: &mut Option<SystemTime>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match entry.file_type() {
            Ok(ft) if ft.is_dir() => scan(&path, newest),
            Ok(_) => {
                if let Ok(mtime) = entry.metadata().and_then(|m| m.modified())
                    && newest.is_none_or(|cur| mtime > cur)
                {
                    *newest = Some(mtime);
                }
            }
            Err(_) => {}
        }
    }
}
