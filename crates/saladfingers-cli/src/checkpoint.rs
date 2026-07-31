// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers checkpoint show|fetch` — read the checkpoint a run left in storage.
//!
//! Checkpoints exist so a long training job survives losing its node, which means the
//! useful artifact usually outlives the run that produced it: a job cut short at step
//! 21,000 still has 21,000 steps of work in the bucket, and the next run should start
//! from it rather than from zero. `--output` cannot deliver that — output collection only
//! happens when a job finishes cleanly, which is exactly the case where the checkpoint is
//! least interesting.
//!
//! The agent rotates checkpoints between the slots of a ring, so the current one lives at
//! `ckpt/slot0/…` or `ckpt/slot1/…` depending on how many times it rotated. The metadata
//! object is the index that resolves it, and these commands read it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use saladfingers_protocol::transfer;
use saladfingers_protocol::{CheckpointMeta, VersionProbe};

use crate::cli::{CheckpointArgs, CheckpointFetchArgs};
use crate::config::Config;
use crate::presign::S3Backend;
use crate::spec;

/// Long enough to download a large checkpoint, short enough to stay a bounded credential.
const EXPIRY: Duration = Duration::from_secs(6 * 3600);

/// Open the storage backend a checkpoint command reads through.
fn backend_of(cfg: &Config) -> Result<(reqwest::Client, S3Backend)> {
    let storage = cfg
        .storage
        .as_ref()
        .context("`checkpoint` needs an S3-compatible [storage] backend")?;
    Ok((
        transfer::transfer_client()?,
        S3Backend::from_config(storage)?,
    ))
}

/// `saladfingers checkpoint show RUN_ID`
///
/// # Errors
/// Returns an error when storage is unconfigured, unreachable, or holds no checkpoint for
/// the run.
pub async fn show(cfg: Config, args: CheckpointArgs) -> Result<()> {
    let (http, backend) = backend_of(&cfg)?;
    let meta = resolve(&http, &backend, &args.run_id, args.shard).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&meta)?);
        return Ok(());
    }
    println!("run          {} (shard {})", args.run_id, args.shard);
    println!(
        "step         {}",
        meta.step
            .map_or_else(|| "unknown".to_string(), |s| s.to_string())
    );
    println!("slot         {}", meta.slot);
    println!("parts        {}", meta.parts);
    println!("size         {}", human_bytes(meta.bytes));
    println!("uploaded     {}", meta.uploaded_at.to_rfc3339());
    println!("sha256       {}", meta.sha256);
    Ok(())
}

/// `saladfingers checkpoint fetch RUN_ID [--dest DIR]`
///
/// # Errors
/// Returns an error when storage is unconfigured, holds no checkpoint for the run, or the
/// download fails its checksum.
pub async fn fetch(cfg: Config, args: CheckpointFetchArgs) -> Result<()> {
    let (http, backend) = backend_of(&cfg)?;
    let dest = args.dest.map_or_else(
        || {
            PathBuf::from("sf-out")
                .join(&args.target.run_id)
                .join(args.target.shard.to_string())
                .join("ckpt")
        },
        PathBuf::from,
    );
    let meta = fetch_into(
        &http,
        &backend,
        &args.target.run_id,
        args.target.shard,
        &dest,
    )
    .await?;
    if args.target.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dest": dest.display().to_string(),
                "meta": meta,
            }))?
        );
    } else {
        println!("{}", dest.display());
    }
    Ok(())
}

/// Read the committed checkpoint metadata for a run's shard — the object that names the
/// live slot.
///
/// Whatever decodes is returned: `show` displays a checkpoint's own account of itself,
/// including one whose part count is nonsense, because that reading *is* the diagnosis.
/// Acting on the numbers is [`fetch_into`]'s job, and that is where they are bounded.
///
/// # Errors
/// Returns an error when storage holds no checkpoint for the run, the object cannot be
/// read, or it was written by an agent speaking a different protocol version.
pub async fn resolve(
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard: u32,
) -> Result<CheckpointMeta> {
    let key = spec::ckpt_meta_key(&spec::shard_prefix(run_id, shard));
    // A fixed-size control document, so it takes the control deadline: without one, a
    // storage endpoint that accepts the connection and never answers hangs the command
    // with no output and no way to tell that apart from a slow download.
    let resp = http
        .get(backend.presign_get(&key, EXPIRY))
        .timeout(transfer::CONTROL_TIMEOUT)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("fetching checkpoint metadata")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "no checkpoint for run '{run_id}' shard {shard} ({})",
        resp.status()
    );
    // The object is a few hundred bytes, and under a shared prefix its key is writable
    // by other runs — bound the body before buffering it, not after.
    anyhow::ensure!(
        !resp.content_length().is_some_and(|len| len > 1024 * 1024),
        "checkpoint metadata object is implausibly large ({} bytes); refusing to buffer it",
        resp.content_length().unwrap_or_default()
    );
    let body = resp
        .bytes()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading checkpoint metadata")?;
    // An agent of another version may have written a layout this CLI cannot address. Say
    // so, rather than presigning keys that do not exist and reporting the resulting 404s
    // as a lost checkpoint. Probing `v` first is what makes that message possible: a full
    // decode of a v1 object fails with `missing field 'slot'`, which reads like corruption.
    let probe: VersionProbe =
        serde_json::from_slice(&body).context("decoding checkpoint metadata")?;
    anyhow::ensure!(
        probe.v == saladfingers_protocol::PROTOCOL_VERSION,
        "checkpoint metadata is protocol v{} but this CLI speaks v{}",
        probe.v,
        saladfingers_protocol::PROTOCOL_VERSION
    );
    serde_json::from_slice(&body).context("decoding checkpoint metadata")
}

/// Download the live slot of a run's checkpoint into `dest`, returning the metadata that
/// described it.
///
/// # Errors
/// Returns an error when the metadata cannot be resolved, records an unusable part count,
/// or the downloaded bytes fail the recorded checksum.
pub async fn fetch_into(
    http: &reqwest::Client,
    backend: &S3Backend,
    run_id: &str,
    shard: u32,
    dest: &Path,
) -> Result<CheckpointMeta> {
    let meta = resolve(http, backend, run_id, shard).await?;
    anyhow::ensure!(meta.parts > 0, "checkpoint metadata records no data parts");
    // Every numeric field below comes from the node, which is untrusted (security.md,
    // Assumption 1). `parts` drives `(0..parts)` presigned-URL generation, so it is
    // bounded before use — in the spirit of `runner::admit_output`, though at the
    // protocol ceiling rather than the writing run's `max_parts`: this reader does not
    // need to match the writer's configuration to download what exists, it only refuses
    // the impossible (a claim of billions of parts would exhaust memory signing URLs
    // for keys that cannot exist).
    anyhow::ensure!(
        meta.parts <= spec::MAX_ARTIFACT_PARTS_LIMIT,
        "checkpoint metadata claims {} parts, past the {} the protocol allows — \
         the metadata object is malformed",
        meta.parts,
        spec::MAX_ARTIFACT_PARTS_LIMIT
    );
    // `slot` picks the key stem. Out of ring it can only 404, but a 404 on every part
    // reads as "the checkpoint is gone" — the exact misdiagnosis the version probe
    // exists to prevent, so name the real problem instead.
    anyhow::ensure!(
        meta.slot < spec::CHECKPOINT_SLOTS,
        "checkpoint metadata names slot {} but the ring has {} slots — \
         the metadata object is malformed",
        meta.slot,
        spec::CHECKPOINT_SLOTS
    );
    // The checksum is compared byte-for-byte later, so a malformed one can only ever
    // fail — but it would fail as "integrity check failed", which reads as corruption
    // of the data rather than of the metadata.
    anyhow::ensure!(
        meta.sha256.len() == 64 && meta.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "checkpoint metadata records a malformed sha256 (not 64 hex characters)"
    );

    let stem = spec::ckpt_slot_stem(&spec::shard_prefix(run_id, shard), meta.slot);
    let get_urls: Vec<String> = (0..meta.parts)
        .map(|k| backend.presign_get(&transfer::part_key(&stem, k), EXPIRY))
        .collect();

    eprintln!(
        "fetching checkpoint (step {}, {}) → {}",
        meta.step
            .map_or_else(|| "unknown".to_string(), |s| s.to_string()),
        human_bytes(meta.bytes),
        dest.display()
    );
    // The sha256 is checked before anything is extracted, so a torn or truncated slot
    // fails here instead of producing a half-written checkpoint directory.
    transfer::download_artifact(http, &get_urls, dest, true, Some(&meta.sha256))
        .await
        .context("downloading checkpoint")?;
    Ok(meta)
}

fn human_bytes(bytes: u64) -> String {
    #[allow(clippy::cast_precision_loss)]
    let b = bytes as f64;
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = b;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The layout is wire-visible: the agent's URLs are signed for these keys at submit
    /// time, and `fetch` re-derives them hours later. Pin the shared helper both sides
    /// call, so a change has to break this rather than silently 404 every fetch.
    #[test]
    fn a_slots_key_is_per_slot_and_per_shard() {
        let stem = |shard, slot| spec::ckpt_slot_stem(&spec::shard_prefix("sf-x", shard), slot);
        assert_eq!(stem(0, 0), "runs/sf-x/0/ckpt/slot0/data");
        assert_eq!(stem(3, 1), "runs/sf-x/3/ckpt/slot1/data");
        assert_eq!(
            spec::ckpt_meta_key(&spec::shard_prefix("sf-x", 3)),
            "runs/sf-x/3/ckpt/meta.json"
        );
    }

    #[test]
    fn human_bytes_scales() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(700 * 1024 * 1024), "700.0 MiB");
    }
}
