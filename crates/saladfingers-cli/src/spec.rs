// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Assemble a [`JobSpec`] with all URLs presigned, plus the storage key layout the
//! runner uses to upload inputs and download outputs.

use std::collections::BTreeMap;
use std::time::Duration;

use saladfingers_protocol::transfer::part_key;
use saladfingers_protocol::{
    BandwidthGate, CheckpointSlot, CheckpointSpec, ControlUrls, JobSpec, PROTOCOL_VERSION,
    TransferIn, TransferOut,
};

use crate::presign::S3Backend;

/// Default maximum number of presigned-URL blocks per artifact — the size ceiling for any
/// single input, output, or checkpoint. At 4 GiB per part (`transfer::PART_SIZE`), 64 parts
/// is 256 GiB; override per deployment with `[storage] max_artifact_parts` when a run handles
/// larger artifacts (e.g. big model weights). The output collector enforces the run's
/// effective value as a hard cap on the untrusted result envelope, so a hostile node cannot
/// drive unbounded presigned-URL generation on the operator's host.
pub const DEFAULT_MAX_PARTS: u32 = 64;

/// Hard ceiling for a configured `max_artifact_parts` (4096 × 4 GiB = 16 TiB). Clamps the
/// setting so a mistaken value cannot bloat every job spec or the collector's work.
pub const MAX_ARTIFACT_PARTS_LIMIT: u32 = 4096;

/// Size of the checkpoint slot ring.
///
/// Two is the minimum that satisfies the invariant "never write the slot the committed
/// metadata references", and the minimum is what we want: retention is one complete
/// checkpoint, and each slot costs another `3 × max_parts` presigned URLs in the job spec
/// (PUT, GET and DELETE per part) — 192 per slot at the default `max_parts` of 64, 384
/// for the ring. A third
/// slot would buy nothing: the agent never has more than one upload in flight, so at most
/// two slots are ever live (the committed one and the one being written).
pub const CHECKPOINT_SLOTS: u32 = 2;

/// An input the runner has already uploaded (with GET URLs for exactly its parts).
pub struct UploadedInput {
    /// Destination path inside the container.
    pub dest: String,
    /// Whether the artifact is a `tar|zstd` archive.
    pub archive: bool,
    /// Ordered presigned GET URLs, one per uploaded part.
    pub get_urls: Vec<String>,
}

/// An output the run should collect.
pub struct OutputRequest {
    /// Logical name.
    pub name: String,
    /// Glob of files to collect (relative to the working directory).
    pub src_glob: String,
    /// Whether to archive the collected files.
    pub archive: bool,
}

/// Bandwidth-gate thresholds.
pub struct GateParams {
    /// Minimum download throughput.
    pub min_download_mbps: Option<f64>,
    /// Minimum upload throughput.
    pub min_upload_mbps: Option<f64>,
}

/// Checkpoint configuration for a run.
pub struct CheckpointParams {
    /// The checkpoint directory to watch/restore (in the container).
    pub dir: String,
    /// How often the agent scans for a new checkpoint, in seconds.
    pub interval_secs: u64,
    /// A checkpoint is uploaded once no member changed within this window, in seconds.
    pub quiesce_secs: u64,
}

/// Everything needed to build one shard's [`JobSpec`].
pub struct SpecParams<'a> {
    /// Storage backend for presigning.
    pub backend: &'a S3Backend,
    /// Run id.
    pub run_id: &'a str,
    /// This shard's index.
    pub shard_index: u32,
    /// Total shard count.
    pub shard_count: u32,
    /// Command argv.
    pub command: Vec<String>,
    /// Extra environment.
    pub env: BTreeMap<String, String>,
    /// Already-uploaded inputs.
    pub inputs: &'a [UploadedInput],
    /// Requested outputs.
    pub outputs: &'a [OutputRequest],
    /// Presigned-URL blocks to issue per output/checkpoint part series (the artifact size
    /// ceiling, `max_parts × 4 GiB`). Resolved from `[storage] max_artifact_parts`.
    pub max_parts: u32,
    /// Wall-clock budget.
    pub max_duration_secs: Option<u64>,
    /// Stop signal (`TERM`/`INT`).
    pub stop_signal: Option<String>,
    /// Optional bandwidth gate.
    pub gate: Option<GateParams>,
    /// Optional checkpointing.
    pub checkpoint: Option<CheckpointParams>,
    /// Presigned-URL expiry.
    pub expiry: Duration,
}

/// The storage prefix for a shard.
#[must_use]
pub fn shard_prefix(run_id: &str, shard: u32) -> String {
    format!("runs/{run_id}/{shard}")
}

/// The storage key stem for input `index` (shared across shards).
#[must_use]
pub fn input_stem(run_id: &str, index: usize) -> String {
    format!("runs/{run_id}/in/input{index}")
}

/// The storage key for a shard's job spec.
#[must_use]
pub fn job_key(run_id: &str, shard: u32) -> String {
    format!("{}/job.json", shard_prefix(run_id, shard))
}

/// The storage key stem of one checkpoint slot, under a shard's `base` prefix.
///
/// Both sides of the ring call this: [`build_job_spec`] mints the agent's presigned URLs
/// from it, and `checkpoint fetch` re-derives the same keys to read a slot back. Two
/// literals would let the producer's layout drift from the consumer's while both sides'
/// tests kept passing and every fetch 404'd.
#[must_use]
pub fn ckpt_slot_stem(base: &str, slot: u32) -> String {
    format!("{base}/ckpt/slot{slot}/data")
}

/// The storage key of a shard's checkpoint metadata — the object that names the live slot.
#[must_use]
pub fn ckpt_meta_key(base: &str) -> String {
    format!("{base}/ckpt/meta.json")
}

/// Build a shard's [`JobSpec`] with every URL presigned.
#[must_use]
pub fn build_job_spec(params: SpecParams) -> JobSpec {
    let base = shard_prefix(params.run_id, params.shard_index);
    let backend = params.backend;
    let expiry = params.expiry;
    let max_parts = params.max_parts;

    let urls = ControlUrls {
        result_put: backend.presign_put(&format!("{base}/result.json"), expiry),
        result_get: backend.presign_get(&format!("{base}/result.json"), expiry),
        attempts_put: backend.presign_put(&format!("{base}/attempts.json"), expiry),
        attempts_get: backend.presign_get(&format!("{base}/attempts.json"), expiry),
        log_put: backend.presign_put(&format!("{base}/log.txt"), expiry),
    };

    let inputs = params
        .inputs
        .iter()
        .enumerate()
        .map(|(i, input)| TransferIn {
            name: input_stem(params.run_id, i),
            urls: input.get_urls.clone(),
            dest: input.dest.clone(),
            archive: input.archive,
        })
        .collect();

    let outputs = params
        .outputs
        .iter()
        .map(|out| {
            let stem = format!("{base}/out/{}", out.name);
            let put_urls = (0..max_parts)
                .map(|k| backend.presign_put(&part_key(&stem, k), expiry))
                .collect();
            TransferOut {
                name: out.name.clone(),
                src_glob: out.src_glob.clone(),
                put_urls,
                archive: out.archive,
            }
        })
        .collect();

    let bandwidth_gate = params.gate.map(|gate| BandwidthGate {
        min_download_mbps: gate.min_download_mbps,
        min_upload_mbps: gate.min_upload_mbps,
        sample_bytes: 8 * 1024 * 1024,
        max_reallocations: 5,
        gate_put_url: backend.presign_put(&format!("{base}/gate.bin"), expiry),
        // The download probe range-reads back the object the upload probe wrote — a
        // known-size target, unlike the first input (which can be tiny → an RTT reading).
        gate_get_url: Some(backend.presign_get(&format!("{base}/gate.bin"), expiry)),
    });

    let checkpoint = params.checkpoint.map(|cp| {
        // A ring of slots, not one fixed key set: the agent always uploads to a slot the
        // committed metadata does NOT reference, so an upload cut short by a dying node
        // cannot damage the checkpoint that is currently restorable. The metadata object
        // is still written last and read first, and it now names the slot it describes.
        let meta_key = ckpt_meta_key(&base);
        let slots = (0..CHECKPOINT_SLOTS)
            .map(|slot| {
                let stem = ckpt_slot_stem(&base, slot);
                CheckpointSlot {
                    put_urls: (0..max_parts)
                        .map(|k| backend.presign_put(&part_key(&stem, k), expiry))
                        .collect(),
                    get_urls: (0..max_parts)
                        .map(|k| backend.presign_get(&part_key(&stem, k), expiry))
                        .collect(),
                    delete_urls: (0..max_parts)
                        .map(|k| backend.presign_delete(&part_key(&stem, k), expiry))
                        .collect(),
                }
            })
            .collect();
        CheckpointSpec {
            glob: cp.dir,
            interval_secs: cp.interval_secs,
            quiesce_secs: cp.quiesce_secs,
            slots,
            meta_put_url: backend.presign_put(&meta_key, expiry),
            meta_get_url: backend.presign_get(&meta_key, expiry),
        }
    });

    JobSpec {
        v: PROTOCOL_VERSION,
        run_id: params.run_id.to_string(),
        shard_index: params.shard_index,
        shard_count: params.shard_count,
        command: params.command,
        workdir: None,
        env: params.env,
        stop_signal: params.stop_signal,
        max_duration_secs: params.max_duration_secs,
        max_attempts: None, // agent default (5)
        inputs,
        outputs,
        checkpoint,
        bandwidth_gate,
        urls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> S3Backend {
        S3Backend::new("https://s3.example.com", "auto", "bkt", true, "AK", "SK").unwrap()
    }

    #[test]
    fn builds_a_fully_presigned_spec() {
        let backend = backend();
        let inputs = vec![UploadedInput {
            dest: "/work/data".into(),
            archive: true,
            get_urls: vec!["https://s3.example.com/bkt/runs/sf-x/in/input0.tzst.000?sig".into()],
        }];
        let outputs = vec![OutputRequest {
            name: "ckpt".into(),
            src_glob: "ckpts/**".into(),
            archive: true,
        }];
        let spec = build_job_spec(SpecParams {
            backend: &backend,
            run_id: "sf-x7k2mq",
            shard_index: 0,
            shard_count: 2,
            command: vec!["infurer-train".into()],
            env: BTreeMap::new(),
            inputs: &inputs,
            outputs: &outputs,
            max_parts: 8,
            max_duration_secs: Some(2700),
            stop_signal: Some("INT".into()),
            gate: Some(GateParams {
                min_download_mbps: Some(50.0),
                min_upload_mbps: Some(10.0),
            }),
            checkpoint: Some(CheckpointParams {
                dir: "/work/ckpt".to_string(),
                interval_secs: 30,
                quiesce_secs: 15,
            }),
            expiry: Duration::from_secs(3600),
        });

        assert_eq!(spec.shard_count, 2);
        assert!(spec.urls.result_put.contains("X-Amz-Signature"));
        assert!(
            spec.urls
                .result_put
                .contains("runs/sf-x7k2mq/0/result.json")
        );
        assert_eq!(spec.inputs.len(), 1);
        assert_eq!(spec.inputs[0].dest, "/work/data");
        assert_eq!(spec.outputs.len(), 1);
        // The presigned-block count follows `max_parts`, not a hardcoded constant.
        assert_eq!(spec.outputs[0].put_urls.len() as u32, 8);
        assert!(spec.bandwidth_gate.is_some());
        assert!(
            spec.bandwidth_gate
                .unwrap()
                .gate_put_url
                .contains("gate.bin")
        );

        let ckpt = spec.checkpoint.expect("checkpoint built");
        assert_eq!(ckpt.glob, "/work/ckpt");
        assert_eq!(ckpt.interval_secs, 30);
        assert_eq!(ckpt.slots.len() as u32, CHECKPOINT_SLOTS);
        assert!(ckpt.meta_put_url.contains("ckpt/meta.json"));
        assert!(ckpt.meta_get_url.contains("X-Amz-Signature"));
        for (index, slot) in ckpt.slots.iter().enumerate() {
            // Every slot is fully addressable — read it back, write it, and reclaim it.
            assert_eq!(slot.put_urls.len() as u32, 8);
            assert_eq!(slot.get_urls.len() as u32, 8);
            assert_eq!(slot.delete_urls.len() as u32, 8);
            // Distinct key space per slot: overlapping keys would defeat the whole point.
            let key = format!("ckpt/slot{index}/data");
            assert!(slot.put_urls[0].contains(&key), "{}", slot.put_urls[0]);
            assert!(slot.get_urls[0].contains(&key));
            assert!(slot.delete_urls[0].contains(&key));
            assert!(slot.delete_urls[0].contains("X-Amz-Signature"));
        }
    }
}
