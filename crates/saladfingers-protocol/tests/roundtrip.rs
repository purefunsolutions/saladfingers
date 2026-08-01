// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! JSON round-trip tests for every wire message. If a field's serde attributes
//! drift from the contract, these fail here rather than against a live agent.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use saladfingers_protocol::{
    JobSpec, JobStatus, NodeInfo, PROTOCOL_VERSION, ResultEnvelope, Timings, UploadReport,
    VersionProbe,
    agent_api::{ExecRequest, Health, OutputChunk, OutputPage, Stream},
    envelope::{AttemptRecord, Attempts},
    job::{
        BandwidthGate, CheckpointMeta, CheckpointSlot, CheckpointSpec, ControlUrls, TransferIn,
        TransferOut,
    },
};

fn roundtrip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(value).expect("serialize");
    let back: T = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(value, &back, "round-trip mismatch via {json}");
}

fn sample_urls() -> ControlUrls {
    ControlUrls {
        result_put: "https://s3.example/result?sig=put".into(),
        result_get: "https://s3.example/result?sig=get".into(),
        attempts_put: "https://s3.example/attempts?sig=put".into(),
        attempts_get: "https://s3.example/attempts?sig=get".into(),
        log_put: "https://s3.example/log?sig=put".into(),
    }
}

#[test]
fn jobspec_full_roundtrip() {
    let mut env = BTreeMap::new();
    env.insert("HF_TOKEN".to_string(), "x".to_string());
    let spec = JobSpec {
        v: PROTOCOL_VERSION,
        run_id: "sf-x7k2mq".into(),
        shard_index: 0,
        shard_count: 2,
        command: vec!["infurer-train".into(), "train".into()],
        workdir: Some("/work".into()),
        env,
        stop_signal: Some("INT".into()),
        max_duration_secs: Some(2700),
        max_attempts: Some(5),
        inputs: vec![TransferIn {
            name: "data".into(),
            urls: vec!["https://s3.example/data.000".into()],
            dest: "/work/data".into(),
            archive: true,
        }],
        outputs: vec![TransferOut {
            name: "ckpt".into(),
            src_glob: "ckpts/**".into(),
            put_urls: vec!["https://s3.example/ckpt.000".into()],
            archive: true,
        }],
        checkpoint: Some(CheckpointSpec {
            glob: "ckpts/step_*".into(),
            interval_secs: 60,
            quiesce_secs: 15,
            slots: vec![
                CheckpointSlot {
                    put_urls: vec!["https://s3.example/slot0.000".into()],
                    get_urls: vec!["https://s3.example/slot0.000?get".into()],
                    delete_urls: vec!["https://s3.example/slot0.000?del".into()],
                },
                CheckpointSlot {
                    put_urls: vec!["https://s3.example/slot1.000".into()],
                    get_urls: vec!["https://s3.example/slot1.000?get".into()],
                    delete_urls: vec!["https://s3.example/slot1.000?del".into()],
                },
            ],
            meta_put_url: "https://s3.example/meta?put".into(),
            meta_get_url: "https://s3.example/meta?get".into(),
        }),
        bandwidth_gate: Some(BandwidthGate {
            min_download_mbps: Some(50.0),
            min_upload_mbps: Some(10.0),
            sample_bytes: 8 * 1024 * 1024,
            max_reallocations: 5,
            gate_put_url: "https://s3.example/gate?put".into(),
            gate_get_url: Some("https://s3.example/gate?get".into()),
        }),
        urls: sample_urls(),
    };
    roundtrip(&spec);
}

#[test]
fn jobspec_minimal_omits_optionals() {
    let spec = JobSpec {
        v: PROTOCOL_VERSION,
        run_id: "sf-abc123".into(),
        shard_index: 0,
        shard_count: 1,
        command: vec!["true".into()],
        workdir: None,
        env: BTreeMap::new(),
        stop_signal: None,
        max_duration_secs: None,
        max_attempts: None,
        inputs: vec![],
        outputs: vec![],
        checkpoint: None,
        bandwidth_gate: None,
        urls: sample_urls(),
    };
    let json = serde_json::to_string(&spec).unwrap();
    assert!(
        !json.contains("workdir"),
        "absent optionals must be omitted: {json}"
    );
    assert!(
        !json.contains("checkpoint"),
        "absent optionals must be omitted: {json}"
    );
    roundtrip(&spec);
}

/// The checkpoint metadata is a wire message in both directions — the agent writes it, its
/// own restore path reads it back on a *different node*, and the CLI reads it days later —
/// so it belongs here with the other wire types rather than being covered only in passing
/// v2 has no v1 senders to tolerate, so a slot's GET and DELETE lists are required —
/// defaulted-empty they are silently wrong in both directions (restore reports "no
/// parts" for a checkpoint that exists; reclaim "succeeds" while deleting nothing).
/// This is the test that keeps a stray `#[serde(default)]` from coming back.
#[test]
fn a_slot_without_its_get_or_delete_urls_is_refused_at_decode() {
    for json in [
        r#"{"put_urls":["u"]}"#,
        r#"{"put_urls":["u"],"get_urls":["u"]}"#,
        r#"{"put_urls":["u"],"delete_urls":["u"]}"#,
    ] {
        assert!(
            serde_json::from_str::<CheckpointSlot>(json).is_err(),
            "an incomplete slot must fail loudly at decode: {json}"
        );
    }
}

/// by whichever integration test happened to store one.
#[test]
fn checkpoint_meta_roundtrip() {
    let meta = CheckpointMeta {
        v: PROTOCOL_VERSION,
        slot: 1,
        parts: 3,
        bytes: 12_884_901_888,
        sha256: "a".repeat(64),
        step: Some(21_000),
        uploaded_at: Utc.with_ymd_and_hms(2026, 7, 24, 3, 4, 5).unwrap(),
    };
    roundtrip(&meta);

    // `step` is the one optional: a job whose checkpoint layout reveals no step number
    // must not serialize a null that a stricter reader would reject.
    let json = serde_json::to_string(&CheckpointMeta {
        step: None,
        ..meta.clone()
    })
    .unwrap();
    assert!(
        !json.contains("step"),
        "absent step must be omitted: {json}"
    );

    // The version is what a reader probes before decoding, so it has to be present and
    // addressable without the rest of the object parsing.
    let probe: VersionProbe = serde_json::from_str(&json).expect("probe decodes");
    assert_eq!(probe.v, PROTOCOL_VERSION);
}

#[test]
fn envelope_roundtrip() {
    let ts = Utc.with_ymd_and_hms(2026, 7, 17, 12, 0, 0).unwrap();
    let env = ResultEnvelope {
        v: PROTOCOL_VERSION,
        run_id: "sf-x7k2mq".into(),
        shard_index: 0,
        status: JobStatus::Succeeded,
        exit_code: Some(0),
        error: None,
        timings: Timings {
            agent_start: ts,
            gate_done: Some(ts),
            inputs_done: Some(ts),
            exec_start: Some(ts),
            exec_end: Some(ts),
            outputs_done: Some(ts),
        },
        node: NodeInfo {
            machine_id: Some("mach-a".into()),
            container_group: Some("sf-x7k2mq-0".into()),
            gpu_vendor: Some("nvidia".into()),
            gpu_name: Some("RTX 4090".into()),
            driver_version: Some("560.35".into()),
            vram_mb: Some(24564),
            measured_down_mbps: Some(312.5),
            measured_up_mbps: Some(41.0),
        },
        uploads: vec![UploadReport {
            name: "ckpt".into(),
            parts: 1,
            bytes: 900_000_000,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        }],
        attempts: 1,
        gate_reallocations: 0,
    };
    roundtrip(&env);
}

#[test]
fn job_status_is_snake_case_and_resume_terminal() {
    assert_eq!(
        serde_json::to_string(&JobStatus::TimedOut).unwrap(),
        "\"timed_out\""
    );
    assert_eq!(
        serde_json::to_string(&JobStatus::AgentError).unwrap(),
        "\"agent_error\""
    );
    assert!(JobStatus::Succeeded.is_terminal_for_resume());
    assert!(JobStatus::TimedOut.is_terminal_for_resume());
    assert!(!JobStatus::Failed.is_terminal_for_resume());
    assert!(!JobStatus::Interrupted.is_terminal_for_resume());
}

#[test]
fn attempts_roundtrip() {
    let ts = Utc.with_ymd_and_hms(2026, 7, 17, 12, 0, 0).unwrap();
    let attempts = Attempts {
        v: PROTOCOL_VERSION,
        attempts: vec![AttemptRecord {
            machine_id: "mach-a".into(),
            boot_at: ts,
        }],
        gate_reallocs: 2,
    };
    roundtrip(&attempts);
}

#[test]
fn agent_api_types_roundtrip() {
    roundtrip(&Health {
        v: PROTOCOL_VERSION,
        run_id: "sf-x".into(),
        boot_id: "boot-1".into(),
        uptime_secs: 12,
        execs_running: 1,
    });
    roundtrip(&ExecRequest {
        argv: vec!["nvidia-smi".into()],
        workdir: None,
        env: None,
    });
    roundtrip(&OutputPage {
        chunks: vec![OutputChunk {
            stream: Stream::Stdout,
            offset: 0,
            data_b64: "aGk=".into(),
        }],
        next_cursor: 2,
        exited: false,
        exit_code: None,
        truncated: false,
    });
    assert_eq!(
        serde_json::to_string(&Stream::Stderr).unwrap(),
        "\"stderr\""
    );
}
