<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Storage

Bulk artifacts (datasets in, checkpoints/models out) move through any **S3-compatible**
backend via presigned URLs. The agent only ever receives presigned URLs — never
credentials, and never the Salad API key. Small control-plane objects (job specs,
result envelopes, logs) can also use SaladCloud's S4 (100 MB/file, 30-day expiry). The
agent's own complete copy of a run's container output lands at
`runs/<run-id>/<shard>/log.txt`, which is what `saladfingers logs --uploaded` reads
([run.md](run.md)).

Directory artifacts travel as a `tar | zstd` stream split into ≤ 4 GiB parts
(`<name>.tzst.000`, `.001`, …) behind ordinary presigned PUTs — portable across every
backend, no S3 multipart required. The zstd side is tunable per process:
`SALADFINGERS_ZSTD_LEVEL` (1–22, default 3) and `SALADFINGERS_ZSTD_WINDOW_LOG` (10–31,
also enables long-distance matching) — `saladfingers run --input-zstd-level` covers
staged inputs from the CLI, and baking the variables into the image as `ENV` covers the
agent's checkpoint/output uploads on the node.

## Artifact size limits

Each artifact (input, output, or checkpoint) is capped at **`max_artifact_parts × 4 GiB`**.
`max_artifact_parts` is set in `[storage]` (default **64 = 256 GiB**, clamped to 4096 =
16 TiB). The CLI presigns exactly that many PUT URLs per artifact up front — the agent holds
no credentials and can only write to URLs the CLI issued — so raising the ceiling for large
model weights means raising this value:

```toml
[storage]
max_artifact_parts = 256   # 256 × 4 GiB = 1 TiB
```

On the way back in, extraction is bounded by a **decompression-bomb guard**: a downloaded
artifact may expand to at most **100× its compressed size** (floored at 1 GiB), and a stream
that exceeds that is refused mid-extraction rather than filling the collector's disk. The
result envelope is written by the rented node — which is untrusted — so without this a tiny
`tar | zstd` of zeros could expand to terabytes on your machine. Real outputs (weights,
checkpoints) are high-entropy and nowhere near 100×, so the guard never trips on legitimate
data.

## Checkpoints

A run's checkpoint lives under `runs/<run-id>/<shard>/ckpt/`:

```
ckpt/meta.json          the commit record — which slot is current, its step and sha256
ckpt/slot0/data.tzst.*  ┐ the ring: one of these holds the current checkpoint,
ckpt/slot1/data.tzst.*  ┘ the other is free for the next upload
```

The agent **alternates** between the slots and rewrites `meta.json` only after the new
slot's parts have all landed. That ordering is what makes an interruption survivable: a
node that dies mid-upload leaves a half-written *free* slot, while `meta.json` still points
at the previous, complete one, so the replacement node resumes from it. (Overwriting one
fixed key set instead — the pre-v2 layout — made a torn upload detectable via the checksum
but not recoverable: the old bytes were already gone, and the run restarted from step 0.)
Retention is one checkpoint; the superseded slot's parts are deleted right after the commit,
using presigned DELETE URLs, since the node holds no credentials.

Because the live slot depends on how many times the run rotated, read the checkpoint through
the CLI rather than by guessing a key:

```bash
saladfingers checkpoint show sf-x7k2mq            # step, size, age — no download
saladfingers checkpoint fetch sf-x7k2mq --dest ./ckpt
```

`fetch` verifies the sha256 before extracting anything. It works after the run has ended and
after its container groups are gone — which is the point, since the checkpoint of an
interrupted run is worth more than the output of a finished one.

## Options

### Cloudflare R2 (recommended public default)

Zero egress fees (important when many nodes pull multi-GB files), S3-compatible,
10 GB free then ~$0.015/GB-month. Set `endpoint` to
`https://<accountid>.r2.cloudflarestorage.com`, `region = "auto"`.

### Self-hosted (recommended for heavy use)

Run [Garage](https://garagehq.deuxfleurs.fr/) (Rust, lightweight, NixOS
`services.garage`) or MinIO on a VPS with a large monthly uplink. Unlike a registry,
there is no SaladCloud-side cache — every node downloads over your uplink — so pick a
host with generous egress (e.g. Hetzner's 20 TB/month per node; the node is disposable
and recreatable to reset the quota). Enable path-style addressing.

### AWS S3 / Backblaze B2

- **S3**: works out of the box, but ~$0.09/GB egress makes repeated multi-GB pulls
  expensive.
- **B2**: cheap storage, free egress via the Cloudflare Bandwidth Alliance.

## Backlog

BitTorrent distribution (seed from the storage host and peer across replicas) pairs
naturally with a self-hosted storage node — planned, not in v1.
