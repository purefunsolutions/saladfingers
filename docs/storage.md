<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Storage

Bulk artifacts (datasets in, checkpoints/models out) move through any **S3-compatible**
backend via presigned URLs. The agent only ever receives presigned URLs — never
credentials, and never the Salad API key. Small control-plane objects (job specs,
result envelopes, logs) can also use SaladCloud's S4 (100 MB/file, 30-day expiry).

Directory artifacts travel as a `tar | zstd` stream split into ≤ 4 GiB parts
(`<name>.tzst.000`, `.001`, …) behind ordinary presigned PUTs — portable across every
backend, no S3 multipart required.

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
