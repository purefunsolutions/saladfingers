<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# SaladCloud facts

Ground truth for saladfingers, verified against the official OpenAPI specs
(vendored under [`reference/`](reference/)) and the docs at <https://docs.salad.com>
on 2026-07-17. Prefer the vendored specs for any field not covered here.

## API fundamentals

- Base URL: `https://api.salad.com/api/public`. Auth header: `Salad-Api-Key: <key>`.
  Keys are **user-scoped**; refreshing one invalidates the old key. Rate limit:
  **240 requests/minute per key**.
- There is **no list-organizations / list-projects endpoint** — the org and project
  names must be configured. Projects are created in the portal.
- Errors are RFC-7807 problem JSON (`type`, `title`, `status`, `detail`, `instance`)
  **but may arrive as Cloudflare HTML** — never assume JSON before checking the
  status and content type.
- OpenAPI: `salad-cloud.yaml` (v0.9.0-alpha.17), `salad-cloud-imds.yaml`, `s4.yaml`.
  No official Rust SDK.

## Billing (the whole point)

- **Per-second, and only instances in the `running` state are billed.**
  `allocating` / `downloading` / `creating` are **free** — image download time costs
  nothing, which makes baking large data into images viable.
- Billing starts at `running`, **before** the app is ready → the agent must start in
  well under a second (a small prebuilt binary, no init scripts).
- The GPU hourly price includes vCPU + RAM. Bandwidth is unmetered; storage is not
  separately charged; a stopped group costs $0.
- Priorities `high | medium | low | batch`: `high` is not preempted by other
  workloads; `batch` is cheapest. Per-priority prices come from `gpu-classes`
  (`prices[]`, sent as strings).

## Create container group

Required: `name` (`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`, 2–63, unique per project),
`autostart_policy` (bool), `replicas` (0–500), `restart_policy`
(`always|on_failure|never`), and `container` with `image` (≤2048 chars) and
`resources` — `cpu` (1–16 practical), `memory` (MB, ≤61440 practical), `gpu_classes`
(UUID list; multiple = first-available), `storage_amount` (**BYTES**, ≥1 GiB — one
stale docs page says MB; the spec says bytes), `shm_size` (MB).

Useful optionals: `container.command` (array — **overrides ENTRYPOINT+CMD per group,
no image rebuild**), `container.environment_variables` (values ≤1000 chars each),
`container.priority`, `container.image_caching` (node-level layer cache),
`container.registry_authentication` (`basic` covers GHCR/GitLab/self-hosted),
`networking` (`auth`, `port`, `protocol:"http"`, load balancer, timeouts — default
and max **100000 ms = 100 s**), `country_codes`.

## Lifecycle & reliability

- Group status: `pending|running|stopped|succeeded|deploying|failed`. Instance state:
  `allocating|downloading|creating|running|stopping` (+ `pulling_progress` %).
  Instance actions key on `machine_id`.
- Cold start (published benchmark, 5.53 GB image): first instance ~3 min, 50 % by
  10 min, 80 % by 20 min. Heuristic: **> 2 min/GB stuck in `downloading` = slow
  residential node → reallocate it**.
- `stop` → `start` allocates **new** nodes (full re-download). `recreate` = same
  node, no re-download; `reallocate` = new node.
- Interruptions happen **without warning** (no signal); the platform auto-reallocates
  to another same-class node. Fleet stats: ~1.1 reallocations/hour per 100 instances;
  multi-day single runs < 4 % interruption.
- Exit behavior: exit 0 with `never`/`on_failure` stops the instance (the group can
  reach `succeeded`); repeated non-zero restarts on the same node, then reallocates;
  exit 137 = OOM. System events are visible via the `system-logs` endpoint.
- **A just-deleted group NAME is tombstoned**: re-creating a group with the same name
  within ~minutes of its deletion fails `400 Bad Request` (observed live 2026-07-23:
  `serve rm` then `serve up` with the same `--name` ~2 min later → 400; a fresh name
  succeeded immediately). Name-stable flows (`serve`, `session`) should use a new name
  or wait out the window when re-creating.
- **Empirically unverified** (see [`empirical.md`](empirical.md)): `never` policy ×
  platform node-loss; SIGTERM grace period on stop; `/work` persistence across a
  same-node restart.

## Networking & in-container facts

- Container Gateway: TLS at the Cloudflare edge, one static DNS name per group. The
  app **must listen on IPv6 `[::]`** (binding `0.0.0.0` → gateway 503). HTTP/1.1+2,
  SSE, and WebSockets work within the 100 s timeout cap; body ≤ 1 GB. `auth=true`
  requires the caller to send `Salad-Api-Key`; WebSockets require `auth=false`.
  **Per-instance routing is impossible** — target specific nodes with N
  single-replica groups.
- Only ingress is the gateway. Outbound is unrestricted (residential IP).
- IMDS at `http://169.254.169.254`, header `Metadata: true`: `GET /v1/status`,
  `GET /v1/token` (workload JWT, accepted by S4), `POST /v1/reallocate` /
  `/v1/recreate` / `/v1/restart`.
- Injected env: `SALAD_MACHINE_ID`, `SALAD_INSTANCE_ID`, `SALAD_CONTAINER_GROUP_ID`,
  `SALAD_CONTAINER_GROUP_NAME`, `SALAD_PROJECT_ID/NAME`, `SALAD_ORGANIZATION_ID/NAME`.
- S4 storage (`https://storage-api.salad.com`, auth via `Salad-Api-Key` **or** IMDS
  JWT): **max 100 MB/file, auto-deleted after 30 days**; presigned GET via
  `POST /organizations/{org}/file_tokens/{name}`. Control envelopes only, never
  weights.

## Images & GPUs

- Images are **linux/amd64 only**, **max 35 GB compressed**. Pulled once into
  Salad's internal EU/US cache, then fanned out to nodes.
- NVIDIA: the host injects the driver (`nvidia-smi` works; exact injected library
  paths are undocumented — see [`empirical.md`](empirical.md)). Containers bring
  their own CUDA userspace (cudart/cublas/nvrtc/cudnn). RTX 50-series (`sm_120`)
  needs CUDA ≥ 12.8.
- AMD classes exist (ROCm/HIP; `/dev/kfd` + `/dev/dri`, `rocminfo`/`amd-smi`).
  saladfingers ships hello-world ROCm support (vendor-aware probe, `rocm-runtime`
  image flavor).
- Apple Metal is **not available** on SaladCloud — the fleet is PC nodes running
  linux/amd64 containers.
