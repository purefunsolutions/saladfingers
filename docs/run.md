<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# One-shot runs

`saladfingers run` rents a node, runs one command on it, collects what the command
produced, and deletes the group. The long-lived modes are [serve.md](serve.md); this page
is the one-shot lifecycle and the flags that change its shape. `saladfingers run --help`
is the flag reference.

The load-bearing fact underneath all of it: **only deleting the group stops billing.** The
platform relaunches a container on every exit regardless of exit code or restart policy
(see [empirical.md](empirical.md) E1/E2/E4), so a job that finishes has not finished
paying until the CLI — or `gc` — removes the group.

## The lifecycle

```sh
saladfingers run --profile kernels -- cargo test --release -- --ignored
saladfingers run --image gpu-probe --gpu-class "RTX 3060 (8 GB)" \
  --input ./corpus.tar:/work/corpus --output 'ckpts/latest/**:model' -- ./train
```

- Upload the `--input` artifacts, create one **single-replica group per shard**, poll
  until each writes its result envelope, download the artifacts that envelope lists into
  `./sf-out/<run-id>/<shard>/`, delete the groups, and exit with the job's own exit code.
- The group is deleted **before** its artifacts download, deliberately: a failed download
  can be retried with `attach`, and a billing group cannot be un-billed.
- A shard whose group failed without ever writing an envelope is **kept**, with a tail of
  the platform's system logs printed, because there is nothing else left to look at.
- `allocating`, `downloading` and `creating` are free ([salad-facts.md](salad-facts.md)),
  so a slow image pull costs nothing but wall time.

## Picking the hardware

```sh
saladfingers run --gpu-class "RTX 4090 (24 GB)" --gpu-class "RTX 3090 (24 GB)" \
  --memory-gb 30 --priority batch -- ./bench
saladfingers run --cpu-only -- ./pipeline-smoke-test
```

- `--gpu-class` is repeatable and means *first available*. A name that matches several
  classes is an error listing them, never a pick decided by API list order.
- **`--memory-gb` is host RAM, not VRAM** — the GPU class fixes VRAM. The default of 16
  fails expensively when it is too small: the host OOM-kills the container, the run
  reports **exit 137**, and a benchmark that was killed and restarted mid-flight comes
  back with numbers that look ordinary.
- **`--cpu-only`** requests no GPU class at all, so the group is placed on whatever host
  has the vCPU and RAM. It is opt-in rather than inferred from an omitted `--gpu-class`,
  so a mistyped class name fails loudly instead of quietly renting a CPU box and running
  a CUDA workload on it. It also overrides a profile's `gpu_classes`.
- Priority is `batch` unless you say otherwise. A preempted batch run bills nothing.

## Inputs, outputs, and checkpoints

```sh
saladfingers run --input ./corpus.tar:/work/corpus --output 'ckpts/latest/**:model' \
  --checkpoint /work/ckpt --max-duration 4h -- ./train
```

- Everything moves through presigned URLs, so **no credential ever enters the container**
  ([security.md](security.md)). The result envelope is written by the node, which is
  untrusted, and is validated before anything is downloaded.
- **`/work` does not survive a restart** ([empirical.md](empirical.md) E3): the container
  filesystem is fresh on every start, on same-node restarts and across reallocation
  alike. A `--checkpoint` directory in object storage is the only persistence a long run
  has.
- Size ceilings (`max_artifact_parts × 4 GiB`), the decompression-bomb guard, and the
  zstd tunables are storage policy, not run policy — see [storage.md](storage.md).
- A startup bandwidth gate reallocates a node too slow to be worth keeping before the
  work starts; `--no-gate` skips it.

## Shards (`--replicas N`)

```sh
saladfingers run --replicas 4 -- ./shard-worker
saladfingers logs sf-x7k2mq --uploaded --shard 2
saladfingers tunnel sf-x7k2mq --shard 2
```

- **A shard is a whole single-replica container group, not a replica.** SaladCloud offers
  **no per-instance routing** ([salad-facts.md](salad-facts.md)), so addressing a
  particular node means giving it a group of its own. That constraint is the reason the
  word "shard" exists here, and the reason `tunnel` and `logs --uploaded` take `--shard`.
- Groups are named `sf-<id>` for a single shard and `sf-<id>-0…N-1` for several. Each
  shard gets its own job spec, its own storage prefix `runs/<run-id>/<shard>/`, its own
  result envelope, and — with `--expose-port` — its own gateway URL.
- The command sees `SF_SHARD_INDEX` and `SF_SHARD_COUNT`. Splitting the work is the job's
  business; saladfingers only fans out.

## `--expose-port` — reaching into a running job

```sh
saladfingers run --expose-port 8080 -- ./train --dashboard-addr '[::]:8080'
```

- Publishes one container port through the SaladCloud gateway for the lifetime of the
  run, one gateway per shard. It disappears when the shard's envelope lands and the group
  is deleted — a tunnel is only live while the job is.
- **The gateway is created with `auth=true`.** Every request must carry `Salad-Api-Key`,
  so the port is never reachable from the public internet, and **a browser pointed at the
  gateway URL gets 403** — a browser cannot attach a header to a navigation. The edge
  answers 403 for a missing key and a wrong one alike, whatever the User-Agent. That is what
  `tunnel` below is for. (`serve` uses `auth=false` instead, because its end users are
  meant to reach it and the app enforces its own auth — see [serve.md](serve.md).)
- **The process must listen on IPv6 `[::]`.** The gateway answers **503** for a socket
  bound only to `0.0.0.0` or to loopback ([salad-facts.md](salad-facts.md)). Under
  `serve --proxy` the agent owns the `[::]` socket and the app binds loopback; with
  `--expose-port` there is no proxy, so your process is the gateway's upstream and binds
  `[::]` itself.
- **The command also has to stay up.** `--expose-port` publishes a port; it does not make
  anything listen on it, and a one-shot run ends when its command does. The trap is the
  `sf-agent probe` baked into every image: it defaults to `--emit stdout`, so it prints
  its report, exits 0, and the group is deleted — the gateway is gone before you can
  reach it, which reads exactly like a broken tunnel. Serve it with
  `sf-agent probe --emit http`.
- The gateway's 100 s / 1 GB per-request caps apply here too (see
  [serve.md](serve.md#gateway-limits-why-it-works-this-way)).

## `tunnel` — a local port onto that gateway

```sh
saladfingers tunnel sf-x7k2mq                          # 127.0.0.1:7777 → the run's gateway
saladfingers tunnel sf-x7k2mq --local-port 6006 --shard 2
```

- Listens on `127.0.0.1:<local-port>` (default 7777) and forwards to that shard's
  gateway **with the API key attached**, so a browser or `curl` needs no credential of
  its own. It runs in the foreground until Ctrl-C; the key never leaves your host.
- **The listener is loopback-only and there is deliberately no `--bind`.** Widening it
  would re-publish a port that is private precisely because it needs the key — now
  pre-authenticated for every host on the network. Forward it yourself (`ssh -L`) if you
  want it elsewhere, and own that decision.
- SSE and token streams pass through (responses are streamed, not buffered), but the
  gateway still cuts any single request at **100 s** — a stream that must outlive that
  has to reconnect. Request bodies are buffered and capped at 32 MiB.
- **WebSockets do not work through it.** The gateway carries them only with `auth=false`
  ([salad-facts.md](salad-facts.md)), and `--expose-port` is `auth=true` by construction.

## Logs

```sh
saladfingers logs sf-x7k2mq                  # newest 1000 lines from the last 24 h
saladfingers logs sf-x7k2mq --since 2h --limit 200
saladfingers logs sf-x7k2mq --all
saladfingers logs sf-x7k2mq --follow
saladfingers logs sf-x7k2mq --uploaded       # the agent's complete copy, from [storage]
```

- The default source is SaladCloud's org log query (Axiom-backed, ~90-day retention),
  filtered by container-group name — so **`logs` works after the group is deleted**,
  which is the normal state of a finished run.
- One request returns at most 100 entries, so the window is bisected until every slice
  comes back short. `--since` sets how far back to look, `--limit` caps the lines and
  keeps the newest, `--all` lifts that cap. `--follow` tails a rolling window of its own
  and refuses the three of them rather than ignoring them.
- **Platform log timestamps come from the node's clock**, and skew is real — one measured
  node ran ~73 s behind the control plane ([empirical.md](empirical.md), E6's notes).
  Ordering within one container's stream is trustworthy; ordering against control-plane
  events is not.
- **`--uploaded`** reads the agent's own complete copy of the run's output from
  `runs/<run-id>/<shard>/log.txt` in the `[storage]` bucket: no page cap, no time window,
  no reordering. It needs a configured storage backend, exists only once the agent has
  written it (just before the result envelope), and takes `--shard`. When the two views
  disagree, this is the one to believe.

## Detaching, cancelling, cleaning up

```sh
saladfingers run --detach -- ./long-train
saladfingers ls
saladfingers attach sf-x7k2mq
saladfingers cancel sf-x7k2mq
saladfingers gc --older-than 24h --dry-run
```

- `--detach` returns once the groups exist; the agent owns the data plane, so artifacts
  still upload. `attach` resumes the wait and adopts the earlier process's billed spans.
- Because exiting stops nothing, `--detach` also spawns a **detached reaper** that deletes
  the groups once every shard has finished or a hard cap elapses. If the machine dies, the
  reaper dies with it — `gc` is the backstop.
- `cancel` stops and deletes a run's groups; `gc` reaps leftover `sf-*` groups older than
  `--older-than`.

## When a run goes wrong

| symptom | cause | fix |
| --- | --- | --- |
| browser gets **403** on the gateway URL | `--expose-port` is `auth=true` | `saladfingers tunnel RUN_ID` |
| gateway answers **503** | the process bound `0.0.0.0` or loopback | bind `[::]:PORT` |
| exposed run succeeds instantly, gateway never answers | the command printed and exited instead of serving | `sf-agent probe` needs `--emit http` |
| run reports **exit 137** | host OOM killed the container | raise `--memory-gb` |
| group loops `downloading → creating` | the image is not amd64, or the command replaced an entrypoint it needed | see [empirical.md](empirical.md) |
| logs look thin or out of order | page cap, or node clock skew | widen `--since`, or use `--uploaded` |
| "Access Denied, Check Permissions" | private image, no pull credentials | set the `[registry]` env vars; `run` now refuses before creating anything |

A green exit code is the *node's* report of what happened, not a proof — see
[security.md](security.md), Assumption 1.
