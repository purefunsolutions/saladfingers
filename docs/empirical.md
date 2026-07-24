<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Empirical findings

SaladCloud's docs leave several load-bearing behaviors unspecified. This file records
what we measured on real rented nodes, so the code can rely on facts instead of
guesses. **Populated during milestone M3** (the `gpu-probe` image + the E1–E13
checklist in `PLAN.md`).

Until then, the code treats the items below as UNCERTAIN and errs on the safe side.

## Checklist (pending M3)

| # | Question | Status |
| --- | -------------------------------------------------------------------------- | ------ |
| E1 | `never` + exit 0 → instance stops? group `succeeded`? | **DONE** — NO, it loops (below) |
| E2 | `never` + exit 1 → anything restart? | **DONE** — yes, loops (below) |
| E3 | `on_failure` + exit 1 ×3 → same-node restarts; does `/work` survive? | **DONE** — ~5 restarts, `/work` NOT preserved (below) |
| E4 | `on_failure` + exit 0 → clean stop | **DONE** — only via CLI delete (below) |
| E5 | IMDS reallocate under `never` vs `on_failure` | **DONE** — works (below) |
| E6 | SIGTERM grace period on stop | **DONE** — no catchable SIGTERM on `/stop` (trap never fires); grace ≈ 0; kill lags the ack ~4–80 s |
| E8 | Injected driver library paths; `nvidia-smi -q`; `SALAD_*` env; `/dev/shm` | **DONE** (below) |
| E9 | S4 upload via IMDS JWT from inside a container | **DONE** (works) |
| E10 | Cold-start distribution (probe image + ~8 GB CUDA image) | partial (below) |
| E11 | Ranged GET on a presigned URL; bandwidth spread | **DONE** — 206 on ranged GET; node downlink 755.7 Mbps after the probe fix |
| E12 | Gateway long-poll behavior at `wait_ms` 95 s vs 25 s | **DONE** — 25 s poll returns clean ~25.6 s; `wait_ms=95 s` is agent-capped to ~30.5 s, never nearing the 100 s gateway cut (below) |
| E13 | AMD/ROCm hello-world: `/dev/kfd`, `/dev/dri`, injected ROCm layout | **DONE** — AMD is WSL2 (`/dev/dxg`, no ROCm injected); baked `rocm-runtime` ROCm 7.x enumerates the RX 7800 XT over `/dev/dxg`, and a HIP matmul **executes** (760 GFLOPS, PASS) via the host's `librocdxg` dispatch backend (below) |

## Restart & exit semantics — E1/E2/E4 (live RTX 3060, 2026-07-20)

**A container group relaunches its container whenever it exits — regardless of exit
code OR `restart_policy` — to maintain the desired replica count. Exiting does NOT
stop the group.** This overturns the plan's §2.4 assumption ("exit 0 with
never/on_failure → instance stops, group succeeded"); that is job-queue behavior, not
container-group behavior.

Observed via `…/containers/{name}/system-logs` on single-replica groups:

- **E1 (`never`, exit 0):** `Running → Exited:0 → Starting → Running → Exited:0 → …`
  on the **same** machine, indefinitely. Group state stays `running` (running_count 1).
- **E2 (`never`, exit 1):** identical loop with `Exited:1`. Same node, no reallocation
  after 3+ restarts.
- **E4 (`on_failure`, exit 0) via `saladfingers run`:** reported a clean `Succeeded`
  stop — but ONLY because the CLI **deletes the group** the moment it collects the
  result envelope. The platform itself would have looped like E1.

`restart_policy` (`never` vs `on_failure`) made **no observable difference** here — both
relaunch on exit. Only **deleting the group** (or setting `replicas: 0`) stops it.

### Billing-safety consequence (important)

`saladfingers run` is safe *because the CLI deletes the group* after the first envelope
— not because the agent's exit stops anything. Therefore:

- The agent **cannot** self-stop: the Salad API key never enters the container, and IMDS
  offers only reallocate/recreate/restart — no "stop" (confirmed against
  `salad-cloud-imds.yaml`). The agent's idempotent-resume short-circuit prevents
  *re-running the job* on relaunch but does not stop the *billing loop*.

**FIXED — detached reaper (2026-07-20).** Non-`--detach` runs were always safe (the CLI
deletes the group on collecting the envelope). For `--detach`, `run` now spawns a
detached `saladfingers reap <run_id>` (own process group, survives CLI exit / terminal
close) that polls the run's result envelope in Garage and, once every shard is done — or a
hard cap (2× `max_duration`, ≤ 24 h) elapses — stops + deletes the groups. Verified live: a
`--detach` run reaped itself with no foreground CLI (`status=reaped`, quotas 0). `attach`
still works afterward (the envelope + outputs persist in Garage). **Residual:** if the
*machine* dies before the run finishes, the reaper dies with it — `gc` and the
end-of-session `replicas==0` check remain the backstop. Job queues (deferred) are the real
"run to completion" primitive.

## Restart depth, /work, IMDS reallocate — E3/E5 (live RTX 3060, 2026-07-20)

**E3 — `on_failure` + repeated exit 1:** the container is restarted **~5 times on the
same node**, then the platform **reallocates to a fresh node** and repeats (observed:
5× `Exited:1 → Starting` on machine A, then `Allocated` on machine B, 5× more, …). The
plan guessed ~3; it's ~5.

**E3 — `/work` does NOT survive a restart.** The test exits 0 iff `/work/marker` (written
on the previous boot) is still present; it exited **1 every single time**, on same-node
restarts *and* across reallocation. So the container filesystem (incl. `/work`) is **fresh
on every start** — there is no local persistence to lean on. **Consequence:** the
`exec-done.json` "skip re-exec on restart" optimization is dead; the agent's only
idempotency is the **result envelope in object storage** (which it already uses). Good —
it means no code relies on a false assumption.

**E5 — IMDS self-reallocate works.** A container that `POST`s `http://169.254.169.254/v1/reallocate`
(busybox `wget`, `Metadata: true` header, `{"reason":…}` body) is moved to a new node:
`Instance Reallocated by IMDS Request` (machine A) → `Instance Allocated` (machine B). This
is the exact mechanism the agent's bandwidth gate uses to escape a slow residential node —
confirmed functional end-to-end.

**E6 — SIGTERM grace on `/stop`: DONE (two live runs, RTX 3060 @ batch, 2026-07-21).** A
container running `/bin/sh -c` as PID 1 with a `trap … TERM` handler that emits a `CAUGHT`
marker then a 1 Hz `GRACE k` count-up (design verified locally: SIGTERM → `CAUGHT` →
`GRACE 0,1,2,…`) was `/stop`ped, and its stdout read back via the fixed `logs` command.
Result across **both** runs: **the trap never fired** — zero `CAUGHT`/`GRACE`/SIGTERM
markers. So `/stop` does **not** deliver a catchable SIGTERM to PID 1 before the SIGKILL;
from the container's point of view the grace window is effectively **zero**. The agent must
**not** rely on a SIGTERM handler to finalize on `/stop` — proactive checkpoint + the
envelope-as-commit-record design is load-bearing, not a nicety.

Two more facts fell out, both billing-relevant:

- **`Container Group Stopped` is a desired-state ack, not container death.** It lands ~0.4 s
  after the request, but the container keeps *running and emitting heartbeats* well past it
  before the abrupt kill — **~4 s on one node, ~80 s on another** (per-node variable). Treat
  the group flipping to `stopped` as "the platform accepted the request," not "the GPU is
  freed." For a prompt, reliable halt prefer agent self-exit (`exit 0` under `never`/
  `on_failure` stops the instance) or `DELETE`; always verify `replicas_used == 0` after.
- **Node clock skew is per-node and can be large.** One node's stdout timestamps aligned with
  the control-plane clock (~0 s skew); another was **~73 s behind**. Container-stdout
  `time` values are node-assigned — don't cross-reference them against control-plane events;
  use `system-logs` for wall-clock and the log-entry *ordering* within one container's stream
  (same clock) for relative timing like a grace window.

## Presigned ranged GET — E11 (live Garage, 2026-07-20)

A presigned GET URL on the Garage backend honors HTTP `Range`: `Range: bytes=0-99` →
**`206 Partial Content`, `content-range: bytes 0-99/609`**, 100-byte body. This is the
agent's bandwidth-gate download probe (ranged GET of a fixed window) — validated.

The "bandwidth spread across nodes" half is now measured too (live RTX 3060 @ batch,
2026-07-21). The probe's timed self-test previously reported nothing because
`bandwidth_mb` had no env binding — `SF_PROBE_BANDWIDTH_MB` was never read, so the size
defaulted to 0 and the probe skipped. With that fixed (env binding + a 32 MB default +
a public speed-test fallback when `SF_BANDWIDTH_URL` is unset), a node reported
**`measured_down_mbps: 755.7`** over a ranged GET. A single node isn't a "spread" yet,
but the mechanism now yields per-node Mbps; accumulate across runs for the distribution.

## Gateway long-poll — E12 (live RTX 3060 @ batch, 2026-07-22)

The container gateway (Cloudflare edge) caps a single response at `server_response_timeout` =
**100 s** (default AND max, §2.5). The agent's session long-poll (`GET /v1/exec/{id}/output?cursor=N&wait_ms=…`) clamps `wait_ms` to **`MAX_OUTPUT_WAIT_MS` = 30 000**,
precisely to stay under that. Measured **through the gateway** (session `serve`, `auth=true`, on
a freshly-built `gpu-probe` image) against a `sleep 300` exec that produces no output, so the
output long-poll actually blocks:

- **`wait_ms=25000`** → HTTP **200** at **~25.6 s** — blocks the full 25 s, returns cleanly
  (empty chunk set). The normal case (M5 already used this live).
- **`wait_ms=95000`** → HTTP **200** at **~30.5 s** — the agent clamps the wait to 30 s and
  returns cleanly. A "95 s" long-poll therefore **never reaches** the 100 s gateway cut; the
  30 s clamp makes it impossible to hit.

So E12's naive expectation ("95 s → cut") does **not** hold for our agent, by design: the 30 s
clamp keeps every session request comfortably under the gateway's 100 s limit. We deliberately
never exercise the raw 100 s cut because the agent has no path that holds a single response past
30 s — the cut is just the documented Cloudflare `server_response_timeout` we build under.
Result stable across two runs (25.9/30.96 s and 25.6/30.5 s), the second on a from-scratch image
build.

## Measured driver layout (E8 — live RTX 3060, 2026-07-18)

Probed with the `gpu-probe` image (base flavor, no CUDA layer) on an RTX 3060 (12 GB)
at `batch` priority:

- Injected libraries: **`/usr/lib64/libcuda.so.1`** and **`/usr/lib64/libnvidia-ml.so.1`**
  — exactly the marker-less legacy layout (`/etc/debian_version` absent → `/usr/lib64`;
  see `docs/images.md` for the mechanics). No `/usr/local/nvidia`, no `/opt/rocm`.
- Injected tool: **`/usr/bin/nvidia-smi`** — runnable only because the image ships the
  `/lib64/ld-linux-x86-64.so.2` FHS loader symlink; it reported
  `NVIDIA GeForce RTX 3060, 12288 MiB, driver 610.62` (one-node sample).
- Injected env (names; values are per-run): `SALAD_MACHINE_ID`, `SALAD_INSTANCE_ID`,
  `SALAD_CONTAINER_GROUP_ID/NAME`, `SALAD_PROJECT_ID/NAME`, `SALAD_ORGANIZATION_ID/NAME`,
  and `SALAD_METADATA_URI=http://169.254.169.254:80`.
- IMDS reachable (`Metadata: true`) and **S4 upload with the IMDS workload JWT works**
  (E9) — the agent's zero-secret control-plane path is real.

## First live runs — operational notes (2026-07-17/18)

- **SaladCloud runs containers inside WSL2** on the host PCs (their "Salad Enterprise
  Linux" distro, containerd + NVIDIA Container Toolkit; GPU via `/dev/dxg` paravirt).
  Net image-side effect is the standard legacy injection layout above.
- **`container.command` REPLACES the image ENTRYPOINT+CMD** (k8s-style). A
  subcommand-only argv execs a nonexistent binary → `Instance Start Failure: Other` →
  the group loops `downloading → creating → deploying` forever, indistinguishable from
  a pull problem in the group state. Diagnose via `…/containers/{name}/system-logs`
  (event sequence `Downloading → Starting → Start Failure` = pull OK, exec failed).
- Cold starts for the ~12 MB probe image, create → `running`: **108 s** and **78 s**
  (two samples; the pull itself is seconds — allocation dominates). All pre-`running`
  states are unbilled; the whole two-day bring-up (5 group creates, 2 of them reaching
  `running`) billed ≈ **$0.002**.
- Priority `batch` on RTX 3060 allocated within ~30 s every attempt (n=5).

## AMD/ROCm — E13 (live RX 7800 XT, 2026-07-20)

The **WSL-aware base probe** (2026-07-21) on an RX 7800 XT reported **`gpu_vendor: none`**
but pinned the AMD layout down:

- **WSL2 kernel** (Salad Enterprise Linux); the GPU is exposed only via **`/dev/dxg`**
  (WSL GPU-PV) plus **`/usr/lib/wsl/lib` = `libd3d12.so`, `libd3d12core.so`,
  `libdxcore.so`** — the DirectX paravirt userspace.
- **No `/dev/kfd`, no `/dev/dri`, no `/sys/class/drm` GPU entry, no injected ROCm.**
  Unlike NVIDIA (where `libcuda.so.1` + `nvidia-smi` are host-injected into `/usr/lib64`),
  **Salad injects no ROCm userspace on AMD** — the image must supply it.
- **IMDS, S4-JWT upload, `SALAD_*` env, and bandwidth (264 Mbps) all work** — the control
  plane is identical to NVIDIA nodes; only the GPU userspace differs.

Consequence for detection: because there is no `/dev/kfd` and no `/sys/class/drm` GPU
vendor on the WSL2 node, a container **cannot tell it is on AMD** from device facts alone
until ROCm userspace is baked in (the probe now reports `/dev/dxg` + the WSL libs, and
falls back to the `/sys/class/drm` PCI vendor `0x1002` where present — but this node
exposes neither, so in-container AMD identity requires the baked userspace or the
requested `gpu_class`).

**Both plan follow-ups are now implemented:**

1. **WSL/`dxg`-aware probe** (`probe.rs`): reports the WSL2 kernel, `/dev/dxg`, `/dev/dri`,
   `/sys/class/drm` PCI vendors, and `/usr/lib/wsl/lib`; detects AMD via the PCI vendor and
   runs `rocminfo` when present.
1. **`rocm-runtime` probe image** (`gpu-probe-rocm`, `nix/images.nix`): bakes `clr` +
   `rocminfo` + `rocm-smi` (all from the binary cache — no from-source ROCm/LLVM build) and
   links the binaries onto `PATH`.

**Re-probe with the ROCm image — ROCm WORKS on the WSL2 AMD node (RX 7800 XT @ batch,
2026-07-21).** The `gpu-probe-rocm` image (nixpkgs ROCm 7.2.3: `clr` + `rocminfo` +
`rocm-smi`, plus `libelf`/`libnuma`/`libdrm` — see the gotcha below) was probed and
`rocminfo` **enumerated the GPU**:

- `rocminfo` prints **"WSL environment detected."** and lists **Agent 2 = `gfx1101`,
  "AMD Radeon RX 7800 XT"**, 60 CUs, 16 GB, wavefront 32, ISA `amdgcn-amd-amdhsa--gfx1101`,
  `KERNEL_DISPATCH`. (Agent 1 is the host CPU.)
- So **ROCm 7.x's WSL support reaches the GPU over `/dev/dxg`** — the absent `/dev/kfd` is
  not a blocker. Standard nixpkgs ROCm userspace, baked into the image, is enough; no
  special ROCm-on-WSL distribution is needed.

**Gotcha (fixed in `image-lib.nix`):** the HSA runtime `dlopen`s `libelf.so.1` (and
`libnuma`/`libdrm`) at run time — these are **not** in `rocminfo`'s nix closure (ROCm
expects them from a system `/usr/lib`), so a first probe failed with
`rocminfo: error while loading shared libraries: libelf.so.1`. The `rocm-runtime` flavor
now adds `elfutils`/`numactl`/`libdrm` to `LD_LIBRARY_PATH`.

So E13's "hello-world" is **green**: the probe is vendor-aware (detects AMD, reports the
WSL2/`dxg` layout, parses the GPU agent's name), the `rocm-runtime` image flavor works, and
control-plane parity (IMDS/S4/env) means the agent's data plane already runs on AMD.

### E13 follow-up — the AMD GPU EXECUTES HIP compute (RX 7800 XT @ low, 2026-07-22)

A self-contained multi-arch HIP kernel (`examples/hip-matmul`, AOT-compiled for
gfx1100/1101/1200/1201) ran a 512×512 float matmul on a live node and **passed**:

```
HIP device: AMD Radeon RX 7800 XT (gfx1101), 30 CUs, 16177 MB VRAM, warp 32
matmul 512x512: 760.1 GFLOPS, max rel err 3.49e-07 -> PASS
```

So the fleet's AMD nodes don't just *enumerate* the GPU — they **dispatch and execute**
kernels over `/dev/dxg`, correct to CPU reference. No `/dev/kfd` needed.

**The load-bearing detail (cost two failed runs to find):** HIP *dispatch* needs the host's
`/dev/dxg` backend **`librocdxg.so`**, which is **not** in nixpkgs and **not** in the image.
The SaladCloud AMD/WSL2 host injects it at **`/opt/rocm-host/lib`** (alongside
`/opt/amdgpu/lib/x86_64-linux-gnu`) and **appends those dirs to `LD_LIBRARY_PATH` at run
time**. HSA *enumeration* (`rocminfo`) does not need it — which is why E13 looked green while
the first compute run died with `Cannot load librocdxg.so` → `undefined symbol: hsaKmtOpenKFD`
(the KFD fallback, absent on WSL2).

Three things follow, and pinning them down took a live `LD_TRACE_LOADED_OBJECTS` session:

1. **A compute binary must INHERIT `LD_LIBRARY_PATH`, never `--set` it.** A `makeBinaryWrapper --set LD_LIBRARY_PATH` *replaces* the variable and discards the host's `/opt/rocm-host/lib`
   append → `librocdxg` vanishes (this was a self-inflicted regression that cost two runs).
   `hip-matmul` is left unwrapped; `/opt/rocm-host/lib` + `/opt/amdgpu/lib/x86_64-linux-gnu`
   are also in `injectedLibDirs` as a fallback.
1. **The host's ROCm SHADOWS ours for the nixpkgs query tools.** `/opt/rocm-host/lib` holds a
   *complete* host ROCm — its own `libhsa-runtime64.so.1`, `libamdhip64.so.7`, `libamd_comgr`,
   `librocdxg`. It's on `LD_LIBRARY_PATH`, and `rocminfo`/`rocm-smi` use DT_RUNPATH, so their
   `libhsa` NEEDED resolves to the **host's** libhsa — which then can't find `libelf`/`libnuma`
   (absent from our minimal image) → `exit 127`, `libelf.so.1: cannot open`. `hip-matmul`
   dodges this only because `hipcc` bakes DT_RPATH, so its own `libamdhip64` wins.
   **Fix (`nix/images.nix` `rocmTools`):** wrap the query tools with `makeBinaryWrapper --prefix LD_LIBRARY_PATH` pointing at our `rocm-runtime` + `clr` lib dirs, so the image's libhsa (with
   correct nix RPATHs to libelf/libnuma) wins; `--prefix` keeps the host append so `librocdxg`
   still loads. Confirmed live via `LD_TRACE`: default → host libhsa + `libelf => not found` +
   rc 127; with the prepend → **rc 0, "WSL environment detected", `gfx1101` enumerated**.
1. **The AMD host injection OVERRIDES the image's `LD_LIBRARY_PATH` outright** — image-level
   `Env` additions never appear in the running container (verified: our `/nix/store` prefix was
   gone), so a per-binary wrapper is the *only* `LD_LIBRARY_PATH` tweak that survives on these
   nodes. (An earlier `ldStoreLibs` image-env attempt was reverted as futile.)

**Correction (2026-07-23, release validation A2): point 2's "hip-matmul dodges this via
DT_RPATH" was wrong — hipcc bakes DT_RUNPATH too** (verified with `readelf -d`: `RUNPATH`,
which loses to `LD_LIBRARY_PATH`), so the host's `libamdhip64` preempts ours exactly like the
host's `libhsa` preempts the query tools'. Whether that kills the run is **per-host**: a
self-contained `/opt/rocm-host/lib` loads fine (the Jul-22 760-GFLOPS pass ran on the HOST's
HIP stack, not ours), a leaner one dies `libelf.so.1: cannot open` → exit 127 (run
`sf-44thad`, RX 7800 XT). Every ROCm binary in an image therefore gets the same
`--prefix LD_LIBRARY_PATH` wrapper over our `clr`/`rocm-comgr`/`rocm-runtime` lib dirs —
our copy of every soname the host dir also ships, so the nix closure (whose RUNPATHs reach
libelf & co.) wins while the host's append still supplies `librocdxg`. Confirmed live on an
RX 7800 XT whose injected dir lacks libelf: unwrapped → exit 127; wrapped (run `sf-aqjyo2`)
→ **747.3 GFLOPS, max rel err 3.49e-07, PASS, exit 0**.

**Backlog:** an infurer ROCm/HIP backend (kernels + serving), building on this proven path.

## Measured CUDA/ROCm closure sizes

Per-flavor **GPU-userspace lib-set closure** — the unpacked size a flavor's GPU layer
(`gpuLibs` in `nix/image-lib.nix`) adds to an image, deduplicated across the flavor's
packages. Measured offline (2026-07-22) with **`nix path-info --closure-size`** over a
`buildEnv` of each flavor's lib set, against the flake's pinned nixpkgs. CUDA packages come
from the `allowUnfree`+`cudaSupport` import's `cudaPackages_12_9` (12.9) via each package's
`lib` output (`getLib`); ROCm from `rocmPackages` (7.2.3), packages taken directly — exactly
as `gpuLibs` assembles them. Figures are unpacked NAR bytes, so they bound on-node disk and
the 35 GB image cap; image *pull* is unbilled (§2.3), so the compressed layer size doesn't
affect run cost.

| Flavor | Package set | Unpacked closure | Notes |
| -------------- | -------------------------------------------- | ------------------------ | -------------------------------------------------------------------------------------- |
| `none` | _(empty)_ | **~base** (no GPU layer) | base = `sf-agent` + busybox + cacert (the ~12 MB probe image); the GPU *driver* is host-injected, never baked |
| `cuda-min` | `cuda_cudart` | **68.7 MiB** | CUDA runtime API only — smallest GPU flavor, **~34× lighter than `cuda-full`**; enough for AOT-compiled kernel-test images, so cold starts stay fast |
| `cuda-runtime` | `cuda_cudart libcublas cuda_nvrtc libcurand` | **1.23 GiB** (1260 MiB) | `libcublas` alone is ~860 MiB of the closure |
| `cuda-full` | `… + cudnn` | **2.28 GiB** (2337 MiB) | `cudnn` 9.22 adds **~1.05 GiB** over `cuda-runtime` |
| `rocm-runtime` | `clr rocminfo rocm-smi` | **882.9 MiB** (0.86 GiB) | HIP runtime (`clr`) + query tools; `clr`'s ~870 MiB closure subsumes almost all of it |

Reproduce (rocm-runtime shown; swap the lib set, and for the CUDA flavors use the
`{ config = { allowUnfree = true; cudaSupport = true; }; }` import's `cudaPackages_12_9`,
mapping each package through `lib.getOutput "lib"`):

```
nix build --impure --no-link --print-out-paths --expr \
  'with import <nixpkgs> {}; buildEnv { name = "f"; paths = with rocmPackages; [clr rocminfo rocm-smi]; }' \
| xargs nix path-info --closure-size -h
```

The CUDA figures are identical to the byte under the channel `<nixpkgs>` and the flake's
pinned nixpkgs; `rocm-runtime` lands within ~1% (874.0 MiB vs 882.9 MiB). Two of the plan's
pre-measurement estimates ran high: `cuda-full` is **2.3 GiB**, not the guessed ~3.9 GB
(cudnn's `lib` output is leaner than feared), and `rocm-runtime` is **0.86 GiB**, well under
the 1.5–2.5 GB guess; `cuda-runtime` (~1.2 GiB) matches. These are lib-set sizes, **not**
built-image sizes — e.g. the kernel-test *image* is ~2115 MiB because it also bakes
infurer's compiled kernels, not because `cuda-min` (69 MiB) is large.
