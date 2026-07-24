<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Images

saladfingers builds minimal, layered, linux/amd64 OCI images with
[nix2container](https://github.com/nlewo/nix2container). Every image carries the
the `sf-agent` binary at `/bin/sf-agent` (the entrypoint) and busybox, so
`container.command = ["/bin/sh", "-c", "…"]` always works and per-run behavior can be
changed via SaladCloud's `container.command` / env without rebuilding the image.

## Why nix2container

- Layers stream from `/nix/store` at push time — no multi-GB tarball is ever
  materialized locally (critical near SaladCloud's 35 GB compressed image cap).
- Its patched skopeo skips blobs the registry already has, so a large baked-weights
  layer uploads once per registry, ever.
- Explicit `buildLayer` pinning keeps CUDA and each weights entry in stable layers
  that dedup across image versions (and stay in SaladCloud's 30-day blob cache).

## `mkSaladImage` flavors (`gpu = …`)

| flavor | userspace libraries baked in | ~unpacked | ~compressed |
| -------------- | -------------------------------------------------------------- | --------: | ----------: |
| `none` | none (base image: sf-agent + busybox + cacert) | ~12 MB | ~12 MB |
| `cuda-min` | cudart only (kernel tests: link libcudart, launch own kernels) | ~1.2 GB | **0.63 GB** |
| `cuda-runtime` | cudart, cublas(+Lt), nvrtc, curand | ~1.15 GB | ~0.55 GB |
| `cuda-full` | cuda-runtime + cudnn | ~3.9 GB | ~1.9 GB |
| `rocm-runtime` | clr (HIP), rocminfo, rocm-smi | ~1.5–2.5 GB | (measure) |

Sizes are estimates; measured values land here after M3. The host injects the GPU
**driver** (`libcuda.so.1` / amdgpu); images bring only userspace.

## Host driver injection: what images must provide

SaladCloud (and any nvidia-container-toolkit host) injects the driver via the legacy
prestart hook. Verified against libnvidia-container source and confirmed on a live
node (`docs/empirical.md`):

- **Driver libs land in `/usr/lib64`** for images without `/etc/debian_version` (ours),
  or `/usr/lib/x86_64-linux-gnu` with it; injected binaries land in `/usr/bin`. The
  hook auto-creates missing mount targets — no FHS stub dirs needed.
- **A writable `/etc` is required**: the hook runs the *host's* ldconfig chrooted into
  the rootfs to regenerate `/etc/ld.so.cache` and create the `libcuda.so.1` SONAME
  links; if that fails the container never starts.
- **Nix glibc never reads `/etc/ld.so.cache`** (nixpkgs patches it out), so the baked
  `LD_LIBRARY_PATH` is what makes the injected `libcuda.so.1` resolvable to our
  binaries — `mkSaladImage` includes `/usr/lib64`, `/usr/lib/x86_64-linux-gnu`,
  `/usr/local/nvidia/lib{,64}`, and `/usr/lib/wsl/lib` (SaladCloud nodes are WSL2).
- **`/lib64/ld-linux-x86-64.so.2`** (symlinked to the image's glibc) is what lets
  injected FHS binaries like `nvidia-smi` execute at all; `mkSaladImage` bakes it.
- Injection triggers on **`NVIDIA_VISIBLE_DEVICES`** in the merged OCI env;
  `mkSaladImage` bakes `=all` plus `NVIDIA_DRIVER_CAPABILITIES=compute,utility` so
  images also work on hosts that don't set it platform-side.
- Deliberately **not** in images: ldconfig / a prebuilt `ld.so.cache` (unnecessary —
  see above), `/etc/debian_version` (would only relocate the lib dir), and the
  `CUDA_VERSION` env var (it flips the toolkit into legacy `cuda>=X.Y` requirement
  checks that can fail on older host drivers).

## SM-arch × GPU-class policy

CUDA kernels are compiled where the *consumer* builds them (e.g. infurer's
`INFURER_SM_ARCHS`), not in this library. Recommended split:

- **RTX 30/40-series** image: `INFURER_SM_ARCHS = "86,89"`, CUDA 12.9 userspace.
- **RTX 50-series** image: `INFURER_SM_ARCHS = "120"`, CUDA ≥ 12.8 userspace.

Point each image's `gpu_classes` at matching hardware; never run a 50-series card
against a CUDA-12.x-old image.

## Baking vs fetching weights

Image download time is **unbilled**, so baking multi-GB weights into an image is the
cheap, repeatable path (one entry per safetensors shard → one dedup-friendly layer
each, ≤ ~5 GB compressed per entry). Runtime fetch (`SF_PREFETCH_n=url=>dir`) is
billed time but iterates faster during bring-up.

## Using the flakeModule

`mkSaladImage` is callable directly (`inputs.saladfingers.lib.mkSaladImage`), but a
flake-parts consumer should import `inputs.saladfingers.flakeModules.default` and
declare images as options instead. Before:

```nix
perSystem = {pkgs, ...}: {
  packages.kernel-test-image = inputs.saladfingers.lib.mkSaladImage {
    inherit pkgs;
    name = "kernel-test";
    gpu = "cuda-min";
    cudaPackages = inf.cudaPackages;
    contents = [kernelTests];
  };
};
```

After:

```nix
imports = [inputs.saladfingers.flakeModules.default];   # or .images — same module

perSystem = _: {
  saladfingers.images.kernel-test = {
    gpu = "cuda-min";
    cudaPackages = inf.cudaPackages;
    contents = [kernelTests];
  };
};
```

What the module guarantees:

- **`saladfingers.images.<name>` → `packages.<name>-image`.** That suffix is a
  contract, not a convention: `saladfingers image push <name>` runs
  `nix run <root>#packages.<system>.<name>-image.copyTo` (see
  `crates/saladfingers-cli/src/image.rs`). Deriving it from the attribute name is the
  point — a misspelled package attribute is no longer possible, and `push` works for
  any declared image without further wiring.
- **`pkgs` and `name` are injected** — `pkgs` is the consumer's `perSystem` pkgs,
  `name` is the attribute key. Setting either in a definition is an error. Everything
  else (`tag`, `gpu`, `cudaPackages`, `rocmPackages`, `contents`, `extraContents`,
  `cmd`, `entrypoint`, `env`, `ports`, `weights`, `maxLayers`) is passed through to
  `mkSaladImage` untouched.
- **No nix2container input, no agent rebuild.** The module closes over *saladfingers'*
  `self` and `inputs`, so the prebuilt `sf-agent` and nix2container come from
  saladfingers' lockfile; the consumer's inputs need only saladfingers itself (plus
  whatever supplies their CUDA/ROCm packages).

saladfingers' own images (`nix/images.nix`) are declared through this same module, so
CI builds it on every push.

## Building & pushing

```sh
nix build .#packages.x86_64-linux.gpu-probe-image     # build a manifest (no push)
saladfingers image push gpu-probe --tag v1            # build + skopeo push, record digest
```

## The image lockfile

`image push` records the pushed digest in `saladfingers-images.lock` at the root of
whichever repository you run it from — i.e. **your** project, not saladfingers. Commit it:
that file is what makes a deploy reproducible.

```json
{
  "gpu-probe": {
    "ref": "registry.example.com/my-org/salad/gpu-probe@sha256:abc…",
    "digest": "sha256:abc…",
    "flakeRev": "…",
    "pushedAt": "…"
  }
}
```

Every command that deploys an image resolves a **bare image name** through that lockfile to
the pinned `ref`, so you deploy exactly the image that was pushed rather than whatever a
mutable tag points at now:

```sh
saladfingers run --image gpu-probe -- ./my-test     # → …/gpu-probe@sha256:abc… (pinned)
saladfingers run --image ghcr.io/org/img:v1 -- …    # a literal ref is used as given
```

Anything that is not a lockfile key — a tag ref, a `@sha256:` ref, any full registry
reference — passes through untouched, so an explicit reference is never rewritten. With no
lockfile present (the common case when deploying by literal ref) nothing changes.

Two resolution chains, differing only in their fallback:

| commands | image comes from |
| --- | --- |
| `run`, `session create`, `serve up` | `--image` → the profile's `image` → lockfile |
| `gpu-probe`, `bench startup`, `doctor --live` | `--image` → `SALADFINGERS_PROBE_IMAGE` → lockfile |

The probe commands are not profile-driven, which is why they fall back to an environment
variable instead; the name-pinning step is identical.
