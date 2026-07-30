<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# macOS

Everything works on `aarch64-darwin`, including `saladfingers image push` — a Mac builds
and pushes the same **linux/amd64** images a Linux host does, with **no Linux builder**.
This page explains why that is possible, when you still need one, and how to set it up.

## Why a Mac can build a linux/amd64 image

A Nix container image is **assembled from prebuilt binaries, never compiled**. That single
fact is what makes the whole thing work:

- The image *contents* (`sf-agent`, busybox, glibc, the CUDA/ROCm userspace) are
  x86_64-linux store paths that **substitute from a binary cache as-is**. Nothing needs to
  run them.
- nix2container computes layers from store **metadata** (`exportReferencesGraph`) and tars
  the paths. It never executes what it packages.
- The OCI architecture is a **flag**, not a property of the builder: nix2container
  hardcodes `OS = "linux"` and takes the arch from `--arch`.

So the only derivations that must actually *run* are the assembly glue — `buildEnv`
symlink trees, a `runCommand` writing `/etc`, and the layer/manifest JSON. Those are
`allowSubstitutes = false` (they can never be fetched), so they must be built wherever the
push happens. Pinning them to x86_64-linux was incidental.

`mkSaladImage` therefore takes two package sets: **`pkgs`** is the *target* (what lands in
the image — always linux/amd64), and **`nativePkgs`** is what *assembles* it (defaults to
`pkgs`, so the all-Linux path is bit-identical). On a Mac the glue is aarch64-darwin, the
contents stay x86_64-linux, and `copyTo` becomes a darwin app you can just run.

**The consequence worth remembering: an x86_64-linux builder is needed only for image
contents that must be *compiled*.** Contents that substitute from a cache need no x86_64
capability at all.

### `sf-agent` is compiled — so it is cross-compiled

`sf-agent` is baked into every image, and it is x86_64-linux machine code. Normally it
substitutes from a binary cache. But crane's `src` is the *whole workspace*, so while
developing saladfingers itself, any edit anywhere forces the agent to rebuild — and a Mac
cannot build an x86_64-linux derivation.

So on `aarch64-darwin` the agent is **cross-compiled** instead
(`packages.aarch64-darwin.sf-agent-linux`): rust-overlay's toolchain takes a `targets`
override, `pkgsCross.gnu64` supplies the C compiler that `aws-lc-sys` needs for its C and
assembly sources, and the resulting derivation's `system` is `aarch64-darwin`. Both halves
of that cross toolchain come **entirely from the binary cache** — no gcc or glibc is
compiled locally.

`mkSaladImage` picks it up automatically when a Mac assembles a linux/amd64 image. The
result is that a Mac builds the complete image alone, even with modified sources.

Three details, because they are not obvious and they bite:

- `rust-toolchain.toml` is **not** edited to add the target. It feeds every system's
  toolchain, so a target added there would change the x86_64-linux derivation hash and
  cost a binary-cache hit on the Linux path.
- Darwin's `strip`, nixpkgs' patchELF hook, and crane's darwin re-signing pass all have to
  be disabled (`dontStrip`, `dontPatchELF`, crane's `doNotSign`) — the output is a foreign
  ELF that none of them can handle.
- A single `patchelf` then sets the interpreter and RUNPATH. This is **not** cosmetic:
  without a RUNPATH the agent cannot resolve `libgcc_s.so.1` and will not start in the
  container, and the cross toolchain's glibc is a *different store path* from the one the
  image already carries for its `/lib64` loader symlink, which would otherwise pull a
  second redundant glibc into every image. Afterwards the ELF headers are identical to a
  Linux-built agent: same interpreter, same RUNPATH, same 57 MB closure.

Measured on an M3 (`nix build --dry-run`, cold store):

| image | must be built | fetched |
| --- | --- | --- |
| `gpu-probe-image` (darwin-native) | 13 derivations, all aarch64-darwin | 76 MiB |
| `infurer-containers#kernel-test-image` | 15 derivations, of which **only `infurer.drv`** (nvcc + rustc) is real x86_64 work | 176 MiB / 5.4 GiB unpacked |

## (a) Darwin-native — the default, nothing to set up

```sh
nix build .#packages.aarch64-darwin.gpu-probe-image   # linux/amd64 image, native build
saladfingers image push gpu-probe --tag v1
```

`image push` picks the system automatically: the **darwin** attribute on macOS,
`x86_64-linux` elsewhere. Override with `--system`, `SALADFINGERS_IMAGE_SYSTEM`, or
`[build] image_system`.

Images are declared once — under `saladfingers.imageSystem` (default `x86_64-linux`) — but
the flakeModule emits `<name>-image` in **every** system of your flake, so a consumer only
has to add `aarch64-darwin` to its `systems` list. Nothing else changes.

The trade-off: the image closure is substituted onto **your** machine and pushed over
**your** uplink. That is right for probe-sized images and a normal connection. For
multi-GB CUDA images on a slow link, use (c).

## (b) A local x86_64-linux builder — for *other* compiled contents

Not needed for saladfingers itself — the agent cross-compiles (above). You want this when
*your own* image contents must be compiled for x86_64-linux and are not set up to
cross-compile: CUDA kernels via nvcc, a Rust workspace of your own, and so on.

The working recipe is an Apple-Virtualization (`vz`) NixOS guest with nixpkgs' own
`virtualisation.rosetta` module enabled. That module mounts the Rosetta runtime the host
exposes over virtiofs, registers it with binfmt, and adds `x86_64-linux` to the guest
nix's `extra-platforms` (plus the sandbox paths builds need) — so one aarch64-linux VM
serves **both** Linux systems, with x86_64 *translated by Rosetta* rather than
software-emulated by qemu. Verified here: an x86_64-linux `nix build` delegated to such a
guest runs inside the nix sandbox and completes at translation speed; nvcc and rustc are
perfectly happy under it. Needs `softwareupdate --install-rosetta` once on the host, and
some aarch64-linux builder the first time the guest image itself is built.

Build it from stock parts: `pkgs.lima` (`vmType: vz`, `rosetta.enabled: true`) plus a
NixOS guest with `virtualisation.rosetta.enable = true`, wired into `nix.buildMachines`.
That is a couple of hundred lines of nix-darwin module with no third-party input, and it
is what this project's own infrastructure runs. (An off-the-shelf module,
[`nix-rosetta-builder`](https://github.com/cpick/nix-rosetta-builder), exists; note it
replaces `pkgs.lima` with a personal fork — review it yourself before putting it in a
build machine's position of trust.)

No local VM at all is also a fine answer: rent a small x86_64-linux VPS from any cloud
provider, install Nix, and list it in `nix.buildMachines` — everything builds natively
there, no Rosetta involved. It pairs especially well with `--on` (below): the same box
does the compiling *and* the pushing, so on a slow home uplink the multi-GB image closure
never touches your machine at all, and a datacenter uplink does the registry upload.

Older advice this page used to give — nix-darwin's `linux-builder`, or qemu user-emulation
via `binfmt` — is superseded. Both software-emulate; Rosetta translates. If the VM will
also build NixOS disk images (anything spawning a *nested* QEMU), give it real KVM with
lima's `nestedVirtualization: true` (Apple silicon M3+, macOS 15+) — without it the inner
QEMU falls back to software emulation and such builds take hours.

## (c) `--on <ssh-host>` — build and push somewhere else

```sh
saladfingers image push kernel-test --on user@builder.example.com
```

Evaluation stays local, but the **store** is the remote: the image closure is substituted
straight onto that machine from *its* binary caches and pushed from there. Only the
derivation graph (kilobytes) crosses your link. Use it when the remote's uplink beats
yours, or when you want a beefier machine to do the compiling.

`--on` implies `--system x86_64-linux` (the remote does the build, so your platform is
irrelevant). The remote needs only `nix` and `ssh` — `copy-to` brings its own skopeo.

Credentials: `skopeo login` runs **locally**, so bad credentials fail before an expensive
build; the authfile is then streamed over the encrypted channel into a `0600` file in a
`0700` remote temp dir, removed afterwards. The token never appears in a command line or
process table on either machine. This does make the build host **trusted** — it holds a
registry push token for the duration; see [security.md](security.md).

Set a permanent default with:

```toml
[build]
host = "user@builder.example.com"
```

## Checking your setup

`saladfingers doctor` reports `nix`, `skopeo`, the image system a push would use, and
whether a builder for it is reachable. All of it is warn-only — these matter only for
`image push`.
