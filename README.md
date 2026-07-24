<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# saladfingers

Rent cheap consumer GPUs from [SaladCloud](https://salad.com) for **minimum billed
seconds**, plus the [Nix](https://nixos.org) tooling to build and push the GPU
container images those jobs run in.

SaladCloud bills per second, and **only while an instance is `running`** — image
download and container creation are free. saladfingers is built around that fact: a
tiny static agent (`sf-agent`) is baked into every image, starts in well under a
second, does the work, ships results to object storage, and exits so billing stops.

It is a small, general-purpose tool. Its first consumer is a from-scratch Rust LLM
project that needs real NVIDIA hardware for CUDA kernel testing, inference, and
training — but nothing here is specific to that project.

> Status: `run`, `session`, and `serve` modes are implemented and validated live on
> SaladCloud GPUs. See `docs/` for the design and empirical findings.

## What it does

- **`saladfingers run -- <cmd>`** — run a one-shot job on a rented GPU: create a
  single-replica group, stream logs, ship artifacts to S3-compatible storage, and
  delete the group. Billed ≈ the actual work.
- **`saladfingers session`** — an interactive GPU dev box for fast iteration
  (`exec`, `cp`, `logs`) with an idle deadman so it never bills forgotten.
- **`saladfingers serve`** — put an inference server behind the SaladCloud gateway
  with an idle-stop watchdog.
- **`saladfingers image push`** — build minimal, CUDA/ROCm layered,
  `sf-agent`-equipped images with `mkSaladImage` and push them by digest, recording
  the digest so `run --image <name>` deploys exactly what was pushed.
- **`saladfingers doctor` / `gpu-classes` / `quotas` / `bench` / `gpu-probe`** —
  read-only inspection and empirical node probing.

## Building

```sh
nix build              # the saladfingers CLI
nix develop            # dev shell with the pinned Rust toolchain + tooling
nix flake check        # clippy (-D warnings), tests, docs, formatting
```

Inside the dev shell:

```sh
cargo build --workspace
cargo nextest run --workspace
treefmt                # format everything
```

## Usage

```sh
saladfingers init                        # write ~/.config/saladfingers/config.toml
saladfingers doctor                      # validate config, check quotas
saladfingers gpu-classes                 # list GPU classes and prices
saladfingers run --profile kernels -- cargo test --release -- --ignored
```

Configuration layers (highest wins): CLI flags > environment > `./saladfingers.toml`

> `~/.config/saladfingers/config.toml`. The Salad API key is read from
> `SALAD_API_KEY`, `SALAD_API_KEY_FILE`, or `~/.config/saladfingers/api-key` (mode
> `0600`) — **never** from a committed file, and it is never passed into a container.
> See `saladfingers.toml.example`.

## Architecture

Four Rust crates:

- **`saladfingers-protocol`** — the wire contract between the CLI and the agent
  (`JobSpec`, `ResultEnvelope`, the session HTTP API, the transfer format).
- **`saladfingers-api`** — a hand-written, typed client for the SaladCloud REST API
  and S4 storage.
- **`saladfingers-agent`** — `sf-agent`, a small binary baked into every image
  (`run` / `serve` / `probe` modes).
- **`saladfingers-cli`** — the `saladfingers` binary.

Plus a Nix image library (`nix/image-lib.nix`) exporting `mkSaladImage`, which a
project imports to define its own CUDA/ROCm images and get a pushable,
digest-pinned OCI image. flake-parts consumers get it as a module
(`flakeModules.default`) instead:

```nix
flake-parts.lib.mkFlake {inherit inputs;} {
  imports = [inputs.saladfingers.flakeModules.default];

  perSystem = _: {
    saladfingers.images.kernel-test = {
      gpu = "cuda-min";              # none|cuda-min|cuda-runtime|cuda-full|rocm-runtime
      cudaPackages = inf.cudaPackages;
      contents = [kernelTests];
    };
  };
}
```

Each `saladfingers.images.<name>` becomes `packages.<name>-image` — exactly the
attribute `saladfingers image push <name>` builds and pushes. `pkgs` and the image
name come from the module, `sf-agent` and nix2container from saladfingers' own
locked inputs, so a consumer needs neither in their flake. saladfingers builds its
own images through the same module (`nix/images.nix`).

Docs live under [`docs/`](docs/): SaladCloud facts, [session &
serve](docs/serve.md) usage, image/layer policy, registry and storage runbooks, the
macOS cross-build story, the empirical node findings, and the
[security model](docs/security.md) — trust boundaries plus the two assumptions that
follow from never putting a credential inside a container.

## License

Licensed under any of [MIT](LICENSES/MIT.txt), [Apache-2.0](LICENSES/Apache-2.0.txt), or
[BSD-3-Clause](LICENSES/BSD-3-Clause.txt) at your option.
