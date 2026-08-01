<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Platforms

Where saladfingers runs, and what each part actually requires. macOS specifics live in
[macos.md](macos.md); this page is about everything else, including systems with no Nix.

## What needs Nix, and what does not

Only **`saladfingers image push`** needs Nix — it is the one command that *builds*
something. Everything else is a plain Rust binary talking to the SaladCloud API and
S3-compatible storage over HTTPS:

| command | needs |
| --- | --- |
| `run`, `tunnel`, `session`, `serve`, `attach`, `ls`, `logs`, `checkpoint`, `gc`, `cancel` | nothing but the binary |
| `doctor`, `gpu-classes`, `quotas`, `cost`, `bench`, `gpu-probe` | nothing but the binary |
| `image push` | `nix` + `skopeo`, and a flake using the saladfingers flakeModule |

So on a machine without Nix you can already do everything except build images, provided
you deploy by **literal reference**:

```sh
saladfingers run --image ghcr.io/org/img@sha256:abc… -- ./my-test
```

A literal ref is never rewritten and needs no lockfile — the lockfile only maps *bare
names* to pinned digests (see [images.md](images.md)). Build the image however you like;
if it carries `sf-agent` at `/bin/sf-agent`, saladfingers will drive it.

`sf-agent` itself is Linux-only (it runs *inside* the container). The `saladfingers` CLI
builds and runs on Linux and macOS, x86_64 and aarch64.

## Non-NixOS Linux

Nothing is NixOS-specific. On any distro, install Nix (the
[Determinate installer](https://github.com/DeterminateSystems/nix-installer) or your
distro's package), and `image push` works exactly as on NixOS — it shells out to `nix` and
`skopeo` and cares about neither the init system nor the filesystem layout. `nix develop`
provides `skopeo`; otherwise install it from your distro.

## Roadmap: no-Nix workflows

**Not implemented — this section is a plan, not a description.** The intent is that
saladfingers should install the way software normally installs on each platform, rather
than requiring Nix as a distribution mechanism.

- **Distro packages.** A `.deb` for Debian/Ubuntu built the ordinary cargo way
  (`cargo-deb`), an rpm later, so `apt install saladfingers` gets you the CLI with no Nix
  anywhere. The CLI has no unusual runtime dependencies, so this is packaging work, not
  porting work.

- **A statically linked `sf-agent`.** An `x86_64-unknown-linux-musl` build would be a
  single dependency-free binary a Dockerfile can consume directly:

  ```dockerfile
  COPY sf-agent /bin/sf-agent
  ENTRYPOINT ["/bin/sf-agent"]
  ```

  This looks viable: the agent loads no shared library at run time (no `dlopen`), so a
  fully static build has nothing to break. That would make every `docker build` /
  `podman` / `buildah` / Kaniko workflow a first-class way to produce a saladfingers image,
  with `mkSaladImage` remaining the option that gives you layer dedup and no local tarball.

- **`saladfingers image record`.** A command to write an externally built image's digest
  into `saladfingers-images.lock`, so deploys stay digest-pinned and reproducible even when
  the image was not built by `image push`.
