<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# macOS / cross-building

The `saladfingers` CLI and `sf-agent` build natively on `aarch64-darwin`
(`nix build`, `nix develop` work). But **all images are x86_64-linux** (SaladCloud is
linux/amd64 only), and the image packages are x86_64-linux-only
by design. This is a runbook, not automation.

## (a) Remote builder to an x86_64-linux box — the supported path

On the Mac, add the Linux box as a Nix remote builder. In `/etc/nix/machines`:

```
ssh-ng://<user>@<linux-box> x86_64-linux - 16 1 big-parallel,kvm - -
```

and in `nix.conf`:

```
builders = @/etc/nix/machines
builders-use-substitutes = true
```

Then from the Mac:

```sh
nix build .#packages.x86_64-linux.gpu-probe-image
```

**Push from the Linux box or CI, not the Mac** — pushing needs the layer store paths
locally, and copying a multi-GB closure back to the laptop defeats the purpose.

## (b) nix-darwin linux-builder — fallback

`nix.linux-builder.enable = true` spins up a local x86_64-linux VM. Resize its disk
well beyond the default before building CUDA/weights images.

## (c) qemu user-emulation — rejected

Building multi-GB images under qemu-binfmt user emulation is I/O-bound at single-digit
MB/s. Not viable for these closures.
