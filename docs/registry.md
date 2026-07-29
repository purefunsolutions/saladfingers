<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Registry

saladfingers is **registry-agnostic**: set `[registry] base` and `auth_kind` in your
config and it pushes there. SaladCloud pulls each image once into its own EU/US cache
and fans out to nodes, so a self-hosted registry pays upstream bandwidth only once
per image version.

Set credentials via the environment variables named in your config
(`username_env` / `password_env`) — never in a committed file. saladfingers maps them
onto SaladCloud's `registry_authentication` (`basic` for GHCR/GitLab/Quay/self-hosted,
`docker_hub` for Docker Hub).

## Pushing (`saladfingers image push`)

`image push` reads the registry base from `[registry] base` (or the
`SALADFINGERS_REGISTRY_REF` env var). There is **no default registry** — an unset base
is a hard error. It authenticates with `skopeo login … --password-stdin` (the token is
fed on stdin, never on the command line), pushes via the image's nix2container
`.copyTo` app, and records the digest-pinned ref in the committed
`saladfingers-images.lock`.

### Layer compression

`image push` pins the layer format and level rather than leaving them to skopeo:
**gzip level 9**, forced. What each setting produces, measured on a 1342 MiB CUDA image
(that figure is the *uncompressed* layer bytes, i.e. what `none` sends) over a
~160 KiB/s uplink:

| setting | on the wire | compress time | upload |
| --- | ---: | ---: | ---: |
| none | 1342 MiB | – | ~2.3 h |
| **gzip -9** | **756 MiB** | 16 s | ~80 min |
| zstd | 613 MiB | 59 s | ~65 min |

That is a menu, not a before/after — pushes were never raw. skopeo already gzips when
copying to a registry, but at the compressor's own default level (`flate.DefaultCompression`,
6), which is 798 MiB on the same image. So pinning buys ~42 MiB per push and an off
switch — and, mainly, it makes the format something you chose.

**The flags only govern layers the destination does not already hold.** A registry that
already has a layer's blob gets it referenced rather than re-uploaded, and the manifest
then points at whatever compression that blob was *first* pushed with. Measured on
`gpu-probe`: re-pushing it at `-9` over a copy made before this change yielded a manifest
where 8 of 11 blobs were byte-identical to the old level-6 push, and only 3 matched a
fresh `-9` compression (a fresh level-6 and a fresh level-9 build of that image share just
1 of 12 blobs, so this is blob reuse, not the two levels coinciding). A level or format
change therefore applies in full to layers that are new to the destination; to force it
for the rest, push to a repository that does not already have them.

**gzip is the default despite zstd being 143 MiB smaller, and this is not a preference:
SaladCloud nodes cannot decompress zstd layers.** A zstd push saves upload time and then
cannot be started — the worst of both. Deploying one image pushed both ways to the same
GPU class at batch: gzip reached `running` in 2 min 17 s, zstd never did, burning 10+
instances across 2 machines in 20 minutes on repeated `Instance Start Failure: Other`.
It downloads fine and then fails to unpack, and the event names nothing — it is
indistinguishable from flaky hardware. zstd wants containerd ≥ 1.5 / Docker ≥ 23 on the
puller and the nodes evidently predate that. See
[`salad-facts.md`](salad-facts.md) for the full measurement.

So `SALADFINGERS_PUSH_COMPRESSION=zstd` is **refused**, with an error explaining why. The
setting is still reachable for an image that is not bound for SaladCloud, but only by the
long spelling:

```sh
SALADFINGERS_PUSH_COMPRESSION=zstd-salad-cannot-pull-this saladfingers image push NAME
```

The acknowledgement is the value rather than a separate `_I_KNOW=1` variable so that it
stays visible wherever the setting is written down — a script, a CI variable, a shell
history — instead of sitting two files away from the thing it excuses. If SaladCloud
gains zstd support, the name reads as false and the gate can simply be deleted.

`SALADFINGERS_PUSH_COMPRESSION=none` sends raw and needs no ceremony (it is slow, not
broken), and `SALADFINGERS_PUSH_COMPRESSION_LEVEL` overrides the level. Any other value
is rejected before the push authenticates, rather than failing later in skopeo's
vocabulary.

Two things not worth trying: **the zstd level knob is coarse** — skopeo compresses with
Go's klauspost/compress, whose zstd folds numeric levels onto four speed tiers (11, 15 and
22 gave byte-identical output; hence the default of 19, since the top tier starts at 10),
while gzip honours its full 1–9 range; and **xz and bzip2 are unavailable** rather than
merely slower (skopeo has no xz compressor, and the OCI spec defines no `+bzip2` layer
media type). gzip and zstd are the whole menu.

Push credentials are resolved independently for the username and password, in order:

1. the env var **named by** `[registry] push_username_env` / `push_password_env`;
1. the conventional `SALADFINGERS_REGISTRY_PUSH_USER` / `SALADFINGERS_REGISTRY_PUSH_PASS`
   (these hold the value directly);
1. the pull-credential env vars named by `username_env` / `password_env`.

So a registry that uses one credential for both pull and push needs no extra config;
a registry with a separate push token gets it via the dedicated env vars above.

## Options

### Self-hosted standalone registry (recommended for heavy use)

Run [CNCF Distribution](https://distribution.github.io/distribution/) (`registry:2`)
or [zot](https://zotregistry.dev/) on a small VPS:

- htpasswd basic auth maps 1:1 to SaladCloud `basic` auth.
- TLS via Caddy/nginx; the registry **must be publicly reachable over HTTPS** so
  SaladCloud can pull.
- Optionally S3-backed. Because SaladCloud pulls once per version, even a modest
  uplink is plenty.

Pros: private, no third-party quotas, cheap at scale. Cons: you operate it.

### GitLab container registry

Zero extra service if you already use GitLab; deploy-token basic auth. But multi-GB
CUDA/weights blobs bloat GitLab storage and backups, registry GC is fiddly, and
availability is coupled to GitLab. Fine for small images, less so for baked weights.

### GHCR / Docker Hub (easiest for public users)

- **GHCR** (`ghcr.io`): public images have free egress; private images use a PAT
  (basic auth) with plan storage/bandwidth caps.
- **Docker Hub**: simplest; native `docker_hub` auth. Watch pull-rate limits and the
  single free private repo.
