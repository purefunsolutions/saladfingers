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
