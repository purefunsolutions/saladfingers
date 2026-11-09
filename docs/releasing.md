<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Releasing to crates.io

The four crates — `saladfingers` (the CLI), `saladfingers-api`, `saladfingers-protocol`
and `saladfingers-agent` — are published to crates.io together, at one version, from
GitLab CI. The process is the one the infurer project's repositories use, and so are
its scripts: `nix/release.sh`, `nix/publish-releases.sh` and their test
`nix/release-test.nix` are furymodel's, unchanged. Keep them that way, and copy them
over by hand when furymodel's change. What is particular to saladfingers lives in
`nix/release.nix`.

## Versions

A release is `MAJOR.MINOR.PATCH`, or one of its pre-releases: `alpha.N`, then
`beta.N`, then `rc.N`. A version changes only in a release commit,
`saladfingers: <version>`; master keeps it until the next release commit.

`release` refuses the version master already has, so the first release can't be the
`0.1.0` the crates carry today: release a pre-release such as `0.1.0-alpha.1` first,
or another version such as `0.1.1`.

## Starting a release

From a checkout of the repository, with the signing key available to git:

```sh
nix run .#release -- 0.1.0-alpha.1   # --abort 0.1.0-alpha.1 drops it again
```

It works in a worktree of its own on `origin/master`, so the checkout's state doesn't
matter. It makes the release commit, checks that it changes nothing but versions (in
the manifests, the path dependencies' requirements and `Cargo.lock`), shows the diff
and asks before it signs the commit and the tag `v<version>`. Then it pushes both to
GitLab and opens the merge request, set to merge when its pipeline passes.

## What CI does

- **Before every merge**, the `publish-check` job runs
  `nix run .#publish-releases -- --check`: `cargo publish --dry-run`, which packages
  the crates and builds them from their packages in the release job's own environment.
  A merge request whose crates couldn't be published doesn't merge.
- **On master**, after `publish-github` has pushed master to GitHub, the `release` job
  runs `nix run .#publish-releases`. For each release tag merged into master that is
  not released yet, it checks that the tag is annotated, signed with the key whose
  fingerprint is `RELEASE_SIGNER` in `.gitlab-ci.yml`, and names its commit's version.
  It then pushes the tag to GitHub and publishes whatever crates of that version
  crates.io doesn't have yet. A run after a failure carries on where the last one
  stopped, and a run with nothing new does nothing.

Tags get no pipeline of their own, and `publish-github` pushes master alone, so a tag
reaches GitHub only through the `release` job, once checked.

## One-time setup

In GitLab, under the project's *Settings → CI/CD → Variables* and *Repository →
Protected tags*:

- `CARGO_REGISTRY_TOKEN`: masked, protected and hidden. A crates.io API token named
  `gitlab-ci saladfingers`, with the scopes `publish-new` and `publish-update`, limited
  to the crates `saladfingers` and `saladfingers-*`, and expiring after a year.
  crates.io's Trusted Publishing, which needs no token, doesn't support self-managed
  GitLab yet (2026-11).
- `RELEASE_SIGNING_KEY`: a protected *file* variable holding the public half of the
  release signing key:
  `gpg --export --armor 3606AD2B342F70B3B306E724BF5BDF55A9735223`.
- `v*` as a protected tag.

`GITHUB_DEPLOY_KEY_B64`, the GitHub deploy key that `publish-github` already uses,
pushes the release tags too.

## On crates.io

The first release publishes the crates under the crates.io account that owns the
token, and that account owns them from then on. A version can be yanked, but never
deleted or published again. crates.io rate-limits new crate names. If the first
release's `release` job stops partway through the four new crates, run it again:
it carries on with the crates that are missing.
