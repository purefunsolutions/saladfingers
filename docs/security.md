<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# Security model

Who is trusted, what the code defends against, and the two assumptions that fall out of
the design and cannot be engineered away.

## Trust boundaries

| Party | Trusted? | Notes |
| --- | --- | --- |
| The operator's machine — CLI, config, storage keys, Salad API key | **Yes** | Holds every credential. |
| A `--on <ssh-host>` build host | **Yes** | Same side of the boundary as the operator's machine: during `image push --on` it holds a registry push token (a `0600` file, removed afterwards) and produces the image that will run. Point it only at hosts you control. |
| The SaladCloud control plane | **Yes** | Issues gateway URLs, schedules groups, is the billing authority. |
| **The rented GPU node**, and the `sf-agent` running on it | **No** | Consumer hardware owned by strangers. It executes your command and can read anything inside the container. |
| End users of `serve --proxy` | **No** | The gateway fronts it with `auth=false`; the app behind it enforces its own auth. |
| Anything that can reach `saladfingers tunnel`'s loopback port | **Yes, by construction** | The tunnel attaches the operator's Salad API key to every forwarded request, so a local process reaching it acts with that key against that run's gateway. The listener is loopback-only and has no flag to widen it. |

The load-bearing consequence: **no credential ever enters a container.** The agent
receives presigned URLs, plus the instance-metadata JWT for S4 — never the SaladCloud API
key, never storage keys. Both assumptions below are the price of that choice.

## What is defended

So that the assumptions below are read in context, these are already handled in code:

- Every field of the untrusted `ResultEnvelope` is validated before use: artifact names
  must be plain relative paths and must match an output the run actually declared; the
  reported part count is capped at the run's `max_artifact_parts`; a zero-part artifact is
  refused rather than silently satisfying the SHA-256 gate.
- Archive extraction is confined by `unpack_in` (no path traversal, no absolute or
  symlinked entries) and capped at 100× the compressed size, so a hostile `tar | zstd`
  cannot fill the collector's disk.
- The `serve --proxy` reverse proxy targets a fixed loopback address, strips hop-by-hop
  headers, and does not follow upstream redirects.
- `run --expose-port` creates the gateway with `auth=true`, so an exposed training
  dashboard is never public, and `saladfingers tunnel` binds loopback only with no option
  to widen it — a wider bind would re-publish that deliberately private port to the local
  network, pre-authenticated with the operator's key.
- `saladfingers tunnel` does not follow upstream redirects either, and for a sharper
  reason than the in-container proxy: it attaches `Salad-Api-Key`, and a **custom** header
  is one reqwest does not strip when a redirect crosses hosts (it drops only
  `AUTHORIZATION`/`COOKIE`/…). Following one 3xx from an app behind the gateway — an
  app-level open redirect is enough — would therefore hand the operator's account-wide
  key to whatever host it named. The 3xx goes to the browser instead, with a `Location`
  naming the gateway rewritten onto the tunnel so the browser is not walked off it.
- The session API compares its bearer token in constant time and fails closed when the
  token is unset.
- Local secrets are written owner-only at creation (0600 file, 0700 directory), and a
  hand-written, world-readable API key file is flagged on use.

## Assumption 1 — the result envelope is self-reported

`ResultEnvelope` is written by `sf-agent` **on the rented node**, through a presigned PUT
the node fully controls. `saladfingers run` maps its `status` / `exit_code` onto the CLI's
own process exit code, which is exactly what makes `saladfingers run -- cargo test` usable
in CI.

A compromised node can therefore report `Succeeded` / exit 0 for work it never ran, or
report failure for work that succeeded. The per-artifact `sha256` does not close this: it
protects against a **torn multi-part reassembly** (a node that died mid-upload leaving a
mixed-generation stream), not against a malicious node, which can simply make its bytes
and its hash agree.

What *is* guaranteed is the envelope's **shape**, not its **semantics**: a hostile envelope
cannot escape the output directory, name an artifact you never asked for, or exhaust your
disk — but whether the job genuinely passed is the node's word.

**This is not fixable in software here.** The node runs the work, so any attestation it
produces is produced by code the node controls. Real assurance would need hardware
attestation (a TEE), which a consumer GPU fleet does not offer.

**Guidance.** Treat the exit code as a *correctness* signal, not an *integrity* guarantee.
Do not gate a security-relevant decision — a release, a signing step, a deployment — solely
on a rented node's self-report. If you need integrity, verify independently on trusted
hardware: have the job emit an artifact you re-check locally, or compare against a digest
computed somewhere you trust.

## Assumption 2 — presigned URLs are long-lived capabilities that cannot be revoked

The `JobSpec` handed to each run contains every presigned URL that run needs: input GETs,
output PUTs, checkpoint PUT/GET/**DELETE**s, the log PUT, the gate probe, and the
attempts/result PUT+GET. The spec itself is fetched through a presigned GET passed to the container as
`SF_JOB_URL`.

The DELETEs are the one destructive verb in the set, and they are there because retention
is one checkpoint: after committing a new one the agent removes the slot it superseded,
and it holds no credentials with which to do that any other way. They are scoped to the
checkpoint slot keys this spec already writes — the run's own, or the shared name's under
`--checkpoint-prefix` (below) — the same objects its PUTs already overwrite, so they add
no reach, only a way to lose data faster than an overwrite would.

Lifetime is deliberately generous, because the agent may still need to upload hours later
after a reallocation, long after the CLI process is gone:

```
expiry = clamp(2 × max_duration, floor = 72 h, ceiling = 7 days)
```

Note the **72-hour floor**: shortening a run's `--max-duration` does *not* shrink the
window below three days. The result-envelope GET is likewise pinned at 72 h.

Anyone who obtains a spec — or the URLs inside it — can, until expiry, read that run's
inputs, overwrite its outputs and checkpoints, and forge its result envelope. And a
presigned URL **cannot be individually revoked**: the only remedies are rotating the
storage credentials that signed it (which invalidates every outstanding URL) or deleting
the objects it points at.

The blast radius is bounded: URLs are scoped to specific object keys under
`runs/<run_id>/`, not to the bucket, so a leaked spec exposes that run's objects and
nothing else.

**`--checkpoint-prefix` widens that one boundary, deliberately.** A shared checkpoint
lives under `ckpts/<name>/` rather than the run, so a leaked spec from *any* run using
that name carries PUT/GET/DELETE over the shared checkpoint — including runs that have not
started yet. That is the same trade the feature exists to make (the checkpoint outliving
its run is the point), but it means a shared name is only as private as the least careful
run that uses it. `gc` does not reap these; `saladfingers checkpoint rm --prefix` does.

**Guidance.** Treat a `JobSpec` as a secret — it is a bundle of live capabilities, not
metadata. If one leaks, rotate the storage credentials named by `[storage] access_key_env` / `secret_key_env`; that invalidates outstanding URLs across all runs.
`saladfingers gc` removes a run's remote objects when it reaps that run's leaked group,
closing the window early for it — a run that completed normally keeps its objects (and
their URLs' targets) until then, so credential rotation is the remedy that always works.

## Known limitations and testing status

**The two assumptions are accepted residual risk, not defects.**
[Assumption 1](#assumption-1--the-result-envelope-is-self-reported) (a node self-reports
its own result) and
[Assumption 2](#assumption-2--presigned-urls-are-long-lived-capabilities-that-cannot-be-revoked)
(a presigned URL is a live, unrevocable capability) both fall out of the load-bearing
design choice that **no credential ever enters a container**. Neither can be engineered
away at this layer — closing them would need hardware attestation and per-request URL
revocation that a consumer GPU fleet and plain presigned storage do not offer. They are
documented and bounded, not open findings.

**How the properties are actually assured.** Every protection in
[What is defended](#what-is-defended) is backed by code-review reasoning plus unit and
integration tests, and every fix in the hardening series landed with a regression test
that asserts the guard trips on the crafted input that motivated it. That establishes the
checks are present and fire on the inputs we thought to write.

**What has *not* happened: adversarial testing.** The system has never been run against a
deliberately hostile SaladCloud node, and there has been no penetration test or red-team
exercise. The unit tests assert the *intended* check fires on a *crafted* input; none of
them stand up the full stack with a genuinely malicious node in the loop and observe the
outcome end to end. The one exception is the reverse proxy: request-target host injection
is covered by an adversarial raw-TCP integration test
(`crates/saladfingers-agent/tests/proxy.rs`) that writes `@host`, `//host`, absolute-form,
and percent-encoded-`@` request targets straight to the running proxy socket and asserts a
separate attacker listener is *never* contacted. That is the closest thing to adversarial
testing in the tree today — the exception, not the rule.

**An adversarial-test checklist.** A hostile-node or red-team exercise should drive the
actual untrusted-node surface — a malicious node writing the result envelope and serving
the agent/proxy — and confirm each attempt is refused, bounded, or logged, never a
compromise. Concretely, what to test:

- **Envelope artifact name, path traversal:** a `ResultEnvelope` artifact whose `name`
  carries `..` or an absolute/rooted path, aiming to write outside the run's output tree
  (should hit the `is_safe_relative` path-shape gate in `admit_output`, `runner.rs`).
- **Envelope artifact name, undeclared output:** a well-formed *relative* name the run
  never declared, aiming to materialise an unexpected file (should hit the declared-output
  allow-list in `admit_output`).
- **Envelope part count, resource exhaustion:** an artifact claiming more `parts` than the
  run's `max_artifact_parts`, aiming to drive `(0..parts)` presigned-URL generation to OOM
  the host (should hit the part-count cap in `admit_output`).
- **Envelope zero-part artifact:** an artifact reported present with zero parts and the
  empty-string SHA-256, aiming to satisfy the integrity gate vacuously (should hit the "no
  parts to download" refusal in `transfer.rs::download_artifact`).
- **Torn / mixed-generation reassembly:** multi-part objects from different upload
  generations stitched together, aiming to pass a partial or stale artifact off as whole
  (should hit the per-artifact SHA-256 check in `download_artifact`).
- **Decompression bomb:** a tiny `tar|zstd` output or checkpoint that expands enormously,
  aiming to fill the collector's disk (should hit the 100× `MAX_DECOMPRESS_RATIO` cap in
  `transfer.rs::decompress`).
- **Tar entry escaping the destination:** an archive entry with a traversal, absolute, or
  symlinked path, aiming to write outside the extraction root (should hit the `unpack_in`
  guard in `decompress`).
- **Proxy request-target host injection (SSRF):** crafted request targets steering the
  proxy's upstream fetch off its fixed loopback address (already covered by the raw-TCP
  test above — re-run it as the template for any new vector).
- **Proxy open-redirect → SSRF amplification:** the fronted app returns a 3xx pointing at
  an attacker host, aiming to make the proxy chase it server-side (should be neutralised by
  `redirect(Policy::none())`, which returns the redirect to the client instead of
  following it).
- **Agent bearer token, brute force / timing:** many or near-miss tokens against the
  session / `serve --proxy` API, probing for a length- or content-dependent timing signal
  (should be flattened by the constant-time `ct_eq` compare; also confirm no route beyond
  the health check answers unauthenticated).
- **Agent API with `SF_AGENT_TOKEN` unset:** start the agent with no token and confirm it
  fails closed — refusing to serve the exec/file API unless `--allow-unauthenticated` is
  passed explicitly.
- **Spec substitution:** overwrite the `job.json` object between the CLI's upload and the
  agent's fetch, aiming to swap in a different spec (should hit the now-armed
  `SF_JOB_SHA256` check, where the agent recomputes the digest and refuses to run on a
  mismatch).
- **Presigned-URL abuse inside the validity window:** using a leaked input / output /
  checkpoint / envelope URL from a `JobSpec` to read that run's inputs or forge its outputs
  (the [Assumption 2](#assumption-2--presigned-urls-are-long-lived-capabilities-that-cannot-be-revoked)
  surface — expected to *succeed* within `runs/<run_id>/` until expiry; this test documents
  the blast radius, not a defended boundary).

**Bottom line.** The untrusted-node boundary has been reviewed repeatedly, and every
concrete finding to date is fixed and regression-tested. But the honest next increase in
assurance is adversarial testing against a genuinely hostile node — exercising the
checklist above with a real node in the loop — not another pass of static review.
