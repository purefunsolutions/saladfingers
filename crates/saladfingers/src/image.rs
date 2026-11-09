// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers image push NAME [--tag T]` — build a GPU image with nix2container,
//! push it to the configured registry, and record the pushed digest in the committed
//! `saladfingers-images.lock`.
//!
//! Sequence: resolve the registry base from config (no default — a loud error if
//! unset) → construct the tagged destination ref → `skopeo login … --password-stdin`
//! into a private temp `--authfile` (never `--dest-creds`, which would leak the token
//! into argv) → build+push via the image's nix2container `.copyTo` app, capturing the
//! pushed digest from a `--digestfile` → merge `{name → {ref, digest, flakeRev,
//! pushedAt}}` into the lockfile (digest-pinned `ref`, sorted keys, pretty JSON).
//!
//! Where that build+push runs is the one platform-dependent part. `.copyTo` is a package
//! of the system it is built under, so it must be a system this machine can execute:
//! `x86_64-linux` on Linux, but the *darwin* attribute on macOS, which assembles the same
//! linux/amd64 image natively (see `nix/image-lib.nix`). `--on <ssh-host>` instead builds
//! and pushes on a remote, keeping the multi-GB closure off this machine's link entirely.
//!
//! Security: this module reads the registry host, org, and credentials only by
//! *reference* (config keys / env-var names). No registry host, org, or secret is
//! ever hard-coded here, and the push token is passed to skopeo on stdin — never on
//! the command line or in any log line.

use std::collections::BTreeMap;
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::cli::ImagePushArgs;
use crate::config::{Config, RegistryConfig};
use crate::output::{OutputFormat, print_json, print_table, table};

/// The lockfile, at the repo root. `image push` writes it into whichever repository it
/// runs from — i.e. the *consuming* project, which commits it so deploys are reproducible.
pub const LOCKFILE_NAME: &str = "saladfingers-images.lock";

/// Env var that overrides the configured registry base (`[registry] base`).
const REGISTRY_REF_ENV: &str = "SALADFINGERS_REGISTRY_REF";
/// Conventional env var holding the push username directly (not a name-of-var).
///
/// `pub(crate)` so `doctor` checks the same variable `image push` reads, rather
/// than a second copy of the string that could drift from it.
pub(crate) const PUSH_USER_ENV: &str = "SALADFINGERS_REGISTRY_PUSH_USER";
/// Conventional env var holding the push password/token directly (see
/// [`PUSH_USER_ENV`] for why this is `pub(crate)`).
pub(crate) const PUSH_PASS_ENV: &str = "SALADFINGERS_REGISTRY_PUSH_PASS";
/// Layer compression for the push: `gzip` (default), `none`, or — only via the
/// long spelling in [`ZSTD_ACKNOWLEDGED`] — zstd. Anything else is rejected.
///
/// **gzip is not a preference, it is a requirement: SaladCloud nodes cannot
/// unpack zstd layers.** Measured by pushing one image both ways and deploying
/// each to the same GPU class at batch: the `tar+gzip` copy reached `running` in
/// 2 min 17 s, while the `tar+zstd` one never did, burning 10+ instances across
/// 2 machines in 20 minutes on repeated `Instance Start Failure: Other`. It
/// downloads fine and then fails to unpack, so the fault is the image rather
/// than any one machine — and the event names nothing that points at
/// compression, which makes it indistinguishable from flaky hardware and an
/// expensive thing to rediscover. `docs/salad-facts.md` records the measurement;
/// the failed run cost nothing, since only `running` bills.
///
/// What each setting produces, measured on a 1342 MiB CUDA image (that figure
/// is the *uncompressed* layer bytes, i.e. what `none` sends):
///
/// | setting  | on the wire | compress time |
/// |----------|-------------|---------------|
/// | none     |    1342 MiB | –             |
/// | gzip -9  |     756 MiB | 16 s          |
/// | zstd     |     613 MiB | 59 s          |
///
/// Read that as a menu, not as a before/after: pushes were never raw. skopeo
/// already gzips when copying to a registry, at the compressor's own default
/// level — 798 MiB on the same image (see [`GZIP_MAX_LEVEL`]). So pinning this
/// buys ~42 MiB per push, plus an off switch, plus the guarantee that a
/// Salad-bound image is never accidentally zstd.
///
/// **These flags only govern layers the destination does not already hold.** A
/// registry that already has a layer's blob gets it referenced, not re-uploaded,
/// and the manifest then points at whatever compression that blob was *first*
/// pushed with. Measured on the 58.7 MiB `gpu-probe` image: pushing it at `-9`
/// over a registry copy made before this change produced a manifest in which 8
/// of its 11 blobs were byte-identical to the old level-6 push, and only 3
/// matched a fresh `-9` compression of the same image (which shares just 1 of 12
/// blobs with a fresh level-6 one, so this is reuse, not the two levels
/// coinciding). Changing the level or the format therefore takes full effect on
/// layers new to that destination; to force it for the rest, push to a
/// repository that does not already have them.
///
/// Two dead ends, recorded so nobody re-derives them:
///
///   * **The zstd level knob is coarse.** skopeo compresses with Go's
///     klauspost/compress (pure Go — not the reference C library), whose
///     `EncoderLevelFromZstd` folds numeric levels onto four speed tiers:
///     levels 11, 15 and 22 all produced byte-identical output in the same
///     59 s. The real C library at `-19 --ultra --long=27` reaches 541 MiB on
///     the same bytes, 12% better than Go's best — but getting it would mean
///     forking skopeo onto a cgo zstd binding.
///   * **xz and bzip2 are not options.** skopeo has no xz compressor at all
///     (`cannot find compressor for "xz"`), and while it recognises bzip2 the
///     OCI spec defines no `+bzip2` layer media type, so the destination
///     rejects it. gzip and zstd are the whole menu.
const PUSH_COMPRESSION_ENV: &str = "SALADFINGERS_PUSH_COMPRESSION";
/// Overrides the compression level (see [`PUSH_COMPRESSION_ENV`] for the format).
const PUSH_COMPRESSION_LEVEL_ENV: &str = "SALADFINGERS_PUSH_COMPRESSION_LEVEL";
/// Universally pullable, unlike zstd — which SaladCloud nodes cannot decompress.
const DEFAULT_PUSH_COMPRESSION: &str = "gzip";
/// The only spelling that selects zstd. A plain `zstd` is refused.
///
/// zstd is the better compressor and the worse outcome: the push succeeds, ~19%
/// faster, and the image then cannot be started on any SaladCloud node. Every
/// cost lands after the setting is out of sight — a full upload, then a
/// reallocation loop indistinguishable from bad hardware — which is exactly the
/// shape of mistake a short, guessable value invites.
///
/// So the acknowledgement is the value rather than a second variable: it is
/// visible in the shell history, CI config, or script that sets it, where a
/// separate `..._I_KNOW=1` two files away would not be. It also states the
/// specific fact rather than generic bravado, so when SaladCloud does support
/// zstd this name reads as false and it is obvious the gate should go: delete
/// this constant and accept `"zstd"` in [`compression_args_from`].
const ZSTD_ACKNOWLEDGED: &str = "zstd-salad-cannot-pull-this";
/// Maximum gzip, and worth pinning: leaving the level unset is *not* "best
/// effort". containers-image's gzipCompressor takes its `level == nil` branch,
/// `pgzip.NewWriter` → `flate.DefaultCompression` (6), which measured 798 MiB
/// where `-9` gives 756 MiB on the same image. Uploading runs at ~220 KiB/s
/// against a compressor doing tens of MB/s, so the highest level is
/// unconditionally the right trade — gzip is never the bottleneck.
const GZIP_MAX_LEVEL: &str = "9";
/// zstd's *tier* selector, not a literal level: klauspost quantizes everything
/// from 10 upwards onto its top tier, and the 613 MiB above was measured there.
/// A plain 9 would land one tier below it and quietly miss that number.
const ZSTD_BEST_TIER_LEVEL: &str = "19";
/// Env var overriding the flake system images are built under (default below).
const IMAGE_SYSTEM_ENV: &str = "SALADFINGERS_IMAGE_SYSTEM";
/// The system images are *declared* under (`saladfingers.imageSystem`), and the build
/// system for any non-macOS host. Images are linux/amd64 whatever builds them.
const DEFAULT_IMAGE_SYSTEM: &str = "x86_64-linux";

/// One recorded image entry in `saladfingers-images.lock`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LockEntry {
    /// Digest-pinned reference (`<base>/<name>@sha256:…`) — what `run` deploys.
    #[serde(rename = "ref")]
    image_ref: String,
    /// The pushed manifest digest (`sha256:…`).
    digest: String,
    /// `git rev-parse HEAD` of the flake pushed from (`…-dirty` if the tree was dirty).
    #[serde(rename = "flakeRev")]
    flake_rev: String,
    /// When the push happened (RFC3339 UTC).
    #[serde(rename = "pushedAt")]
    pushed_at: String,
}

/// The lockfile: image name → entry. A `BTreeMap` gives a stable, sorted key order.
type Lockfile = BTreeMap<String, LockEntry>;

/// `saladfingers image push NAME [--tag T]`.
///
/// Synchronous (it shells out to `skopeo` and `nix`), like `init`; dispatched without
/// `.await`.
///
/// # Errors
/// Returns an error if the registry is unconfigured, push credentials are missing,
/// or any of the login / build-push / lockfile steps fail.
pub fn push(cfg: Config, args: ImagePushArgs) -> Result<()> {
    let base = resolve_registry_base(&cfg)?;
    let host = registry_host(&base)?;
    // Resolved up front: a rejected compression setting must fail here, not after
    // an authentication and however much of a multi-GB upload.
    let compression = compression_args()?;
    let (user, pass) = resolve_push_credentials(cfg.registry.as_ref())?;

    let tagged_ref = tagged_ref(&base, &args.name, &args.tag);
    let system = image_system(&args, &cfg);
    let remote = remote_host(&args, &cfg).map(str::to_string);
    let root = repo_root();

    // A private (0700) temp dir holds the authfile (registry token, 0600 via skopeo)
    // and the digestfile. Dropped at the end → both are removed.
    let tmp = tempfile::Builder::new()
        .prefix("saladfingers-push-")
        .tempdir()
        .context("creating temp dir for authfile/digestfile")?;
    let authfile = tmp.path().join("auth.json");
    let digestfile = tmp.path().join("digest");

    eprintln!("image push {}: authenticating to {host}...", args.name);
    skopeo_login(&host, &user, &pass, &authfile)?;

    let digest = if let Some(remote) = remote.as_deref() {
        eprintln!(
            "image push {}: building and pushing docker://{tagged_ref} on {remote} \
             (system {system})...",
            args.name
        );
        push_via_remote(
            &root,
            remote,
            &system,
            &args.name,
            &tagged_ref,
            &authfile,
            &compression,
        )?
    } else {
        eprintln!(
            "image push {}: building and pushing docker://{tagged_ref} (system {system})...",
            args.name
        );
        run_copy_to(
            &root,
            &system,
            &args.name,
            &tagged_ref,
            &digestfile,
            &authfile,
            &compression,
        )?;
        read_digest(&digestfile)?
    };

    let image_ref = digest_ref(&base, &args.name, &digest);
    let flake_rev = flake_rev(&root);
    let pushed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let entry = LockEntry {
        image_ref: image_ref.clone(),
        digest: digest.clone(),
        flake_rev: flake_rev.clone(),
        pushed_at: pushed_at.clone(),
    };
    let lock_path = root.join(LOCKFILE_NAME);
    let mut lock = load_lockfile(&lock_path)?;
    lock.insert(args.name.clone(), entry);
    write_lockfile(&lock_path, &lock)?;
    eprintln!(
        "image push {}: recorded in {}",
        args.name,
        lock_path.display()
    );

    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&serde_json::json!({
            "name": args.name,
            "ref": image_ref,
            "digest": digest,
            "tag": args.tag,
            "flakeRev": flake_rev,
            "pushedAt": pushed_at,
            "lockfile": lock_path.display().to_string(),
        }))?,
        OutputFormat::Table => {
            let mut t = table(&["field", "value"]);
            t.add_row(vec!["name".to_string(), args.name.clone()]);
            t.add_row(vec!["ref".to_string(), image_ref]);
            t.add_row(vec!["digest".to_string(), digest]);
            t.add_row(vec!["tag".to_string(), args.tag.clone()]);
            t.add_row(vec!["flakeRev".to_string(), flake_rev]);
            t.add_row(vec!["pushedAt".to_string(), pushed_at]);
            t.add_row(vec![
                "lockfile".to_string(),
                lock_path.display().to_string(),
            ]);
            print_table(&t);
        }
    }
    Ok(())
}

// ---- registry resolution --------------------------------------------------

/// Resolve the registry base: `SALADFINGERS_REGISTRY_REF` env > `[registry] base`.
/// There is no default registry (saladfingers is registry-agnostic), so an unset
/// base is a hard, actionable error.
fn resolve_registry_base(cfg: &Config) -> Result<String> {
    if let Some(v) = non_empty_env(REGISTRY_REF_ENV) {
        return Ok(v);
    }
    cfg.registry
        .as_ref()
        .map(|r| r.base.trim().to_string())
        .filter(|b| !b.is_empty())
        .context(
            "no container registry configured — set `[registry] base` in your config \
             (or the SALADFINGERS_REGISTRY_REF env var). saladfingers has no default \
             registry; see docs/registry.md",
        )
}

/// The hostname portion of a registry base (`registry.example.com/org/x` →
/// `registry.example.com`; a `host:port` prefix is preserved). This is what
/// `skopeo login` authenticates against.
fn registry_host(base: &str) -> Result<String> {
    let stripped = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .unwrap_or(base);
    let host = stripped.split('/').next().unwrap_or("").trim();
    if host.is_empty() {
        bail!("could not determine the registry host from base {base:?}");
    }
    Ok(host.to_string())
}

/// The tagged push destination (`<base>/<name>:<tag>`), used by `docker://`.
fn tagged_ref(base: &str, name: &str, tag: &str) -> String {
    format!("{}/{name}:{tag}", base.trim_end_matches('/'))
}

/// The digest-pinned reference recorded in the lockfile (`<base>/<name>@<digest>`).
fn digest_ref(base: &str, name: &str, digest: &str) -> String {
    format!("{}/{name}@{digest}", base.trim_end_matches('/'))
}

/// Resolve push credentials, returning `(username, password)`.
///
/// Each value is resolved independently in this order (documented for users in
/// `docs/registry.md`):
/// 1. the env var *named by* `[registry] push_username_env` / `push_password_env`;
/// 2. the conventional `SALADFINGERS_REGISTRY_PUSH_USER` / `_PASS` (direct value);
/// 3. the env var named by the pull creds `[registry] username_env` / `password_env`.
///
/// A missing value is a hard error (we never push anonymously).
fn resolve_push_credentials(reg: Option<&RegistryConfig>) -> Result<(String, String)> {
    let (user, user_src) = pick_credential(
        reg.and_then(|r| r.push_username_env.as_deref()),
        PUSH_USER_ENV,
        reg.and_then(|r| r.username_env.as_deref()),
        &non_empty_env,
    )
    .context(
        "no registry push username — set SALADFINGERS_REGISTRY_PUSH_USER (or point \
         `[registry] push_username_env` / `username_env` at the env var holding it)",
    )?;
    let (pass, pass_src) = pick_credential(
        reg.and_then(|r| r.push_password_env.as_deref()),
        PUSH_PASS_ENV,
        reg.and_then(|r| r.password_env.as_deref()),
        &non_empty_env,
    )
    .context(
        "no registry push password — set SALADFINGERS_REGISTRY_PUSH_PASS (or point \
         `[registry] push_password_env` / `password_env` at the env var holding it)",
    )?;
    if let Some(warning) = pull_fallback_warning(user_src, pass_src) {
        eprintln!("{warning}");
    }
    Ok((user, pass))
}

/// Which step of the resolution chain produced a credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialSource {
    /// The env var named by `[registry] push_username_env` / `push_password_env`.
    NamedPush,
    /// The conventional [`PUSH_USER_ENV`] / [`PUSH_PASS_ENV`].
    Convention,
    /// Last resort: the PULL credential. Legitimate when one token carries both
    /// scopes, and a trap otherwise — see [`pull_fallback_warning`].
    PullFallback,
}

/// Pure credential-picking used by [`resolve_push_credentials`]; `env` is the
/// environment lookup (real env in production, a fake map in tests).
///
/// Returns the value together with the step that produced it, so the caller can
/// tell a deliberate single-token setup from an unconfigured one.
fn pick_credential(
    named_push: Option<&str>,
    convention_var: &str,
    named_pull: Option<&str>,
    env: &impl Fn(&str) -> Option<String>,
) -> Option<(String, CredentialSource)> {
    named_push
        .and_then(env)
        .map(|v| (v, CredentialSource::NamedPush))
        .or_else(|| env(convention_var).map(|v| (v, CredentialSource::Convention)))
        .or_else(|| {
            named_pull
                .and_then(env)
                .map(|v| (v, CredentialSource::PullFallback))
        })
}

/// The warning to print when a push is about to authenticate with the PULL
/// credential, or `None` when it is not.
///
/// The pull credential is the one handed to SaladCloud nodes at deploy time, so
/// it is routinely read-only. When it is, this push has already failed — but it
/// fails at the registry, several minutes and every blob later, as "requested
/// access to the resource is denied" with no mention of credentials at all. A
/// single token holding both scopes is a legitimate setup, so this stays a
/// warning rather than an error; it just refuses to be silent.
///
/// Either half is enough: a real push username paired with the pull token as its
/// password is denied exactly the same way.
fn pull_fallback_warning(user_src: CredentialSource, pass_src: CredentialSource) -> Option<String> {
    if user_src != CredentialSource::PullFallback && pass_src != CredentialSource::PullFallback {
        return None;
    }
    Some(format!(
        "image push: warning — no push credential configured, falling back to the PULL \
         credential. That credential is the one handed to SaladCloud nodes for deploy-time \
         pulls and is usually read-only, in which case login will succeed and the first layer \
         upload will be denied. Set {PUSH_USER_ENV} / {PUSH_PASS_ENV}, or point \
         `[registry] push_username_env` / `push_password_env` at the env vars holding them."
    ))
}

// ---- external tools -------------------------------------------------------

/// `skopeo login <host> --username <user> --password-stdin --authfile <authfile>`.
/// The password is written to skopeo's stdin — never passed on the command line.
fn skopeo_login(host: &str, user: &str, pass: &str, authfile: &Path) -> Result<()> {
    let mut child = Command::new("skopeo")
        .arg("login")
        .arg(host)
        .arg("--username")
        .arg(user)
        .arg("--password-stdin")
        .arg("--authfile")
        .arg(authfile)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning `skopeo login` (is skopeo on PATH? run inside `nix develop`)")?;
    {
        let mut stdin = child.stdin.take().context("skopeo stdin unavailable")?;
        stdin
            .write_all(pass.as_bytes())
            .context("writing registry password to skopeo stdin")?;
        // stdin dropped here → EOF, so skopeo proceeds.
    }
    let out = child
        .wait_with_output()
        .context("waiting for `skopeo login`")?;
    if !out.status.success() {
        // stderr may mention the username but never the password (stdin-fed).
        bail!(
            "skopeo login to {host} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// The skopeo flags that pin layer compression, or empty for
/// `SALADFINGERS_PUSH_COMPRESSION=none`.
///
/// A function rather than flags inlined at the call site, so that every push path
/// gets them by construction — a path that forgets them sends whatever skopeo
/// defaults to, which is the one outcome [`PUSH_COMPRESSION_ENV`] exists to rule out.
///
/// # Errors
/// Returns an error for an unrecognised format, and for a bare `zstd` — see
/// [`ZSTD_ACKNOWLEDGED`].
fn compression_args() -> Result<Vec<String>> {
    compression_args_from(&non_empty_env)
}

/// Pure form of [`compression_args`]; `env` is the environment lookup (real env in
/// production, a fake map in tests), the same seam [`pick_credential`] uses.
fn compression_args_from(env: &impl Fn(&str) -> Option<String>) -> Result<Vec<String>> {
    let requested =
        env(PUSH_COMPRESSION_ENV).unwrap_or_else(|| DEFAULT_PUSH_COMPRESSION.to_string());
    let format = match requested.as_str() {
        // Raw: slow to upload, but every node can start it.
        "none" => return Ok(Vec::new()),
        "gzip" => "gzip",
        ZSTD_ACKNOWLEDGED => "zstd",
        "zstd" => bail!(
            "{PUSH_COMPRESSION_ENV}=zstd refused: SaladCloud nodes cannot unpack zstd \
             layers. The push itself would succeed — and then every run of this image \
             would loop forever on \"Instance Start Failure: Other\", reallocating across \
             node after node, which looks exactly like flaky hardware and says nothing \
             about compression. If this image is not bound for SaladCloud, set \
             {PUSH_COMPRESSION_ENV}={ZSTD_ACKNOWLEDGED}."
        ),
        other => bail!(
            "{PUSH_COMPRESSION_ENV}={other:?} is not a format skopeo can produce — it \
             offers only gzip and zstd (no xz, and no OCI media type for bzip2). Use \
             `gzip` (the default), `none`, or `{ZSTD_ACKNOWLEDGED}`."
        ),
    };
    let level = env(PUSH_COMPRESSION_LEVEL_ENV).unwrap_or_else(|| {
        if format == "gzip" {
            GZIP_MAX_LEVEL
        } else {
            ZSTD_BEST_TIER_LEVEL
        }
        .to_string()
    });
    // `--dest-force-compress-format` is the operative flag: without it skopeo keeps
    // the source layers as they are and the requested format is a no-op.
    Ok(vec![
        "--dest-compress-format".to_string(),
        format.to_string(),
        "--dest-compress-level".to_string(),
        level,
        "--dest-force-compress-format".to_string(),
    ])
}

// ---- remote build + push (`--on`) -----------------------------------------

/// Build and push entirely on `host`, returning the pushed digest.
///
/// Evaluation stays local (`--eval-store auto`) but the *store* is the remote, so the
/// image closure is substituted straight onto that machine from its own binary caches
/// and pushed from there — nothing multi-GB crosses this machine's link. That is the
/// whole point of `--on`: use it when the remote's uplink beats yours.
///
/// The registry token never touches a command line or the remote's process table:
/// `skopeo login` has already run locally, and the resulting authfile is streamed over
/// the encrypted channel into a 0600 file inside a 0700 remote temp dir, which is
/// removed again whether or not the push succeeded.
fn push_via_remote(
    root: &Path,
    host: &str,
    system: &str,
    name: &str,
    tagged_ref: &str,
    authfile: &Path,
    compression: &[String],
) -> Result<String> {
    let attr = format!("{}#packages.{system}.{name}-image.copyTo", root.display());
    let out = capture(
        Command::new("nix")
            .arg("build")
            .arg("--store")
            .arg(format!("ssh-ng://{host}"))
            .arg("--eval-store")
            .arg("auto")
            .arg("--no-link")
            .arg("--print-out-paths")
            .arg(&attr)
            .current_dir(root),
        "building the image on the remote store (`nix build --store ssh-ng://…`)",
    )?;
    let copy_to = out
        .lines()
        .next_back()
        .map(str::trim)
        .filter(|p| p.starts_with("/nix/store/"))
        .with_context(|| format!("`nix build {attr}` printed no store path (got {out:?})"))?
        .to_string();

    let tmp = remote_mktemp(host)?;
    // From here on every early return must still clean up the remote dir.
    let result = (|| -> Result<String> {
        let auth = format!("{tmp}/auth.json");
        let digest = format!("{tmp}/digest");

        let bytes = std::fs::read(authfile).context("reading the local authfile to stream")?;
        ssh_stdin(
            host,
            &format!("umask 077 && cat > {}", shell_quote(&auth)),
            &bytes,
            "streaming the registry authfile to the remote",
        )?;

        let push = format!(
            "{} docker://{} --digestfile {} --authfile {}{}",
            shell_quote(&format!("{copy_to}/bin/copy-to")),
            shell_quote(tagged_ref),
            shell_quote(&digest),
            shell_quote(&auth),
            // The same compression the local path uses. The remote's uplink is the
            // reason to use `--on` at all, so sending raw layers there would defeat it.
            compression
                .iter()
                .map(|a| format!(" {}", shell_quote(a)))
                .collect::<String>(),
        );
        // Inherited stdio: the skopeo upload is the slow part, so its progress must show.
        let status = Command::new("ssh")
            .arg(host)
            .arg(&push)
            .status()
            .context("spawning `ssh … copy-to` (is ssh on PATH?)")?;
        if !status.success() {
            bail!("remote image push failed (`copy-to` on {host} exited with {status})");
        }

        let raw = ssh_capture(
            host,
            &format!("cat {}", shell_quote(&digest)),
            "reading the pushed digest from the remote",
        )?;
        parse_digest(&raw)
    })();

    // Best-effort: the push already succeeded or failed on its own merits and a failed
    // cleanup must not mask that, but a leftover authfile is worth saying out loud.
    if let Err(e) = ssh_capture(
        host,
        &format!("rm -rf {}", shell_quote(&tmp)),
        "removing the remote temp dir",
    ) {
        eprintln!("warning: could not remove {host}:{tmp} (it holds a registry token): {e:#}");
    }
    result
}

/// `mktemp -d` on the remote, validated before it is ever interpolated into a command.
fn remote_mktemp(host: &str) -> Result<String> {
    let raw = ssh_capture(
        host,
        "umask 077 && mktemp -d /tmp/saladfingers-push-XXXXXXXX",
        "creating a temp dir on the remote",
    )?;
    let path = raw.trim().to_string();
    if !is_safe_remote_tmp(&path) {
        bail!("remote mktemp returned an unexpected path: {path:?}");
    }
    Ok(path)
}

/// Whether a remote `mktemp -d` result is the shape we asked for. Guards against a
/// remote whose shell profile prints banners, and keeps `rm -rf` pointed somewhere sane.
fn is_safe_remote_tmp(path: &str) -> bool {
    path.strip_prefix("/tmp/saladfingers-push-")
        .is_some_and(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// Single-quote a value for the remote shell (ssh always runs the command through one).
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Run `ssh host <cmd>` and capture stdout.
fn ssh_capture(host: &str, cmd: &str, what: &str) -> Result<String> {
    capture(Command::new("ssh").arg(host).arg(cmd), what)
}

/// Run a prepared command and capture stdout, failing with its stderr.
fn capture(cmd: &mut Command, what: &str) -> Result<String> {
    let out = cmd
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("spawning: {what}"))?;
    if !out.status.success() {
        bail!(
            "{what} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Run `ssh host <cmd>` feeding `input` on stdin (used to stream the authfile).
fn ssh_stdin(host: &str, cmd: &str, input: &[u8], what: &str) -> Result<()> {
    let mut child = Command::new("ssh")
        .arg(host)
        .arg(cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning: {what}"))?;
    {
        let mut stdin = child.stdin.take().context("ssh stdin unavailable")?;
        stdin
            .write_all(input)
            .with_context(|| format!("writing stdin for: {what}"))?;
    }
    let out = child
        .wait_with_output()
        .with_context(|| format!("waiting for: {what}"))?;
    if !out.status.success() {
        bail!(
            "{what} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// Build and push the image via its nix2container `.copyTo` app, writing the pushed
/// digest to `digestfile`. Inherits stdio so nix build/push progress is visible.
///
/// `compression` comes from [`compression_args`], resolved by the caller so an
/// unusable setting is rejected before anything is authenticated or uploaded.
fn run_copy_to(
    root: &Path,
    system: &str,
    name: &str,
    tagged_ref: &str,
    digestfile: &Path,
    authfile: &Path,
    compression: &[String],
) -> Result<()> {
    let attr = format!("{}#packages.{system}.{name}-image.copyTo", root.display());
    let status = Command::new("nix")
        .arg("run")
        .arg(&attr)
        .arg("--")
        .arg(format!("docker://{tagged_ref}"))
        .arg("--digestfile")
        .arg(digestfile)
        .arg("--authfile")
        .arg(authfile)
        .args(compression)
        .current_dir(root)
        .status()
        .context("spawning `nix run … copyTo` (is nix on PATH?)")?;
    if !status.success() {
        bail!(
            "image build/push failed (`nix run {attr}` exited with {status}).\n\
             If the attribute does not exist, this saladfingers predates cross-system \
             image packages — update it, or build under the declaring system with \
             `--system {DEFAULT_IMAGE_SYSTEM}` (which needs a {DEFAULT_IMAGE_SYSTEM} \
             builder configured)."
        );
    }
    Ok(())
}

/// The `<system>` component of the image flake attribute, highest precedence first:
/// `--system` > `SALADFINGERS_IMAGE_SYSTEM` > `[build] image_system` > the default for
/// this host ([`default_image_system`]).
fn image_system(args: &ImagePushArgs, cfg: &Config) -> String {
    let trimmed = |s: &str| -> Option<String> {
        let t = s.trim();
        (!t.is_empty()).then(|| t.to_string())
    };
    args.system
        .as_deref()
        .and_then(trimmed)
        .or_else(|| non_empty_env(IMAGE_SYSTEM_ENV))
        .or_else(|| cfg.build.image_system.as_deref().and_then(trimmed))
        .unwrap_or_else(|| {
            default_image_system(
                remote_host(args, cfg).is_some(),
                env::consts::OS,
                env::consts::ARCH,
            )
            .to_string()
        })
}

/// The image system to build when nothing was configured explicitly.
///
/// `.copyTo` is an executable of the system it was built under, so it has to be one this
/// machine can run. A macOS host therefore builds the *darwin* attribute: the image is
/// still linux/amd64, but its assembly derivations are native, so no Linux builder is
/// involved at all (see `nix/image-lib.nix`). Every other host keeps `x86_64-linux`,
/// which is both the declaring system and — on Linux — natively runnable.
///
/// With `--on`, the build happens on the remote, so the local platform is irrelevant.
fn default_image_system(remote: bool, os: &str, arch: &str) -> &'static str {
    if remote || os != "macos" {
        return DEFAULT_IMAGE_SYSTEM;
    }
    match arch {
        "x86_64" => "x86_64-darwin",
        // aarch64 today; anything else macOS might run would still be arm64.
        _ => "aarch64-darwin",
    }
}

/// The SSH host that should build and push, if any: `--on` > `[build] host`.
fn remote_host<'a>(args: &'a ImagePushArgs, cfg: &'a Config) -> Option<&'a str> {
    args.on
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| configured_build_host(cfg))
}

/// The `[build] host`, if set — the `--on` default. Used by `doctor` to report where a
/// push would build.
#[must_use]
pub fn configured_build_host(cfg: &Config) -> Option<&str> {
    cfg.build
        .host
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// The image system a push would use with no `--system` flag, for `doctor` to report.
#[must_use]
pub fn effective_image_system(cfg: &Config) -> String {
    non_empty_env(IMAGE_SYSTEM_ENV)
        .or_else(|| {
            cfg.build
                .image_system
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            default_image_system(
                configured_build_host(cfg).is_some(),
                env::consts::OS,
                env::consts::ARCH,
            )
            .to_string()
        })
}

/// Whether a flake system's derivations can execute on this machine — i.e. whether a
/// `.copyTo` built for it could be run directly. Rosetta/binfmt can widen this in
/// practice, so it is only used to decide whether to *look* for a remote builder.
#[must_use]
pub fn is_locally_runnable(system: &str) -> bool {
    let Some((arch, os)) = system.split_once('-') else {
        return false;
    };
    let os_matches = match env::consts::OS {
        "macos" => os == "darwin",
        other => os == other,
    };
    os_matches && arch == env::consts::ARCH
}

/// Read + validate the pushed digest from the digestfile written by `copyTo`.
fn read_digest(digestfile: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(digestfile)
        .context("reading the pushed digest (copyTo did not write a --digestfile)")?;
    parse_digest(&raw)
}

/// Validate a digest as written by `copyTo`, from wherever it was read — a local file,
/// or the stdout of a `cat` over ssh.
fn parse_digest(raw: &str) -> Result<String> {
    let digest = raw.trim().to_string();
    if !digest.starts_with("sha256:") || digest.len() <= "sha256:".len() {
        bail!("copyTo wrote an unexpected digest: {digest:?}");
    }
    Ok(digest)
}

// ---- git / repo root ------------------------------------------------------

/// The repository root (`git rev-parse --show-toplevel`), or the current dir if this
/// is not a git checkout. The lockfile lives here and `nix run` targets this flake.
fn repo_root() -> PathBuf {
    run_git(Path::new("."), &["rev-parse", "--show-toplevel"])
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// The flake revision to record: `git rev-parse HEAD`, plus `-dirty` if the tree has
/// uncommitted changes to tracked files (nix's notion of dirty). `unknown` outside git.
fn flake_rev(root: &Path) -> String {
    match run_git(root, &["rev-parse", "HEAD"]) {
        Some(rev) if git_is_dirty(root) => format!("{rev}-dirty"),
        Some(rev) => rev,
        None => "unknown".to_string(),
    }
}

/// Whether the tree has uncommitted changes to tracked files (staged or unstaged).
/// Untracked files (e.g. a not-yet-committed lockfile) don't count — nix's flake
/// build ignores them, so they must not flip `flakeRev` to `-dirty`.
fn git_is_dirty(root: &Path) -> bool {
    !git_ok(root, &["diff", "--quiet"]) || !git_ok(root, &["diff", "--cached", "--quiet"])
}

/// Run git in `dir`, returning trimmed stdout on success (else `None`).
fn run_git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Whether a git command exits 0.
fn git_ok(dir: &Path, args: &[&str]) -> bool {
    matches!(
        Command::new("git").args(args).current_dir(dir).status(),
        Ok(s) if s.success()
    )
}

// ---- lockfile -------------------------------------------------------------

/// Resolve an image argument to the reference to deploy.
///
/// A bare image *name* — a key written by `image push` into [`LOCKFILE_NAME`] at the repo
/// root — resolves to the digest-pinned ref recorded for it, so a deploy lands exactly the
/// image that was pushed rather than whatever a mutable tag points at now. Anything that is
/// not a lockfile key (a full registry ref, a tag ref, a `@sha256:` ref) passes through
/// untouched, so this can never break an explicit reference.
///
/// Absence of a lockfile is normal, not an error — most projects deploy by literal ref.
#[must_use]
pub fn resolve_image_ref(image: &str) -> String {
    resolve_image_ref_in(&repo_root(), image)
}

/// Resolve the image a deploy command should use: the explicit `--image` flag, else the
/// profile's `image`, then [`resolve_image_ref`] so a bare name becomes the digest pinned by
/// the last `image push`.
///
/// Shared by `run`, `session create`, and `serve up` so their precedence, error text, and
/// pinning behaviour cannot drift apart — they were previously three copies of the same
/// lines, which is exactly how `run` came to pin while the other two did not.
///
/// # Errors
/// Returns an error naming both sources when neither supplies a non-empty image.
pub fn resolve_deploy_image(explicit: Option<&str>, profile_image: Option<&str>) -> Result<String> {
    resolve_deploy_image_in(&repo_root(), explicit, profile_image)
}

/// [`resolve_deploy_image`] against an explicit repo root (the seam that makes it testable).
fn resolve_deploy_image_in(
    root: &Path,
    explicit: Option<&str>,
    profile_image: Option<&str>,
) -> Result<String> {
    let requested = explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| profile_image.map(str::trim).filter(|s| !s.is_empty()))
        .context("no image (pass --image or set it in the profile)")?;
    let resolved = resolve_image_ref_in(root, requested);
    if resolved != requested {
        eprintln!("image '{requested}' → {resolved} (digest-pinned from {LOCKFILE_NAME})");
    }
    Ok(resolved)
}

/// [`resolve_image_ref`] against an explicit repo root (the seam that makes it testable).
fn resolve_image_ref_in(root: &Path, image: &str) -> String {
    let path = root.join(LOCKFILE_NAME);
    match load_lockfile(&path) {
        // A missing lockfile yields an empty map, so this is also the no-lockfile path.
        Ok(lock) => lock
            .get(image)
            .map(|entry| entry.image_ref.clone())
            .unwrap_or_else(|| image.to_string()),
        // A corrupt lockfile must not block a deploy that uses a literal ref — but it must
        // not silently look like pinning either, so say so and fall through.
        Err(e) => {
            eprintln!(
                "warning: ignoring unreadable {}: {e:#} — deploying '{image}' as given",
                path.display()
            );
            image.to_string()
        }
    }
}

/// Load the lockfile, or an empty map if it does not exist yet.
fn load_lockfile(path: &Path) -> Result<Lockfile> {
    match std::fs::read(path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Lockfile::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Serialize the lockfile: pretty JSON, sorted keys (via `BTreeMap`), trailing newline.
fn serialize_lockfile(lock: &Lockfile) -> Result<String> {
    let mut s = serde_json::to_string_pretty(lock).context("serializing lockfile")?;
    s.push('\n');
    Ok(s)
}

/// Write the lockfile to `path`.
fn write_lockfile(path: &Path, lock: &Lockfile) -> Result<()> {
    std::fs::write(path, serialize_lockfile(lock)?)
        .with_context(|| format!("writing {}", path.display()))
}

// ---- helpers --------------------------------------------------------------

/// A trimmed, non-empty environment variable, or `None`.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn image_names_resolve_to_the_pinned_digest_and_refs_pass_through() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // No lockfile yet: everything passes through untouched, with no error.
        assert_eq!(resolve_image_ref_in(root, "gpu-probe"), "gpu-probe");

        let pinned = "registry.example.com/my-org/salad/gpu-probe@sha256:abc123";
        let mut lock = Lockfile::new();
        lock.insert(
            "gpu-probe".to_string(),
            LockEntry {
                image_ref: pinned.to_string(),
                digest: "sha256:abc123".to_string(),
                flake_rev: "deadbeef".to_string(),
                pushed_at: "2026-07-23T00:00:00Z".to_string(),
            },
        );
        write_lockfile(&root.join(LOCKFILE_NAME), &lock).unwrap();

        // A bare name recorded by `image push` becomes the digest-pinned ref.
        assert_eq!(resolve_image_ref_in(root, "gpu-probe"), pinned);
        // An unrecorded name is left alone (it may be a ref the user typed).
        assert_eq!(resolve_image_ref_in(root, "not-pushed"), "not-pushed");
        // An explicit ref is never rewritten, even a tag of a name that IS in the lockfile.
        for literal in [
            "registry.example.com/my-org/salad/gpu-probe:v1",
            "ghcr.io/other/img@sha256:def456",
        ] {
            assert_eq!(resolve_image_ref_in(root, literal), literal);
        }
    }

    #[test]
    fn deploy_image_precedence_is_flag_then_profile_then_lockfile() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let pinned = "registry.example.com/my-org/salad/trainer@sha256:abc123";
        let mut lock = Lockfile::new();
        lock.insert(
            "trainer".to_string(),
            LockEntry {
                image_ref: pinned.to_string(),
                digest: "sha256:abc123".to_string(),
                flake_rev: "deadbeef".to_string(),
                pushed_at: "2026-07-23T00:00:00Z".to_string(),
            },
        );
        write_lockfile(&root.join(LOCKFILE_NAME), &lock).unwrap();

        // The explicit flag wins over the profile...
        assert_eq!(
            resolve_deploy_image_in(root, Some("trainer"), Some("other")).unwrap(),
            pinned
        );
        // ...the profile is used when no flag was given...
        assert_eq!(
            resolve_deploy_image_in(root, None, Some("trainer")).unwrap(),
            pinned
        );
        // ...and an empty/whitespace flag falls back rather than winning with nothing.
        assert_eq!(
            resolve_deploy_image_in(root, Some("  "), Some("trainer")).unwrap(),
            pinned
        );

        // A literal ref is never rewritten, from either source.
        let literal = "ghcr.io/org/img:v1";
        assert_eq!(
            resolve_deploy_image_in(root, Some(literal), None).unwrap(),
            literal
        );

        // Neither source: one shared error message for all three deploy commands.
        let err = resolve_deploy_image_in(root, None, None).unwrap_err();
        assert!(err.to_string().contains("no image"), "{err}");
    }

    #[test]
    fn a_corrupt_lockfile_does_not_block_a_literal_ref() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join(LOCKFILE_NAME), "{ not json").unwrap();
        // Falls through (with a warning) rather than failing the deploy.
        assert_eq!(
            resolve_image_ref_in(root, "registry.example.com/x/y:v1"),
            "registry.example.com/x/y:v1"
        );
    }

    #[test]
    fn registry_host_extracts_hostname_with_optional_port_and_scheme() {
        assert_eq!(
            registry_host("registry.example.com/my-org/salad").unwrap(),
            "registry.example.com"
        );
        assert_eq!(
            registry_host("registry.example.com:5000/my-org").unwrap(),
            "registry.example.com:5000"
        );
        assert_eq!(registry_host("ghcr.io/my-org").unwrap(), "ghcr.io");
        // Host-only base (no namespace path).
        assert_eq!(registry_host("localhost:5000").unwrap(), "localhost:5000");
        // A scheme is tolerated and stripped.
        assert_eq!(
            registry_host("https://registry.example.com/x").unwrap(),
            "registry.example.com"
        );
        assert!(registry_host("").is_err());
        assert!(registry_host("/leading-slash").is_err());
    }

    #[test]
    fn refs_are_constructed_tagged_and_digest_pinned() {
        let base = "registry.example.com/my-org/salad";
        assert_eq!(
            tagged_ref(base, "gpu-probe", "v1"),
            "registry.example.com/my-org/salad/gpu-probe:v1"
        );
        assert_eq!(
            digest_ref(base, "gpu-probe", "sha256:abc123"),
            "registry.example.com/my-org/salad/gpu-probe@sha256:abc123"
        );
        // A trailing slash on the base is normalized away.
        assert_eq!(
            tagged_ref("registry.example.com/my-org/", "kernel-test", "latest"),
            "registry.example.com/my-org/kernel-test:latest"
        );
    }

    #[test]
    fn macos_defaults_to_a_native_darwin_image_system() {
        // macOS builds the darwin attribute: same linux/amd64 image, native assembly,
        // no Linux builder needed.
        assert_eq!(
            default_image_system(false, "macos", "aarch64"),
            "aarch64-darwin"
        );
        assert_eq!(
            default_image_system(false, "macos", "x86_64"),
            "x86_64-darwin"
        );
        // Every other host keeps the declaring system...
        assert_eq!(
            default_image_system(false, "linux", "x86_64"),
            "x86_64-linux"
        );
        assert_eq!(
            default_image_system(false, "linux", "aarch64"),
            "x86_64-linux"
        );
        // ...and with --on the local platform is irrelevant: the remote does the build.
        assert_eq!(
            default_image_system(true, "macos", "aarch64"),
            "x86_64-linux"
        );
    }

    #[test]
    fn remote_tmp_paths_are_validated_before_use() {
        assert!(is_safe_remote_tmp("/tmp/saladfingers-push-a1b2C3d4"));
        // A remote whose shell profile prints a banner, or any injection attempt, must
        // not reach the `rm -rf`.
        for bad in [
            "",
            "/tmp",
            "/tmp/saladfingers-push-",
            "/tmp/other",
            "/tmp/saladfingers-push-x y",
            "/tmp/saladfingers-push-x; rm -rf /",
            "Welcome!\n/tmp/saladfingers-push-abcd",
            "../../etc",
        ] {
            assert!(!is_safe_remote_tmp(bad), "must reject {bad:?}");
        }
    }

    #[test]
    fn shell_quoting_survives_embedded_quotes() {
        assert_eq!(shell_quote("/tmp/x"), "'/tmp/x'");
        // The classic break-out attempt closes the quote; the escape must re-open it.
        assert_eq!(shell_quote("a'b"), r"'a'\''b'");
        assert_eq!(shell_quote("a b; rm -rf /"), "'a b; rm -rf /'");
    }

    #[test]
    fn digest_validation_rejects_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good");
        std::fs::write(&good, "sha256:deadbeef\n").unwrap();
        assert_eq!(read_digest(&good).unwrap(), "sha256:deadbeef");

        let bad = dir.path().join("bad");
        std::fs::write(&bad, "not-a-digest").unwrap();
        assert!(read_digest(&bad).is_err());

        let empty = dir.path().join("empty");
        std::fs::write(&empty, "sha256:").unwrap();
        assert!(read_digest(&empty).is_err());
    }

    #[test]
    fn credential_resolution_prefers_push_then_convention_then_pull() {
        let env: HashMap<&str, &str> = HashMap::from([
            ("PUSH_USER_VAR", "push-user"),
            (PUSH_USER_ENV, "convention-user"),
            ("PULL_USER_VAR", "pull-user"),
        ]);
        let lookup = |k: &str| env.get(k).map(|v| (*v).to_string());

        // 1. named push env wins.
        assert_eq!(
            pick_credential(
                Some("PUSH_USER_VAR"),
                PUSH_USER_ENV,
                Some("PULL_USER_VAR"),
                &lookup
            ),
            Some(("push-user".to_string(), CredentialSource::NamedPush))
        );
        // 2. convention var when no named push env.
        assert_eq!(
            pick_credential(None, PUSH_USER_ENV, Some("PULL_USER_VAR"), &lookup),
            Some(("convention-user".to_string(), CredentialSource::Convention))
        );
        // 3. pull creds as last resort — reported as such, because that is the
        //    case the operator needs to be told about.
        assert_eq!(
            pick_credential(Some("UNSET"), "UNSET_CONV", Some("PULL_USER_VAR"), &lookup),
            Some(("pull-user".to_string(), CredentialSource::PullFallback))
        );
        // Nothing configured → None.
        assert_eq!(pick_credential(None, "UNSET_CONV", None, &lookup), None);
    }

    /// The fallback is reported whenever it is taken and never otherwise — a
    /// correctly configured push must not print a warning that costs an
    /// operator time to rule out.
    #[test]
    fn pull_fallback_is_reported_for_either_half_only() {
        use CredentialSource::{Convention, NamedPush, PullFallback};

        for (user, pass) in [
            (NamedPush, NamedPush),
            (NamedPush, Convention),
            (Convention, NamedPush),
            (Convention, Convention),
        ] {
            assert_eq!(
                pull_fallback_warning(user, pass),
                None,
                "{user:?}/{pass:?} is fully configured and must stay quiet"
            );
        }

        // Either half falling back is enough: a real push username with the pull
        // token as its password is denied exactly the same way.
        for (user, pass) in [
            (PullFallback, NamedPush),
            (NamedPush, PullFallback),
            (PullFallback, PullFallback),
        ] {
            let warning = pull_fallback_warning(user, pass)
                .unwrap_or_else(|| panic!("{user:?}/{pass:?} must warn"));
            // The warning has to name the fix, not just the problem.
            assert!(warning.contains(PUSH_USER_ENV), "{warning}");
            assert!(warning.contains(PUSH_PASS_ENV), "{warning}");
        }
    }

    /// Build the env lookup the compression tests share.
    fn env_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// The whole point of the setting: a push must never be left to whatever
    /// skopeo defaults to, and must never reach SaladCloud as zstd by accident.
    #[test]
    fn compression_defaults_to_forced_gzip_9() {
        let env = env_of(&[]);
        let args = compression_args_from(&|k: &str| env.get(k).cloned()).unwrap();
        assert_eq!(
            args,
            [
                "--dest-compress-format",
                "gzip",
                "--dest-compress-level",
                "9",
                "--dest-force-compress-format",
            ]
        );
    }

    /// `--dest-force-compress-format` is what makes the format take effect at
    /// all, so it must be present whenever anything else is.
    #[test]
    fn compression_is_all_or_nothing() {
        let off = env_of(&[(PUSH_COMPRESSION_ENV, "none")]);
        assert!(
            compression_args_from(&|k: &str| off.get(k).cloned())
                .unwrap()
                .is_empty(),
            "`none` must send no compression flags at all"
        );

        // Even with a level set, `none` stays off rather than half-applying it.
        let off_with_level = env_of(&[
            (PUSH_COMPRESSION_ENV, "none"),
            (PUSH_COMPRESSION_LEVEL_ENV, "9"),
        ]);
        assert!(
            compression_args_from(&|k: &str| off_with_level.get(k).cloned())
                .unwrap()
                .is_empty()
        );
    }

    /// A bare `zstd` is the guessable spelling and the expensive mistake, so it
    /// is refused rather than obeyed — and the refusal has to hand over the
    /// exact value that works, or it is just an obstacle.
    #[test]
    fn bare_zstd_is_refused_and_names_the_way_through() {
        let env = env_of(&[(PUSH_COMPRESSION_ENV, "zstd")]);
        let err = compression_args_from(&|k: &str| env.get(k).cloned())
            .expect_err("a bare `zstd` must not push")
            .to_string();
        assert!(err.contains(ZSTD_ACKNOWLEDGED), "{err}");
        // The reason, not just the rule: this is what the operator has to weigh.
        assert!(err.contains("cannot unpack zstd"), "{err}");
    }

    /// Acknowledged zstd pushes as zstd — the gate refuses a spelling, not the
    /// format. Level 19, not gzip's 9: klauspost folds numeric levels onto four
    /// speed tiers with the top one starting at 10, so a shared `9` would drop
    /// the push a tier below the 613 MiB figure `docs/registry.md` quotes.
    #[test]
    fn acknowledged_zstd_pushes_zstd_in_its_top_tier() {
        let env = env_of(&[(PUSH_COMPRESSION_ENV, ZSTD_ACKNOWLEDGED)]);
        let args = compression_args_from(&|k: &str| env.get(k).cloned()).unwrap();
        assert_eq!(
            args,
            [
                "--dest-compress-format",
                "zstd",
                "--dest-compress-level",
                "19",
                "--dest-force-compress-format",
            ],
            "the acknowledgement must reach skopeo as plain `zstd`, at its top tier"
        );
    }

    /// A typo must not reach skopeo and fail there, several seconds and one
    /// authentication later, in its vocabulary rather than ours.
    #[test]
    fn an_unknown_format_is_rejected_up_front() {
        // The last one is a near-miss of the acknowledgement: matching is exact,
        // so a half-remembered spelling fails loudly rather than falling back.
        for bad in ["gzipp", "xz", "bzip2", "ZSTD", "zstd-salad-cannot-pull-it"] {
            let env = env_of(&[(PUSH_COMPRESSION_ENV, bad)]);
            assert!(
                compression_args_from(&|k: &str| env.get(k).cloned()).is_err(),
                "{bad:?} must be rejected here, not handed to skopeo"
            );
        }
    }

    #[test]
    fn an_explicit_level_overrides_both_defaults() {
        for (setting, format, level) in [("gzip", "gzip", "1"), (ZSTD_ACKNOWLEDGED, "zstd", "3")] {
            let env = env_of(&[
                (PUSH_COMPRESSION_ENV, setting),
                (PUSH_COMPRESSION_LEVEL_ENV, level),
            ]);
            let args = compression_args_from(&|k: &str| env.get(k).cloned()).unwrap();
            assert_eq!(args[1], format);
            assert_eq!(args[3], level, "explicit level must win for {format}");
        }
    }

    #[test]
    fn lockfile_merges_preserves_other_entries_and_sorts_keys() {
        let mut lock = Lockfile::new();
        lock.insert(
            "gpu-probe".to_string(),
            LockEntry {
                image_ref: "reg/my-org/gpu-probe@sha256:aaa".to_string(),
                digest: "sha256:aaa".to_string(),
                flake_rev: "rev1".to_string(),
                pushed_at: "2026-07-21T00:00:00Z".to_string(),
            },
        );
        // Merge a second entry whose key sorts before the first.
        lock.insert(
            "cuda-min".to_string(),
            LockEntry {
                image_ref: "reg/my-org/cuda-min@sha256:bbb".to_string(),
                digest: "sha256:bbb".to_string(),
                flake_rev: "rev2-dirty".to_string(),
                pushed_at: "2026-07-21T01:00:00Z".to_string(),
            },
        );

        let json = serialize_lockfile(&lock).unwrap();
        assert!(json.ends_with('\n'), "trailing newline");
        // Sorted keys: cuda-min before gpu-probe.
        let cuda_at = json.find("cuda-min").unwrap();
        let probe_at = json.find("gpu-probe").unwrap();
        assert!(cuda_at < probe_at, "keys must be sorted");
        // The JSON field name is `ref` (not the Rust field name).
        assert!(json.contains("\"ref\""));
        assert!(json.contains("\"flakeRev\""));
        assert!(json.contains("\"pushedAt\""));

        // Round-trips and preserves both entries.
        let parsed: Lockfile = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, lock);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed["cuda-min"].digest, "sha256:bbb");
    }

    #[test]
    fn lockfile_load_missing_is_empty_then_overwrites_same_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LOCKFILE_NAME);
        assert!(load_lockfile(&path).unwrap().is_empty());

        let mut lock = Lockfile::new();
        lock.insert(
            "gpu-probe".to_string(),
            LockEntry {
                image_ref: "reg/gpu-probe@sha256:old".to_string(),
                digest: "sha256:old".to_string(),
                flake_rev: "rev1".to_string(),
                pushed_at: "2026-07-21T00:00:00Z".to_string(),
            },
        );
        write_lockfile(&path, &lock).unwrap();

        // Re-pushing the same name replaces its entry, keeping the map single-keyed.
        let mut reloaded = load_lockfile(&path).unwrap();
        reloaded.insert(
            "gpu-probe".to_string(),
            LockEntry {
                image_ref: "reg/gpu-probe@sha256:new".to_string(),
                digest: "sha256:new".to_string(),
                flake_rev: "rev2".to_string(),
                pushed_at: "2026-07-21T02:00:00Z".to_string(),
            },
        );
        write_lockfile(&path, &reloaded).unwrap();

        let final_lock = load_lockfile(&path).unwrap();
        assert_eq!(final_lock.len(), 1);
        assert_eq!(final_lock["gpu-probe"].digest, "sha256:new");
    }
}
