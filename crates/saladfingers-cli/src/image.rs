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
//! Security: this module reads the registry host, org, and credentials only by
//! *reference* (config keys / env-var names). No registry host, org, or secret is
//! ever hard-coded here, and the push token is passed to skopeo on stdin — never on
//! the command line or in any log line.

use std::collections::BTreeMap;
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
const PUSH_USER_ENV: &str = "SALADFINGERS_REGISTRY_PUSH_USER";
/// Conventional env var holding the push password/token directly.
const PUSH_PASS_ENV: &str = "SALADFINGERS_REGISTRY_PUSH_PASS";
/// Env var overriding the flake system images are built under (default below).
const IMAGE_SYSTEM_ENV: &str = "SALADFINGERS_IMAGE_SYSTEM";
/// Images are linux/amd64 only and defined under this system in `nix/images.nix`,
/// regardless of the host (a macOS host pushes via a remote x86_64-linux builder).
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
    let (user, pass) = resolve_push_credentials(cfg.registry.as_ref())?;

    let tagged_ref = tagged_ref(&base, &args.name, &args.tag);
    let system = image_system();
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

    eprintln!(
        "image push {}: building and pushing docker://{tagged_ref}...",
        args.name
    );
    run_copy_to(
        &root,
        &system,
        &args.name,
        &tagged_ref,
        &digestfile,
        &authfile,
    )?;

    let digest = read_digest(&digestfile)?;
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
    let user = pick_credential(
        reg.and_then(|r| r.push_username_env.as_deref()),
        PUSH_USER_ENV,
        reg.and_then(|r| r.username_env.as_deref()),
        &non_empty_env,
    )
    .context(
        "no registry push username — set SALADFINGERS_REGISTRY_PUSH_USER (or point \
         `[registry] push_username_env` / `username_env` at the env var holding it)",
    )?;
    let pass = pick_credential(
        reg.and_then(|r| r.push_password_env.as_deref()),
        PUSH_PASS_ENV,
        reg.and_then(|r| r.password_env.as_deref()),
        &non_empty_env,
    )
    .context(
        "no registry push password — set SALADFINGERS_REGISTRY_PUSH_PASS (or point \
         `[registry] push_password_env` / `password_env` at the env var holding it)",
    )?;
    Ok((user, pass))
}

/// Pure credential-picking used by [`resolve_push_credentials`]; `env` is the
/// environment lookup (real env in production, a fake map in tests).
fn pick_credential(
    named_push: Option<&str>,
    convention_var: &str,
    named_pull: Option<&str>,
    env: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    named_push
        .and_then(env)
        .or_else(|| env(convention_var))
        .or_else(|| named_pull.and_then(env))
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

/// Build and push the image via its nix2container `.copyTo` app, writing the pushed
/// digest to `digestfile`. Inherits stdio so nix build/push progress is visible.
fn run_copy_to(
    root: &Path,
    system: &str,
    name: &str,
    tagged_ref: &str,
    digestfile: &Path,
    authfile: &Path,
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
        .current_dir(root)
        .status()
        .context("spawning `nix run … copyTo` (is nix on PATH?)")?;
    if !status.success() {
        bail!("image build/push failed (`nix run {attr}` exited with {status})");
    }
    Ok(())
}

/// The `<system>` component of the image flake attribute.
fn image_system() -> String {
    non_empty_env(IMAGE_SYSTEM_ENV).unwrap_or_else(|| DEFAULT_IMAGE_SYSTEM.to_string())
}

/// Read + validate the pushed digest from the digestfile written by `copyTo`.
fn read_digest(digestfile: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(digestfile)
        .context("reading the pushed digest (copyTo did not write a --digestfile)")?;
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
            Some("push-user".to_string())
        );
        // 2. convention var when no named push env.
        assert_eq!(
            pick_credential(None, PUSH_USER_ENV, Some("PULL_USER_VAR"), &lookup),
            Some("convention-user".to_string())
        );
        // 3. pull creds as last resort.
        assert_eq!(
            pick_credential(Some("UNSET"), "UNSET_CONV", Some("PULL_USER_VAR"), &lookup),
            Some("pull-user".to_string())
        );
        // Nothing configured → None.
        assert_eq!(pick_credential(None, "UNSET_CONV", None, &lookup), None);
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
