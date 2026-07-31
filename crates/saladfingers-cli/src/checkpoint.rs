// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers checkpoint show|fetch` — read the checkpoint a run left in storage.
//!
//! Checkpoints exist so a long training job survives losing its node, which means the
//! useful artifact usually outlives the run that produced it: a job cut short at step
//! 21,000 still has 21,000 steps of work in the bucket, and the next run should start
//! from it rather than from zero. `--output` cannot deliver that — output collection only
//! happens when a job finishes cleanly, which is exactly the case where the checkpoint is
//! least interesting.
//!
//! The agent rotates checkpoints between the slots of a ring, so the current one lives at
//! `…/slot0/…` or `…/slot1/…` depending on how many times it rotated. The metadata object
//! is the index that resolves it, and these commands read it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use saladfingers_protocol::transfer;
use saladfingers_protocol::{CheckpointMeta, VersionProbe};

use crate::cli::{CheckpointArgs, CheckpointFetchArgs};
use crate::config::Config;
use crate::presign::S3Backend;
use crate::spec;

/// Long enough to download a large checkpoint, short enough to stay a bounded credential.
const EXPIRY: Duration = Duration::from_secs(6 * 3600);

/// What a checkpoint command is pointed at: a run's own checkpoint, or a shared one.
///
/// The two are mutually exclusive and exactly one is required — clap enforces that with a
/// required `ArgGroup`, and this type carries the same guarantee into the code, so no path
/// has to invent behaviour for "neither" or "both". The pair of `Option`s it replaces had
/// three unreachable arms, one of which would have produced the key `runs//0/ckpt`.
#[derive(Debug, Clone)]
pub enum Target {
    /// The checkpoint stored inside a run, which `gc` reaps with the run.
    Run(String),
    /// A shared checkpoint, addressed by name and outliving every run that writes it.
    Prefix(String),
}

impl Target {
    /// Read the target out of parsed arguments, validating whichever name was given.
    ///
    /// # Errors
    /// Returns an error if the name cannot be part of a storage key. Both names are
    /// checked, not just the new one: the prefix by design, the run id because `fetch`
    /// also builds a local path from it, where `..` would climb out of `sf-out/`.
    pub fn from_args(args: &CheckpointArgs) -> Result<Self> {
        match (&args.prefix, &args.run_id) {
            (Some(prefix), _) => {
                spec::validate_checkpoint_prefix(prefix)?;
                Ok(Self::Prefix(prefix.clone()))
            }
            (None, Some(run_id)) => {
                spec::validate_checkpoint_prefix(run_id)
                    .with_context(|| format!("'{run_id}' is not a usable run id"))?;
                Ok(Self::Run(run_id.clone()))
            }
            (None, None) => anyhow::bail!("pass a run id or --prefix NAME"),
        }
    }

    /// The storage prefix holding this checkpoint's slots and metadata.
    #[must_use]
    pub fn base(&self, shard: u32) -> String {
        match self {
            Self::Run(run_id) => spec::checkpoint_base(run_id, shard, None),
            Self::Prefix(name) => spec::checkpoint_base("", shard, Some(name)),
        }
    }

    /// How to name this checkpoint in output.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Run(run_id) => format!("run {run_id}"),
            Self::Prefix(name) => format!("prefix '{name}'"),
        }
    }

    /// The label/value pair `show` prints, padded so the value sits in the same column
    /// as the metadata fields below it.
    fn show_heading(&self) -> String {
        match self {
            Self::Run(run_id) => format!("run          {run_id}"),
            Self::Prefix(name) => format!("prefix       {name}"),
        }
    }
}

/// Where `fetch` extracts when `--dest` is not given.
///
/// `sf-out/<run-id-or-prefix>/<shard>/ckpt` — the `--help` text and docs promise exactly
/// this shape, and nothing else pins it: swap the joins and every other test stays green
/// while every scripted fetch lands somewhere new.
fn default_dest(target: &Target, shard: u32) -> PathBuf {
    let name = match target {
        Target::Run(name) | Target::Prefix(name) => name,
    };
    PathBuf::from("sf-out")
        .join(name)
        .join(shard.to_string())
        .join("ckpt")
}

/// Open the storage backend a checkpoint command reads through.
fn backend_of(cfg: &Config) -> Result<(reqwest::Client, S3Backend)> {
    let storage = cfg
        .storage
        .as_ref()
        .context("`checkpoint` needs an S3-compatible [storage] backend")?;
    Ok((
        transfer::transfer_client()?,
        S3Backend::from_config(storage)?,
    ))
}

/// `saladfingers checkpoint show RUN_ID` / `--prefix NAME`
///
/// # Errors
/// Returns an error when storage is unconfigured, unreachable, or holds no checkpoint at
/// the requested location.
pub async fn show(cfg: Config, args: CheckpointArgs) -> Result<()> {
    let target = Target::from_args(&args)?;
    let (http, backend) = backend_of(&cfg)?;
    let meta = resolve(&http, &backend, &target, args.shard).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&meta)?);
        return Ok(());
    }
    println!("{} (shard {})", target.show_heading(), args.shard);
    println!(
        "step         {}",
        meta.step
            .map_or_else(|| "unknown".to_string(), |s| s.to_string())
    );
    println!("slot         {}", meta.slot);
    println!("parts        {}", meta.parts);
    println!("size         {}", human_bytes(meta.bytes));
    println!("uploaded     {}", meta.uploaded_at.to_rfc3339());
    println!("sha256       {}", meta.sha256);
    Ok(())
}

/// `saladfingers checkpoint fetch RUN_ID|--prefix NAME [--dest DIR]`
///
/// # Errors
/// Returns an error when storage is unconfigured, holds no checkpoint at the requested
/// location, or the download fails its checksum.
pub async fn fetch(cfg: Config, args: CheckpointFetchArgs) -> Result<()> {
    let target = Target::from_args(&args.target)?;
    let (http, backend) = backend_of(&cfg)?;
    let dest = args
        .dest
        .map_or_else(|| default_dest(&target, args.target.shard), PathBuf::from);
    let meta = fetch_into(&http, &backend, &target, args.target.shard, &dest).await?;
    if args.target.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dest": dest.display().to_string(),
                "meta": meta,
            }))?
        );
    } else {
        println!("{}", dest.display());
    }
    Ok(())
}

/// Read the committed checkpoint metadata for a target's shard — the object that names the
/// live slot.
///
/// Whatever decodes is returned: `show` displays a checkpoint's own account of itself,
/// including one whose part count is nonsense, because that reading *is* the diagnosis.
/// Acting on the numbers is [`fetch_into`]'s job, and that is where they are bounded.
///
/// # Errors
/// Returns an error when storage holds no checkpoint there, the object cannot be read, or
/// it was written by an agent speaking a different protocol version.
pub async fn resolve(
    http: &reqwest::Client,
    backend: &S3Backend,
    target: &Target,
    shard: u32,
) -> Result<CheckpointMeta> {
    let key = spec::ckpt_meta_key(&target.base(shard));
    // A fixed-size control document, so it takes the control deadline: without one, a
    // storage endpoint that accepts the connection and never answers hangs the command
    // with no output and no way to tell that apart from a slow download.
    let resp = http
        .get(backend.presign_get(&key, EXPIRY))
        .timeout(transfer::CONTROL_TIMEOUT)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("fetching checkpoint metadata")?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "no checkpoint for {} shard {shard} ({}){}",
            target.describe(),
            resp.status(),
            prefix_hint(target)
        );
    }
    // The object is a few hundred bytes, and under a shared prefix its key is writable
    // by other runs — bound the body before buffering it, not after.
    anyhow::ensure!(
        !resp.content_length().is_some_and(|len| len > 1024 * 1024),
        "checkpoint metadata object is implausibly large ({} bytes); refusing to buffer it",
        resp.content_length().unwrap_or_default()
    );
    let body = resp
        .bytes()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading checkpoint metadata")?;
    // An agent of another version may have written a layout this CLI cannot address. Say
    // so, rather than presigning keys that do not exist and reporting the resulting 404s
    // as a lost checkpoint. Probing `v` first is what makes that message possible: a full
    // decode of a v1 object fails with `missing field 'slot'`, which reads like corruption.
    let probe: VersionProbe =
        serde_json::from_slice(&body).context("decoding checkpoint metadata")?;
    anyhow::ensure!(
        probe.v == saladfingers_protocol::PROTOCOL_VERSION,
        "checkpoint metadata is protocol v{} but this CLI speaks v{}",
        probe.v,
        saladfingers_protocol::PROTOCOL_VERSION
    );
    serde_json::from_slice(&body).context("decoding checkpoint metadata")
}

/// When a run's own checkpoint is missing and local state records that the run wrote to a
/// shared prefix, say so — the checkpoint is not gone, it is one flag away.
///
/// A hint appended to the error rather than a silent redirect: this reads local state, so
/// the same command on another machine has to behave the same way, and a command that
/// quietly addressed a different key depending on what is in `~/.local/state` would be
/// worse than the error it replaced.
fn prefix_hint(target: &Target) -> String {
    let Target::Run(run_id) = target else {
        return String::new();
    };
    match crate::state::load_run(run_id) {
        Ok(Some(run)) => run.checkpoint_prefix.map_or_else(String::new, |prefix| {
            format!(" — that run checkpointed to prefix '{prefix}'; use --prefix {prefix}")
        }),
        _ => String::new(),
    }
}

/// Download the live slot of a checkpoint into `dest`, returning the metadata that
/// described it.
///
/// # Errors
/// Returns an error when the metadata cannot be resolved, records an unusable part count,
/// or the downloaded bytes fail the recorded checksum.
pub async fn fetch_into(
    http: &reqwest::Client,
    backend: &S3Backend,
    target: &Target,
    shard: u32,
    dest: &Path,
) -> Result<CheckpointMeta> {
    let meta = resolve(http, backend, target, shard).await?;
    anyhow::ensure!(meta.parts > 0, "checkpoint metadata records no data parts");
    // Every numeric field below comes from the node, which is untrusted (security.md,
    // Assumption 1). `parts` drives `(0..parts)` presigned-URL generation, so it is
    // bounded before use — in the spirit of `runner::admit_output`, though at the
    // protocol ceiling rather than the writing run's `max_parts`: this reader does not
    // need to match the writer's configuration to download what exists, it only refuses
    // the impossible (a claim of billions of parts would exhaust memory signing URLs
    // for keys that cannot exist).
    anyhow::ensure!(
        meta.parts <= spec::MAX_ARTIFACT_PARTS_LIMIT,
        "checkpoint metadata claims {} parts, past the {} the protocol allows — \
         the metadata object is malformed",
        meta.parts,
        spec::MAX_ARTIFACT_PARTS_LIMIT
    );
    // `slot` picks the key stem. Out of ring it can only 404, but a 404 on every part
    // reads as "the checkpoint is gone" — the exact misdiagnosis the version probe
    // exists to prevent, so name the real problem instead.
    anyhow::ensure!(
        meta.slot < spec::CHECKPOINT_SLOTS,
        "checkpoint metadata names slot {} but the ring has {} slots — \
         the metadata object is malformed",
        meta.slot,
        spec::CHECKPOINT_SLOTS
    );
    // The checksum is compared byte-for-byte later, so a malformed one can only ever
    // fail — but it would fail as "integrity check failed", which reads as corruption
    // of the data rather than of the metadata.
    anyhow::ensure!(
        meta.sha256.len() == 64 && meta.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "checkpoint metadata records a malformed sha256 (not 64 hex characters)"
    );

    let stem = spec::ckpt_slot_stem(&target.base(shard), meta.slot);
    let get_urls: Vec<String> = (0..meta.parts)
        .map(|k| backend.presign_get(&transfer::part_key(&stem, k), EXPIRY))
        .collect();

    eprintln!(
        "fetching checkpoint (step {}, {}) → {}",
        meta.step
            .map_or_else(|| "unknown".to_string(), |s| s.to_string()),
        human_bytes(meta.bytes),
        dest.display()
    );
    // The sha256 is checked before anything is extracted, so a torn or truncated slot
    // fails here instead of producing a half-written checkpoint directory.
    transfer::download_artifact(http, &get_urls, dest, true, Some(&meta.sha256))
        .await
        .context("downloading checkpoint")?;
    Ok(meta)
}

fn human_bytes(bytes: u64) -> String {
    #[allow(clippy::cast_precision_loss)]
    let b = bytes as f64;
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = b;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The layout is wire-visible: the agent's URLs are signed for these keys at submit
    /// time, and `fetch` re-derives them hours later. Pin the shared helpers both sides
    /// call, so a change has to break this rather than silently 404 every fetch.
    #[test]
    fn a_run_scoped_checkpoint_lives_under_its_run() {
        let target = Target::Run("sf-x".into());
        assert_eq!(target.base(0), "runs/sf-x/0/ckpt");
        assert_eq!(
            spec::ckpt_slot_stem(&target.base(3), 1),
            "runs/sf-x/3/ckpt/slot1/data"
        );
        assert_eq!(
            spec::ckpt_meta_key(&target.base(3)),
            "runs/sf-x/3/ckpt/meta.json"
        );
    }

    /// The whole point of a prefix: reachable from a *later* run, with a different id —
    /// so the run id must not appear in the key at all.
    #[test]
    fn a_prefixed_checkpoint_is_addressed_without_any_run_id() {
        let target = Target::Prefix("t77m".into());
        assert_eq!(target.base(0), "ckpts/t77m/0");
        assert_eq!(target.base(2), "ckpts/t77m/2");
        assert_eq!(
            spec::ckpt_meta_key(&target.base(2)),
            "ckpts/t77m/2/meta.json"
        );
    }

    /// `fetch`'s default `--dest` is built from the name, so `..` in a run id would climb
    /// out of `sf-out/`. The prefix has always been validated; the run id reaches the same
    /// two constructions and was not.
    #[test]
    fn a_name_that_would_escape_its_directory_is_refused() {
        let args = |run_id: Option<&str>, prefix: Option<&str>| CheckpointArgs {
            run_id: run_id.map(str::to_string),
            prefix: prefix.map(str::to_string),
            shard: 0,
            json: false,
        };
        assert!(Target::from_args(&args(Some("../../etc"), None)).is_err());
        assert!(Target::from_args(&args(None, Some("a/b"))).is_err());
        assert!(Target::from_args(&args(Some("sf-x7k2mq"), None)).is_ok());
        assert!(Target::from_args(&args(None, Some("tinystories-77m"))).is_ok());
    }

    /// The `--help` text and run.md promise this exact shape; nothing else pins it, and a
    /// swapped join ships every scripted fetch into a different directory.
    #[test]
    fn the_default_dest_is_the_documented_shape() {
        assert_eq!(
            default_dest(&Target::Run("sf-x7k2mq".into()), 0),
            PathBuf::from("sf-out/sf-x7k2mq/0/ckpt")
        );
        assert_eq!(
            default_dest(&Target::Prefix("tinystories-77m".into()), 2),
            PathBuf::from("sf-out/tinystories-77m/2/ckpt")
        );
    }

    /// `Target::from_args` leans on clap's required `ArgGroup` to make "neither" and
    /// "both" unreachable — and that group is declared on `CheckpointArgs` but consumed
    /// through `#[command(flatten)]` by `fetch`, which is exactly the kind of plumbing a
    /// clap upgrade or refactor can quietly loosen. If it does, `from_args` prefers
    /// `--prefix` silently; these keep the loosening loud instead.
    #[test]
    fn a_checkpoint_target_is_exactly_one_of_run_id_and_prefix() {
        use clap::Parser as _;
        for argv in [
            ["saladfingers", "checkpoint", "show"].as_slice(),
            &[
                "saladfingers",
                "checkpoint",
                "show",
                "sf-x",
                "--prefix",
                "p",
            ],
            &["saladfingers", "checkpoint", "fetch"],
            &[
                "saladfingers",
                "checkpoint",
                "fetch",
                "sf-x",
                "--prefix",
                "p",
            ],
        ] {
            assert!(
                crate::cli::Cli::try_parse_from(argv).is_err(),
                "clap must refuse {argv:?}"
            );
        }
        for argv in [
            ["saladfingers", "checkpoint", "show", "sf-x"].as_slice(),
            &["saladfingers", "checkpoint", "fetch", "--prefix", "p"],
        ] {
            assert!(
                crate::cli::Cli::try_parse_from(argv).is_ok(),
                "clap must accept {argv:?}"
            );
        }
    }

    #[test]
    fn human_bytes_scales() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(700 * 1024 * 1024), "700.0 MiB");
    }
}
