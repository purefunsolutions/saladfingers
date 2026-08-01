// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Checkpoint watcher + restore for `sf-agent run`.
//!
//! When a job declares a [`CheckpointSpec`], the agent:
//! - **probes** the remote checkpoint's metadata at boot and **restores** it into the
//!   checkpoint directory *before* exec (resume path — the job reads it back and
//!   continues), and
//! - runs a **watcher** *during* exec that, on a fixed interval, uploads the checkpoint
//!   directory once it has quiesced (no member changed within `quiesce_secs`) and its
//!   contents changed since the last upload. On exec end the watcher uploads until the
//!   directory stops moving, up to [`FINAL_STABILITY_PASSES`] — the supervised child is
//!   gone by then, but a checkpoint written during the final tar has no later scan to
//!   catch it — and exits.
//!
//! **Slot ring.** Each upload goes to a slot of [`CheckpointSpec::slots`] that the
//! committed metadata does *not* reference, and only then is the metadata rewritten to
//! name the new slot. Writing the parts first and the metadata last already made a torn
//! upload *detectable* — restore verifies the sha256 before extracting — but while the
//! keys were fixed it could not make the previous checkpoint *survivable*: the new parts
//! had already overwritten the old bytes, so a node dying in the commit window left an
//! unreadable pair and the run restarted from step 0. With the ring the old slot stays
//! intact and referenced until the new one is complete, so an interruption costs one
//! interval, never the whole run. Retention is one checkpoint: after a successful commit
//! the agent deletes the parts of the slot it just superseded.
//!
//! The transfer itself reuses the same `tar|zstd` part-series engine as inputs/outputs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::StatusCode;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use saladfingers_protocol::job::{CheckpointSpec, JobSpec};
use saladfingers_protocol::transfer;
use saladfingers_protocol::{PROTOCOL_VERSION, VersionProbe};

pub use saladfingers_protocol::job::CheckpointMeta;

/// Where the restorable checkpoint lives, carried from [`restore`] to the watcher so the
/// first upload knows which slot it must not touch, and how much of the ring a previous
/// incarnation of this run may have left behind.
#[derive(Debug, Clone, Copy)]
pub struct RestoredState {
    /// Slot the committed metadata references, if any.
    pub live: Option<u32>,
    /// True when a metadata object existed, i.e. some earlier node ran this job and may
    /// have left partial parts in the other slot(s).
    pub had_remote: bool,
}

impl RestoredState {
    /// Nothing remote: a first life, or a job without checkpointing.
    const FRESH: Self = Self {
        live: None,
        had_remote: false,
    };
}

/// What [`probe`] learned about the remote checkpoint, carried to [`restore`].
///
/// Probing is separated from extracting so the run can validate the checkpoint *before*
/// it spends anything else: the metadata is one small GET, while the inputs it would
/// otherwise download first can be hundreds of gigabytes — re-paid on every relaunch of a
/// run whose checkpoint turns out to be permanently unreadable.
#[derive(Debug, Clone)]
pub enum ProbedCheckpoint {
    /// The job has no checkpoint spec.
    Disabled,
    /// No checkpoint is stored yet — a first life.
    Fresh,
    /// A committed checkpoint exists; its metadata passed every check.
    Live(CheckpointMeta),
}

/// Fetch and validate the committed checkpoint metadata, without downloading the data.
///
/// # Errors
/// Returns an error when a checkpoint exists (or may exist) but cannot be used: the
/// metadata cannot be fetched or decoded, was written by another protocol version, names
/// a slot outside the ring, records more parts than this run presigned URLs for, or the
/// ring itself is degenerate.
///
/// **Every one of those errors is fatal to the run** — see the call site in
/// [`crate::run`]. Continuing without the metadata is not a "fresh start": the ring
/// would not know which slot is live, so the next upload could land on the committed
/// one, and the commit after it reclaims the other. Either way the last good checkpoint
/// is gone. Failing costs one relaunch cycle, bounded by the attempt cap; continuing
/// costs the work the checkpoint represents.
pub async fn probe(http: &reqwest::Client, spec: &JobSpec) -> Result<ProbedCheckpoint> {
    let Some(ckpt) = spec.checkpoint.as_ref() else {
        return Ok(ProbedCheckpoint::Disabled);
    };
    // The ring invariant — never write the slot the committed metadata names — needs a
    // slot that is NOT the live one to exist. With one slot the rotation degenerates to
    // overwrite-in-place and every non-destructive promise this module makes is silently
    // void, so a degenerate ring is refused up front rather than discovered from a torn
    // checkpoint. The spec is wire input; nothing guarantees it was built by this CLI.
    anyhow::ensure!(
        ckpt.slots.len() >= 2,
        "checkpoint spec carries {} slot(s); the ring needs at least 2 to keep the \
         committed checkpoint safe while the next one uploads",
        ckpt.slots.len()
    );
    let Some(body) = fetch_meta(http, &ckpt.meta_get_url).await? else {
        return Ok(ProbedCheckpoint::Fresh);
    };
    // A version we cannot read is a hard error, not a fresh start: ignoring it would
    // retrain from step 0 and then overwrite the metadata still pointing at the existing
    // checkpoint, turning a fixable mismatch into lost work. Probe `v` before the full
    // decode so a v1 object reports the mismatch rather than `missing field 'slot'`.
    let version: VersionProbe =
        serde_json::from_slice(&body).context("decoding checkpoint metadata")?;
    anyhow::ensure!(
        version.v == PROTOCOL_VERSION,
        "remote checkpoint metadata is protocol v{} but this agent speaks v{PROTOCOL_VERSION}; \
         refusing to run, because starting fresh would retrain from step 0 and then overwrite \
         the metadata that still points at the existing checkpoint",
        version.v
    );
    let meta: CheckpointMeta =
        serde_json::from_slice(&body).context("decoding checkpoint metadata")?;
    let slot = meta.slot;
    let Some(slot_urls) = ckpt.slots.get(slot as usize) else {
        anyhow::bail!(
            "remote checkpoint names slot {slot} but this job's ring has {} slots",
            ckpt.slots.len()
        );
    };
    // A shared prefix makes the writer and the reader different runs, so the reader can
    // have been given fewer part URLs than the checkpoint actually has. Truncating would
    // download a prefix of the stream and fail the sha256, reporting corruption for what
    // is really a configuration mismatch.
    anyhow::ensure!(
        meta.parts == 0 || meta.parts as usize <= slot_urls.get_urls.len(),
        "remote checkpoint has {} parts but this run presigned only {} GET URLs per slot; \
         rerun with a [storage] max_artifact_parts of at least {}",
        meta.parts,
        slot_urls.get_urls.len(),
        meta.parts
    );
    // The checksum gets compared byte-for-byte later, so a malformed one can only ever
    // fail — but it would fail as "integrity check failed", which reads as corruption.
    // Name the real problem here instead.
    anyhow::ensure!(
        meta.sha256.len() == 64 && meta.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "checkpoint metadata records a malformed sha256 (not 64 hex characters)"
    );
    Ok(ProbedCheckpoint::Live(meta))
}

/// Restore the probed checkpoint into the checkpoint directory and report which slot it
/// came from, so the watcher knows where the ring stands.
///
/// # Errors
/// Returns an error if the checkpoint fails to download or extract — fatal to the run,
/// for the reasons on [`probe`].
pub async fn restore(
    http: &reqwest::Client,
    spec: &JobSpec,
    probed: ProbedCheckpoint,
) -> Result<RestoredState> {
    let Some(ckpt) = spec.checkpoint.as_ref() else {
        return Ok(RestoredState::FRESH);
    };
    // Create the directory up front, not just on the restore path: a checkpointed job
    // may assume the dir exists the way it assumes the workdir exists. When it was
    // created only while extracting a remote checkpoint, a FIRST life (nothing remote
    // yet) started without it — a job writing `dir/step` then failed every write and
    // the watcher scanned a nonexistent dir, so no checkpoint was ever born and every
    // interruption restarted from zero (caught live: an IMDS-reallocate test job looped
    // from step 0 indefinitely instead of resuming). The error is worth a line: the
    // same silent from-zero loop comes back if this dir cannot be created at all.
    let dir = ckpt_dir(spec, ckpt);
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        tracing::warn!(
            "could not create checkpoint dir {}: {e}; checkpointing will likely record nothing",
            dir.display()
        );
    }
    let meta = match probed {
        ProbedCheckpoint::Disabled | ProbedCheckpoint::Fresh => return Ok(RestoredState::FRESH),
        ProbedCheckpoint::Live(meta) => meta,
    };
    let slot = meta.slot;
    if meta.parts == 0 {
        // An empty but committed checkpoint: nothing to extract, yet the ring must still
        // treat this slot as live. Reporting FRESH here would reset the rotation to slot
        // 0 and mark every slot known-empty, so a previous incarnation's debris would
        // never be swept and the first upload could target the committed slot.
        return Ok(RestoredState {
            live: Some(slot),
            had_remote: true,
        });
    }
    let slot_urls = ckpt
        .slots
        .get(slot as usize)
        .expect("probe validated the slot against this same spec");
    transfer::download_artifact(
        http,
        &slot_urls.get_urls[..meta.parts as usize],
        &dir,
        true,
        Some(&meta.sha256),
    )
    .await
    .context("restoring checkpoint")?;
    tracing::info!(
        parts = meta.parts, slot, step = meta.step, dir = %dir.display(),
        "checkpoint restored"
    );
    Ok(RestoredState {
        live: Some(slot),
        had_remote: true,
    })
}

/// Attempts at the metadata object before [`probe`] gives up. A restore failure now
/// fails the whole run, so one dropped connection must not cost a relaunch cycle. Three
/// matches [`crate::run`]'s envelope and attempts-ledger fetches.
const META_FETCH_ATTEMPTS: u32 = 3;

/// The metadata object is a few hundred bytes. Anything near this cap is not checkpoint
/// metadata, and under a shared prefix the key is writable by other runs — so the body
/// is bounded before it is buffered, not after.
const META_MAX_BYTES: u64 = 1024 * 1024;

/// Attempts at the commit PUT before the ring falls back to marking the live slot
/// uncertain. Re-PUTting the same bytes is idempotent, so a retry resolves a lost ACK
/// outright — the retry either overwrites identical content or lands the commit — and
/// the uncertainty machinery becomes the backstop rather than the first response.
const COMMIT_PUT_ATTEMPTS: u32 = 3;

/// Fetch the committed checkpoint metadata. `Ok(None)` means no checkpoint is stored yet
/// — the only outcome that lets a run start fresh.
///
/// Transport failures, 5xx, and the throttle/timeout 4xx pair (429/408) are retried,
/// matching [`transfer`]'s retry policy; any other 4xx is authoritative and reported
/// immediately, since no retry fixes a URL signed for a key that does not exist.
async fn fetch_meta(http: &reqwest::Client, url: &str) -> Result<Option<Vec<u8>>> {
    let mut last: Option<anyhow::Error> = None;
    for attempt in 1..=META_FETCH_ATTEMPTS {
        match fetch_meta_once(http, url).await {
            Ok(found) => return Ok(found),
            Err(FetchMetaError::Fatal(e)) => return Err(e),
            Err(FetchMetaError::Transient(e)) => {
                if attempt < META_FETCH_ATTEMPTS {
                    tracing::warn!(
                        "checkpoint metadata fetch attempt {attempt} failed: {e:#}; retrying"
                    );
                    // The pid-derived jitter decorrelates a fleet: N shards booting
                    // together probe in the same second, get throttled together, and
                    // without it would re-collide on every identical backoff step.
                    let jitter = u64::from(std::process::id()) % 512;
                    tokio::time::sleep(Duration::from_millis((500 << attempt) + jitter)).await;
                }
                last = Some(e);
            }
        }
    }
    Err(last
        .expect("the loop runs at least once and only stores an error before continuing")
        .context(format!(
            "fetching checkpoint metadata failed {META_FETCH_ATTEMPTS} times"
        )))
}

/// Why a metadata fetch failed, so [`fetch_meta`] knows whether another attempt can help.
enum FetchMetaError {
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

async fn fetch_meta_once(
    http: &reqwest::Client,
    url: &str,
) -> std::result::Result<Option<Vec<u8>>, FetchMetaError> {
    // `without_url` throughout: the metadata URL is presigned, and its `X-Amz-Signature`
    // is a live capability over this key. These errors now reach the result envelope,
    // which is itself stored and read back by the CLI, so a leak here would persist the
    // capability far beyond the node.
    let resp = http
        .get(url)
        .timeout(transfer::CONTROL_TIMEOUT)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("fetching checkpoint metadata")
        .map_err(FetchMetaError::Transient)?;
    let status = resp.status();
    // 404 is the storage saying "no such key" — the one authoritative absent.
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    // 403 is ambiguous. Some backends answer it for an absent key (which must stay a
    // fresh start, or a first life can never boot there), but it is also what an
    // EXPIRED presigned URL returns — and treating that as absent silently retrains
    // from step 0 while a perfectly good checkpoint sits unreachable. Three signals
    // pull the two apart, strongest first:
    //
    // - the URL itself: a presigned URL carries its own `X-Amz-Date` + `X-Amz-Expires`,
    //   so this side can compute the window's end without trusting anyone's wording.
    //   The server already said 403, so "our copy of the URL is past its window" is
    //   corroboration, not a lone clock's opinion.
    // - the S3 error `<Code>`: `SignatureDoesNotMatch` / `InvalidAccessKeyId` (rotated
    //   or corrupted credentials) can never mean "absent" — the URL is broken.
    // - the message text mentioning expiry, for backends whose code is a generic
    //   `AccessDenied`.
    //
    // Anything else is treated as absent but leaves a trace, because a bucket policy
    // that denies GETs would otherwise be indistinguishable from a first run.
    if status == StatusCode::FORBIDDEN {
        let body = resp.bytes().await.unwrap_or_default();
        let text = String::from_utf8_lossy(&body);
        let past_window = url_expiry(url).is_some_and(|end| SystemTime::now() > end);
        if past_window || text.to_ascii_lowercase().contains("expire") {
            return Err(FetchMetaError::Fatal(anyhow::anyhow!(
                "the checkpoint metadata URL has expired{}; the run has outlived its \
                 presigned window, so a checkpoint may exist that this run can no longer \
                 read — refusing to start over it",
                if past_window {
                    " (its own X-Amz-Date + X-Amz-Expires are in the past)"
                } else {
                    ""
                }
            )));
        }
        if let Some(code @ ("SignatureDoesNotMatch" | "InvalidAccessKeyId")) = s3_error_code(&text)
        {
            return Err(FetchMetaError::Fatal(anyhow::anyhow!(
                "storage rejected the checkpoint metadata URL ({code}); the URL is \
                 broken — likely rotated storage credentials — not an absent \
                 checkpoint, so starting fresh over it would be a guess"
            )));
        }
        let snippet: String = text.chars().take(200).collect();
        tracing::warn!(
            "checkpoint metadata GET answered 403; treating as \"no checkpoint stored\" \
             (body: {snippet:?})"
        );
        return Ok(None);
    }
    let resp = resp
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context("checkpoint metadata status")
        .map_err(|e| {
            // Mirror `transfer::retryable`: throttle/timeout 4xx behave like 5xx.
            // Everything else in 4xx is definitive — no retry mints a new signature.
            let transient = status.is_server_error()
                || status == StatusCode::REQUEST_TIMEOUT
                || status == StatusCode::TOO_MANY_REQUESTS;
            if transient {
                FetchMetaError::Transient(e)
            } else {
                FetchMetaError::Fatal(e)
            }
        })?;
    if resp
        .content_length()
        .is_some_and(|len| len > META_MAX_BYTES)
    {
        return Err(FetchMetaError::Fatal(anyhow::anyhow!(
            "checkpoint metadata object is implausibly large ({} bytes); refusing to buffer it",
            resp.content_length().unwrap_or_default()
        )));
    }
    let body = resp
        .bytes()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading checkpoint metadata")
        .map_err(FetchMetaError::Transient)?;
    Ok(Some(body.to_vec()))
}

/// When a presigned URL's window ends, computed from its own `X-Amz-Date` (signing time,
/// `YYYYMMDDTHHMMSSZ`) and `X-Amz-Expires` (validity in seconds) query parameters.
/// `None` when either is absent or unreadable — a URL this code cannot date is simply
/// not dated, never guessed at.
fn url_expiry(url: &str) -> Option<SystemTime> {
    let query = url.split_once('?')?.1;
    let mut signed_at = None;
    let mut expires_secs = None;
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some(("X-Amz-Date", v)) => {
                signed_at = chrono::NaiveDateTime::parse_from_str(v, "%Y%m%dT%H%M%SZ").ok();
            }
            Some(("X-Amz-Expires", v)) => expires_secs = v.parse::<u64>().ok(),
            _ => {}
        }
    }
    let start = SystemTime::UNIX_EPOCH
        + Duration::from_secs(u64::try_from(signed_at?.and_utc().timestamp()).ok()?);
    Some(start + Duration::from_secs(expires_secs?))
}

/// The `<Code>` of an S3 XML error body, if one is present.
fn s3_error_code(body: &str) -> Option<&str> {
    let start = body.find("<Code>")? + "<Code>".len();
    let end = body[start..].find("</Code>")? + start;
    Some(&body[start..end])
}

/// The checkpoint directory, resolved against the job workdir. The child runs in the
/// workdir, so a relative `--checkpoint ckpts` means `<workdir>/ckpts` — resolving
/// against the agent's own CWD (typically `/`) would watch a directory the trainer
/// never writes, leaving checkpointing silently inert. An absolute path wins as-is.
fn ckpt_dir(spec: &JobSpec, ckpt: &CheckpointSpec) -> PathBuf {
    crate::run::workdir(spec).join(&ckpt.glob)
}

/// If the child was force-killed with writes this fresh, the tail of the checkpoint is
/// likely torn; skip the final upload rather than clobber the last good remote one.
const DIRTY_STOP_FRESH_WRITE: Duration = Duration::from_secs(2);

/// Upload attempts the watcher makes when exec has ended.
///
/// The periodic path needs only one: anything that lands during an upload is caught by
/// the next interval. The stop path has no next interval, and the archive is built by
/// streaming the live directory — so a checkpoint written while that tar runs would be
/// captured half-done or missed, with nothing left to notice it. Extra passes see the
/// newer mtime and upload again.
///
/// Bounded rather than repeated until settled: the supervised child is gone by now (exec
/// waited on it), but nothing kills its process group, so an orphaned grandchild can keep
/// writing forever. Two extra passes cover a checkpoint that landed during the final tar;
/// a directory still moving after that gets a warning instead of a livelock.
const FINAL_STABILITY_PASSES: usize = 3;

/// Spawn the checkpoint watcher. It runs until `stop` is notified (exec ended), then
/// uploads until the directory settles (bounded by [`FINAL_STABILITY_PASSES`]) and
/// exits. `restored` carries what [`restore`] found, so the first
/// upload knows which slot is live and must not be written. `dirty` is set by the
/// supervisor when the child had to be SIGKILLed — its freshest writes are then suspect.
/// Returns a no-op handle when the job has no checkpoint spec.
#[must_use]
pub fn spawn_watcher(
    http: reqwest::Client,
    spec: JobSpec,
    restored: RestoredState,
    stop: Arc<Notify>,
    dirty: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(ckpt) = spec.checkpoint.clone() else {
            return;
        };
        let dir = ckpt_dir(&spec, &ckpt);
        watch_loop(&http, &ckpt, &dir, restored, &stop, &dirty).await;
    })
}

/// Tracks which slot holds the committed checkpoint and how much of each slot is known to
/// hold data, so reclamation deletes exactly the stale parts and nothing more.
struct Ring {
    /// Slot the remote metadata currently names.
    live: Option<u32>,
    /// Set when a commit PUT failed in a way that leaves the truth unknown: a transport
    /// error after the request left may mean the metadata landed and only the ACK was
    /// lost. Until the next upload re-reads the metadata, `live` may be one commit
    /// stale — and writing "the free slot" based on a stale `live` targets the slot the
    /// storage actually considers committed.
    live_uncertain: bool,
    /// Parts known to exist per slot. `None` = unknown: an earlier incarnation of this
    /// run may have died mid-upload there, so reclaiming it has to sweep the whole part
    /// range rather than trust a count we never observed.
    known: Vec<Option<u32>>,
}

impl Ring {
    fn new(slots: usize, restored: RestoredState) -> Self {
        // A first life has nothing remote at all, so every slot is known-empty and the
        // first reclaim issues zero DELETEs. A resumed run inherits whatever earlier
        // incarnations left behind — including a partial upload of unknown length, which
        // the committed part count would understate — so every slot starts unknown and
        // the first reclaim of each sweeps the full range once to establish the truth.
        let unwritten = (!restored.had_remote).then_some(0);
        Self {
            live: restored.live,
            live_uncertain: false,
            known: vec![unwritten; slots],
        }
    }

    /// The slot to write next: any slot the committed metadata does not reference.
    /// `max(1)` keeps a malformed empty ring out of a modulo-by-zero panic in the watcher
    /// task; the caller then fails cleanly on the missing slot.
    fn target(&self) -> u32 {
        match self.live {
            Some(live) => (live + 1) % self.known.len().max(1) as u32,
            None => 0,
        }
    }
}

async fn watch_loop(
    http: &reqwest::Client,
    ckpt: &CheckpointSpec,
    dir: &Path,
    restored: RestoredState,
    stop: &Notify,
    dirty: &std::sync::atomic::AtomicBool,
) {
    let interval = Duration::from_secs(ckpt.interval_secs.max(1));
    let mut last_uploaded: Option<SystemTime> = None;
    let mut ring = Ring::new(ckpt.slots.len(), restored);

    loop {
        let stopping = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.notified() => true,
        };

        // On the periodic path one pass is right: whatever lands during it is caught by
        // the next interval. The stop path has no next interval, so it makes up to
        // FINAL_STABILITY_PASSES passes — see the constant. `exhausted` is the one end
        // condition the after-loop warning may claim: every other way out (settled, torn
        // skip, upload failure) already spoke for itself, and blaming a surviving writer
        // for those would send the operator hunting a process that does not exist.
        let passes = if stopping { FINAL_STABILITY_PASSES } else { 1 };
        let mut exhausted = true;
        for pass in 0..passes {
            let Some(mtime) = latest_mtime(dir) else {
                exhausted = false;
                break;
            };
            let changed = last_uploaded.is_none_or(|prev| mtime > prev);
            if !changed {
                exhausted = false;
                break;
            }
            // Quiescent = nothing written recently. On stop the writer (exec) is already
            // gone, so the directory is settled — upload regardless. Exception: a
            // force-killed child with writes fresher than the kill window likely died
            // MID-write; the ring would keep the previous checkpoint safe either way, but
            // committing a torn *local* directory would still leave the newest remote
            // checkpoint unusable, so keep the previous one instead. Re-evaluated every
            // pass, so an orphan that starts writing between passes still aborts the rest.
            let torn = stopping
                && dirty.load(std::sync::atomic::Ordering::SeqCst)
                && SystemTime::now()
                    .duration_since(mtime)
                    .is_ok_and(|idle| idle < DIRTY_STOP_FRESH_WRITE);
            let quiescent = stopping
                || SystemTime::now()
                    .duration_since(mtime)
                    .is_ok_and(|idle| idle.as_secs() >= ckpt.quiesce_secs);
            if torn {
                tracing::warn!(
                    "skipping final checkpoint upload: child was force-killed mid-write; \
                     keeping the previous remote checkpoint"
                );
                exhausted = false;
                break;
            }
            if !quiescent {
                exhausted = false;
                break;
            }
            match upload(http, ckpt, dir, &mut ring).await {
                Ok(meta) => {
                    last_uploaded = Some(mtime);
                    tracing::info!(
                        parts = meta.parts,
                        slot = meta.slot,
                        step = meta.step,
                        "checkpoint uploaded"
                    );
                    reclaim(http, ckpt, &mut ring).await;
                }
                Err(e) => {
                    tracing::warn!("checkpoint upload failed: {e:#}");
                    exhausted = false;
                    break;
                }
            }
            if stopping && pass + 1 < passes {
                // `changed` compares the mtime snapshot taken BEFORE this upload, so a
                // write that landed while the archive was being built makes the next
                // pass see a newer one — but only a write that already landed. If the
                // directory has not moved since the snapshot, the pass loop is done and
                // the shutdown owes no sleep; this is the normal case, and it keeps the
                // stability passes free for every run whose final upload was clean. When
                // it HAS moved, wait out the same window a torn stop uses, so a write in
                // progress finishes rather than being captured half-done.
                if latest_mtime(dir).is_none_or(|m| m <= mtime) {
                    exhausted = false;
                    break;
                }
                tokio::time::sleep(DIRTY_STOP_FRESH_WRITE).await;
            }
        }

        if stopping
            && exhausted
            && latest_mtime(dir).is_some_and(|m| last_uploaded.is_none_or(|prev| m > prev))
        {
            tracing::warn!(
                "checkpoint directory is still being written after {FINAL_STABILITY_PASSES} \
                 final passes — something outlived the supervised child and is still writing \
                 to {}; the last committed checkpoint is what was uploaded, anything newer \
                 is not",
                dir.display()
            );
        }

        if stopping {
            return;
        }
    }
}

/// Upload the checkpoint directory as a `tar|zstd` part series into the ring's target
/// slot, then write the metadata object last — the commit. Until that PUT lands the
/// previous slot is still complete and still referenced, so a failure here costs one
/// checkpoint interval, not the run.
async fn upload(
    http: &reqwest::Client,
    ckpt: &CheckpointSpec,
    dir: &Path,
    ring: &mut Ring,
) -> Result<CheckpointMeta> {
    // A failed commit PUT may have landed anyway — a reset connection loses the ACK, not
    // necessarily the write. Choosing "the free slot" from a possibly-stale `live` can
    // target the slot storage now considers committed, so the truth is re-read first.
    // 404/403 says nothing was ever committed; a fetch failure skips this interval — an
    // upload whose target cannot be chosen safely is an upload not worth starting.
    if ring.live_uncertain {
        let named = match fetch_meta(http, &ckpt.meta_get_url).await {
            Ok(Some(body)) => serde_json::from_slice::<CheckpointMeta>(&body)
                .ok()
                .map(|m| m.slot),
            Ok(None) => None,
            Err(e) => {
                return Err(
                    e.context("re-reading the checkpoint metadata after an unacknowledged commit")
                );
            }
        };
        ring.live = named;
        ring.live_uncertain = false;
        tracing::info!(
            live = ?ring.live,
            "re-resolved the live checkpoint slot after an unacknowledged commit"
        );
    }
    let slot = ring.target();
    let urls = ckpt
        .slots
        .get(slot as usize)
        .with_context(|| format!("checkpoint slot {slot} missing from job spec"))?;
    // The parts land before the count is known; a failure mid-series leaves an unknown
    // number of them behind, so mark the slot unswept before the first byte moves.
    let prior = ring.known[slot as usize];
    ring.known[slot as usize] = None;
    // Read the step BEFORE archiving, so the label describes the bytes captured.
    //
    // This used to be read after the upload returned, and it lost a finished
    // 30,000-step checkpoint. `upload_artifact` tars the directory at the moment
    // it is called; the label was then taken from the directory as it stood
    // whenever the transfer finished. On sf-4rgs62 those were 16 minutes apart
    // (inherited measurement — this port ran nothing on Salad):
    //
    //   03:03:48  upload starts, tarring a dir whose newest checkpoint is 15000
    //   03:04:52  fails 4x on the node's DNS, retrying the SAME spooled archive
    //   03:07:16  training writes step 30000 into that same directory
    //   03:19:36  upload finally succeeds; latest_step(dir) now reads 30000
    //
    // The committed metadata therefore announced step 30000 over a body holding
    // step 15000, and `checkpoint show` reported a checkpoint that did not exist
    // — so a fetch silently returned the wrong weights. Any retry spanning a
    // checkpoint boundary reproduced it.
    //
    // Reading first closes the retry-spanning window entirely: `upload_artifact`
    // spools the archive once and every retry re-sends THAT file, so nothing
    // after this line can change what was captured. What remains is the tar pass
    // itself — `append_dir_all` streams the live directory, so a write landing
    // mid-tar may be caught whole, in part, or not at all, and the label still
    // describes the state this read saw. That window is one compression pass,
    // not the whole upload. The watcher covers it: an interval scan sees the
    // newer mtime and uploads again, and on the stop path `watch_loop` makes
    // extra passes for the same reason.
    let step = latest_step(dir);
    let report = transfer::upload_artifact(http, dir, true, &urls.put_urls, "checkpoint").await?;
    // If the directory moved while the archive was being built, say so. The label is
    // still honest about what was captured; this says a newer state exists that this
    // object does not describe. `after != step` also covers the step going backwards or
    // becoming unreadable, so the message does not claim a direction.
    let after = latest_step(dir);
    if after != step {
        let show = |s: Option<u64>| s.map_or_else(|| "none".to_string(), |v| v.to_string());
        tracing::warn!(
            "checkpoint directory changed during upload (archived step {}, directory now at \
             step {}); the uploaded archive holds the earlier state",
            show(step),
            show(after)
        );
    }
    let meta = CheckpointMeta {
        v: PROTOCOL_VERSION,
        slot,
        parts: report.parts,
        bytes: report.bytes,
        sha256: report.sha256,
        step,
        uploaded_at: Utc::now(),
    };
    // The commit. Retried on transient failure BEFORE falling back to the uncertainty
    // machinery, because a re-PUT of the same bytes is idempotent and resolves the
    // lost-ACK case outright: if the first attempt landed and only its response was
    // lost, the retry overwrites it with identical content and returns the 200 the
    // first one dropped — the ring never has to guess. Only when every attempt fails
    // does `live` become uncertain, and the next upload re-reads the metadata.
    //
    // `without_url`: the meta URL is presigned; keep its signature out of warn logs.
    let mut committed: Result<()> = Ok(());
    for attempt in 1..=COMMIT_PUT_ATTEMPTS {
        let sent = http
            .put(&ckpt.meta_put_url)
            .timeout(transfer::CONTROL_TIMEOUT)
            .json(&meta)
            .send()
            .await;
        let transient;
        (transient, committed) = match sent {
            Ok(resp) => {
                let status = resp.status();
                let retryable = status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS;
                let outcome = resp
                    .error_for_status()
                    .map(|_| ())
                    .map_err(reqwest::Error::without_url)
                    .context("checkpoint metadata PUT status");
                (retryable, outcome)
            }
            // A transport error IS the maybe-lost-ACK case, so it always retries.
            Err(e) => (
                true,
                Err(anyhow::Error::from(reqwest::Error::without_url(e))
                    .context("uploading checkpoint metadata")),
            ),
        };
        match &committed {
            Ok(()) => break,
            Err(e) if transient && attempt < COMMIT_PUT_ATTEMPTS => {
                tracing::warn!("checkpoint commit attempt {attempt} failed: {e:#}; retrying");
                tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            }
            Err(_) => break,
        }
    }
    if let Err(e) = committed {
        ring.live_uncertain = true;
        return Err(e);
    }
    ring.live = Some(slot);
    // High-water mark, not the fresh count: a shorter checkpoint written over a longer
    // torn one leaves the surplus parts behind, and reclaiming only `report.parts` would
    // orphan them. An unknown prior stays unknown until a full sweep clears it.
    ring.known[slot as usize] = prior.map(|p| p.max(report.parts));
    Ok(meta)
}

/// Delete the parts of every slot the freshly committed metadata does not reference —
/// retention is one complete checkpoint. Best-effort: a slot that fails to clear is
/// storage waste, not a correctness problem (restore reads only the slot the metadata
/// names), and it will be overwritten or retried on a later rotation.
async fn reclaim(http: &reqwest::Client, ckpt: &CheckpointSpec, ring: &mut Ring) {
    let live = ring.live;
    for (index, urls) in ckpt.slots.iter().enumerate() {
        if live == Some(index as u32) {
            continue;
        }
        // Known count → delete exactly those keys; unknown → sweep the whole range, since
        // a previous incarnation may have left any number of parts behind.
        let n = match ring.known[index] {
            Some(0) => continue,
            Some(parts) => (parts as usize).min(urls.delete_urls.len()),
            None => urls.delete_urls.len(),
        };
        let mut failed = 0usize;
        let mut first_error: Option<String> = None;
        for url in &urls.delete_urls[..n] {
            // A DELETE carries no body and answers 204 — as fixed-size as a control
            // request gets, so it takes CONTROL_TIMEOUT. Without one, an endpoint that
            // accepts the connection and never answers would hang this sweep, and with
            // it the whole watcher: the run would then never write its result envelope
            // and would bill until something deleted the group.
            //
            // S3 DELETE of an absent key succeeds, so a sweep of a never-written slot is
            // a no-op rather than an error.
            let outcome = http
                .delete(url)
                .timeout(transfer::CONTROL_TIMEOUT)
                .send()
                .await
                .map_err(reqwest::Error::without_url);
            let ok = match outcome {
                Ok(resp) => {
                    let status = resp.status();
                    let ok = status.is_success() || status == StatusCode::NOT_FOUND;
                    if !ok && first_error.is_none() {
                        first_error = Some(status.to_string());
                    }
                    ok
                }
                Err(e) => {
                    if first_error.is_none() {
                        // Through anyhow so `{:#}` renders the source chain — the whole
                        // point of carrying this is telling a systematic mis-sign from
                        // one flaky object, and the cause lives in the source.
                        first_error = Some(format!("{:#}", anyhow::Error::from(e)));
                    }
                    false
                }
            };
            if !ok {
                failed += 1;
            }
        }
        if failed == 0 {
            ring.known[index] = Some(0);
            tracing::debug!(
                slot = index,
                parts = n,
                "superseded checkpoint slot reclaimed"
            );
        } else {
            // Carry the first failure: a systematically wrong DELETE URL (mis-signed, or
            // expired) fails every part identically, and a bare count cannot tell that
            // apart from one flaky object.
            tracing::warn!(
                slot = index,
                failed,
                error = first_error.unwrap_or_default(),
                "could not fully reclaim superseded checkpoint slot; retrying on next rotation"
            );
        }
    }
}

/// The highest `step_<digits>` directory under `dir`, the trainer's checkpoint layout.
/// Absent (a job with some other layout) → `None`, and the metadata simply omits it.
fn latest_step(dir: &Path) -> Option<u64> {
    let entries = std::fs::read_dir(dir).ok()?;
    entries
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|ft| ft.is_dir()))
        .filter_map(|e| {
            e.file_name()
                .to_str()
                .and_then(|n| n.strip_prefix("step_"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .max()
}

/// The most recent modification time of any file under `dir`, or `None` if the directory
/// is absent or empty. Used both to detect change and to gauge quiescence.
fn latest_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest: Option<SystemTime> = None;
    scan(dir, &mut newest);
    newest
}

fn scan(dir: &Path, newest: &mut Option<SystemTime>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match entry.file_type() {
            Ok(ft) if ft.is_dir() => scan(&path, newest),
            Ok(_) => {
                if let Ok(mtime) = entry.metadata().and_then(|m| m.modified())
                    && newest.is_none_or(|cur| mtime > cur)
                {
                    *newest = Some(mtime);
                }
            }
            Err(_) => {}
        }
    }
}
