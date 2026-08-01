// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Artifact transfer format shared by both ends.
//!
//! A logical artifact is a `tar | zstd` stream split at fixed byte boundaries into
//! numbered part objects (`<name>.tzst.000`, `.001`, …). Reassembly is ordered
//! concatenation into a single zstd decoder — the byte split needs no frame
//! alignment. Single files (`archive = false`) skip the tar wrapper.
//!
//! The engine compresses a source (file or directory) into a temp file, splits it
//! into parts, and streams each to a presigned PUT; download reverses that.

use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::UploadReport;

/// Byte size of one part in a series (4 GiB). Presigned simple PUTs require a known
/// `Content-Length`, so uploads spool one part at a time.
pub const PART_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Filename suffix for an archived (tar+zstd) artifact stream.
pub const ARCHIVE_SUFFIX: &str = ".tzst";

/// Decompression-bomb guard: on extraction an artifact may expand to at most this multiple
/// of its downloaded (compressed) size. Real outputs — model weights, checkpoints — are
/// high-entropy and barely compress, so 100× is enormous headroom for anything legitimate,
/// while a hostile `tar|zstd` of zeros (which zstd shrinks thousands-fold) is refused long
/// before it can fill the operator's disk. Enforced by [`download_artifact`] via `decompress`.
pub const MAX_DECOMPRESS_RATIO: u64 = 100;

/// Floor for the decompression-bomb limit. A small artifact — whose `compressed × ratio`
/// would itself be tiny — is always allowed to expand to at least this much, so a legitimate
/// small output is never mistaken for a bomb (1 GiB comfortably covers any real small output).
pub const MIN_DECOMPRESS_LIMIT: u64 = 1024 * 1024 * 1024;

/// Environment knob for the outgoing zstd level, read at each compress call.
const ZSTD_LEVEL_ENV: &str = "SALADFINGERS_ZSTD_LEVEL";
/// Environment knob for the outgoing zstd window log.
const ZSTD_WINDOW_LOG_ENV: &str = "SALADFINGERS_ZSTD_WINDOW_LOG";

/// zstd level for outgoing artifacts. Overridable with `SALADFINGERS_ZSTD_LEVEL`
/// (1–22 — this is real libzstd through the `zstd` crate, so the whole range is
/// live, unlike the pure-Go implementation skopeo uses for image layers, whose
/// level knob quantizes to four tiers). The variable is read by whichever
/// process is compressing: tuning the agent's node-side uploads means an
/// image-level `ENV`, because `run --env` never reaches the agent's own
/// environment — it is applied only to the training command.
///
/// Default 3, because of what this engine actually carries.
///
/// What flows through here depends on the caller. The agent's uploads —
/// checkpoints and output weights — are f32 and effectively incompressible,
/// and they are compressed on a rented node mid-training, so the default stays
/// low. Datasets, the one genuinely compressible payload, reach the node either
/// baked into the image as a layer (never touching this path) or staged per run
/// with `run --input`, where the CLI raises the level for its own process via
/// [`set_compression`]. Measured on a real 294 MiB `model.safetensors`:
///
/// | level | size            | time |
/// |-------|-----------------|------|
/// | 3     | 271 MiB (92.6%) |  0 s |
/// | 19    | 271 MiB (92.6%) | 39 s |
///
/// Byte-for-byte the same output for 39 s of extra CPU — and that CPU is spent
/// on a rented node in the middle of training, once per checkpoint.
///
/// For a compressible upload, raise it: on the 941 MiB tokenized corpus level 3
/// emits 472 MiB and level 19 emits 311 MiB (200 s), which on a 220 KiB/s
/// uplink is 37 minutes against 24. Level 22 reaches 283 MiB but takes 720 s —
/// nearly 4× the CPU for ~3 more minutes saved, so 19 is where the curve
/// flattens.
fn zstd_level() -> i32 {
    zstd_level_from(ZSTD_LEVEL_OVERRIDE.load(Ordering::Relaxed), &real_env)
}

/// Pure form of [`zstd_level`]: `override_raw` is the loaded override cell and
/// `env` the environment lookup (real env in production, a fake map in tests) —
/// the same seam the CLI's skopeo compression flags use.
fn zstd_level_from(override_raw: i32, env: &impl Fn(&str) -> Option<String>) -> i32 {
    compression_override(override_raw, 1..=22)
        .or_else(|| env_zstd_level_from(env))
        .unwrap_or(3)
}

/// The validated `SALADFINGERS_ZSTD_LEVEL`, if set, parseable and in 1–22.
/// Values are trimmed first (a padded `" 19 "` counts); anything else is
/// ignored rather than clamped.
fn env_zstd_level_from(env: &impl Fn(&str) -> Option<String>) -> Option<i32> {
    env(ZSTD_LEVEL_ENV)
        .and_then(|v| v.trim().parse::<i32>().ok())
        .filter(|l| (1..=22).contains(l))
}

/// The validated `SALADFINGERS_ZSTD_LEVEL` from the real environment — for the
/// one caller that must weigh it against a flag BEFORE [`set_compression`]
/// stores the winner (`run --input-zstd-level`).
pub fn env_zstd_level() -> Option<i32> {
    env_zstd_level_from(&real_env)
}

/// Process-wide compression overrides, set by [`set_compression`].
///
/// `i32::MIN` means "unset" so that any legal value stays expressible.
static ZSTD_LEVEL_OVERRIDE: AtomicI32 = AtomicI32::new(i32::MIN);
static ZSTD_WINDOW_LOG_OVERRIDE: AtomicI32 = AtomicI32::new(i32::MIN);

/// The valid subrange of a loaded override cell, or `None` for the sentinel and
/// for out-of-range stores — [`set_compression`] stores unvalidated; validity
/// is judged at each read.
fn compression_override(raw: i32, valid: std::ops::RangeInclusive<i32>) -> Option<i32> {
    (raw != i32::MIN && valid.contains(&raw)).then_some(raw)
}

/// Raise compression for this process only.
///
/// Scoped rather than global on purpose. The two ends upload very different
/// things: the CLI sends **inputs** (datasets — compressible), while the agent
/// on the rented node sends **checkpoints and weights** (f32 — not). Because
/// those are separate processes, the CLI can turn this up for its uploads with
/// no risk of the setting reaching the node's checkpoint path, where it would
/// only burn training CPU. See the tables on [`zstd_level`].
pub fn set_compression(level: Option<i32>, window_log: Option<u32>) {
    if let Some(l) = level {
        ZSTD_LEVEL_OVERRIDE.store(l, Ordering::Relaxed);
    }
    if let Some(w) = window_log {
        ZSTD_WINDOW_LOG_OVERRIDE.store(w as i32, Ordering::Relaxed);
    }
}

/// Window log for outgoing artifacts, via `SALADFINGERS_ZSTD_WINDOW_LOG`
/// (10–31, i.e. 1 KiB–2 GiB). Unset by default, meaning libzstd's per-level
/// window.
///
/// Off by default for the same reason as the level: the payloads this engine
/// carries are incompressible weights, where a large window buys nothing and
/// costs memory. libzstd clamps the window to the input, so it is a ceiling
/// rather than a flat allocation — but a 294 MiB checkpoint would still size
/// its window to match, on a node that is simultaneously training.
///
/// Measured on the 941 MiB tokenized TinyStories training set, each result
/// round-tripped and checksummed against the source:
///
/// | setting                   | size    | time  |
/// |---------------------------|---------|-------|
/// | level 3 (the default)     | 472 MiB |   2 s |
/// | level 19, default window  | 318 MiB | 196 s |
/// | level 19, window log 31   | 311 MiB | 200 s |
/// | level 20, window log 31   | 300 MiB | 326 s |
///
/// The window is worth about 2% here — not the order of magnitude "repetitive
/// text" might suggest. The payload is `u16` GPT-2 token ids, which carry
/// little long-range duplication at the BYTE level even though the underlying
/// prose repeats heavily. The level is the larger lever, and most of what it
/// offers has arrived by 19.
///
/// The knob is still exposed: it costs nothing when the data does not suit it,
/// and a corpus shipped as raw text rather than pre-tokenized ids would be a
/// very different case. Setting it enables long-distance matching alongside the
/// window, since a large window without LDM mostly costs memory rather than
/// finding matches.
///
/// Set it to a smaller log if a node ever proves memory-tight: the agent shares
/// this path for checkpoint uploads, and while the clamp keeps the cost
/// proportional to the artifact, a multi-hundred-MB checkpoint will size its
/// window accordingly.
fn zstd_window_log() -> Option<u32> {
    zstd_window_log_from(ZSTD_WINDOW_LOG_OVERRIDE.load(Ordering::Relaxed), &real_env)
}

/// Pure form of [`zstd_window_log`] — see [`zstd_level_from`].
fn zstd_window_log_from(override_raw: i32, env: &impl Fn(&str) -> Option<String>) -> Option<u32> {
    if let Some(w) = compression_override(override_raw, 10..=31) {
        return Some(w as u32);
    }
    env(ZSTD_WINDOW_LOG_ENV)
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|l| (10..=31).contains(l))
}

/// The real-environment adapter for the seams above. Trimming and
/// empty-filtering live in the parsers — the testable side of the seam — so
/// this stays a plain lookup.
fn real_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Ceiling the decoder will accept, as a window log.
///
/// libzstd's streaming decoder defaults to refusing any frame declaring a
/// window above `ZSTD_WINDOWLOG_LIMIT_DEFAULT` (27 — 128 MiB), so anything
/// compressed with a larger window would download fine and then fail to
/// extract. Since [`zstd_window_log`] can go to 31, the decoder is raised to
/// match.
///
/// This is a limit, not an allocation: the decoder still sizes its buffer from
/// the frame header, so ordinary artifacts are unaffected. It only removes a
/// refusal that would otherwise strand an upload we had already paid for.
const ZSTD_DECODER_WINDOW_LOG_MAX: u32 = 31;

/// Apply the tuning above to a fresh encoder.
fn tune_encoder<W: Write>(encoder: &mut zstd::Encoder<'_, W>) -> Result<()> {
    if let Some(log) = zstd_window_log() {
        encoder
            .set_parameter(zstd::zstd_safe::CParameter::EnableLongDistanceMatching(
                true,
            ))
            .context("enabling zstd long-distance matching")?;
        encoder
            .set_parameter(zstd::zstd_safe::CParameter::WindowLog(log))
            .context("setting zstd window log")?;
    }
    Ok(())
}

/// Connect timeout for a transfer request: a node that cannot open the TCP connection
/// this quickly is treated as dead (retried, then the run reallocates).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// TCP keepalive. With no read timeout (see [`transfer_client`]) this is the
/// mechanism that notices a peer which died mid-transfer, in tens of seconds
/// rather than the kernel's ~15 min default. It measures the CONNECTION, not
/// the transfer's duration, so it cannot mistake a large upload for a broken
/// one.
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Per-part transfer attempts before giving up. A single transient blip (5xx, dropped
/// connection, dropped peer) must not torch a whole multi-part artifact.
const MAX_TRANSFER_ATTEMPTS: u32 = 4;

/// How many same-host redirects a credentialed client will follow before giving up.
///
/// A service may legitimately move a path within itself; five hops is plenty for that,
/// and anything longer is a loop. Load-bearing for [`transfer_client`], which deliberately
/// has no total timeout: without this cut-off a self-redirecting endpoint would spin the
/// client forever.
const MAX_SAME_HOST_REDIRECTS: usize = 5;

/// A [`reqwest::ClientBuilder`] with this workspace's credential-safety policy applied.
/// Callers add their own timeouts on top.
///
/// **Every client that carries a credential must start here.** Two ways a redirect leaks
/// one, both verified against reqwest rather than assumed:
///
/// - **A custom header survives a cross-host redirect.** reqwest strips only the standard
///   sensitive names (`AUTHORIZATION`, `COOKIE`, `PROXY_AUTHORIZATION`, …). `Salad-Api-Key`
///   and the agent's bearer token are custom, so with the default policy — follow up to ten
///   — one 3xx hands them to whatever host it names.
/// - **`Referer` carries the previous URL, query and all.** For a presigned URL the query
///   *is* the credential (`X-Amz-Signature=…`), so following a redirect from one publishes
///   a live storage capability to the redirect target. reqwest sets `Referer` by default;
///   this turns it off.
///
/// The policy is therefore: follow a redirect that stays on the same origin, because a
/// service may legitimately move a path; refuse one that crosses to another origin, by
/// name, because no legitimate case needs it and every case leaks. A presigned URL is
/// signed for one host anyway, so a followed cross-host redirect could not have
/// authenticated — it could only have handed the signature over.
///
/// (`tunnel` is the one deliberate exception and does not use this: a reverse proxy must
/// hand a 3xx back to the browser rather than resolve it, so it sets `Policy::none()` and
/// rewrites the `Location`. Same reasoning, different correct answer.)
pub fn credentialed_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .referer(false)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let same_origin = attempt.previous().last().is_some_and(|prev| {
                prev.scheme() == attempt.url().scheme()
                    && prev.host_str() == attempt.url().host_str()
                    && prev.port_or_known_default() == attempt.url().port_or_known_default()
            });
            if !same_origin {
                attempt.error(
                    "refusing a redirect to a different host: this request carries a \
                     credential that would travel with it",
                )
            } else if attempt.previous().len() > MAX_SAME_HOST_REDIRECTS {
                attempt.error("too many redirects")
            } else {
                attempt.follow()
            }
        }))
}

/// An HTTP client tuned for large artifact transfers.
///
/// No total-request deadline, and — because this client serves uploads too —
/// **no read timeout either**. Both are the same trap in different clothes: any
/// cap on how long a transfer may take is a cap on how large a transfer may be.
///
/// The total `.timeout()` went first, after it pinned a 4 GiB part at ~115 Mbps
/// regardless of the node. `.read_timeout()` then reintroduced the identical
/// failure in the upload direction, because during a PUT the client is
/// *writing* and receives nothing until the server answers the completed body.
/// Time-since-last-received-byte therefore grows monotonically through a
/// perfectly healthy upload and trips at 120 s — meaning an artifact that could
/// not be uploaded within 120 s could never be uploaded at all.
///
/// This was not hypothetical. An 880 MB checkpoint (77M params: 294 MB of
/// weights plus 588 MB of AdamW moments) failed four identical attempts spaced
/// exactly 121 s apart, from a node with a healthy network, against an nginx
/// already configured with `client_max_body_size 0`, `proxy_request_buffering
/// off` and 3600 s proxy timeouts. Checkpointing had never worked for a model
/// big enough to need it.
///
/// A dead peer is still caught, by TCP keepalive and the connect timeout —
/// mechanisms that watch the CONNECTION rather than the transfer's duration,
/// and so cannot confuse a big upload with a broken one.
///
/// Every URL this client fetches is presigned, i.e. the URL itself is the credential, so
/// it takes the redirect and `Referer` policy from [`credentialed_client_builder`].
///
/// # Errors
/// Returns an error if the TLS backend fails to initialize.
pub fn transfer_client() -> reqwest::Result<reqwest::Client> {
    credentialed_client_builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .build()
}

/// Deadline for one control-plane request: the job spec, the attempts ledger, the result
/// envelope, checkpoint metadata. Apply it per request with `.timeout(CONTROL_TIMEOUT)`.
///
/// [`transfer_client`] deliberately carries no timeout of any kind, and the SAME client
/// serves these. That is right for a blob whose size is not known in advance and wrong for
/// a JSON document of a few kilobytes: without a bound here, a storage endpoint that
/// accepts the connection and then never answers hangs the agent for the whole of the run's
/// max duration and bills every second of it. TCP keepalive does not cover this — it
/// notices a peer that DIED, not one that is alive and simply not replying.
///
/// This does not reintroduce the trap that removed the read timeout. That bound was a cap
/// on how long a *transfer* could take, and so a cap on how large one could be; these
/// requests carry a fixed, small body, so bounding them bounds no size at all.
///
/// The bandwidth-gate samples are deliberately NOT given this deadline. They are small
/// transfers, not fixed-size documents: tens of MiB on precisely the slow node the gate
/// exists to detect can legitimately take longer than any control deadline, and timing
/// them out would misread "slow, which is what I am here to measure" as "hung". A dead
/// peer mid-sample is still caught by keepalive and the connect timeout.
pub const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);

/// Storage key for part `index` of the artifact named `name`.
///
/// ```
/// use saladfingers_protocol::transfer::part_key;
/// assert_eq!(part_key("runs/sf-x/out/model", 0), "runs/sf-x/out/model.tzst.000");
/// assert_eq!(part_key("a", 42), "a.tzst.042");
/// ```
#[must_use]
pub fn part_key(name: &str, index: u32) -> String {
    format!("{name}{ARCHIVE_SUFFIX}.{index:03}")
}

/// Number of parts needed to hold `total_bytes`.
#[must_use]
pub fn part_count(total_bytes: u64) -> u32 {
    if total_bytes == 0 {
        return 1;
    }
    u32::try_from(total_bytes.div_ceil(PART_SIZE)).unwrap_or(u32::MAX)
}

/// Lowercase-hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Compress `source` (a file or directory) to `tar|zstd`, split it into ≤
/// [`PART_SIZE`] parts, and stream each part to its presigned PUT URL.
///
/// # Errors
/// Returns an error if compression, upload, or the URL count fails.
pub async fn upload_artifact(
    http: &reqwest::Client,
    source: &Path,
    archive: bool,
    put_urls: &[String],
    name: &str,
) -> Result<UploadReport> {
    let source = source.to_path_buf();
    let (temp, sha256, total) =
        tokio::task::spawn_blocking(move || compress(&source, archive)).await??;
    upload_spooled(http, &temp, sha256, total, put_urls, name).await
}

/// Compress a SET of paths (each named by its path relative to `base`) into one `tar|zstd`
/// archive and upload it as a part series. Used for glob outputs, where a single pattern can
/// match several files or directories that must all land in one named artifact.
///
/// # Errors
/// Returns an error if compression, upload, or the URL count fails.
pub async fn upload_archive(
    http: &reqwest::Client,
    base: &Path,
    rel_paths: &[String],
    put_urls: &[String],
    name: &str,
) -> Result<UploadReport> {
    let base = base.to_path_buf();
    let rels = rel_paths.to_vec();
    let (temp, sha256, total) =
        tokio::task::spawn_blocking(move || compress_entries(&base, &rels)).await??;
    upload_spooled(http, &temp, sha256, total, put_urls, name).await
}

/// Split a spooled temp file into ≤[`PART_SIZE`] parts and stream each to its PUT URL,
/// retrying transient failures per part.
async fn upload_spooled(
    http: &reqwest::Client,
    temp: &NamedTempFile,
    sha256: String,
    total: u64,
    put_urls: &[String],
    name: &str,
) -> Result<UploadReport> {
    let parts = part_count(total);
    if parts as usize > put_urls.len() {
        bail!(
            "artifact '{name}' needs {parts} parts but only {} PUT URLs were provided",
            put_urls.len()
        );
    }
    let path = temp.path().to_path_buf();
    for index in 0..parts {
        let offset = u64::from(index) * PART_SIZE;
        let len = (total - offset).min(PART_SIZE);
        put_part_with_retry(
            http,
            &put_urls[index as usize],
            &path,
            offset,
            len,
            &format!("uploading part {index} of '{name}'"),
        )
        .await?;
    }
    Ok(UploadReport {
        name: name.to_string(),
        parts,
        bytes: total,
        sha256,
    })
}

/// Download an artifact from its ordered presigned GET URLs, decoding the reassembled
/// `tar|zstd` stream into `dest` (a directory when `archive`, else a file).
///
/// # Errors
/// Returns an error on download or decode failure, or on an unsafe tar entry.
pub async fn download_artifact(
    http: &reqwest::Client,
    get_urls: &[String],
    dest: &Path,
    archive: bool,
    expected_sha256: Option<&str>,
) -> Result<()> {
    // An artifact that is reported as present must have at least one part. An empty series
    // would download nothing, leave the temp file at zero bytes, and then trivially satisfy
    // the SHA-256 gate below with the empty-string digest — reaching `decompress` (and its
    // `create_dir_all(dest)`) without a single byte ever fetched. Refuse it: every honest
    // caller passes a non-empty part list (`part_count` is always ≥ 1), so this only rejects
    // a malformed or hostile "0-part" artifact.
    if get_urls.is_empty() {
        bail!("artifact has no parts to download");
    }
    let temp = NamedTempFile::new().context("creating download temp file")?;
    {
        let mut out = tokio::fs::File::from_std(temp.reopen()?);
        for (index, url) in get_urls.iter().enumerate() {
            // Each part is a fixed-length object, so a retry re-seeks to the part's start
            // and overwrites whatever a failed attempt left behind — an exact rewrite.
            let part_start = out.stream_position().await?;
            let mut attempt = 0;
            loop {
                attempt += 1;
                match download_part(http, url, &mut out, part_start).await {
                    Ok(()) => break,
                    Err(e) if attempt >= MAX_TRANSFER_ATTEMPTS || !retryable(&e) => {
                        return Err(e).with_context(|| format!("downloading part {index}"));
                    }
                    Err(e) => {
                        let backoff = transfer_backoff(attempt);
                        tracing::warn!(
                            "download part {index} attempt {attempt} failed: {e:#}; retrying in {backoff:?}"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }
        out.flush().await?;
    }
    // Verify integrity before decoding. Parts overwrite fixed keys, so a node that died
    // mid-upload can leave a torn, mixed-generation stream; without this check a corrupt
    // reassembly would extract silently or hand the caller garbage. Checkpoints rotate
    // between ring slots now, so a torn one no longer replaces the restorable copy — but
    // it can still be the slot being *written*, and outputs overwrite in place regardless.
    if let Some(expected) = expected_sha256 {
        let path = temp.path().to_path_buf();
        let actual = tokio::task::spawn_blocking(move || sha256_file(&path)).await??;
        if !actual.eq_ignore_ascii_case(expected) {
            bail!("artifact integrity check failed: expected sha256 {expected}, got {actual}");
        }
    }
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || decompress(temp.path(), &dest, archive)).await?
}

async fn put_part(
    http: &reqwest::Client,
    url: &str,
    path: &Path,
    offset: u64,
    len: u64,
) -> Result<()> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let stream = ReaderStream::new(file.take(len));
    let body = reqwest::Body::wrap_stream(stream);
    // `without_url`: keep the presigned signature out of error text and retry logs.
    http.put(url)
        .header(reqwest::header::CONTENT_LENGTH, len)
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    Ok(())
}

/// [`put_part`] with bounded retry. The part re-streams from the spooled temp file each
/// attempt, so a PUT is idempotent — one transient failure never torches the artifact.
async fn put_part_with_retry(
    http: &reqwest::Client,
    url: &str,
    path: &Path,
    offset: u64,
    len: u64,
    label: &str,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match put_part(http, url, path, offset, len).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt >= MAX_TRANSFER_ATTEMPTS || !retryable(&e) => {
                return Err(e).context(format!("{label} failed after {attempt} attempt(s)"));
            }
            Err(e) => {
                let backoff = transfer_backoff(attempt);
                tracing::warn!("{label} attempt {attempt} failed: {e:#}; retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// Download one part into `out`, re-seeking to `part_start` first so a retried attempt
/// overwrites any partial bytes a previous attempt wrote (parts are fixed-length objects).
async fn download_part(
    http: &reqwest::Client,
    url: &str,
    out: &mut tokio::fs::File,
    part_start: u64,
) -> Result<()> {
    out.seek(std::io::SeekFrom::Start(part_start)).await?;
    // `without_url`: reqwest errors carry the full URL — for a presigned URL that is a
    // live capability (the signature is in the query string) and must not reach error
    // text or retry-warning logs. The part index in the caller's context is enough.
    let mut resp = http
        .get(url)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    while let Some(chunk) = resp.chunk().await.map_err(reqwest::Error::without_url)? {
        out.write_all(&chunk).await?;
    }
    Ok(())
}

/// Backoff before transfer attempt `attempt` (1-based): 500 ms, 1 s, 2 s, …
fn transfer_backoff(attempt: u32) -> Duration {
    Duration::from_millis(500u64 << attempt.saturating_sub(1).min(6))
}

/// A transfer error is worth retrying unless it is a definitive client error — a bad or
/// expired presigned URL (403/404) no retry can fix. Throttle/timeout 4xx and every 5xx,
/// connect, read-stall, or dropped-stream error is transient. Non-HTTP errors (local IO)
/// are not retried: reopening the same file will fail the same way.
fn retryable(e: &anyhow::Error) -> bool {
    let Some(re) = e.downcast_ref::<reqwest::Error>() else {
        return false;
    };
    match re.status() {
        Some(s) if s.is_client_error() => {
            s == reqwest::StatusCode::REQUEST_TIMEOUT || s == reqwest::StatusCode::TOO_MANY_REQUESTS
        }
        _ => true,
    }
}

/// Streaming lowercase-hex SHA-256 of a file's bytes. Runs in a blocking context.
fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let mut out = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(out)
}

fn compress(source: &Path, archive: bool) -> Result<(NamedTempFile, String, u64)> {
    let temp = NamedTempFile::new().context("creating upload temp file")?;
    let writer = HashWriter::new(temp.reopen()?);
    let mut encoder = zstd::Encoder::new(writer, zstd_level()).context("creating zstd encoder")?;
    tune_encoder(&mut encoder)?;
    if archive {
        let mut builder = tar::Builder::new(&mut encoder);
        if source.is_dir() {
            builder
                .append_dir_all(".", source)
                .with_context(|| format!("archiving {}", source.display()))?;
        } else {
            let file_name = source.file_name().context("source has no file name")?;
            let mut file = std::fs::File::open(source)?;
            builder.append_file(file_name, &mut file)?;
        }
        builder.finish()?;
        drop(builder);
    } else {
        let mut file =
            std::fs::File::open(source).with_context(|| format!("opening {}", source.display()))?;
        std::io::copy(&mut file, &mut encoder)?;
    }
    let hasher = encoder.finish().context("finalizing zstd stream")?;
    let sha256 = hasher.finalize_hex();
    let total = temp.as_file().metadata()?.len();
    Ok((temp, sha256, total))
}

/// Compress a set of `rel_paths` (each resolved against `base` and named by that relative
/// path) into one `tar|zstd` stream. Directories are archived recursively; files individually.
fn compress_entries(base: &Path, rel_paths: &[String]) -> Result<(NamedTempFile, String, u64)> {
    let temp = NamedTempFile::new().context("creating upload temp file")?;
    let writer = HashWriter::new(temp.reopen()?);
    let mut encoder = zstd::Encoder::new(writer, zstd_level()).context("creating zstd encoder")?;
    tune_encoder(&mut encoder)?;
    {
        let mut builder = tar::Builder::new(&mut encoder);
        for rel in rel_paths {
            let full = base.join(rel);
            if full.is_dir() {
                builder
                    .append_dir_all(rel, &full)
                    .with_context(|| format!("archiving {}", full.display()))?;
            } else {
                builder
                    .append_path_with_name(&full, rel)
                    .with_context(|| format!("archiving {}", full.display()))?;
            }
        }
        builder.finish()?;
    }
    let hasher = encoder.finish().context("finalizing zstd stream")?;
    let sha256 = hasher.finalize_hex();
    let total = temp.as_file().metadata()?.len();
    Ok((temp, sha256, total))
}

fn decompress(compressed: &Path, dest: &Path, archive: bool) -> Result<()> {
    let compressed_len = std::fs::metadata(compressed)?.len();
    decompress_limited(compressed, dest, archive, decompress_limit(compressed_len))
}

/// The decompression-bomb ceiling for a stream of `compressed_len` bytes: `MAX_DECOMPRESS_RATIO`×
/// the compressed size, floored at [`MIN_DECOMPRESS_LIMIT`]. `saturating_mul` so a (hypothetical)
/// enormous compressed size can't wrap the limit to a small value.
fn decompress_limit(compressed_len: u64) -> u64 {
    compressed_len
        .saturating_mul(MAX_DECOMPRESS_RATIO)
        .max(MIN_DECOMPRESS_LIMIT)
}

/// Extract `compressed` into `dest`, refusing to write more than `limit` decompressed bytes.
/// The limit turns a decompression bomb into a bounded, cleanly-failed extraction instead of
/// an unbounded write to the operator's disk.
fn decompress_limited(compressed: &Path, dest: &Path, archive: bool, limit: u64) -> Result<()> {
    let file = std::fs::File::open(compressed)?;
    let mut decoder = zstd::Decoder::new(file).context("creating zstd decoder")?;
    // Accept the large-window frames `zstd_window_log` can produce; without this
    // the decoder refuses anything above 128 MiB and the artifact is unreadable.
    decoder
        .window_log_max(ZSTD_DECODER_WINDOW_LOG_MAX)
        .context("raising zstd decoder window limit")?;
    let mut reader = LimitedReader::new(decoder, limit);
    if archive {
        std::fs::create_dir_all(dest)?;
        let mut archive = tar::Archive::new(&mut reader);
        for entry in archive.entries()? {
            let mut entry = entry?;
            // `unpack_in` refuses to write outside `dest` (path-traversal guard).
            if !entry.unpack_in(dest)? {
                bail!("refused an unsafe tar entry");
            }
        }
    } else {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out = std::fs::File::create(dest)?;
        std::io::copy(&mut reader, &mut out)?;
    }
    Ok(())
}

/// A `Read` adapter that fails once more than `limit` total bytes have passed through it —
/// the decompression-bomb guard. Wrapping the zstd decoder means a `tar|zstd` stream that
/// expands past the cap errors mid-extraction rather than writing unbounded bytes to disk.
struct LimitedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.remaining = self.remaining.checked_sub(n as u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "decompressed size exceeds the allowed limit (possible decompression bomb)",
            )
        })?;
        Ok(n)
    }
}

/// A `Write` adapter that SHA-256-hashes everything written through it.
struct HashWriter<W> {
    inner: W,
    hasher: Sha256,
}

impl<W: Write> HashWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finalize_hex(self) -> String {
        let digest = self.hasher.finalize();
        let mut out = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_keys_are_zero_padded_and_ordered() {
        assert_eq!(part_key("x", 0), "x.tzst.000");
        assert_eq!(part_key("x", 7), "x.tzst.007");
        assert_eq!(part_key("dir/x", 123), "dir/x.tzst.123");
    }

    /// A redirect must not carry a credential to another host — and for these clients the
    /// credential is the URL itself, so `Referer` is a leak channel as surely as a header.
    /// A reqwest `Client` exposes neither setting, so both are shown by behaviour: a
    /// second host that records what it receives must record nothing at all.
    ///
    /// Same-origin redirects are still followed, because a service may legitimately move
    /// a path and refusing that would be a regression with no security benefit.
    #[tokio::test]
    async fn a_credentialed_client_refuses_a_cross_host_redirect_and_sends_no_referer() {
        use std::sync::{Arc, Mutex};

        // Host B: records every request it is handed.
        let seen: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_b = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
            let recorder = Arc::clone(&recorder);
            async move {
                recorder.lock().unwrap().push(
                    headers
                        .get("referer")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                );
                "landed"
            }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // Host A: `/away` leaves for host B, `/home` redirects within itself. What lands
        // on `/arrived` is recorded too, because the same-origin follow is the ONLY
        // request reqwest would ever attach a `Referer` to — host B can never observe
        // the header, since the whole point is that host B is never reached.
        let arrived: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let arrived_rec = Arc::clone(&arrived);
        let away_to = format!("{host_b}/collect");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_a = format!("http://{}", listener.local_addr().unwrap());
        use axum::response::IntoResponse as _;
        let app = axum::Router::new().fallback(
            move |uri: axum::http::Uri, headers: axum::http::HeaderMap| {
                let away_to = away_to.clone();
                let arrived_rec = Arc::clone(&arrived_rec);
                async move {
                    match uri.path() {
                        "/away" => (
                            axum::http::StatusCode::FOUND,
                            [(axum::http::header::LOCATION, away_to)],
                        )
                            .into_response(),
                        "/home" => (
                            axum::http::StatusCode::FOUND,
                            [(axum::http::header::LOCATION, "/arrived".to_string())],
                        )
                            .into_response(),
                        _ => {
                            arrived_rec.lock().unwrap().push(
                                headers
                                    .get("referer")
                                    .and_then(|v| v.to_str().ok())
                                    .map(str::to_string),
                            );
                            "arrived".into_response()
                        }
                    }
                }
            },
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let http = transfer_client().unwrap();

        // The URL stands in for a presigned one: its query IS the credential.
        let signed = format!("{host_a}/away?X-Amz-Signature=deadbeefcafef00d");
        let err = http
            .get(&signed)
            .send()
            .await
            .expect_err("a cross-host redirect must be refused, not followed");
        assert!(
            format!("{err}").contains("different host") || err.is_redirect(),
            "unexpected error: {err}"
        );
        assert!(
            seen.lock().unwrap().is_empty(),
            "the other host was contacted at all — the signed URL travelled with the request"
        );

        // Same origin still works, and still sends no Referer.
        let resp = http
            .get(format!("{host_a}/home?X-Amz-Signature=deadbeefcafef00d"))
            .send()
            .await
            .expect("a same-origin redirect is legitimate and must still be followed");
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "arrived");
        // The follow that just happened is the one request that could carry a `Referer`
        // — and the URL it would repeat has a live signature in its query.
        let arrived = arrived.lock().unwrap();
        assert_eq!(
            arrived.len(),
            1,
            "exactly one request must land on /arrived"
        );
        assert_eq!(
            arrived[0], None,
            "the same-origin follow carried a Referer — `.referer(false)` is gone from \
             the builder, and with it the query of every presigned URL a redirect touches"
        );
    }

    /// The loop cap is the one line of the redirect policy the other tests cannot see —
    /// and for this client it guards against an infinite hang, not just waste:
    /// [`transfer_client`] deliberately has no total timeout, so without the cap a
    /// self-redirecting endpoint would spin it forever.
    #[tokio::test]
    async fn a_same_origin_redirect_loop_is_cut_at_the_cap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        use axum::response::IntoResponse as _;
        let app = axum::Router::new().fallback(move || {
            let counter = Arc::clone(&counter);
            async move {
                // Terminate eventually even with the cap broken, so a regression fails
                // the assertions below instead of hanging the test.
                if counter.fetch_add(1, Ordering::SeqCst) >= 25 {
                    return "escaped".into_response();
                }
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, "/loop".to_string())],
                )
                    .into_response()
            }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let http = transfer_client().unwrap();
        let err = http
            .get(format!("{base}/loop"))
            .send()
            .await
            .expect_err("a same-origin redirect loop must be cut, not followed forever");
        assert!(
            format!("{err}").contains("too many redirects") || err.is_redirect(),
            "unexpected error: {err}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            MAX_SAME_HOST_REDIRECTS + 1,
            "the cap allows the original request plus MAX_SAME_HOST_REDIRECTS follows"
        );
    }

    #[test]
    fn part_count_rounds_up() {
        assert_eq!(part_count(0), 1);
        assert_eq!(part_count(1), 1);
        assert_eq!(part_count(PART_SIZE), 1);
        assert_eq!(part_count(PART_SIZE + 1), 2);
    }

    #[test]
    fn sha256_matches_known_vector() {
        // SHA-256 of the empty string.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn download_rejects_an_empty_part_series() {
        // A "0-part" artifact must be refused outright. Otherwise it downloads nothing, the
        // temp file stays empty, and its SHA-256 (the empty-string digest) matches an
        // attacker-supplied `expected` — handing control to `decompress`/`create_dir_all`
        // with a caller-chosen `dest` and no bytes ever fetched.
        let http = reqwest::Client::new();
        let dst = tempfile::tempdir().unwrap();
        let empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let err = download_artifact(&http, &[], dst.path(), true, Some(empty_hash))
            .await
            .expect_err("an empty part series must be rejected before extraction");
        assert!(
            err.to_string().contains("no parts"),
            "unexpected error: {err:#}"
        );
        // Nothing was created at the destination.
        assert!(std::fs::read_dir(dst.path()).unwrap().next().is_none());
    }

    #[test]
    fn decompress_limit_scales_with_size_and_has_a_floor() {
        // Ratio for large inputs...
        let ten_gib = 10 * 1024 * 1024 * 1024;
        assert_eq!(decompress_limit(ten_gib), ten_gib * MAX_DECOMPRESS_RATIO);
        // ...floor for small ones (a tiny artifact still gets generous room).
        assert_eq!(decompress_limit(0), MIN_DECOMPRESS_LIMIT);
        assert_eq!(decompress_limit(1), MIN_DECOMPRESS_LIMIT);
    }

    #[test]
    fn decompress_rejects_a_bomb_but_admits_a_normal_artifact() {
        // 256 KiB of zeros compresses to almost nothing but expands right back — a stand-in
        // for a decompression bomb (a hostile node's real one would be far larger).
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("big.bin"), vec![0u8; 256 * 1024]).unwrap();
        let (compressed, _sha, _total) = compress(src.path(), true).unwrap();

        // A limit below the decompressed size is a suspected bomb → refused mid-extraction.
        let tight = tempfile::tempdir().unwrap();
        let err = decompress_limited(compressed.path(), tight.path(), true, 64 * 1024).unwrap_err();
        assert!(
            format!("{err:#}").contains("decompression bomb"),
            "unexpected error: {err:#}"
        );

        // The very same artifact extracts cleanly under a generous limit.
        let ok = tempfile::tempdir().unwrap();
        decompress_limited(compressed.path(), ok.path(), true, 8 * 1024 * 1024).unwrap();
        assert_eq!(
            std::fs::read(ok.path().join("big.bin")).unwrap(),
            vec![0u8; 256 * 1024]
        );
    }

    // A tiny in-memory object store: PUT stores, GET returns.
    async fn storage_server() -> (
        String,
        std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
    ) {
        use axum::body::Bytes;
        use axum::extract::{Path as AxPath, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use axum::routing::put;

        type Store = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>;
        let store: Store =
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let app = axum::Router::new()
            .route(
                "/{*key}",
                put(
                    |AxPath(key): AxPath<String>, State(s): State<Store>, body: Bytes| async move {
                        s.lock().unwrap().insert(key, body.to_vec());
                        StatusCode::OK
                    },
                )
                .get(
                    |AxPath(key): AxPath<String>, State(s): State<Store>| async move {
                        match s.lock().unwrap().get(&key) {
                            Some(v) => (StatusCode::OK, v.clone()).into_response(),
                            None => StatusCode::NOT_FOUND.into_response(),
                        }
                    },
                ),
            )
            .with_state(store.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, store)
    }

    /// A per-request timeout must bound a hung endpoint even though the client itself
    /// has none — the whole basis for [`CONTROL_TIMEOUT`].
    ///
    /// The trap it guards is a server that ACCEPTS the connection and then never
    /// answers. `transfer_client` carries no timeout at all, and TCP keepalive does
    /// not help here: the peer is alive, it just is not replying. Without the
    /// per-request bound the agent waits on a JSON document for the whole of the run's
    /// max duration, billing every second. A short timeout stands in for the real 60 s
    /// so the test costs milliseconds.
    #[tokio::test]
    async fn a_per_request_timeout_bounds_a_hung_endpoint() {
        use axum::routing::get;

        let app = axum::Router::new().route(
            "/hang",
            get(|| async {
                // Longer than any patience this test has; never actually completes.
                tokio::time::sleep(Duration::from_secs(300)).await;
                "never"
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // The real client: no total timeout, no read timeout.
        let http = transfer_client().unwrap();
        let started = std::time::Instant::now();
        let err = http
            .get(format!("{base}/hang"))
            .timeout(Duration::from_millis(200))
            .send()
            .await
            .expect_err("a hung endpoint must not be waited on forever");

        assert!(err.is_timeout(), "expected a timeout, got {err}");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "took {:?} — the per-request timeout did not apply",
            started.elapsed()
        );
        // And the other direction, which is what makes the bound load-bearing rather
        // than decorative: WITHOUT a per-request timeout the same request is still
        // pending, because nothing in the client will ever stop it.
        let unbounded = tokio::time::timeout(
            Duration::from_millis(500),
            http.get(format!("{base}/hang")).send(),
        )
        .await;
        assert!(
            unbounded.is_err(),
            "the client is supposed to have no timeout of its own; something bounded it"
        );
    }

    #[tokio::test]
    async fn roundtrip_archive_directory() {
        let (base, store) = storage_server().await;
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello world").unwrap();
        std::fs::create_dir(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("sub").join("b.bin"), vec![7u8; 5000]).unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/obj.tzst.000")];
        let report = upload_artifact(&http, src.path(), true, &urls, "obj")
            .await
            .unwrap();
        assert_eq!(report.parts, 1);
        assert_eq!(report.sha256.len(), 64);
        assert!(store.lock().unwrap().contains_key("obj.tzst.000"));

        let dst = tempfile::tempdir().unwrap();
        download_artifact(&http, &urls, dst.path(), true, Some(&report.sha256))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(dst.path().join("a.txt")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(dst.path().join("sub").join("b.bin")).unwrap(),
            vec![7u8; 5000]
        );
    }

    #[tokio::test]
    async fn roundtrip_single_file() {
        let (base, _store) = storage_server().await;
        let src = tempfile::tempdir().unwrap();
        let src_file = src.path().join("model.safetensors");
        std::fs::write(&src_file, vec![42u8; 12345]).unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/model.tzst.000")];
        let report = upload_artifact(&http, &src_file, false, &urls, "model")
            .await
            .unwrap();

        let dst = tempfile::tempdir().unwrap();
        let dst_file = dst.path().join("out.safetensors");
        download_artifact(&http, &urls, &dst_file, false, Some(&report.sha256))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dst_file).unwrap(), vec![42u8; 12345]);
    }

    /// Build the env lookup the compression-tunable tests share.
    fn env_of(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn a_set_compression_override_beats_the_env() {
        let env = env_of(&[(ZSTD_LEVEL_ENV, "7"), (ZSTD_WINDOW_LOG_ENV, "12")]);
        let env = |k: &str| env.get(k).cloned();
        assert_eq!(zstd_level_from(19, &env), 19);
        assert_eq!(zstd_window_log_from(26, &env), Some(26));
    }

    #[test]
    fn the_unset_sentinel_falls_through_to_the_env_then_the_default() {
        let set = env_of(&[(ZSTD_LEVEL_ENV, "7"), (ZSTD_WINDOW_LOG_ENV, "12")]);
        let set = |k: &str| set.get(k).cloned();
        let empty = env_of(&[]);
        let empty = |k: &str| empty.get(k).cloned();
        assert_eq!(zstd_level_from(i32::MIN, &set), 7);
        assert_eq!(zstd_level_from(i32::MIN, &empty), 3);
        assert_eq!(zstd_window_log_from(i32::MIN, &set), Some(12));
        assert_eq!(zstd_window_log_from(i32::MIN, &empty), None);
    }

    /// [`set_compression`] stores unvalidated and validity is judged at each
    /// read — an illegal store must fall through to the env, never clamp.
    #[test]
    fn an_out_of_range_override_is_ignored_on_read_not_clamped() {
        let env = env_of(&[(ZSTD_LEVEL_ENV, "7"), (ZSTD_WINDOW_LOG_ENV, "12")]);
        let env = |k: &str| env.get(k).cloned();
        let empty = env_of(&[]);
        let empty = |k: &str| empty.get(k).cloned();
        assert_eq!(zstd_level_from(0, &env), 7);
        assert_eq!(zstd_level_from(23, &env), 7);
        assert_eq!(zstd_level_from(23, &empty), 3);
        assert_eq!(zstd_window_log_from(9, &env), Some(12));
        assert_eq!(zstd_window_log_from(32, &env), Some(12));
        assert_eq!(zstd_window_log_from(32, &empty), None);
    }

    #[test]
    fn a_malformed_env_value_is_ignored() {
        for bad in ["banana", "", "3.5"] {
            let env = env_of(&[(ZSTD_LEVEL_ENV, bad), (ZSTD_WINDOW_LOG_ENV, bad)]);
            let env = |k: &str| env.get(k).cloned();
            assert_eq!(zstd_level_from(i32::MIN, &env), 3, "level {bad:?}");
            assert_eq!(zstd_window_log_from(i32::MIN, &env), None, "window {bad:?}");
        }
    }

    /// Pins the trim semantics the plain [`real_env`] adapter relies on: the
    /// parsers must accept padded values, the way `image.rs`'s `non_empty_env`
    /// would have trimmed them before parsing.
    #[test]
    fn a_padded_env_value_still_parses() {
        let env = env_of(&[(ZSTD_LEVEL_ENV, " 19 "), (ZSTD_WINDOW_LOG_ENV, " 26 ")]);
        let env = |k: &str| env.get(k).cloned();
        assert_eq!(zstd_level_from(i32::MIN, &env), 19);
        assert_eq!(zstd_window_log_from(i32::MIN, &env), Some(26));
    }

    #[test]
    fn an_out_of_range_env_is_ignored_not_clamped() {
        for (level, window) in [("0", "9"), ("23", "32")] {
            let env = env_of(&[(ZSTD_LEVEL_ENV, level), (ZSTD_WINDOW_LOG_ENV, window)]);
            let env = |k: &str| env.get(k).cloned();
            assert_eq!(zstd_level_from(i32::MIN, &env), 3, "level {level}");
            assert_eq!(
                zstd_window_log_from(i32::MIN, &env),
                None,
                "window {window}"
            );
        }
    }

    #[test]
    fn env_range_edges_are_live() {
        for (level, window) in [("1", "10"), ("22", "31")] {
            let env = env_of(&[(ZSTD_LEVEL_ENV, level), (ZSTD_WINDOW_LOG_ENV, window)]);
            let env = |k: &str| env.get(k).cloned();
            assert_eq!(
                zstd_level_from(i32::MIN, &env),
                level.parse::<i32>().unwrap()
            );
            assert_eq!(
                zstd_window_log_from(i32::MIN, &env),
                Some(window.parse::<u32>().unwrap())
            );
        }
    }

    /// The decoder must accept the large-window frames [`set_compression`] can
    /// produce. Streaming compression never pledges a source size, so libzstd
    /// skips its window clamp and even a KiB-scale frame declares the full 2^31
    /// window in its header — a default decoder (limit 2^27, 128 MiB) refuses
    /// it with "Frame requires too much memory", so no large payload is needed
    /// to prove [`ZSTD_DECODER_WINDOW_LOG_MAX`] load-bearing. (nextest runs one
    /// process per test, so the process-global override set here cannot leak.)
    #[tokio::test]
    async fn a_window_log_31_upload_round_trips_through_the_raised_decoder() {
        set_compression(Some(19), Some(31));
        let (base, _store) = storage_server().await;
        let src = tempfile::tempdir().unwrap();
        let src_file = src.path().join("payload.bin");
        // Non-trivial bytes so the frame carries real compressed blocks.
        let payload: Vec<u8> = (0..8192u32).map(|i| (i * 31 % 251) as u8).collect();
        std::fs::write(&src_file, &payload).unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/payload.tzst.000")];
        let report = upload_artifact(&http, &src_file, false, &urls, "payload")
            .await
            .unwrap();

        let dst = tempfile::tempdir().unwrap();
        let dst_file = dst.path().join("payload.out");
        download_artifact(&http, &urls, &dst_file, false, Some(&report.sha256))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dst_file).unwrap(), payload);
    }

    // A storage server whose PUT and GET for a chosen key fail the first `flaky` times
    // (503), then succeed — exercises the per-part upload AND download retry loops.
    async fn flaky_storage_server(
        fail_first: usize,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
    ) {
        use axum::body::Bytes;
        use axum::extract::{Path as AxPath, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use axum::routing::put;

        type Store = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>;
        #[derive(Clone)]
        struct AppState {
            store: Store,
            put_fails: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            get_fails: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }
        let store: Store =
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let state = AppState {
            store: store.clone(),
            put_fails: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(fail_first)),
            get_fails: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(fail_first)),
        };
        use std::sync::atomic::Ordering;
        let app = axum::Router::new()
            .route(
                "/{*key}",
                put(
                    |AxPath(key): AxPath<String>, State(s): State<AppState>, body: Bytes| async move {
                        if s.put_fails.load(Ordering::SeqCst) > 0 {
                            s.put_fails.fetch_sub(1, Ordering::SeqCst);
                            return StatusCode::SERVICE_UNAVAILABLE;
                        }
                        s.store.lock().unwrap().insert(key, body.to_vec());
                        StatusCode::OK
                    },
                )
                .get(
                    |AxPath(key): AxPath<String>, State(s): State<AppState>| async move {
                        if s.get_fails.load(Ordering::SeqCst) > 0 {
                            s.get_fails.fetch_sub(1, Ordering::SeqCst);
                            return StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        match s.store.lock().unwrap().get(&key) {
                            Some(v) => (StatusCode::OK, v.clone()).into_response(),
                            None => StatusCode::NOT_FOUND.into_response(),
                        }
                    },
                ),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, store)
    }

    #[tokio::test]
    async fn upload_and_download_retry_transient_failures() {
        // Fail the first two PUTs and the first two GETs with 503; the retry loop
        // (MAX_TRANSFER_ATTEMPTS = 4) must still complete the round trip.
        let (base, _store) = flaky_storage_server(2).await;
        let src = tempfile::tempdir().unwrap();
        let src_file = src.path().join("m.bin");
        std::fs::write(&src_file, vec![9u8; 4096]).unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/m.tzst.000")];
        let report = upload_artifact(&http, &src_file, false, &urls, "m")
            .await
            .expect("upload should survive 2 transient PUT failures");

        let dst = tempfile::tempdir().unwrap();
        let dst_file = dst.path().join("out.bin");
        download_artifact(&http, &urls, &dst_file, false, Some(&report.sha256))
            .await
            .expect("download should survive 2 transient GET failures");
        assert_eq!(std::fs::read(&dst_file).unwrap(), vec![9u8; 4096]);
    }

    #[tokio::test]
    async fn download_rejects_corrupt_reassembly() {
        // A tampered object must fail the sha256 gate instead of extracting garbage.
        let (base, store) = storage_server().await;
        let src = tempfile::tempdir().unwrap();
        let src_file = src.path().join("m.bin");
        std::fs::write(&src_file, vec![1u8; 2048]).unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/m.tzst.000")];
        let report = upload_artifact(&http, &src_file, false, &urls, "m")
            .await
            .unwrap();

        // Corrupt the stored bytes after upload (simulates a torn / mixed-generation part).
        store
            .lock()
            .unwrap()
            .get_mut("m.tzst.000")
            .unwrap()
            .push(0xff);

        let dst = tempfile::tempdir().unwrap();
        let dst_file = dst.path().join("out.bin");
        let err = download_artifact(&http, &urls, &dst_file, false, Some(&report.sha256))
            .await
            .expect_err("corrupt reassembly must be rejected");
        assert!(
            err.to_string().contains("integrity check failed"),
            "unexpected error: {err:#}"
        );
        assert!(!dst_file.exists(), "corrupt data must not be extracted");
    }

    #[tokio::test]
    async fn upload_archive_bundles_multiple_relative_paths() {
        // A fan-out glob output: several files under a base archived into one artifact,
        // preserving their paths relative to the base on extract.
        let (base, _store) = storage_server().await;
        let work = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(work.path().join("out")).unwrap();
        std::fs::write(work.path().join("out").join("a.bin"), b"aaa").unwrap();
        std::fs::write(work.path().join("out").join("b.bin"), b"bbbb").unwrap();
        std::fs::write(work.path().join("top.txt"), b"top").unwrap();

        let http = reqwest::Client::new();
        let urls = vec![format!("{base}/bundle.tzst.000")];
        let rels = vec![
            "out/a.bin".to_string(),
            "out/b.bin".to_string(),
            "top.txt".to_string(),
        ];
        let report = upload_archive(&http, work.path(), &rels, &urls, "bundle")
            .await
            .unwrap();

        let dst = tempfile::tempdir().unwrap();
        download_artifact(&http, &urls, dst.path(), true, Some(&report.sha256))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(dst.path().join("out").join("a.bin")).unwrap(),
            b"aaa"
        );
        assert_eq!(
            std::fs::read(dst.path().join("out").join("b.bin")).unwrap(),
            b"bbbb"
        );
        assert_eq!(std::fs::read(dst.path().join("top.txt")).unwrap(), b"top");
    }
}
