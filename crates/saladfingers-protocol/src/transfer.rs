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

/// Connect timeout for a transfer request: a node that cannot open the TCP connection
/// this quickly is treated as dead (retried, then the run reallocates).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Read-stall timeout. reqwest resets this on every successful read, so it fires only when
/// a connection delivers no bytes for this long — a genuinely stalled node, never a
/// slow-but-alive one. Deliberately NOT a total-request deadline: a healthy multi-GB part
/// must be allowed to take exactly as long as its honest bandwidth needs.
const READ_STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// TCP keepalive so a peer that dies mid-upload — the write side, which `read_timeout` does
/// not cover — is noticed in tens of seconds, not the kernel's ~15 min default.
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Per-part transfer attempts before giving up. A single transient blip (5xx, dropped
/// connection, read stall) must not torch a whole multi-part artifact.
const MAX_TRANSFER_ATTEMPTS: u32 = 4;

/// An HTTP client tuned for large artifact transfers.
///
/// It detects a dead or stalled node aggressively — connect timeout, read-stall timeout,
/// TCP keepalive — but imposes **no total-request deadline**. A total `.timeout()` caps
/// sustained throughput at `part_size / timeout`: the bug this replaces pinned a 4 GiB
/// part at ~115 Mbps regardless of the node, so any checkpoint or output that could not
/// move inside 300 s could never upload at all. The transfer engine must never use one.
///
/// # Errors
/// Returns an error if the TLS backend fails to initialize.
pub fn transfer_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_STALL_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .build()
}

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
    // reassembly would extract silently (checkpoints) or hand the caller garbage.
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
    let mut encoder = zstd::Encoder::new(writer, 3).context("creating zstd encoder")?;
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
    let mut encoder = zstd::Encoder::new(writer, 3).context("creating zstd encoder")?;
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
    let decoder = zstd::Decoder::new(file).context("creating zstd decoder")?;
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
