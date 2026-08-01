// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Presigned URLs for S3-compatible object storage.
//!
//! Backend-agnostic: works with Cloudflare R2, Backblaze B2, MinIO, Garage, or any
//! S3-compatible endpoint — this project does not target AWS S3 specifically. The
//! endpoint, region, bucket, and addressing style come from config; credentials come
//! from the environment variables the config names.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use url::Url;

use crate::config::StorageConfig;

/// A presigning handle for one S3-compatible bucket.
pub struct S3Backend {
    bucket: Bucket,
    credentials: Credentials,
}

impl S3Backend {
    /// Build from storage config, reading credentials from the referenced env vars.
    ///
    /// # Errors
    /// Returns an error if the endpoint is invalid or credentials are missing.
    pub fn from_config(storage: &StorageConfig) -> Result<Self> {
        let access = env_var(storage.access_key_env.as_deref(), "access_key_env")?;
        let secret = env_var(storage.secret_key_env.as_deref(), "secret_key_env")?;
        Self::new(
            &storage.endpoint,
            storage.region.as_deref().unwrap_or("auto"),
            &storage.bucket,
            storage.path_style,
            &access,
            &secret,
        )
    }

    /// Build a backend directly.
    ///
    /// # Errors
    /// Returns an error if the endpoint URL is invalid or the bucket cannot be built.
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        path_style: bool,
        access: &str,
        secret: &str,
    ) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .with_context(|| format!("invalid storage endpoint '{endpoint}'"))?;
        let style = if path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let bucket = Bucket::new(endpoint, style, bucket.to_string(), region.to_string())
            .context("building S3 bucket")?;
        Ok(Self {
            bucket,
            credentials: Credentials::new(access, secret),
        })
    }

    /// Presign a GET URL for `key`, valid for `expires`.
    #[must_use]
    pub fn presign_get(&self, key: &str, expires: Duration) -> String {
        self.bucket
            .get_object(Some(&self.credentials), key)
            .sign(expires)
            .to_string()
    }

    /// Presign a PUT URL for `key`, valid for `expires`.
    #[must_use]
    pub fn presign_put(&self, key: &str, expires: Duration) -> String {
        self.bucket
            .put_object(Some(&self.credentials), key)
            .sign(expires)
            .to_string()
    }

    /// Presign a DELETE URL for `key`, valid for `expires`.
    ///
    /// Handed to the agent so it can reclaim a superseded checkpoint slot. The agent
    /// holds no credentials by design, and `delete_prefix` below needs them (it lists
    /// the bucket first), so slot reclamation has to travel as a presigned URL like
    /// every other storage operation the agent performs.
    #[must_use]
    pub fn presign_delete(&self, key: &str, expires: Duration) -> String {
        self.bucket
            .delete_object(Some(&self.credentials), key)
            .sign(expires)
            .to_string()
    }

    /// List every object key under `prefix`, following continuation tokens.
    ///
    /// # Errors
    /// Returns an error if a list request fails.
    pub async fn list_keys(&self, http: &reqwest::Client, prefix: &str) -> Result<Vec<String>> {
        let expires = Duration::from_secs(300);
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut list = self.bucket.list_objects_v2(Some(&self.credentials));
            list.query_mut().insert("prefix", prefix.to_string());
            if let Some(t) = &token {
                list.query_mut().insert("continuation-token", t.clone());
            }
            let url = list.sign(expires);
            // `without_url`: the list URL is signed — its `X-Amz-Signature` is a live
            // capability over this bucket and must not reach error text. `.context` does
            // not help: the reqwest source still carries the URL, and anyhow prints the
            // whole chain under `{:#}` / `Debug` (which is what `main` renders).
            //
            // CONTROL_TIMEOUT: a list response is a bounded control document, and without
            // a deadline an endpoint that accepts the connection and never answers hangs
            // `gc` and `checkpoint rm` forever, after their confirmation prompts.
            let body = http
                .get(url)
                .timeout(saladfingers_protocol::transfer::CONTROL_TIMEOUT)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
                .context("listing objects")?
                .error_for_status()
                .map_err(reqwest::Error::without_url)
                .context("list objects status")?
                .text()
                .await
                .map_err(reqwest::Error::without_url)?;
            let parsed = rusty_s3::actions::ListObjectsV2::parse_response(&body)
                .context("parsing list-objects response")?;
            keys.extend(parsed.contents.iter().map(|obj| obj.key.clone()));
            match parsed.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(keys)
    }

    /// Delete every object under `prefix`, returning `(deleted, failed)` counts.
    ///
    /// The failed count is the caller's to judge: `gc` treats leftovers as storage waste
    /// to report and move past, while `checkpoint rm` treats them as the job not done —
    /// a prefix reported clean while its parts survived would be resumed from, silently.
    ///
    /// # Errors
    /// Returns an error if listing the bucket fails.
    pub async fn delete_prefix(
        &self,
        http: &reqwest::Client,
        prefix: &str,
    ) -> Result<(usize, usize)> {
        let expires = Duration::from_secs(300);
        let keys = self.list_keys(http, prefix).await?;
        let mut deleted = 0usize;
        let mut failed = 0usize;
        for key in &keys {
            let del = self
                .bucket
                .delete_object(Some(&self.credentials), key)
                .sign(expires);
            // Timeout for the same reason as the list: one stalled DELETE must not wedge
            // the whole sweep.
            let ok = matches!(
                http.delete(del)
                    .timeout(saladfingers_protocol::transfer::CONTROL_TIMEOUT)
                    .send()
                    .await,
                Ok(r) if r.status().is_success()
            );
            if ok {
                deleted += 1;
            } else {
                failed += 1;
            }
        }
        Ok((deleted, failed))
    }
}

fn env_var(name: Option<&str>, field: &str) -> Result<String> {
    let name = name.with_context(|| format!("storage {field} is not set in config"))?;
    let value = std::env::var(name)
        .with_context(|| format!("storage credential env var ${name} is not set"))?;
    if value.trim().is_empty() {
        bail!("storage credential env var ${name} is empty");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    #[test]
    fn presigned_urls_are_well_formed() {
        let backend = S3Backend::new(
            "https://acct.r2.cloudflarestorage.com",
            "auto",
            "my-bucket",
            true,
            "AKID",
            "SECRET",
        )
        .unwrap();
        let get = backend.presign_get("runs/sf-x/job.json", Duration::from_secs(3600));
        assert!(get.contains("my-bucket"), "{get}");
        assert!(get.contains("runs/sf-x/job.json"), "{get}");
        assert!(get.contains("X-Amz-Signature"), "{get}");

        let put = backend.presign_put("runs/sf-x/out/model.tzst.000", Duration::from_secs(3600));
        assert!(put.contains("X-Amz-Signature"), "{put}");
    }

    /// Presign a PUT, presign a GET for the same key, and byte-compare the round-tripped
    /// body. Proves the SaladCloud data path works with no credentials on the wire (the
    /// whole point of presigning).
    async fn round_trip(backend: &S3Backend) {
        let key = "smoke/presign-round-trip.txt";
        let body = b"saladfingers presign round-trip".as_slice();
        let http = reqwest::Client::new();

        let put = backend.presign_put(key, Duration::from_secs(300));
        let resp = http.put(&put).body(body.to_vec()).send().await.unwrap();
        assert!(resp.status().is_success(), "PUT failed: {}", resp.status());

        let get = backend.presign_get(key, Duration::from_secs(300));
        let got = http.get(&get).send().await.unwrap();
        assert!(got.status().is_success(), "GET failed: {}", got.status());
        assert_eq!(
            got.bytes().await.unwrap().as_ref(),
            body,
            "round-trip mismatch"
        );

        // The DELETE leg is what reclaims a superseded checkpoint slot on a node that
        // holds no credentials. Only a real S3 endpoint can tell that URL apart from a
        // GET's: signing the wrong verb still yields a plausible URL with a signature in
        // it, and the only symptom in production would be every reclaim 403ing into a
        // warn on a node whose logs nobody reads — retention silently becoming forever.
        let del = backend.presign_delete(key, Duration::from_secs(300));
        let resp = http.delete(&del).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "DELETE failed: {}",
            resp.status()
        );
        let gone = http.get(&get).send().await.unwrap();
        assert_eq!(
            gone.status(),
            reqwest::StatusCode::NOT_FOUND,
            "object survived its presigned DELETE"
        );

        // The same three verbs, minted by `build_job_spec` itself. Nothing offline can
        // tell which verb a URL was signed for — the method lives in the signature, not
        // the query — so a copy-paste slip that fills `delete_urls` from `presign_get`
        // yields URLs every string assertion accepts and every DELETE 403s, silently.
        // Only a real endpoint refuses the wrong verb, so each list is exercised here.
        let spec = crate::spec::build_job_spec(crate::spec::SpecParams {
            backend,
            run_id: "sf-verbs1",
            shard_index: 0,
            shard_count: 1,
            command: vec!["true".into()],
            env: std::collections::BTreeMap::new(),
            inputs: &[],
            outputs: &[],
            max_parts: 1,
            max_duration_secs: None,
            stop_signal: None,
            gate: None,
            checkpoint: Some(crate::spec::CheckpointParams {
                dir: "ckpt".into(),
                interval_secs: 30,
                quiesce_secs: 15,
                prefix: None,
            }),
            expiry: Duration::from_secs(300),
        });
        let ckpt = spec.checkpoint.expect("checkpoint spec");
        let slot = &ckpt.slots[0];
        let put = http
            .put(&slot.put_urls[0])
            .body(b"verb-check".to_vec())
            .send()
            .await
            .unwrap();
        assert!(put.status().is_success(), "spec put_urls: {}", put.status());
        let got = http.get(&slot.get_urls[0]).send().await.unwrap();
        assert!(got.status().is_success(), "spec get_urls: {}", got.status());
        let del = http.delete(&slot.delete_urls[0]).send().await.unwrap();
        assert!(
            del.status().is_success(),
            "spec delete_urls signed for the wrong verb: {}",
            del.status()
        );
        let meta_put = http
            .put(&ckpt.meta_put_url)
            .body(b"{}".to_vec())
            .send()
            .await
            .unwrap();
        assert!(meta_put.status().is_success(), "spec meta_put_url");
        let meta_got = http.get(&ckpt.meta_get_url).send().await.unwrap();
        assert!(meta_got.status().is_success(), "spec meta_get_url");
        // `delete_prefix` is a bulk delete driven by a LIST, so its blast radius depends on
        // how the endpoint matches the prefix string — which only a real one settles.
        // `checkpoint rm --prefix foo` must not take `foobar` with it, and `gc` on one run
        // must not reap the next run whose id starts the same way.
        // Composed from the same helper `checkpoint rm` uses, so a change there is what
        // this catches — a literal here would keep passing while the command drifted.
        let target = format!("smoke/{}", crate::spec::checkpoint_prefix_root("foo"));
        let neighbour = format!("smoke/{}a", crate::spec::checkpoint_prefix_root("foobar"));
        for key in [
            format!("{target}a"),
            format!("{target}b"),
            neighbour.clone(),
        ] {
            let put = backend.presign_put(&key, Duration::from_secs(300));
            let resp = http.put(&put).body(b"x".to_vec()).send().await.unwrap();
            assert!(resp.status().is_success(), "seeding {key}");
        }
        let (removed, failed) = backend.delete_prefix(&http, &target).await.unwrap();
        assert_eq!(
            (removed, failed),
            (2, 0),
            "should have taken exactly foo's two objects"
        );
        let survivor = backend.presign_get(&neighbour, Duration::from_secs(300));
        let resp = http.get(&survivor).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "a neighbouring prefix was deleted too: {}",
            resp.status()
        );
        // Leave the store as this leg found it: `round_trip` is written to run against
        // any backend, and a persistent one would count the leaked neighbour next time.
        let cleanup = backend.presign_delete(&neighbour, Duration::from_secs(300));
        assert!(
            http.delete(&cleanup)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }

    // --- ephemeral local Garage -------------------------------------------------

    /// Fixed local-only secrets for the ephemeral instance. The server is 127.0.0.1-bound
    /// and lives only for the test, so these need not be secret — they just must not be
    /// empty. `rpc_secret` must be 32 bytes (64 hex chars) — Garage validates the length.
    const RPC_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ADMIN_TOKEN: &str = "sf-test-admin-token-0123456789abcdef";

    /// A running ephemeral Garage instance. The `_`-prefixed fields are guards, owned
    /// only for their `Drop`: `_server` is kill_on_drop so the garage process dies when
    /// the test ends, and `_dir` (declared after, so it drops after the kill) then
    /// deletes the config/metadata/data dirs. The endpoint + credentials come back as
    /// plain fields — deliberately not via `SALADFINGERS_S3_*` env vars, which would
    /// need `unsafe` to set and would shadow whatever the developer's shell has sourced.
    struct GarageInstance {
        _server: tokio::process::Child,
        _dir: TempDir,
        endpoint: String,
        bucket: String,
        access_key: String,
        secret_key: String,
    }

    /// Reserve a free loopback port by binding `127.0.0.1:0` and dropping the listener —
    /// the same trick `free_agent_port()` in the agent proxy tests uses. Best-effort: the
    /// port is free at the moment of binding, not guaranteed to stay free.
    fn free_loopback_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    /// Resolve the garage binary path: `SALADFINGERS_GARAGE_BIN` first (set by Nix), else
    /// `"garage"` from `PATH`. Returns `None` if not found so the test can self-skip.
    fn garage_bin() -> Option<String> {
        if let Ok(p) = std::env::var("SALADFINGERS_GARAGE_BIN")
            && !p.trim().is_empty()
        {
            return Some(p);
        }
        // Probe PATH for a bare `garage`.
        if let Ok(paths) = std::env::var("PATH") {
            for dir in paths.split(':') {
                if std::path::Path::new(dir).join("garage").is_file() {
                    return Some("garage".to_string());
                }
            }
        }
        None
    }

    /// Run a `garage -c <cfg> <args…>` command and return its stdout, failing the test on
    /// a non-zero exit. Stderr is inherited so server-side errors surface in `cargo test`
    /// output.
    async fn garage_cmd(cfg: &str, args: &[&str]) -> String {
        let bin = garage_bin().expect("garage binary resolved before provisioning");
        let mut cmd = tokio::process::Command::new(&bin);
        cmd.arg("-c")
            .arg(cfg)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let output = cmd.output().await.expect("spawning garage CLI");
        assert!(
            output.status.success(),
            "garage {:?} failed with status {}",
            args,
            output.status
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// Boot an ephemeral 127.0.0.1-bound Garage, provision a cluster layout + bucket + key,
    /// and hand back the endpoint + credentials the test needs. Returns `None` (after
    /// printing "skipping:") when the garage binary is unavailable, so a bare `cargo test`
    /// without garage installed passes instead of failing.
    ///
    /// # Panics
    /// Panics on any other failure (port/config/provisioning) — those are real bugs, not
    /// "garage missing", and should fail loudly.
    async fn local_garage_or_skip() -> Option<GarageInstance> {
        let dir = tempfile::tempdir().expect("creating garage tempdir");
        let metadata_dir = dir.path().join("meta");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&metadata_dir).expect("creating metadata dir");
        std::fs::create_dir_all(&data_dir).expect("creating data dir");

        let s3_port = free_loopback_port();
        let rpc_port = free_loopback_port();
        let admin_port = free_loopback_port();
        let config_path = dir.path().join("garage.toml");

        // Bucket name unique per run to dodge collisions with a stale lingering instance.
        // S3 bucket names are DNS labels: lowercase a-z, 0-9, hyphens, and must not start
        // or end with a hyphen/dot. The tempdir name (e.g. `.tmpS3BoCZ`) isn't a valid label
        // on its own, so sanitize it down to `[a-z0-9-]`, strip leading/trailing hyphens,
        // and fall back to `run` if nothing usable remains.
        let suffix: String = dir
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| {
                let s: String = n
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() {
                            c.to_ascii_lowercase()
                        } else {
                            '-'
                        }
                    })
                    .collect();
                let s = s.trim_matches('-').to_string();
                if s.is_empty() { "run".to_string() } else { s }
            })
            .unwrap_or_else(|| "run".to_string());
        let bucket = format!("saladfingers-smoke-{suffix}");

        let toml = format!(
            r#"
metadata_dir = "{metadata_dir}"
data_dir = "{data_dir}"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "{RPC_SECRET}"

[s3_api]
api_bind_addr = "127.0.0.1:{s3_port}"
s3_region = "garage"

[s3_web]
bind_addr = "127.0.0.1:0"
root_domain = ".garage.web"

[admin]
api_bind_addr = "127.0.0.1:{admin_port}"
admin_token = "{ADMIN_TOKEN}"
"#,
            metadata_dir = metadata_dir.display(),
            data_dir = data_dir.display(),
        );
        std::fs::write(&config_path, toml).expect("writing garage.toml");
        let config_path = config_path
            .to_str()
            .expect("config path is utf-8")
            .to_string();

        let bin = match garage_bin() {
            Some(b) => b,
            None => {
                eprintln!("skipping: garage binary not found");
                return None;
            }
        };

        let mut server = tokio::process::Command::new(&bin)
            .arg("-c")
            .arg(&config_path)
            .arg("server")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawning garage server");
        // Take the pipe handles out of the child and drain them in the background so the
        // OS pipe buffers can't fill and stall the server. The `server` Child itself stays
        // owned by `local_garage_or_skip` (and, via the returned struct, the test) so its
        // `kill_on_drop` reaps the process when the test ends.
        let stdout = server.stdout.take().expect("stdout");
        let stderr = server.stderr.take().expect("stderr");
        tokio::spawn(async move {
            let mut stdout = stdout;
            let mut stderr = stderr;
            let mut buf_out = [0u8; 4096];
            let mut buf_err = [0u8; 4096];
            loop {
                tokio::select! {
                    r = stdout.read(&mut buf_out) => match r { Ok(0) | Err(_) => break, _ => {} },
                    r = stderr.read(&mut buf_err) => match r { Ok(0) | Err(_) => break, _ => {} },
                }
            }
        });

        // Poll the S3 API port until Garage accepts connections (~15s budget).
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", s3_port))
                .await
                .is_ok()
            {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("garage S3 port did not come up on 127.0.0.1:{s3_port}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Provision the cluster layout, then the bucket + key. `garage server` (the
        // nixpkgs 1.x version has no `--single-node` flag) starts unconfigured; without an
        // applied layout every admin RPC fails with "could not reach quorum", so assign
        // this node a role and apply it before creating anything. Newer garage versions
        // that accept `--single-node` would fold these steps in; this path is a superset
        // that works across both.
        let node_id = garage_cmd(&config_path, &["node", "id"])
            .await
            .lines()
            .next()
            .and_then(|l| l.split('@').next())
            .map(str::to_string)
            .unwrap_or_else(|| panic!("garage node id printed no node id"));
        garage_cmd(
            &config_path,
            &["layout", "assign", "-c", "1G", "-z", "dc1", &node_id],
        )
        .await;
        garage_cmd(&config_path, &["layout", "apply", "--version", "1"]).await;

        garage_cmd(&config_path, &["bucket", "create", &bucket]).await;
        let key_out = garage_cmd(&config_path, &["key", "create", "sf-test-key"]).await;
        let access_key = parse_field(&key_out, "key id").expect("parsing garage access key");
        let secret_key = parse_field(&key_out, "secret key").expect("parsing garage secret key");
        garage_cmd(
            &config_path,
            &[
                "bucket",
                "allow",
                "--read",
                "--write",
                &bucket,
                "--key",
                "sf-test-key",
            ],
        )
        .await;

        Some(GarageInstance {
            _server: server,
            _dir: dir,
            endpoint: format!("http://127.0.0.1:{s3_port}"),
            bucket,
            access_key,
            secret_key,
        })
    }

    /// Parse a field from `garage key create` output. The CLI prints lines like
    /// `Key ID: GK...` and `Secret key: ...` (label casing/spacing varies across garage
    /// versions), so match case-insensitively on the label, then take the colon-delimited
    /// value.
    fn parse_field(out: &str, label: &str) -> Option<String> {
        let label = label.to_ascii_lowercase();
        for line in out.lines() {
            let line = line.trim();
            let lower = line.to_ascii_lowercase();
            if lower.strip_prefix(&label).is_some() {
                // The lowercase `lower` lines up with `line` at the same offsets; find
                // the first ':' after the label in the original and take what follows.
                let after_label = &line[label.len()..];
                if let Some(idx) = after_label.find(':') {
                    let v = after_label[idx + ':'.len_utf8()..].trim();
                    if !v.is_empty() {
                        return Some(v.to_string());
                    }
                }
            }
        }
        None
    }

    /// Presign round-trip against an **ephemeral local Garage** booted inside the test —
    /// no real backend, no real credentials, no `~/.config/saladfingers/env.sh`
    /// dependency, keeping the "no deployment details in the repo" property. Self-skips
    /// (prints "skipping:") when the garage binary is unavailable, so a bare `cargo test`
    /// without garage still passes.
    #[tokio::test]
    async fn presign_round_trip() {
        let Some(garage) = local_garage_or_skip().await else {
            return;
        };
        let backend = S3Backend::new(
            &garage.endpoint,
            "garage",
            &garage.bucket,
            true,
            &garage.access_key,
            &garage.secret_key,
        )
        .unwrap();
        round_trip(&backend).await;
    }
}
