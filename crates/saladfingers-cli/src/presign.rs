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

    /// Delete every object under `prefix`, returning the count deleted. Used by `gc` to
    /// reap a finished run's remote artifacts. Best-effort: individual delete failures
    /// are skipped, not fatal.
    ///
    /// # Errors
    /// Returns an error if listing the bucket fails.
    pub async fn delete_prefix(&self, http: &reqwest::Client, prefix: &str) -> Result<usize> {
        let expires = Duration::from_secs(300);
        let mut deleted = 0usize;
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
            let body = http
                .get(url)
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
            for obj in &parsed.contents {
                let del = self
                    .bucket
                    .delete_object(Some(&self.credentials), &obj.key)
                    .sign(expires);
                if matches!(http.delete(del).send().await, Ok(r) if r.status().is_success()) {
                    deleted += 1;
                }
            }
            match parsed.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(deleted)
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

    /// Live smoke: presign a PUT + GET against a real S3-compatible endpoint and
    /// round-trip a small object with no credentials on the wire (the SaladCloud
    /// data path). Endpoint/bucket/region and creds all come from the environment,
    /// so no deployment details live in the repo. Gated on `SALADFINGERS_S3_ENDPOINT`.
    #[tokio::test]
    #[ignore = "live storage round-trip; set SALADFINGERS_S3_* and run --ignored"]
    async fn presign_round_trip_live() {
        let Ok(endpoint) = std::env::var("SALADFINGERS_S3_ENDPOINT") else {
            eprintln!("skipping: SALADFINGERS_S3_ENDPOINT unset");
            return;
        };
        let var = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} must be set"));
        let backend = S3Backend::new(
            &endpoint,
            &std::env::var("SALADFINGERS_S3_REGION").unwrap_or_else(|_| "auto".into()),
            &var("SALADFINGERS_S3_BUCKET"),
            true,
            &var("SALADFINGERS_S3_ACCESS_KEY"),
            &var("SALADFINGERS_S3_SECRET_KEY"),
        )
        .unwrap();

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
    }
}
