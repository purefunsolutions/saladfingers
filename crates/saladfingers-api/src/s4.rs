// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Client for S4 (Salad Simple Storage Service).
//!
//! S4 accepts either the `Salad-Api-Key` header (CLI side) or an IMDS workload-JWT
//! bearer token (agent side) — so a container can write control-plane envelopes
//! without ever holding the Salad API key. Files are capped at 100 MB and expire
//! after 30 days: control envelopes and small results only, never weights.

use std::time::Duration;

use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;

use crate::error::{ApiError, classify_error};
use crate::secret::Secret;

/// Default S4 base URL.
pub const DEFAULT_S4_BASE_URL: &str = "https://storage-api.salad.com";

/// How S4 requests authenticate.
#[derive(Debug, Clone)]
pub enum S4Auth {
    /// The Salad API key (CLI side).
    ApiKey(Secret),
    /// An IMDS workload-identity JWT (agent side).
    Bearer(Secret),
}

/// A client for S4 storage.
pub struct S4Client {
    http: reqwest::Client,
    base_url: String,
    organization: String,
    auth: S4Auth,
}

#[derive(Deserialize)]
struct UrlResponse {
    url: Option<String>,
}

impl S4Client {
    /// Build an S4 client.
    ///
    /// # Errors
    /// Returns [`ApiError::Network`] if the HTTP client cannot be built.
    pub fn new(
        base_url: impl Into<String>,
        organization: impl Into<String>,
        auth: S4Auth,
    ) -> Result<Self, ApiError> {
        // Connect + read-stall timeouts, but no total-request deadline: S4 objects run
        // up to 100 MB and a total cap would bound throughput, while a client with no
        // timeouts at all hangs forever on a dead peer.
        let http = reqwest::Client::builder()
            .user_agent(concat!("saladfingers/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            organization: organization.into(),
            auth,
        })
    }

    /// Convenience constructor against production S4.
    ///
    /// # Errors
    /// Returns [`ApiError::Network`] if the HTTP client cannot be built.
    pub fn production(organization: impl Into<String>, auth: S4Auth) -> Result<Self, ApiError> {
        Self::new(DEFAULT_S4_BASE_URL, organization, auth)
    }

    fn auth_header(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            S4Auth::ApiKey(key) => rb.header("Salad-Api-Key", key.expose()),
            S4Auth::Bearer(jwt) => rb.bearer_auth(jwt.expose()),
        }
    }

    fn file_url(&self, name: &str) -> String {
        format!(
            "{}/organizations/{}/files/{name}",
            self.base_url, self.organization
        )
    }

    fn token_url(&self, name: &str) -> String {
        format!(
            "{}/organizations/{}/file_tokens/{name}",
            self.base_url, self.organization
        )
    }

    /// Upload a small file (≤ 100 MB).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn upload(&self, name: &str, body: Bytes, mime: &str) -> Result<(), ApiError> {
        let url = self.file_url(name);
        let part = Part::bytes(body.to_vec())
            .file_name(name.rsplit('/').next().unwrap_or(name).to_string())
            .mime_str(mime)?;
        let form = Form::new()
            .part("file", part)
            .text("mimeType", mime.to_string());
        let resp = self
            .auth_header(self.http.put(&url))
            .multipart(form)
            .send()
            .await?;
        Self::expect_success(resp, &url).await
    }

    /// Download a file's bytes.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure (including 404).
    pub async fn download(&self, name: &str) -> Result<Bytes, ApiError> {
        let url = self.file_url(name);
        let resp = self.auth_header(self.http.get(&url)).send().await?;
        let status = resp.status();
        if status.is_success() {
            Ok(resp.bytes().await?)
        } else {
            let ct = content_type(&resp);
            let body = resp.text().await?;
            Err(classify_error(status, &ct, &body, None, &url))
        }
    }

    /// Delete a file. A missing file is treated as success.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure (other than 404).
    pub async fn delete(&self, name: &str) -> Result<(), ApiError> {
        let url = self.file_url(name);
        let resp = self.auth_header(self.http.delete(&url)).send().await?;
        match Self::expect_success(resp, &url).await {
            Err(e) if e.is_not_found() => Ok(()),
            other => other,
        }
    }

    /// Mint a presigned GET URL for a file, valid for `expires_secs`.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure, or if no URL is returned.
    pub async fn sign_get(&self, name: &str, expires_secs: u32) -> Result<String, ApiError> {
        let url = self.token_url(name);
        let body = serde_json::json!({ "method": "GET", "exp": expires_secs.to_string() });
        let resp = self
            .auth_header(self.http.post(&url))
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let ct = content_type(&resp);
        let text = resp.text().await?;
        if status.is_success() {
            let parsed: UrlResponse =
                serde_json::from_str(&text).map_err(|source| ApiError::Decode {
                    context: "s4_sign_get",
                    source,
                    snippet: crate::error::snippet(&text),
                })?;
            parsed.url.ok_or_else(|| ApiError::Problem {
                status: status.as_u16(),
                r#type: None,
                title: "S4 returned no signed URL".to_string(),
                detail: None,
                instance: None,
            })
        } else {
            Err(classify_error(status, &ct, &text, None, &url))
        }
    }

    async fn expect_success(resp: reqwest::Response, path: &str) -> Result<(), ApiError> {
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let ct = content_type(&resp);
            let body = resp.text().await?;
            Err(classify_error(status, &ct, &body, None, path))
        }
    }
}

fn content_type(resp: &reqwest::Response) -> String {
    resp.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}
