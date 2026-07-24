// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Instance Metadata Service client (`http://169.254.169.254`).
//!
//! Every request carries the required `Metadata: true` header and bypasses any
//! proxy. The base URL is overridable via `SF_IMDS_BASE` for local testing.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_BASE: &str = "http://169.254.169.254";

/// A client for the SaladCloud Instance Metadata Service.
pub struct ImdsClient {
    http: reqwest::Client,
    base: String,
}

/// `GET /v1/status` response.
#[derive(Debug, Clone, Deserialize)]
pub struct ImdsStatus {
    /// Whether the instance is ready.
    #[serde(default)]
    pub ready: bool,
    /// Whether the instance has started.
    #[serde(default)]
    pub started: bool,
}

#[derive(Deserialize)]
struct TokenResponse {
    jwt: String,
}

impl ImdsClient {
    /// Build an IMDS client.
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be built.
    pub fn new() -> Result<Self> {
        let base = std::env::var("SF_IMDS_BASE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_BASE.to_string());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .no_proxy()
            .build()
            .context("building IMDS client")?;
        Ok(Self { http, base })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// `GET /v1/status`.
    ///
    /// # Errors
    /// Returns an error on transport or non-2xx response.
    pub async fn status(&self) -> Result<ImdsStatus> {
        let resp = self
            .http
            .get(self.url("/v1/status"))
            .header("Metadata", "true")
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    /// `GET /v1/token` — the workload-identity JWT (accepted by S4 as auth).
    ///
    /// # Errors
    /// Returns an error on transport or non-2xx response.
    pub async fn token(&self) -> Result<String> {
        let resp = self
            .http
            .get(self.url("/v1/token"))
            .header("Metadata", "true")
            .send()
            .await?
            .error_for_status()?;
        let parsed: TokenResponse = resp.json().await?;
        Ok(parsed.jwt)
    }

    /// `POST /v1/reallocate` — move this instance to a different node.
    ///
    /// # Errors
    /// Returns an error on transport or non-2xx response.
    pub async fn reallocate(&self, reason: &str) -> Result<()> {
        self.post("/v1/reallocate", &serde_json::json!({ "reason": reason }))
            .await
    }

    /// `POST /v1/recreate` — recreate the container on the same node.
    ///
    /// # Errors
    /// Returns an error on transport or non-2xx response.
    pub async fn recreate(&self) -> Result<()> {
        self.post("/v1/recreate", &serde_json::json!({})).await
    }

    /// `POST /v1/restart` — restart the container on the same node.
    ///
    /// # Errors
    /// Returns an error on transport or non-2xx response.
    pub async fn restart(&self) -> Result<()> {
        self.post("/v1/restart", &serde_json::json!({})).await
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        self.http
            .post(self.url(path))
            .header("Metadata", "true")
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
