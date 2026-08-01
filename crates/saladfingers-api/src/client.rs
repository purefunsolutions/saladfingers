// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! The SaladCloud control-plane client: request engine + typed endpoint methods.
//!
//! Requests run behind a token bucket and a retry loop. Idempotent verbs retry on
//! transport failures, 5xx, and 429; `create` retries only when the request never
//! reached the server (connect error) or was rate-limited, and resolves a 409 by
//! adopting the existing group. Responses are classified by status and content type
//! before any JSON decode (SaladCloud errors may be Cloudflare HTML).

use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;

use crate::error::{ApiError, classify_error, snippet};
use crate::models::{
    ContainerGroup, CreateContainerGroup, GpuAvailability, GpuClass, Instance, InstanceList, Items,
    LogEntriesQuery, LogEntry, Quotas, SystemLogEntry, UpdateContainerGroup,
};
use crate::retry::{RetryPolicy, TokenBucket};
use crate::secret::Secret;

/// Default SaladCloud public API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.salad.com/api/public";

/// Default sustained request rate (leaves headroom under the key-wide 240/min).
pub const DEFAULT_RATE_PER_MIN: u32 = 180;

/// Configuration for a [`SaladClient`].
#[derive(Debug, Clone)]
pub struct SaladClientConfig {
    /// API base URL (override for tests).
    pub base_url: String,
    /// The `Salad-Api-Key` value.
    pub api_key: Secret,
    /// Organization name (no list-orgs endpoint; must be known).
    pub organization: String,
    /// Project name.
    pub project: String,
    /// Client-side sustained request rate per minute.
    pub rate_limit_per_min: u32,
    /// Retry/backoff policy.
    pub retry: RetryPolicy,
}

impl SaladClientConfig {
    /// A config against the production API with default pacing and retry.
    #[must_use]
    pub fn new(
        api_key: Secret,
        organization: impl Into<String>,
        project: impl Into<String>,
    ) -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key,
            organization: organization.into(),
            project: project.into(),
            rate_limit_per_min: DEFAULT_RATE_PER_MIN,
            retry: RetryPolicy::default(),
        }
    }

    /// Override the base URL (used by tests to point at a mock server).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }
}

/// How aggressively a request may be retried.
#[derive(Debug, Clone, Copy)]
enum RetryClass {
    /// Safe to retry on transport failure, 5xx, and 429.
    Idempotent,
    /// Retry only when the request never reached the server, or on 429.
    CreateOnce,
}

/// A typed SaladCloud control-plane client.
pub struct SaladClient {
    http: reqwest::Client,
    cfg: SaladClientConfig,
    bucket: TokenBucket,
}

struct RawResponse {
    status: reqwest::StatusCode,
    content_type: String,
    body: String,
    retry_after: Option<Duration>,
}

impl SaladClient {
    /// Build a client from a config.
    ///
    /// # Errors
    /// Returns [`ApiError::Network`] if the underlying HTTP client cannot be built.
    pub fn new(cfg: SaladClientConfig) -> Result<Self, ApiError> {
        let http = crate::http::credentialed_client_builder()
            .timeout(Duration::from_secs(60))
            .user_agent(concat!("saladfingers/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let bucket = TokenBucket::per_minute(cfg.rate_limit_per_min);
        Ok(Self { http, cfg, bucket })
    }

    fn org_base(&self) -> String {
        format!(
            "{}/organizations/{}",
            self.cfg.base_url, self.cfg.organization
        )
    }

    fn project_base(&self) -> String {
        format!("{}/projects/{}", self.org_base(), self.cfg.project)
    }

    async fn attempt(&self, rb: reqwest::RequestBuilder) -> Result<RawResponse, ApiError> {
        self.bucket.acquire().await;
        let resp = rb
            .header("Salad-Api-Key", self.cfg.api_key.expose())
            .send()
            .await?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            // Clamp: the limit is per-minute, so a Retry-After beyond 60 s is never
            // legitimate — an absurd value (misconfigured proxy, HTTP-date parse as
            // seconds) must not put the CLI to sleep for hours looking hung.
            .map(|s| Duration::from_secs(s.min(60)));
        let body = resp.text().await?;
        Ok(RawResponse {
            status,
            content_type,
            body,
            retry_after,
        })
    }

    async fn sleep_backoff(&self, attempt: u32) {
        let jitter = jitter01();
        tokio::time::sleep(self.cfg.retry.delay(attempt, jitter)).await;
    }

    /// Run a request with retries. `make` builds a fresh request each attempt. The
    /// returned [`RawResponse`] may still carry an error status — the caller
    /// classifies it — but 429/5xx have already been retried per `class`.
    async fn run<F>(&self, make: F, class: RetryClass) -> Result<RawResponse, ApiError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let max = self.cfg.retry.max_attempts;
        let mut last_err: Option<ApiError> = None;
        for attempt in 0..max {
            let is_last = attempt + 1 >= max;
            match self.attempt(make()).await {
                Ok(raw) => {
                    if raw.status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        self.bucket.drain();
                        if is_last {
                            return Ok(raw);
                        }
                        let wait = raw.retry_after.unwrap_or(Duration::from_secs(15));
                        last_err = Some(ApiError::RateLimited {
                            retry_after: raw.retry_after,
                        });
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    if raw.status.is_server_error()
                        && matches!(class, RetryClass::Idempotent)
                        && !is_last
                    {
                        self.sleep_backoff(attempt).await;
                        continue;
                    }
                    return Ok(raw);
                }
                Err(err) => {
                    let retryable = match class {
                        RetryClass::Idempotent => err.is_retryable(),
                        RetryClass::CreateOnce => {
                            matches!(&err, ApiError::Network(ne) if ne.is_connect())
                        }
                    };
                    if retryable && !is_last {
                        self.sleep_backoff(attempt).await;
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(ApiError::RetriesExhausted {
            attempts: max,
            last: Box::new(last_err.unwrap_or(ApiError::RateLimited { retry_after: None })),
        })
    }

    async fn send_json<F, T>(
        &self,
        make: F,
        class: RetryClass,
        context: &'static str,
        path: &str,
    ) -> Result<T, ApiError>
    where
        F: Fn() -> reqwest::RequestBuilder,
        T: DeserializeOwned,
    {
        let raw = self.run(make, class).await?;
        if raw.status.is_success() {
            serde_json::from_str(&raw.body).map_err(|source| ApiError::Decode {
                context,
                source,
                snippet: snippet(&raw.body),
            })
        } else {
            Err(classify_error(
                raw.status,
                &raw.content_type,
                &raw.body,
                raw.retry_after,
                path,
            ))
        }
    }

    async fn send_empty<F>(&self, make: F, class: RetryClass, path: &str) -> Result<(), ApiError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let raw = self.run(make, class).await?;
        if raw.status.is_success() {
            Ok(())
        } else {
            Err(classify_error(
                raw.status,
                &raw.content_type,
                &raw.body,
                raw.retry_after,
                path,
            ))
        }
    }

    // ---- container groups -------------------------------------------------

    /// List all container groups in the project.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn list_container_groups(&self) -> Result<Vec<ContainerGroup>, ApiError> {
        let url = format!("{}/containers", self.project_base());
        let items: Items<ContainerGroup> = self
            .send_json(
                || self.http.get(&url),
                RetryClass::Idempotent,
                "list_container_groups",
                &url,
            )
            .await?;
        Ok(items.items)
    }

    /// Create a container group. A 409 conflict is resolved by adopting the existing
    /// group (so a create that raced its own retry still succeeds).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn create_container_group(
        &self,
        req: &CreateContainerGroup,
    ) -> Result<ContainerGroup, ApiError> {
        let url = format!("{}/containers", self.project_base());
        let result = self
            .send_json(
                || self.http.post(&url).json(req),
                RetryClass::CreateOnce,
                "create_container_group",
                &url,
            )
            .await;
        match result {
            // Any-shape 409 (problem JSON or an HTML/undecodable edge body) means the
            // group already exists — adopt it. Matching only problem-JSON would surface
            // a retried create that raced itself as an error.
            Err(ApiError::Problem { status: 409, .. } | ApiError::Html { status: 409, .. }) => {
                self.get_container_group(&req.name).await
            }
            other => other,
        }
    }

    /// Get a container group by name.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn get_container_group(&self, name: &str) -> Result<ContainerGroup, ApiError> {
        let url = format!("{}/containers/{name}", self.project_base());
        self.send_json(
            || self.http.get(&url),
            RetryClass::Idempotent,
            "get_container_group",
            &url,
        )
        .await
    }

    /// Patch a container group (v1 uses this only to change `replicas`).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn update_container_group(
        &self,
        name: &str,
        patch: &UpdateContainerGroup,
    ) -> Result<ContainerGroup, ApiError> {
        let url = format!("{}/containers/{name}", self.project_base());
        self.send_json(
            || self.http.patch(&url).json(patch),
            RetryClass::Idempotent,
            "update_container_group",
            &url,
        )
        .await
    }

    /// Delete a container group. A missing group is treated as success.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure (other than 404).
    pub async fn delete_container_group(&self, name: &str) -> Result<(), ApiError> {
        let url = format!("{}/containers/{name}", self.project_base());
        match self
            .send_empty(|| self.http.delete(&url), RetryClass::Idempotent, &url)
            .await
        {
            Err(e) if e.is_not_found() => Ok(()),
            other => other,
        }
    }

    /// Start a stopped container group.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn start_container_group(&self, name: &str) -> Result<(), ApiError> {
        let url = format!("{}/containers/{name}/start", self.project_base());
        self.send_empty(|| self.http.post(&url), RetryClass::Idempotent, &url)
            .await
    }

    /// Stop a running container group (billing ends; nodes are released).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn stop_container_group(&self, name: &str) -> Result<(), ApiError> {
        let url = format!("{}/containers/{name}/stop", self.project_base());
        self.send_empty(|| self.http.post(&url), RetryClass::Idempotent, &url)
            .await
    }

    /// Fetch a container group's system logs.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn get_system_logs(&self, group: &str) -> Result<Vec<SystemLogEntry>, ApiError> {
        let url = format!("{}/containers/{group}/system-logs", self.project_base());
        let items: Items<SystemLogEntry> = self
            .send_json(
                || self.http.get(&url),
                RetryClass::Idempotent,
                "get_system_logs",
                &url,
            )
            .await?;
        Ok(items.items)
    }

    // ---- instances --------------------------------------------------------

    /// List the instances of a container group.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn list_instances(&self, group: &str) -> Result<Vec<Instance>, ApiError> {
        let url = format!("{}/containers/{group}/instances", self.project_base());
        let list: InstanceList = self
            .send_json(
                || self.http.get(&url),
                RetryClass::Idempotent,
                "list_instances",
                &url,
            )
            .await?;
        Ok(list.instances)
    }

    /// Get one instance.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn get_instance(&self, group: &str, machine_id: &str) -> Result<Instance, ApiError> {
        let url = format!(
            "{}/containers/{group}/instances/{machine_id}",
            self.project_base()
        );
        self.send_json(
            || self.http.get(&url),
            RetryClass::Idempotent,
            "get_instance",
            &url,
        )
        .await
    }

    /// Reallocate an instance to a different node.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn reallocate_instance(&self, group: &str, machine_id: &str) -> Result<(), ApiError> {
        let url = format!(
            "{}/containers/{group}/instances/{machine_id}/reallocate",
            self.project_base()
        );
        self.send_empty(|| self.http.post(&url), RetryClass::Idempotent, &url)
            .await
    }

    /// Recreate an instance on the same node (no image re-download).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn recreate_instance(&self, group: &str, machine_id: &str) -> Result<(), ApiError> {
        let url = format!(
            "{}/containers/{group}/instances/{machine_id}/recreate",
            self.project_base()
        );
        self.send_empty(|| self.http.post(&url), RetryClass::Idempotent, &url)
            .await
    }

    /// Restart an instance on the same node.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn restart_instance(&self, group: &str, machine_id: &str) -> Result<(), ApiError> {
        let url = format!(
            "{}/containers/{group}/instances/{machine_id}/restart",
            self.project_base()
        );
        self.send_empty(|| self.http.post(&url), RetryClass::Idempotent, &url)
            .await
    }

    // ---- organization -----------------------------------------------------

    /// List the organization's GPU classes.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn list_gpu_classes(&self) -> Result<Vec<GpuClass>, ApiError> {
        let url = format!("{}/gpu-classes", self.org_base());
        let items: Items<GpuClass> = self
            .send_json(
                || self.http.get(&url),
                RetryClass::Idempotent,
                "list_gpu_classes",
                &url,
            )
            .await?;
        Ok(items.items)
    }

    /// Get the organization's quotas.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn get_quotas(&self) -> Result<Quotas, ApiError> {
        let url = format!("{}/quotas", self.org_base());
        self.send_json(
            || self.http.get(&url),
            RetryClass::Idempotent,
            "get_quotas",
            &url,
        )
        .await
    }

    /// Query the organization's log entries (Axiom-backed).
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn query_log_entries(
        &self,
        query: &LogEntriesQuery,
    ) -> Result<Vec<LogEntry>, ApiError> {
        let url = format!("{}/log-entries", self.org_base());
        let items: Items<LogEntry> = self
            .send_json(
                || self.http.post(&url).json(query),
                RetryClass::Idempotent,
                "query_log_entries",
                &url,
            )
            .await?;
        Ok(items.items)
    }

    /// Get GPU availability for the organization.
    ///
    /// # Errors
    /// Returns an [`ApiError`] on transport or API failure.
    pub async fn gpu_availability(&self) -> Result<Vec<GpuAvailability>, ApiError> {
        let url = format!("{}/availability/sce-gpu-availability", self.org_base());
        let items: Items<GpuAvailability> = self
            .send_json(
                || self.http.get(&url),
                RetryClass::Idempotent,
                "gpu_availability",
                &url,
            )
            .await?;
        Ok(items.items)
    }
}

/// A jitter fraction in `[0, 1)` derived from the wall clock (no `rand` dependency).
fn jitter01() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000) / 1_000.0
}
