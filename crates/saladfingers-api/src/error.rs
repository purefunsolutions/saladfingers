// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Error taxonomy for the SaladCloud client.
//!
//! The API returns RFC-7807 problem JSON, but edge/Cloudflare errors arrive as HTML
//! — so responses are classified by status and content type before any JSON decode.

use std::time::Duration;

/// An error from the SaladCloud control plane or S4.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Transport-level failure (connect, TLS, timeout).
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    /// HTTP 429. `retry_after` is parsed from the `Retry-After` header when present.
    #[error("rate limited (429)")]
    RateLimited {
        /// Suggested wait before retrying.
        retry_after: Option<Duration>,
    },

    /// HTTP 404, surfaced as its own variant so callers (gc, wait loops) can treat a
    /// missing resource as control flow.
    #[error("not found: {0}")]
    NotFound(String),

    /// A structured RFC-7807 problem response.
    #[error("API error {status}: {title}{}", .detail.as_deref().map(|d| format!(" — {d}")).unwrap_or_default())]
    Problem {
        /// HTTP status code.
        status: u16,
        /// Problem type URI.
        r#type: Option<String>,
        /// Short human-readable title.
        title: String,
        /// Longer detail.
        detail: Option<String>,
        /// Problem instance URI.
        instance: Option<String>,
    },

    /// A non-JSON error body (typically a Cloudflare HTML page).
    #[error("HTTP {status} with non-JSON body (edge error): {snippet}")]
    Html {
        /// HTTP status code.
        status: u16,
        /// First ~200 chars of the body, tag-stripped.
        snippet: String,
    },

    /// A 2xx body that failed to decode into the expected type.
    #[error("failed to decode {context} response: {source}")]
    Decode {
        /// What we were decoding.
        context: &'static str,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
        /// First ~200 chars of the body.
        snippet: String,
    },

    /// All retry attempts were exhausted.
    #[error("retries exhausted after {attempts} attempts: {last}")]
    RetriesExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// The last error seen.
        #[source]
        last: Box<ApiError>,
    },
}

impl ApiError {
    /// Whether this error class is worth retrying for an idempotent request.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            ApiError::Network(e) => e.is_connect() || e.is_timeout() || e.is_request(),
            ApiError::RateLimited { .. } => true,
            ApiError::Html { status, .. } => matches!(status, 502..=504),
            _ => false,
        }
    }

    /// Whether this error is a 404.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, ApiError::NotFound(_))
    }
}

/// Minimal RFC-7807 problem shape for error decoding.
#[derive(serde::Deserialize)]
struct ProblemDetails {
    r#type: Option<String>,
    title: Option<String>,
    status: Option<u16>,
    detail: Option<String>,
    instance: Option<String>,
}

/// Classify a non-2xx response into an [`ApiError`]. Shared by the control-plane and
/// S4 clients so error handling never drifts between them.
pub(crate) fn classify_error(
    status: reqwest::StatusCode,
    content_type: &str,
    body: &str,
    retry_after: Option<Duration>,
    path: &str,
) -> ApiError {
    let code = status.as_u16();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return ApiError::RateLimited { retry_after };
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return ApiError::NotFound(path.to_string());
    }
    let looks_json = content_type.contains("json") || body.trim_start().starts_with('{');
    if looks_json && let Ok(p) = serde_json::from_str::<ProblemDetails>(body) {
        return ApiError::Problem {
            status: p.status.unwrap_or(code),
            r#type: p.r#type,
            title: p
                .title
                .unwrap_or_else(|| status.canonical_reason().unwrap_or("error").to_string()),
            detail: p.detail,
            instance: p.instance,
        };
    }
    ApiError::Html {
        status: code,
        snippet: snippet(body),
    }
}

/// Strip HTML tags and collapse whitespace into a short snippet for error messages.
#[must_use]
pub(crate) fn snippet(body: &str) -> String {
    let mut out = String::with_capacity(200);
    let mut in_tag = false;
    let mut last_space = false;
    for ch in body.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if in_tag => {}
            c if c.is_whitespace() => {
                if !last_space && !out.is_empty() {
                    out.push(' ');
                    last_space = true;
                }
            }
            c => {
                out.push(c);
                last_space = false;
                if out.len() >= 200 {
                    break;
                }
            }
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snippet_strips_tags_and_collapses_whitespace() {
        let html = "<html>  <body><h1>502   Bad\nGateway</h1></body></html>";
        assert_eq!(snippet(html), "502 Bad Gateway");
    }

    #[test]
    fn not_found_and_retryable_classification() {
        assert!(ApiError::NotFound("x".into()).is_not_found());
        assert!(ApiError::RateLimited { retry_after: None }.is_retryable());
        assert!(
            ApiError::Html {
                status: 503,
                snippet: "x".into()
            }
            .is_retryable()
        );
        assert!(
            !ApiError::Html {
                status: 400,
                snippet: "x".into()
            }
            .is_retryable()
        );
    }
}
