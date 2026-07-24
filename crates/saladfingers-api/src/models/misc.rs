// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Quotas, system logs, org log queries, and the generic list envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Generic `{ "items": [...] }` list envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct Items<T> {
    /// The list contents.
    #[serde(default = "Vec::new")]
    pub items: Vec<T>,
}

/// Organization quotas response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quotas {
    /// Container-group replica quotas.
    pub container_groups_quotas: ContainerGroupsQuotas,
    /// When the quota record was created.
    #[serde(default)]
    pub create_time: Option<DateTime<Utc>>,
    /// When the quota record was updated.
    #[serde(default)]
    pub update_time: Option<DateTime<Utc>>,
}

/// Replica quotas and per-minute action caps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerGroupsQuotas {
    /// Total replica quota across all container groups.
    pub container_replicas_quota: u32,
    /// Replicas currently in use.
    pub container_replicas_used: u32,
    /// Max reallocations per minute.
    #[serde(default)]
    pub max_container_group_reallocations_per_minute: Option<u32>,
    /// Max recreates per minute.
    #[serde(default)]
    pub max_container_group_recreates_per_minute: Option<u32>,
    /// Max restarts per minute.
    #[serde(default)]
    pub max_container_group_restarts_per_minute: Option<u32>,
}

impl Quotas {
    /// Replicas still available to allocate.
    #[must_use]
    pub fn replicas_available(&self) -> u32 {
        self.container_groups_quotas
            .container_replicas_quota
            .saturating_sub(self.container_groups_quotas.container_replicas_used)
    }
}

/// One system-log entry for a container group.
#[derive(Debug, Clone, Deserialize)]
pub struct SystemLogEntry {
    /// Instance the event pertains to.
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Container group name.
    #[serde(default)]
    pub container_group_name: Option<String>,
    /// When the entry was recorded.
    #[serde(default)]
    pub create_time: Option<DateTime<Utc>>,
    /// Events in this entry.
    #[serde(default)]
    pub events: Vec<SystemLogEvent>,
}

/// A single system event (e.g. `Instance Exited:0`).
#[derive(Debug, Clone, Deserialize)]
pub struct SystemLogEvent {
    /// Event name.
    #[serde(default)]
    pub name: Option<String>,
    /// Event time.
    #[serde(default)]
    pub time: Option<DateTime<Utc>>,
}

/// Org log-entries query request (Axiom-backed). `start_time`, `end_time`, and a
/// non-empty `query` are required by the API — an empty query is rejected. The query
/// uses SaladCloud's log query language, e.g.
/// `resource.labels.container_group_name = "my-group"` (quotes must be escaped in JSON).
#[derive(Debug, Clone, Serialize)]
pub struct LogEntriesQuery {
    /// Start of the time range.
    #[serde(serialize_with = "serialize_millis")]
    pub start_time: DateTime<Utc>,
    /// End of the time range.
    #[serde(serialize_with = "serialize_millis")]
    pub end_time: DateTime<Utc>,
    /// Non-empty filter in SaladCloud's log query language (field operator value).
    pub query: String,
    /// Max rows per page (the API validates this to the range 1..=100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    /// `asc` (chronological) or `desc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<String>,
}

/// Serialize a timestamp to RFC 3339 with millisecond precision — the log-query API
/// rejects the microsecond/nanosecond precision chrono emits by default.
fn serialize_millis<S: serde::Serializer>(
    dt: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// One org log entry.
#[derive(Debug, Clone, Deserialize)]
pub struct LogEntry {
    /// Log timestamp.
    #[serde(default)]
    pub time: Option<DateTime<Utc>>,
    /// Container stdout/stderr in text form (severity `default`).
    #[serde(default)]
    pub text_log: Option<String>,
    /// Structured platform event (severity `info`); carries a `message` field and
    /// often `gpu_class_name`. Present instead of `text_log` for lifecycle events
    /// like `Instance Running` / `Instance Ready`.
    #[serde(default)]
    pub json_log: Option<serde_json::Value>,
    /// Severity level (`default` for container output, `info`/`warning`/… for events).
    #[serde(default)]
    pub severity: Option<String>,
    /// The resource (labels identify the container group / instance).
    #[serde(default)]
    pub resource: Option<LogEntryResource>,
}

/// The resource a log entry belongs to. `labels` carry `container_group_name` etc.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LogEntryResource {
    /// Resource labels (e.g. `container_group_name`, `machine_id`).
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    /// Resource type.
    #[serde(default, rename = "type")]
    pub resource_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_query_timestamps_are_millisecond_precision() {
        // The log-query API rejects sub-millisecond digits; chrono emits nanoseconds by
        // default, so the serializer must truncate to milliseconds.
        let ts: DateTime<Utc> = "2023-11-14T22:13:20.123456789Z".parse().expect("valid ts");
        let query = LogEntriesQuery {
            start_time: ts,
            end_time: ts,
            query: "resource.labels.container_group_name = \"g\"".to_string(),
            page_size: Some(100),
            sort_order: Some("desc".to_string()),
        };
        let json = serde_json::to_value(&query).expect("serialize");
        assert_eq!(json["start_time"], "2023-11-14T22:13:20.123Z");
        assert_eq!(json["end_time"], "2023-11-14T22:13:20.123Z");
        assert_eq!(
            json["query"],
            "resource.labels.container_group_name = \"g\""
        );
    }

    #[test]
    fn log_entry_parses_container_and_platform_variants() {
        // Container stdout arrives as `text_log`; platform lifecycle events as `json_log`.
        let stdout: LogEntry =
            serde_json::from_str(r#"{"text_log":"hello","severity":"default"}"#).expect("parse");
        assert_eq!(stdout.text_log.as_deref(), Some("hello"));
        assert!(stdout.json_log.is_none());

        let event: LogEntry = serde_json::from_str(
            r#"{"json_log":{"message":"Instance Running"},"severity":"info"}"#,
        )
        .expect("parse");
        assert!(event.text_log.is_none());
        assert_eq!(event.json_log.expect("json")["message"], "Instance Running");
    }
}
