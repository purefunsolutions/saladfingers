// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Typed client for the SaladCloud public REST API and S4 storage.
//!
//! The API is alpha and its errors may arrive as Cloudflare HTML, so this client is
//! hand-written (no codegen): read models tolerate unknown/new fields, and responses
//! are classified by status and content type before any JSON decode. See
//! [`error::ApiError`] for the taxonomy and [`client::SaladClient`] for the methods.

pub mod client;
pub mod error;
pub mod http;
pub mod models;
pub mod retry;
pub mod s4;
pub mod secret;

pub use client::{DEFAULT_BASE_URL, DEFAULT_RATE_PER_MIN, SaladClient, SaladClientConfig};
pub use error::ApiError;
pub use retry::{RetryPolicy, TokenBucket};
pub use s4::{DEFAULT_S4_BASE_URL, S4Auth, S4Client};
pub use secret::Secret;

pub use models::{
    BasicAuth, ContainerGroup, ContainerGroupState, ContainerGroupsQuotas, ContainerPriority,
    CreateContainer, CreateContainerGroup, GpuAvailability, GpuClass, GpuClassPrice, GroupStatus,
    Instance, InstanceState, Items, LoadBalancer, LogEntriesQuery, LogEntry, LogEntryResource,
    Networking, NetworkingInfo, Quotas, RegistryAuthentication, Resources, RestartPolicy,
    SystemLogEntry, UpdateContainerGroup,
};
