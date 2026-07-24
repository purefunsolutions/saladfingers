// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Serde models for the SaladCloud API.
//!
//! Read models ignore unknown fields and mark not-guaranteed fields `Option`; state
//! enums carry an `Unknown` catch-all. Request models omit absent optionals.

pub mod container_group;
pub mod gpu;
pub mod instance;
pub mod misc;

pub use container_group::{
    BasicAuth, ContainerGroup, ContainerGroupState, ContainerPriority, CreateContainer,
    CreateContainerGroup, DockerHubAuth, GIB, GroupStatus, InstanceStatusCounts, LoadBalancer,
    Networking, NetworkingInfo, RegistryAuthentication, Resources, RestartPolicy,
    UpdateContainerGroup,
};
pub use gpu::{GpuAvailability, GpuClass, GpuClassPrice};
pub use instance::{Instance, InstanceList, InstanceState};
pub use misc::{
    ContainerGroupsQuotas, Items, LogEntriesQuery, LogEntry, LogEntryResource, Quotas,
    SystemLogEntry, SystemLogEvent,
};
