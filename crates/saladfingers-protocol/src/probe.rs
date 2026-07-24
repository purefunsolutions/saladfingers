// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! [`ProbeReport`] — what `sf-agent probe` reports back about a rented node.
//!
//! The probe exists to pin down what SaladCloud's docs don't specify: the exact
//! injected driver library paths, which vendor tools are present, real bandwidth,
//! and the injected `SALAD_*` environment. The CLI renders this for `doctor --live`
//! and archives it into `docs/empirical.md`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// GPU vendor detected inside the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GpuVendor {
    /// NVIDIA (CUDA).
    Nvidia,
    /// AMD (ROCm/HIP).
    Amd,
    /// No GPU detected.
    None,
}

/// A node environment probe result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeReport {
    /// Protocol version.
    pub v: u32,
    /// Detected GPU vendor.
    pub gpu_vendor: GpuVendor,
    /// GPU model name, when detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_name: Option<String>,
    /// Driver version, when detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_version: Option<String>,
    /// GPU memory in MiB, when detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vram_mb: Option<u64>,
    /// Injected `SALAD_*` environment variables.
    #[serde(default)]
    pub salad_env: BTreeMap<String, String>,
    /// Discovered GPU library paths (`libcuda.so*`, `libamdhip64.so*`, …).
    #[serde(default)]
    pub library_paths: Vec<String>,
    /// Discovered vendor tools mapped to their resolved paths (`nvidia-smi`, …).
    #[serde(default)]
    pub tools: BTreeMap<String, String>,
    /// Raw `nvidia-smi -q` / `rocminfo` output, when captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smi_output: Option<String>,
    /// Measured download throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_down_mbps: Option<f64>,
    /// Measured upload throughput.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_up_mbps: Option<f64>,
    /// Whether the IMDS endpoint was reachable from inside the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imds_reachable: Option<bool>,
    /// Whether a test upload to S4 using the IMDS workload JWT succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s4_jwt_upload_ok: Option<bool>,
    /// Free-form observations.
    #[serde(default)]
    pub notes: Vec<String>,
}

impl ProbeReport {
    /// An empty report with no GPU detected.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            v: crate::PROTOCOL_VERSION,
            gpu_vendor: GpuVendor::None,
            gpu_name: None,
            driver_version: None,
            vram_mb: None,
            salad_env: BTreeMap::new(),
            library_paths: Vec::new(),
            tools: BTreeMap::new(),
            smi_output: None,
            measured_down_mbps: None,
            measured_up_mbps: None,
            imds_reachable: None,
            s4_jwt_upload_ok: None,
            notes: Vec::new(),
        }
    }
}
