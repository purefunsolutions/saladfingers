// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! GPU class + availability models.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::container_group::ContainerPriority;

/// A GPU class offered by SaladCloud. `id` is the UUID passed in `gpu_classes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuClass {
    /// GPU class UUID.
    pub id: String,
    /// Display name, e.g. `RTX 4090 (24 GB)`. May contain leading whitespace.
    pub name: String,
    /// `community` or `secure`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_class_type: Option<String>,
    /// Whether the class is currently high-demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_high_demand: Option<bool>,
    /// One price per priority tier.
    #[serde(default)]
    pub prices: Vec<GpuClassPrice>,
}

impl GpuClass {
    /// The hourly price for a given priority, if listed.
    #[must_use]
    pub fn price(&self, priority: ContainerPriority) -> Option<Decimal> {
        self.prices
            .iter()
            .find(|p| p.priority == priority)
            .map(|p| p.price)
    }
}

/// Per-priority hourly price. The API sends the price as a string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuClassPrice {
    /// Priority tier.
    pub priority: ContainerPriority,
    /// Hourly price in USD.
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
}

/// GPU availability for a class (loosely modeled; refined when the `availability`
/// command is built).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuAvailability {
    /// GPU class UUID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Coarse availability level, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_price_tier_does_not_brick_gpu_class_parsing() {
        // The alpha API adding one tier ("turbo") must not make every `gpu-classes`
        // call fail to decode; the unknown tier parses as Unknown and known tiers
        // stay queryable.
        let g: GpuClass = serde_json::from_str(
            r#"{
                "id": "abc",
                "name": "RTX 4090 (24 GB)",
                "prices": [
                    {"priority": "batch", "price": "0.16"},
                    {"priority": "turbo", "price": "9.99"}
                ]
            }"#,
        )
        .expect("unknown tier must not fail the decode");
        assert_eq!(g.prices.len(), 2);
        assert_eq!(
            g.price(ContainerPriority::Batch),
            Some(rust_decimal::Decimal::new(16, 2))
        );
        assert_eq!(g.prices[1].priority, ContainerPriority::Unknown);
    }
}
