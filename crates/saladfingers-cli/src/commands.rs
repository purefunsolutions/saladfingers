// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Read-command handlers: `gpu-classes`, `quotas`, `cost estimate`.

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use saladfingers_api::{ContainerPriority, GpuClass};

use crate::cli::{CostEstimateArgs, GpuClassesArgs, ReadArgs};
use crate::config::Config;
use crate::output::{OutputFormat, print_json, print_table, table};
use crate::state;

/// How long a cached GPU-class list stays fresh.
const GPU_CACHE_TTL_HOURS: i64 = 24;

/// `saladfingers gpu-classes`
pub async fn gpu_classes(cfg: Config, args: GpuClassesArgs) -> Result<()> {
    if args.availability {
        tracing::warn!("--availability is implemented in a later milestone; showing prices only");
    }
    let client = cfg.client()?;
    let mut classes = state::cached_gpu_classes(&client, args.refresh, GPU_CACHE_TTL_HOURS).await?;
    classes.sort_by_key(|c| normalize(&c.name));

    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&classes)?,
        OutputFormat::Table => {
            let mut t = table(&["GPU class", "type", "high", "medium", "low", "batch"]);
            for class in &classes {
                t.add_row(vec![
                    class.name.trim().to_string(),
                    class.gpu_class_type.clone().unwrap_or_default(),
                    price_cell(class, ContainerPriority::High),
                    price_cell(class, ContainerPriority::Medium),
                    price_cell(class, ContainerPriority::Low),
                    price_cell(class, ContainerPriority::Batch),
                ]);
            }
            print_table(&t);
        }
    }
    Ok(())
}

/// `saladfingers quotas`
pub async fn quotas(cfg: Config, args: ReadArgs) -> Result<()> {
    let client = cfg.client()?;
    let quotas = client.get_quotas().await?;
    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&quotas)?,
        OutputFormat::Table => {
            let cg = &quotas.container_groups_quotas;
            let mut t = table(&["metric", "value"]);
            t.add_row(vec![
                "replica quota".to_string(),
                cg.container_replicas_quota.to_string(),
            ]);
            t.add_row(vec![
                "replicas used".to_string(),
                cg.container_replicas_used.to_string(),
            ]);
            t.add_row(vec![
                "replicas available".to_string(),
                quotas.replicas_available().to_string(),
            ]);
            print_table(&t);
        }
    }
    Ok(())
}

/// `saladfingers cost estimate`
pub async fn cost_estimate(cfg: Config, args: CostEstimateArgs) -> Result<()> {
    let priority = parse_priority(&args.priority)?;
    let client = cfg.client()?;
    let classes = state::cached_gpu_classes(&client, false, GPU_CACHE_TTL_HOURS).await?;
    let class = resolve_gpu_class(&classes, &args.gpu_class)
        .with_context(|| format!("no GPU class matching '{}'", args.gpu_class))?;
    let hourly = class.price(priority).with_context(|| {
        format!(
            "class '{}' has no {} price",
            class.name.trim(),
            args.priority
        )
    })?;

    let hours = Decimal::try_from(args.hours).context("invalid --hours")?;
    let replicas = Decimal::from(args.replicas);
    let total = (hourly * hours * replicas).round_dp(4);

    match OutputFormat::from_json_flag(args.json) {
        OutputFormat::Json => print_json(&serde_json::json!({
            "gpu_class": class.name.trim(),
            "gpu_class_id": class.id,
            "priority": args.priority,
            "hourly_usd": hourly.to_string(),
            "hours": args.hours,
            "replicas": args.replicas,
            "estimated_usd": total.to_string(),
        }))?,
        OutputFormat::Table => {
            let mut t = table(&["field", "value"]);
            t.add_row(vec!["GPU class".to_string(), class.name.trim().to_string()]);
            t.add_row(vec!["priority".to_string(), args.priority.clone()]);
            t.add_row(vec!["hourly (USD)".to_string(), format!("${hourly}")]);
            t.add_row(vec!["hours".to_string(), args.hours.to_string()]);
            t.add_row(vec!["replicas".to_string(), args.replicas.to_string()]);
            t.add_row(vec!["estimated (USD)".to_string(), format!("${total}")]);
            print_table(&t);
        }
    }
    Ok(())
}

fn price_cell(class: &GpuClass, priority: ContainerPriority) -> String {
    class
        .price(priority)
        .map_or_else(|| "-".to_string(), |d| format!("${d}"))
}

fn parse_priority(s: &str) -> Result<ContainerPriority> {
    match s.to_ascii_lowercase().as_str() {
        "high" => Ok(ContainerPriority::High),
        "medium" => Ok(ContainerPriority::Medium),
        "low" => Ok(ContainerPriority::Low),
        "batch" => Ok(ContainerPriority::Batch),
        other => bail!("invalid priority '{other}' (expected high|medium|low|batch)"),
    }
}

/// Normalize a GPU-class name/query for matching: keep only alphanumerics, lowercase.
fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Resolve a GPU class by exact UUID, exact normalized name, or normalized substring.
pub(crate) fn resolve_gpu_class<'a>(classes: &'a [GpuClass], query: &str) -> Option<&'a GpuClass> {
    if let Some(exact) = classes.iter().find(|c| c.id == query) {
        return Some(exact);
    }
    let nq = normalize(query);
    if nq.is_empty() {
        return None;
    }
    classes
        .iter()
        .find(|c| normalize(&c.name) == nq)
        .or_else(|| classes.iter().find(|c| normalize(&c.name).contains(&nq)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(id: &str, name: &str) -> GpuClass {
        GpuClass {
            id: id.to_string(),
            name: name.to_string(),
            gpu_class_type: Some("community".to_string()),
            is_high_demand: None,
            prices: vec![],
        }
    }

    #[test]
    fn resolve_matches_uuid_name_and_substring() {
        let classes = vec![
            class("uuid-3060", "RTX 3060 (12 GB)"),
            class("uuid-4090", "RTX 4090 (24 GB)"),
            class("uuid-5080", " RTX 5080 (16 GB)"), // note the leading space from the live API
        ];
        assert_eq!(
            resolve_gpu_class(&classes, "uuid-4090").unwrap().id,
            "uuid-4090"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "RTX 3060 (12 GB)").unwrap().id,
            "uuid-3060"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "rtx4090").unwrap().id,
            "uuid-4090"
        );
        assert_eq!(resolve_gpu_class(&classes, "5080").unwrap().id, "uuid-5080");
        assert!(resolve_gpu_class(&classes, "h100").is_none());
    }
}
