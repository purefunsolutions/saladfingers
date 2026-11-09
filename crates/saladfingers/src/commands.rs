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
        .map_err(|e| anyhow::anyhow!("GPU class '{}': {e}", args.gpu_class))?;
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

/// The class name with its trailing VRAM parenthetical removed, normalized:
/// `"RTX 3060 Ti (8 GB)"` → `"rtx3060ti"`.
///
/// Live names always carry that suffix, so without stripping it a natural query
/// like `"rtx 3060 ti"` can never match exactly and falls through to substring
/// matching — see [`resolve_gpu_class`].
fn normalize_base(name: &str) -> String {
    normalize(name.split('(').next().unwrap_or(name))
}

/// Why a GPU-class query did not resolve to exactly one class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GpuClassError {
    /// Nothing matched, at any tier.
    NotFound,
    /// Several classes matched at the same tier. Carries their display names so
    /// the operator can copy one verbatim.
    Ambiguous(Vec<String>),
}

impl std::fmt::Display for GpuClassError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "no matching GPU class"),
            Self::Ambiguous(names) => write!(
                f,
                "matches {} classes ({}) — pass the exact name or its UUID",
                names.len(),
                names.join(", ")
            ),
        }
    }
}

/// Which rule matched a class, in decreasing order of confidence.
#[derive(Debug, Clone, Copy)]
enum Tier {
    /// The UUID, verbatim.
    Uuid,
    /// The full name, normalized: `"RTX 3060 Ti (8 GB)"`.
    ExactName,
    /// The name without its VRAM parenthetical: `"rtx 3060 ti"`.
    BaseName,
    /// A substring of the normalized name: `"5080"`, `"a5000"`.
    Substring,
}

/// Resolve a GPU class, trying each [`Tier`] in turn.
///
/// **The first tier that matches anything decides**, so a broad tier can never
/// override a precise one, and a tier that matches several classes is an error
/// rather than a pick.
///
/// That rule is the fix for a real footgun: the live class list is unordered, so
/// a bare substring search made `"rtx 3060"` resolve to whichever of
/// `RTX 3060 (12 GB)` / `RTX 3060 (8 GB)` / `RTX 3060 Ti (8 GB)` the API happened
/// to list first — silently renting a different card, at a different price and a
/// different SM arch. Now `"rtx 3060 ti"` is an exact base name, `"rtx 3090"`
/// resolves to the non-Ti, and a genuinely ambiguous query names its candidates
/// instead of flipping a coin.
///
/// # Errors
/// [`GpuClassError::NotFound`] if no tier matched, [`GpuClassError::Ambiguous`]
/// if the first tier that matched hit more than one class.
pub(crate) fn resolve_gpu_class<'a>(
    classes: &'a [GpuClass],
    query: &str,
) -> Result<&'a GpuClass, GpuClassError> {
    let nq = normalize(query);
    if nq.is_empty() {
        return Err(GpuClassError::NotFound);
    }
    for tier in [Tier::Uuid, Tier::ExactName, Tier::BaseName, Tier::Substring] {
        let hits: Vec<&GpuClass> = classes
            .iter()
            .filter(|&c| match tier {
                Tier::Uuid => c.id == query,
                Tier::ExactName => normalize(&c.name) == nq,
                Tier::BaseName => normalize_base(&c.name) == nq,
                Tier::Substring => normalize(&c.name).contains(&nq),
            })
            .collect();
        match hits.as_slice() {
            [] => {}
            [one] => return Ok(one),
            many => {
                return Err(GpuClassError::Ambiguous(
                    many.iter().map(|c| c.name.trim().to_string()).collect(),
                ));
            }
        }
    }
    Err(GpuClassError::NotFound)
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

    /// The real ambiguous names from the live class list, in an order that would
    /// make a first-substring-wins search pick the WRONG card for `"rtx 3060"`
    /// (the Ti is listed first).
    fn live_classes() -> Vec<GpuClass> {
        vec![
            class("uuid-3060ti", "RTX 3060 Ti (8 GB)"),
            class("uuid-3060-8", "RTX 3060 (8 GB)"),
            class("uuid-3060-12", "RTX 3060 (12 GB)"),
            class("uuid-3090", "RTX 3090 (24 GB)"),
            class("uuid-3090ti", "RTX 3090 Ti (24 GB)"),
            class("uuid-4090", "RTX 4090 (24 GB)"),
            class("uuid-5090-lap", "RTX 5090 Laptop (24 GB)"),
            class("uuid-5090", "RTX 5090 (32 GB)"),
            class("uuid-5080", " RTX 5080 (16 GB)"), // note the leading space from the live API
        ]
    }

    #[test]
    fn resolve_matches_uuid_name_and_substring() {
        let classes = live_classes();
        assert_eq!(
            resolve_gpu_class(&classes, "uuid-4090").unwrap().id,
            "uuid-4090"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "RTX 3060 (12 GB)").unwrap().id,
            "uuid-3060-12"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "rtx4090").unwrap().id,
            "uuid-4090"
        );
        assert_eq!(resolve_gpu_class(&classes, "5080").unwrap().id, "uuid-5080");
        assert_eq!(
            resolve_gpu_class(&classes, "h100").unwrap_err(),
            GpuClassError::NotFound
        );
    }

    /// The bug this tier exists for: a base-name query must never be decided by
    /// API list order.
    #[test]
    fn resolve_prefers_exact_base_name_over_substring() {
        let classes = live_classes();
        // "rtx 3060 ti" is a unique base name → the Ti, even though it is also a
        // substring-superset case.
        assert_eq!(
            resolve_gpu_class(&classes, "rtx 3060 ti").unwrap().id,
            "uuid-3060ti"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "RTX 3090 Ti").unwrap().id,
            "uuid-3090ti"
        );
        // Unique base names that a substring search would have made ambiguous
        // against their Ti / Laptop siblings.
        assert_eq!(
            resolve_gpu_class(&classes, "rtx 3090").unwrap().id,
            "uuid-3090"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "rtx 5090").unwrap().id,
            "uuid-5090"
        );
        assert_eq!(
            resolve_gpu_class(&classes, "rtx 5090 laptop").unwrap().id,
            "uuid-5090-lap"
        );
    }

    /// Genuinely ambiguous queries must fail loudly, naming the candidates —
    /// never silently rent one of them.
    #[test]
    fn resolve_reports_ambiguity() {
        let classes = live_classes();
        // Two VRAM variants share the base name "RTX 3060".
        match resolve_gpu_class(&classes, "rtx 3060") {
            Err(GpuClassError::Ambiguous(names)) => {
                assert_eq!(names.len(), 2, "got {names:?}");
                assert!(names.iter().any(|n| n.contains("8 GB")));
                assert!(names.iter().any(|n| n.contains("12 GB")));
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        // A broad substring matches many classes.
        match resolve_gpu_class(&classes, "rtx") {
            Err(GpuClassError::Ambiguous(names)) => assert!(names.len() > 5, "got {names:?}"),
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        // Disambiguating by full name still works.
        assert_eq!(
            resolve_gpu_class(&classes, "RTX 3060 (8 GB)").unwrap().id,
            "uuid-3060-8"
        );
    }

    /// `gpu-probe` and `doctor --live` both default to this class, so it must
    /// resolve on its own. It used to be the bare `"rtx3060"`, which is the one
    /// genuinely ambiguous base name in the live list — this fails the moment a
    /// default is pointed at an ambiguous name again.
    #[test]
    fn the_probe_default_resolves_unambiguously() {
        let classes = live_classes();
        let class = resolve_gpu_class(&classes, crate::cli::DEFAULT_PROBE_GPU_CLASS)
            .expect("the probe default must resolve");
        assert_eq!(class.id, "uuid-3060-8");
    }

    /// Every tier is uniqueness-checked, including the exact-name one. Two live
    /// names that differ only in spacing normalize identically; taking the first
    /// would be the same coin flip this module exists to remove.
    #[test]
    fn duplicate_normalized_names_are_ambiguous_not_first_wins() {
        let mut classes = live_classes();
        classes.push(class("uuid-4090-dup", "RTX 4090 (24GB)"));
        match resolve_gpu_class(&classes, "RTX 4090 (24 GB)") {
            Err(GpuClassError::Ambiguous(names)) => assert_eq!(names.len(), 2, "got {names:?}"),
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }
}
