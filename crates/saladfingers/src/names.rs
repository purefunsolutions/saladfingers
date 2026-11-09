// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Run identifiers and container-group name validation.

use anyhow::{Result, bail};

const RUNID_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const RUNID_LEN: usize = 6;

/// Generate a fresh run id like `sf-x7k2mq`.
#[must_use]
pub fn generate_run_id() -> String {
    let mut bytes = [0u8; RUNID_LEN];
    getrandom::fill(&mut bytes).expect("system RNG unavailable");
    let suffix: String = bytes
        .iter()
        .map(|b| RUNID_ALPHABET[*b as usize % RUNID_ALPHABET.len()] as char)
        .collect();
    format!("sf-{suffix}")
}

/// The container-group name for a run id and optional shard.
#[must_use]
pub fn group_name(run_id: &str, shard: Option<u32>) -> String {
    match shard {
        Some(s) => format!("{run_id}-{s}"),
        None => run_id.to_string(),
    }
}

/// Validate a container-group name against Salad's constraint:
/// `^[a-z][a-z0-9-]{0,61}[a-z0-9]$`, length 2–63.
///
/// # Errors
/// Returns an error describing the first violated rule.
pub fn validate_group_name(name: &str) -> Result<()> {
    let len = name.len();
    if !(2..=63).contains(&len) {
        bail!("group name must be 2-63 characters: {name:?}");
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        bail!("group name must start with a lowercase letter: {name:?}");
    }
    let last = bytes[len - 1];
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        bail!("group name must end with a lowercase letter or digit: {name:?}");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        bail!("group name may only contain a-z, 0-9, and hyphens: {name:?}");
    }
    Ok(())
}

/// Whether a name looks like a saladfingers-created group: `sf-<6>(-<shard>)?`.
#[must_use]
pub fn is_sf_group(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("sf-") else {
        return false;
    };
    let (id, shard) = rest
        .split_once('-')
        .map_or((rest, None), |(id, s)| (id, Some(s)));
    if id.len() != RUNID_LEN
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return false;
    }
    match shard {
        None => true,
        Some(s) => !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// Extract the run id from a group name (`sf-x7k2mq-0` → `sf-x7k2mq`).
#[must_use]
pub fn run_id_of_group(name: &str) -> Option<String> {
    if !is_sf_group(name) {
        return None;
    }
    let rest = name.strip_prefix("sf-")?;
    let id = rest.split_once('-').map_or(rest, |(id, _)| id);
    Some(format!("sf-{id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_ids_are_well_formed() {
        let id = generate_run_id();
        assert!(id.starts_with("sf-"));
        assert_eq!(id.len(), 9);
        assert!(is_sf_group(&id));
        validate_group_name(&id).unwrap();
    }

    #[test]
    fn group_names_for_shards() {
        assert_eq!(group_name("sf-x7k2mq", None), "sf-x7k2mq");
        assert_eq!(group_name("sf-x7k2mq", Some(3)), "sf-x7k2mq-3");
    }

    #[test]
    fn validation_rejects_bad_names() {
        assert!(validate_group_name("sf-x7k2mq").is_ok());
        assert!(validate_group_name("a").is_err()); // too short
        assert!(validate_group_name("1abc").is_err()); // starts with digit
        assert!(validate_group_name("abc-").is_err()); // ends with hyphen
        assert!(validate_group_name("AbC").is_err()); // uppercase
        assert!(validate_group_name("a_b").is_err()); // underscore
    }

    #[test]
    fn sf_group_detection_and_run_id_extraction() {
        assert!(is_sf_group("sf-x7k2mq"));
        assert!(is_sf_group("sf-x7k2mq-0"));
        assert!(is_sf_group("sf-abc123-42"));
        assert!(!is_sf_group("sf-x7k2mq-")); // empty shard
        assert!(!is_sf_group("sf-short")); // wrong id length
        assert!(!is_sf_group("other-x7k2mq"));
        assert!(!is_sf_group("sf-x7k2mq-a")); // non-numeric shard
        assert_eq!(run_id_of_group("sf-x7k2mq-0").as_deref(), Some("sf-x7k2mq"));
        assert_eq!(run_id_of_group("sf-x7k2mq").as_deref(), Some("sf-x7k2mq"));
        assert_eq!(run_id_of_group("nope"), None);
    }
}
