// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Dual table/JSON output helpers.

use std::io::Write;

use anyhow::Result;
use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL};
use serde::Serialize;

/// How a read command renders its result.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// A human-readable table.
    Table,
    /// Pretty-printed JSON.
    Json,
}

impl OutputFormat {
    /// Pick a format from a `--json` flag.
    #[must_use]
    pub fn from_json_flag(json: bool) -> Self {
        if json {
            OutputFormat::Json
        } else {
            OutputFormat::Table
        }
    }
}

/// A comfy-table with the house preset and dynamic column widths.
#[must_use]
pub fn table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(headers.to_vec());
    table
}

/// Print a table to stdout.
pub fn print_table(table: &Table) {
    println!("{table}");
}

/// Pretty-print a value as JSON to stdout with a trailing newline.
///
/// # Errors
/// Returns an error if serialization or writing fails.
pub fn print_json<T: Serialize>(value: &T) -> Result<()> {
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    serde_json::to_writer_pretty(&mut lock, value)?;
    lock.write_all(b"\n")?;
    Ok(())
}
