// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers` — rent SaladCloud GPUs for minimum billed seconds.

use anyhow::Result;
use clap::Parser;
use saladfingers_cli::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    saladfingers_cli::init_tracing();
    let cli = Cli::parse();
    saladfingers_cli::dispatch(cli).await
}
