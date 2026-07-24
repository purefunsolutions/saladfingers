// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `sf-agent` — the in-container agent baked into every saladfingers image.

use clap::Parser;
use saladfingers_agent::{Cli, dispatch, init_tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    dispatch(cli).await
}
