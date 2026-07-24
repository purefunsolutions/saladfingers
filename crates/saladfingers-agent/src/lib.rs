// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! sf-agent library — the in-container agent baked into every saladfingers image.
//!
//! Exposed as a library (like the CLI) so its modules form a public API and stay
//! dead-code-clean as later milestones fill them in. The `sf-agent` binary in
//! `main.rs` is a thin wrapper over [`dispatch`].
//!
//! It must start in well under a second (billing begins the moment an instance
//! reaches `running`, before the app is ready). Modes:
//! - `run`   — one-shot batch job supervisor.
//! - `serve` — interactive session HTTP server / inference reverse proxy (M5/M6).
//! - `probe` — report GPU/driver/bandwidth facts about the node.

pub mod checkpoint;
pub mod imds;
pub mod probe;
pub mod proxy;
pub mod ring;
pub mod run;
pub mod serve;

use clap::{Parser, Subcommand};

/// In-container agent for saladfingers jobs.
#[derive(Debug, Parser)]
#[command(
    name = "sf-agent",
    version,
    about = "In-container agent for saladfingers jobs"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// sf-agent subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a one-shot batch job to completion, then exit.
    Run(run::RunArgs),
    /// Serve the interactive session API on `[::]`.
    Serve(serve::ServeArgs),
    /// Probe the node's GPU/driver/environment and emit a report.
    Probe(probe::ProbeArgs),
}

/// Initialize tracing. Logs go to stderr so stdout stays clean for probe JSON and
/// child-process output.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Dispatch a parsed CLI invocation.
///
/// # Errors
/// Returns an error if the selected mode fails.
pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Run(args) => run::run(args).await,
        Command::Serve(args) => serve::serve(args).await,
        Command::Probe(args) => probe::run(args).await,
    }
}
