// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! saladfingers CLI library.
//!
//! Exposed as a library so the command modules form a public API (and are testable
//! and free of spurious dead-code warnings as later milestones fill them in). The
//! `saladfingers` binary in `main.rs` is a thin wrapper over [`dispatch`].

pub mod admin;
pub mod bench;
pub mod checkpoint;
pub mod cli;
pub mod commands;
pub mod config;
pub mod deploy;
pub mod doctor;
pub mod image;
pub mod logs;
pub mod names;
pub mod output;
pub mod presign;
pub mod probecmd;
pub mod runner;
pub mod serve;
pub mod session;
pub mod spec;
pub mod state;
pub mod tunnel;

use anyhow::Result;

use cli::{
    BenchCommand, CheckpointCommand, Cli, Command, CostCommand, ImageCommand, ServeCommand,
    SessionCommand,
};
use config::Config;

/// Initialize tracing. Logs go to stderr so stdout stays clean for `--json` output.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,reqwest=warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Dispatch a parsed CLI invocation.
///
/// # Errors
/// Returns an error if the command fails.
pub async fn dispatch(cli: Cli) -> Result<()> {
    let config_path = cli.config.clone();
    let org = cli.org.clone();
    let project = cli.project.clone();
    let resolve = || Config::load(config_path.as_deref(), org.as_deref(), project.as_deref());

    match cli.command {
        Command::Init => admin::init(),
        Command::Doctor(args) => doctor::doctor(resolve()?, args).await,
        Command::GpuClasses(args) => commands::gpu_classes(resolve()?, args).await,
        Command::Quotas(args) => commands::quotas(resolve()?, args).await,
        Command::Cost(CostCommand::Estimate(args)) => {
            commands::cost_estimate(resolve()?, args).await
        }
        Command::Run(args) => runner::run(resolve()?, args).await,
        Command::Attach(args) => runner::attach(resolve()?, args).await,
        Command::Tunnel(args) => tunnel::tunnel(resolve()?, args).await,
        Command::Ls(args) => admin::ls(resolve()?, args).await,
        Command::Status(args) => admin::status(resolve()?, args).await,
        Command::Watch(args) => admin::watch(resolve()?, args).await,
        Command::Cancel(args) => runner::cancel(resolve()?, args).await,
        Command::Reap(args) => runner::reap(resolve()?, args).await,
        Command::Logs(args) => logs::logs(resolve()?, args).await,
        Command::Checkpoint(sub) => match sub {
            CheckpointCommand::Show(args) => checkpoint::show(resolve()?, args).await,
            CheckpointCommand::Fetch(args) => checkpoint::fetch(resolve()?, args).await,
            CheckpointCommand::Rm(args) => checkpoint::rm(resolve()?, args).await,
        },
        Command::Gc(args) => admin::gc(resolve()?, args).await,
        Command::Session(sub) => match sub {
            SessionCommand::Create(args) => session::create(resolve()?, args).await,
            SessionCommand::Ls(args) => session::ls(resolve()?, args).await,
            SessionCommand::Exec(args) => session::exec(resolve()?, args).await,
            SessionCommand::Cp(args) => session::cp(resolve()?, args).await,
            SessionCommand::Logs(args) => session::logs(resolve()?, args).await,
            SessionCommand::Stop(args) => session::stop(resolve()?, args).await,
            SessionCommand::Rm(args) => session::rm(resolve()?, args).await,
        },
        Command::Serve(sub) => match sub {
            ServeCommand::Up(args) => serve::up(resolve()?, args).await,
            ServeCommand::Status(args) => serve::status(resolve()?, args).await,
            ServeCommand::Autostop(args) => serve::autostop(resolve()?, args).await,
            ServeCommand::Down(args) => serve::down(resolve()?, args).await,
            ServeCommand::Resume(args) => serve::resume(resolve()?, args).await,
            ServeCommand::Rm(args) => serve::rm(resolve()?, args).await,
        },
        Command::Bench(BenchCommand::Startup(args)) => bench::bench_startup(resolve()?, args).await,
        Command::Image(ImageCommand::Push(args)) => image::push(resolve()?, args),
        Command::GpuProbe(args) => probecmd::gpu_probe(resolve()?, args).await,
    }
}
