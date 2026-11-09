// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Command-line interface definition (clap derive).
//!
//! The full command tree is defined here so `saladfingers --help` documents the
//! whole surface from M0. Handlers are filled in over milestones M1–M6.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Default GPU class for `gpu-probe` and `doctor --live`.
///
/// Spelled in full because the live list carries two RTX 3060 variants whose base
/// names collide, so the bare `"rtx3060"` this replaced is ambiguous. The 8 GB one
/// because these two commands only need *a* node to answer a question about the
/// fleet — never VRAM — and it is the cheaper of the pair: $0.03/h at batch
/// against $0.04/h for the 12 GB.
pub const DEFAULT_PROBE_GPU_CLASS: &str = "RTX 3060 (8 GB)";

/// Loopback port `saladfingers tunnel` listens on unless `--local-port` says otherwise.
///
/// Shared with `run --expose-port`'s "watch it with" hint, which prints the URL a browser
/// should open: two literals would drift the moment either changed, and the hint would
/// then send the operator to a port nothing is listening on.
pub const DEFAULT_TUNNEL_PORT: u16 = 7777;

/// Long help shared by every `--gpu-class` argument, so the resolution rules are
/// stated once rather than repeated across the six commands that take one.
///
/// Attached as `long_help` rather than `help`: each site keeps its own short doc
/// comment for `-h`, and this appears under `--help`.
const GPU_CLASS_HELP: &str = "\
GPU class name or UUID.

Matched ignoring case, spaces and punctuation, in decreasing order of confidence:
exact UUID; exact name (\"RTX 4090 (24 GB)\"); exact name without the VRAM suffix
(\"rtx 3090\" resolves to RTX 3090 (24 GB), not the Ti); then a substring, if it
matches exactly one class.

A query matching several classes is an error listing them, so the live list's
near-duplicates (\"RTX 3060 (8 GB)\" vs \"(12 GB)\") are never decided by API list
order. Run `saladfingers gpu-classes` for the full list.";

/// Rent SaladCloud GPUs for minimum billed seconds.
#[derive(Debug, Parser)]
#[command(name = "saladfingers", version, about, long_about = None)]
pub struct Cli {
    /// Path to the global config file (default: `~/.config/saladfingers/config.toml`).
    #[arg(long, env = "SALADFINGERS_CONFIG", global = true)]
    pub config: Option<PathBuf>,

    /// Organization override.
    #[arg(long, env = "SALADFINGERS_ORG", global = true)]
    pub org: Option<String>,

    /// Project override.
    #[arg(long, env = "SALADFINGERS_PROJECT", global = true)]
    pub project: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Interactively write the global config.
    Init,
    /// Validate configuration; with `--live`, probe a rented GPU.
    Doctor(DoctorArgs),
    /// List GPU classes and per-priority prices.
    GpuClasses(GpuClassesArgs),
    /// Show organization quotas.
    Quotas(ReadArgs),
    /// Estimate the cost of a run.
    #[command(subcommand)]
    Cost(CostCommand),
    /// Run a one-shot batch job on a rented GPU.
    Run(RunArgs),
    /// Re-attach to a detached run.
    Attach(RunIdArgs),
    /// Open a loopback proxy onto a run's `--expose-port` gateway.
    ///
    /// The gateway is authenticated, so the exposed port is unreachable from
    /// the public internet — and equally unreachable from a browser, which
    /// cannot attach `Salad-Api-Key` to a navigation. This forwards
    /// `http://127.0.0.1:LOCAL_PORT` to it with the key attached, streaming
    /// responses so the dashboard's SSE survives. The key stays on this host.
    Tunnel(TunnelArgs),
    /// List runs merged with their live group states.
    Ls(LsArgs),
    /// Show a run's status.
    Status(RunIdArgs),
    /// Watch a run's state transitions (read-only).
    Watch(RunIdArgs),
    /// Cancel a run (stop and delete its groups).
    Cancel(RunIdArgs),
    /// Internal: detached reaper that stops a `--detach` run's groups once it
    /// finishes (or a hard cap elapses), so a detached run can't bill forever.
    #[command(hide = true)]
    Reap(RunIdArgs),
    /// Query a run's logs (works after group deletion).
    Logs(LogsArgs),
    /// Inspect, download, or delete an uploaded checkpoint.
    #[command(subcommand)]
    Checkpoint(CheckpointCommand),
    /// Garbage-collect leftover container groups.
    Gc(GcArgs),
    /// Interactive GPU dev session.
    #[command(subcommand)]
    Session(SessionCommand),
    /// Inference serving behind the gateway.
    #[command(subcommand)]
    Serve(ServeCommand),
    /// Benchmark node behavior.
    #[command(subcommand)]
    Bench(BenchCommand),
    /// Build and push GPU container images.
    #[command(subcommand)]
    Image(ImageCommand),
    /// Run the node environment probe on a rented GPU.
    GpuProbe(GpuProbeArgs),
}

/// Common flag for read commands.
#[derive(Debug, Args)]
pub struct ReadArgs {
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

/// Positional run id.
#[derive(Debug, Args)]
pub struct RunIdArgs {
    /// Run identifier (e.g. `sf-x7k2mq`).
    pub run_id: String,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TunnelArgs {
    /// Run identifier (e.g. `sf-x7k2mq`).
    pub run_id: String,
    /// Local loopback port to listen on.
    #[arg(long, default_value_t = DEFAULT_TUNNEL_PORT, value_parser = clap::value_parser!(u16).range(1..=65535))]
    pub local_port: u16,
    /// Which shard's gateway to tunnel to (each shard is its own group, so
    /// each has its own DNS name — per-instance routing is impossible).
    #[arg(long, default_value_t = 0)]
    pub shard: u32,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Also run a live GPU probe on the cheapest class.
    #[arg(long)]
    pub live: bool,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct GpuClassesArgs {
    /// Refresh the cached class list.
    #[arg(long)]
    pub refresh: bool,
    /// Include availability.
    #[arg(long)]
    pub availability: bool,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

/// Cost subcommands.
#[derive(Debug, Subcommand)]
pub enum CostCommand {
    /// Estimate the cost of a run.
    Estimate(CostEstimateArgs),
}

#[derive(Debug, Args)]
pub struct CostEstimateArgs {
    /// GPU class name or UUID.
    #[arg(long, long_help = GPU_CLASS_HELP)]
    pub gpu_class: String,
    /// Priority tier.
    #[arg(long, default_value = "batch")]
    pub priority: String,
    /// Number of hours.
    #[arg(long)]
    pub hours: f64,
    /// Number of replicas.
    #[arg(long, default_value_t = 1)]
    pub replicas: u32,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// zstd level for compressing `--input` uploads (1–22)
    /// [default: $SALADFINGERS_ZSTD_LEVEL, else 19].
    ///
    /// 19 rather than the library's 3 because staged inputs are the one
    /// payload worth compressing hard: a tokenized corpus goes 472 → 311 MiB,
    /// which on a slow uplink is ~13 minutes saved for ~3 of CPU. Applies only
    /// to what THIS process uploads — the agent compresses checkpoints on the
    /// node at the library default (unless the image itself sets
    /// SALADFINGERS_ZSTD_LEVEL), where f32 weights make a higher level pure
    /// waste (measured: byte-identical output for 39 s of extra CPU).
    #[arg(long, value_parser = clap::value_parser!(i32).range(1..=22))]
    pub input_zstd_level: Option<i32>,
    /// zstd window log for `--input` uploads (10–31 = 1 KiB–2 GiB), which also
    /// enables long-distance matching. Unset uses libzstd's per-level window.
    /// Worth ~2% on pre-tokenized ids; more on raw text.
    #[arg(long, value_parser = clap::value_parser!(u32).range(10..=31))]
    pub input_zstd_window_log: Option<u32>,
    /// Profile name from the project config.
    #[arg(long)]
    pub profile: Option<String>,
    /// Image reference override.
    #[arg(long)]
    pub image: Option<String>,
    /// GPU class name or UUID (repeatable = first-available).
    #[arg(long = "gpu-class", long_help = GPU_CLASS_HELP)]
    pub gpu_classes: Vec<String>,
    /// Host RAM in GiB. Overrides the profile; default 16.
    ///
    /// This is HOST RAM, not VRAM — the GPU class fixes VRAM. It gets a flag
    /// rather than staying profile-only because the failure it prevents is
    /// silent and expensive: the host OOM-kills the container, the run reports
    /// **exit 137**, and a benchmark that was killed and restarted mid-flight
    /// comes back with degraded numbers that look entirely ordinary.
    #[arg(long = "memory-gb")]
    pub memory_gb: Option<u32>,
    /// Number of shards (each a single-replica group). Given explicitly, it overrides
    /// the profile in BOTH directions — a profile's 8 with `--replicas 2` runs 2, not 8.
    /// Default: the profile's value, else 1.
    #[arg(long)]
    pub replicas: Option<u32>,
    /// Extra environment variables `KEY=VALUE`.
    #[arg(long = "env")]
    pub env: Vec<String>,
    /// Input `SRC[:DST]` to stage in.
    #[arg(long = "input")]
    pub inputs: Vec<String>,
    /// Output `GLOB[:NAME]` to ship out.
    #[arg(long = "output")]
    pub outputs: Vec<String>,
    /// Rent a CPU-only node: no GPU class is requested, so the group is placed
    /// on whatever host has the vCPU and RAM.
    ///
    /// Opt-in rather than inferred from an omitted `--gpu-class`, because a
    /// mistyped class name should fail loudly instead of quietly renting a
    /// CPU box and running a CUDA workload on it.
    #[arg(long, conflicts_with = "gpu_classes")]
    pub cpu_only: bool,
    /// Publish a container port through the SaladCloud gateway — for watching a
    /// live training dashboard while the run trains.
    ///
    /// The gateway is created with `auth=true`, so the port is NOT reachable
    /// from the public internet: the Cloudflare edge rejects anything without
    /// `Salad-Api-Key` before it reaches the container. A browser cannot send
    /// that header, so reach it with `saladfingers tunnel RUN_ID`, which proxies
    /// `http://127.0.0.1:7777` to the gateway with the key attached.
    ///
    /// The process in the container must listen on IPv6 `[::]`; the gateway
    /// answers 503 for one bound only to `0.0.0.0` or to loopback.
    #[arg(long, value_name = "PORT", value_parser = clap::value_parser!(u16).range(1..=65535))]
    pub expose_port: Option<u16>,
    /// Priority tier.
    #[arg(long)]
    pub priority: Option<String>,
    /// Country allow-list (ISO alpha-2).
    #[arg(long = "country")]
    pub countries: Vec<String>,
    /// Hard wall-clock budget (e.g. `45m`).
    #[arg(long)]
    pub max_duration: Option<String>,
    /// Disable the startup bandwidth gate.
    #[arg(long)]
    pub no_gate: bool,
    /// Checkpoint directory to save + restore across interruptions (e.g. `/work/ckpt`).
    /// The agent restores the latest checkpoint before the command starts and uploads it
    /// as it changes, so a re-run resumes from where it left off.
    #[arg(long)]
    pub checkpoint: Option<String>,
    /// How often (seconds) the agent scans the checkpoint dir for changes.
    #[arg(long, default_value_t = 30)]
    pub checkpoint_interval: u64,
    /// A checkpoint uploads once no file changed within this many seconds.
    #[arg(long, default_value_t = 15)]
    pub checkpoint_quiesce: u64,
    /// Store the checkpoint under a shared name instead of inside this run, so a later
    /// run with the same name resumes from it (e.g. `--checkpoint-prefix tinystories-77m`).
    /// Without it the checkpoint is reaped together with the run that wrote it.
    #[arg(long, value_name = "NAME")]
    pub checkpoint_prefix: Option<String>,
    /// Create the groups and return immediately.
    #[arg(long)]
    pub detach: bool,
    /// Human-readable label for the run.
    #[arg(long)]
    pub name_hint: Option<String>,
    /// The command to run on the GPU (after `--`).
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct LsArgs {
    /// Include terminal/old runs.
    #[arg(long)]
    pub all: bool,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Run identifier.
    pub run_id: String,
    /// Follow the log stream.
    ///
    /// Tails a rolling window of its own, so it takes neither `--since`, `--limit`,
    /// nor `--all` — they are refused rather than silently ignored.
    #[arg(long)]
    pub follow: bool,
    /// Print at most this many entries per group, keeping the newest.
    #[arg(long, default_value_t = 1000, conflicts_with = "follow")]
    pub limit: usize,
    /// Print every entry in the window (no `--limit` cap).
    #[arg(long, conflicts_with = "follow")]
    pub all: bool,
    /// How far back to search (e.g. `90m`, `6h`).
    #[arg(long, default_value = "24h", conflicts_with = "follow")]
    pub since: String,
    /// Print the complete copy the agent uploaded to storage instead of querying the
    /// platform's log service.
    #[arg(long, conflicts_with = "follow")]
    pub uploaded: bool,
    /// Shard whose uploaded output to print.
    ///
    /// Only meaningful with `--uploaded`, which addresses one shard's storage key;
    /// the platform query covers every shard's group at once.
    #[arg(long, default_value_t = 0, requires = "uploaded")]
    pub shard: u32,
}

/// Checkpoint subcommands.
///
/// The agent writes checkpoints into a rotating slot, so which key holds the current one
/// is not predictable from the run id alone — the metadata object is the index, and these
/// commands are how an operator reads it.
#[derive(Debug, Subcommand)]
pub enum CheckpointCommand {
    /// Show the uploaded checkpoint's metadata (step, size, age) without downloading it.
    Show(CheckpointArgs),
    /// Download and extract the uploaded checkpoint.
    Fetch(CheckpointFetchArgs),
    /// Delete a shared checkpoint. `gc` never reaps these — that is what makes them
    /// shared — so this is the only way to remove one.
    Rm(CheckpointRmArgs),
}

#[derive(Debug, Args)]
pub struct CheckpointRmArgs {
    /// Shared checkpoint name, as passed to `run --checkpoint-prefix`. Every shard's copy
    /// goes with it.
    #[arg(long, value_name = "NAME")]
    pub prefix: String,
    /// Skip the confirmation prompt.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("ckpt-target").required(true).args(["run_id", "prefix"])))]
pub struct CheckpointArgs {
    /// Run identifier (e.g. `sf-x7k2mq`) — reads the checkpoint stored inside that run.
    pub run_id: Option<String>,
    /// Shared checkpoint name, as passed to `run --checkpoint-prefix`. Reads the
    /// run-independent checkpoint instead, which is the one that outlives its run.
    #[arg(long, value_name = "NAME")]
    pub prefix: Option<String>,
    /// Shard whose checkpoint to read.
    #[arg(long, default_value_t = 0)]
    pub shard: u32,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct CheckpointFetchArgs {
    #[command(flatten)]
    pub target: CheckpointArgs,
    /// Directory to extract into. Defaults to `./sf-out/<run-id-or-prefix>/<shard>/ckpt`.
    #[arg(long)]
    pub dest: Option<String>,
}

#[derive(Debug, Args)]
pub struct GcArgs {
    /// Only reap groups older than this (e.g. `24h`).
    #[arg(long, default_value = "24h")]
    pub older_than: String,
    /// Show what would be reaped without deleting.
    #[arg(long)]
    pub dry_run: bool,
    /// Do not prompt for confirmation.
    #[arg(long)]
    pub yes: bool,
}

/// Session subcommands.
#[derive(Debug, Subcommand)]
pub enum SessionCommand {
    /// Create an interactive GPU session.
    Create(SessionCreateArgs),
    /// List active sessions.
    Ls(ReadArgs),
    /// Run a command in a session (exit code is propagated).
    Exec(SessionExecArgs),
    /// Copy files to/from a session.
    Cp(SessionCpArgs),
    /// Show a session's logs.
    Logs(SessionLogsArgs),
    /// Stop a session (billing ends).
    Stop(SessionNameArgs),
    /// Remove a session's group.
    Rm(SessionNameArgs),
}

#[derive(Debug, Args)]
pub struct SessionCreateArgs {
    /// Profile name.
    #[arg(long)]
    pub profile: Option<String>,
    /// Image reference (overrides the profile).
    #[arg(long)]
    pub image: Option<String>,
    /// GPU class name or UUID (repeatable; overrides the profile).
    #[arg(long = "gpu-class", long_help = GPU_CLASS_HELP)]
    pub gpu_classes: Vec<String>,
    /// Priority tier (`high|medium|low|batch`; overrides the profile).
    #[arg(long)]
    pub priority: Option<String>,
    /// Maximum session duration (e.g. `4h`).
    #[arg(long, default_value = "4h")]
    pub max_duration: String,
    /// Deadman idle timeout (e.g. `15m`).
    #[arg(long, default_value = "15m")]
    pub deadman: String,
    /// Session name.
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Debug, Args)]
pub struct SessionNameArgs {
    /// Session name.
    pub name: String,
}

#[derive(Debug, Args)]
pub struct SessionExecArgs {
    /// Session name.
    pub name: String,
    /// The command to run (after `--`).
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct SessionCpArgs {
    /// Source (`NAME:PATH` or a local path).
    pub source: String,
    /// Destination (`NAME:PATH` or a local path).
    pub dest: String,
    /// Transfer chunk size (e.g. `32M`).
    #[arg(long, default_value = "32M")]
    pub chunk_size: String,
}

#[derive(Debug, Args)]
pub struct SessionLogsArgs {
    /// Session name.
    pub name: String,
    /// Specific exec id.
    pub exec_id: Option<String>,
}

/// Serve subcommands.
#[derive(Debug, Subcommand)]
pub enum ServeCommand {
    /// Bring up an inference service.
    Up(ServeUpArgs),
    /// Show a service's status and gateway URL.
    Status(SessionNameArgs),
    /// Foreground watchdog that stops the service when idle.
    Autostop(ServeAutostopArgs),
    /// Stop a service.
    Down(SessionNameArgs),
    /// Restart a stopped service.
    Resume(SessionNameArgs),
    /// Remove a service's group.
    Rm(SessionNameArgs),
}

#[derive(Debug, Args)]
pub struct ServeUpArgs {
    /// Profile name.
    #[arg(long)]
    pub profile: Option<String>,
    /// Image reference (overrides the profile).
    #[arg(long)]
    pub image: Option<String>,
    /// GPU class name or UUID (repeatable; overrides the profile).
    #[arg(long = "gpu-class", long_help = GPU_CLASS_HELP)]
    pub gpu_classes: Vec<String>,
    /// Priority tier (`high|medium|low|batch`; overrides the profile).
    #[arg(long)]
    pub priority: Option<String>,
    /// Service name.
    #[arg(long)]
    pub name: Option<String>,
    /// Maximum service duration (e.g. `4h`).
    #[arg(long, default_value = "4h")]
    pub max_duration: String,
    /// The app's listen port inside the container.
    #[arg(long)]
    pub app_port: u16,
    /// The command to run (after `--`).
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct ServeAutostopArgs {
    /// Service name.
    pub name: String,
    /// Stop after this idle period (e.g. `30m`).
    #[arg(long, default_value = "30m")]
    pub idle_timeout: String,
}

/// Bench subcommands.
#[derive(Debug, Subcommand)]
pub enum BenchCommand {
    /// Measure cold-start times for a class/image.
    Startup(BenchStartupArgs),
}

#[derive(Debug, Args)]
pub struct BenchStartupArgs {
    /// GPU class name or UUID.
    #[arg(long, long_help = GPU_CLASS_HELP)]
    pub gpu_class: String,
    /// Image reference.
    #[arg(long)]
    pub image: Option<String>,
    /// Number of samples.
    #[arg(short = 'n', long, default_value_t = 5)]
    pub count: u32,
    /// Priority tier.
    #[arg(long, default_value = "batch")]
    pub priority: String,
}

/// Image subcommands.
#[derive(Debug, Subcommand)]
pub enum ImageCommand {
    /// Build and push a GPU image, recording its digest.
    Push(ImagePushArgs),
}

#[derive(Debug, Args)]
pub struct ImagePushArgs {
    /// Image name (a `saladfingers.images.<name>`).
    pub name: String,
    /// Tag to push (the recorded lockfile ref is always digest-pinned).
    #[arg(long, default_value = "latest")]
    pub tag: String,
    /// Build and push on this SSH host instead of locally, so the image closure never
    /// crosses this machine's link (it is substituted straight onto the remote).
    #[arg(long, value_name = "SSH_HOST")]
    pub on: Option<String>,
    /// Flake system whose `<name>-image` attribute to build. Defaults to this machine's
    /// own system on macOS and `x86_64-linux` elsewhere; with `--on`, to `x86_64-linux`.
    /// The image itself is linux/amd64 regardless.
    #[arg(long)]
    pub system: Option<String>,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct GpuProbeArgs {
    /// GPU class name or UUID to probe on.
    #[arg(long, default_value = DEFAULT_PROBE_GPU_CLASS, long_help = GPU_CLASS_HELP)]
    pub gpu_class: String,
    /// Probe image ref (else `SALADFINGERS_PROBE_IMAGE`).
    #[arg(long)]
    pub image: Option<String>,
    /// Emit JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}
