// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! `saladfingers session …` — interactive GPU dev boxes backed by `sf-agent serve`.
//!
//! `create` deploys a single-replica group behind the gateway (`auth=true`) running
//! the agent's session API, with a bearer token and the deadman/max-duration timers
//! that make an idle box self-stop. `exec`/`cp`/`logs` then talk to that agent through
//! the gateway (each request carries the Salad key for the gateway and the bearer for
//! the agent), within the gateway's 100 s / 1 GB limits: output is long-polled and
//! files move in bounded, resumable chunks.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use chrono::Utc;
use reqwest::Method;
use saladfingers_api::{RestartPolicy, SaladClient};
use saladfingers_protocol::agent_api::{
    self, ExecCreated, ExecRequest, FileStat, Health, OutputPage, UploadInit, UploadInitResponse,
    UploadStatus,
};
use sha2::{Digest, Sha256};

use crate::cli::{
    ReadArgs, SessionCpArgs, SessionCreateArgs, SessionExecArgs, SessionLogsArgs, SessionNameArgs,
};
use crate::config::Config;
use crate::deploy::{self, GroupParams, PollOptions};
use crate::output::{OutputFormat, table};
use crate::{names, state};

/// Default dev-box resources. Modest so allocation is reliable at batch; a profile can
/// raise them later.
const SESSION_CPU: u32 = 2;
const SESSION_MEMORY_MB: u32 = 8192;
const SESSION_DISK_GIB: u64 = 20;
/// The port `sf-agent serve` binds inside the container.
const AGENT_PORT: u16 = 8888;
/// Output long-poll wait (server caps at 30 s).
const OUTPUT_WAIT_MS: u64 = 25_000;

/// `saladfingers session create`
pub async fn create(cfg: Config, args: SessionCreateArgs) -> Result<()> {
    let client = cfg.client()?;
    let profile = match &args.profile {
        Some(p) => Some(cfg.profile(p)?.clone()),
        None => None,
    };
    let image = crate::image::resolve_deploy_image(
        args.image.as_deref(),
        profile.as_ref().and_then(|p| p.image.as_deref()),
    )?;
    let mut gpu_classes = args.gpu_classes.clone();
    if gpu_classes.is_empty() {
        gpu_classes = profile
            .as_ref()
            .map(|p| p.gpu_classes.clone())
            .unwrap_or_default();
    }
    if gpu_classes.is_empty() {
        bail!("no GPU class (pass --gpu-class or set gpu_classes in the profile)");
    }
    let priority_str = args
        .priority
        .clone()
        .or_else(|| profile.as_ref().and_then(|p| p.priority.clone()))
        .unwrap_or_else(|| "batch".to_string());
    let priority = deploy::parse_priority(&priority_str)?;

    let name = args.name.clone().unwrap_or_else(names::generate_run_id);
    let token = random_token();
    let deadman_secs = humantime::parse_duration(&args.deadman)
        .with_context(|| format!("invalid --deadman {:?}", args.deadman))?
        .as_secs();
    let max_secs = humantime::parse_duration(&args.max_duration)
        .with_context(|| format!("invalid --max-duration {:?}", args.max_duration))?
        .as_secs();

    let uuids = deploy::resolve_gpu_uuids(&client, &gpu_classes, false).await?;
    let mut env = profile.as_ref().map(|p| p.env.clone()).unwrap_or_default();
    env.insert("SF_AGENT_TOKEN".into(), token.clone());
    env.insert("SF_PORT".into(), AGENT_PORT.to_string());
    env.insert("SF_DEADMAN_SECS".into(), deadman_secs.to_string());
    env.insert("SF_MAX_DURATION_SECS".into(), max_secs.to_string());

    let request = deploy::build_request(GroupParams {
        name: name.clone(),
        image: image.clone(),
        gpu_uuids: uuids,
        priority,
        cpu: SESSION_CPU,
        memory_mb: SESSION_MEMORY_MB,
        disk_gib: SESSION_DISK_GIB,
        command: Some(vec!["/bin/sf-agent".into(), "serve".into()]),
        env,
        gateway_port: Some(AGENT_PORT),
        gateway_auth: true,
        registry_auth: deploy::registry_auth(&cfg),
        restart_policy: RestartPolicy::Never,
        country_codes: vec![],
        shm_mb: None,
    });

    eprintln!("creating session {name} on {gpu_classes:?} (priority {priority:?})...");
    client.create_container_group(&request).await?;

    let mut run = state::RunState {
        v: 1,
        run_id: name.clone(),
        kind: "session".into(),
        created_at: Utc::now(),
        org: cfg.organization.clone(),
        project: cfg.project.clone(),
        profile: args.profile.clone(),
        image: Some(image),
        gpu_classes: gpu_classes.clone(),
        priority: Some(priority_str),
        command: vec![],
        output_names: None,
        max_parts: None,
        groups: vec![state::GroupRef {
            name: name.clone(),
            shard: 0,
            last_state: Some("creating".into()),
            machine_history: vec![],
            running_spans: vec![],
        }],
        status: "creating".into(),
        agent_token: Some(token.clone()),
        max_duration_secs: Some(max_secs),
        result: None,
    };
    state::save_run(&run)?;

    // Wait for the instance to run, then for the agent to answer healthz. Any failure
    // here must not leak the group: it was just created and is (or will be) billing.
    let ready = async {
        deploy::poll_until_running(&client, &name, &PollOptions::default()).await?;
        let agent = connect(&cfg, &client, &name, &token).await?;
        let health = agent.wait_healthy(Duration::from_secs(90)).await?;
        Ok::<_, anyhow::Error>((agent, health))
    }
    .await;
    let (agent, health) = match ready {
        Ok(v) => v,
        Err(e) => {
            eprintln!("session {name} failed to become ready; deleting the group to stop billing");
            let _ = deploy::delete_group(&client, &name).await;
            run.status = "failed".into();
            let _ = state::save_run(&run);
            return Err(e);
        }
    };

    run.status = "running".into();
    if let Some(g) = run.groups.first_mut() {
        g.last_state = Some("running".into());
    }
    state::save_run(&run)?;

    // Self-exit does not stop billing (the platform relaunches the container on every
    // exit — E1/E2), so a detached reaper watches for the box to expire or relaunch
    // (fresh boot_id = deadman/max-duration fired and all session state is gone) and
    // deletes the group. `session rm` remains the interactive path.
    match crate::runner::spawn_reaper(&name, &cfg.organization, &cfg.project) {
        Ok(()) => {}
        Err(e) => eprintln!(
            "warning: session reaper failed to start ({e:#}); remember `saladfingers session rm \
             {name}` — an abandoned box keeps billing until deleted"
        ),
    }

    println!(
        "session {name} ready (boot {}, gateway {})",
        health.boot_id, agent.base
    );
    println!("  exec:  saladfingers session exec {name} -- <cmd>");
    println!(
        "  stop:  saladfingers session stop {name}   (idle self-stops after {})",
        args.deadman
    );
    Ok(())
}

/// `saladfingers session exec NAME -- CMD...` — run a command, stream output, exit with
/// the remote command's code.
pub async fn exec(cfg: Config, args: SessionExecArgs) -> Result<()> {
    let client = cfg.client()?;
    let (agent, _run) = resolve(&cfg, &client, &args.name).await?;
    let created: ExecCreated = agent
        .json(
            Method::POST,
            agent_api::route::EXEC,
            Some(&ExecRequest {
                argv: args.command.clone(),
                workdir: None,
                env: None,
            }),
        )
        .await
        .context("starting exec")?;

    let mut cursor = 0u64;
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();
    let exit_code = loop {
        let page: OutputPage = agent
            .json::<(), _>(
                Method::GET,
                &format!(
                    "{}?cursor={cursor}&wait_ms={OUTPUT_WAIT_MS}",
                    agent_api::route::exec_output(&created.exec_id)
                ),
                None,
            )
            .await
            .context("polling exec output")?;
        for chunk in &page.chunks {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&chunk.data_b64)
                .unwrap_or_default();
            match chunk.stream {
                agent_api::Stream::Stdout => stdout.write_all(&bytes)?,
                agent_api::Stream::Stderr => stderr.write_all(&bytes)?,
            }
        }
        stdout.flush()?;
        stderr.flush()?;
        if page.truncated {
            eprintln!("saladfingers: warning: output ring overflowed; some bytes were skipped");
        }
        cursor = page.next_cursor;
        if page.exited {
            break page.exit_code.unwrap_or(0);
        }
    };
    // Propagate the remote exit code (ssh convention; needed for CI `session exec`).
    std::process::exit(exit_code);
}

/// `saladfingers session cp SRC DST` — copy between local paths and `NAME:PATH`.
pub async fn cp(cfg: Config, args: SessionCpArgs) -> Result<()> {
    let client = cfg.client()?;
    let chunk_bytes = parse_size(&args.chunk_size)?;
    match (parse_remote(&args.source), parse_remote(&args.dest)) {
        (Some((name, remote)), None) => {
            let (agent, _) = resolve(&cfg, &client, name).await?;
            download(&agent, &remote, Path::new(&args.dest), chunk_bytes).await
        }
        (None, Some((name, remote))) => {
            let (agent, _) = resolve(&cfg, &client, name).await?;
            upload(&agent, Path::new(&args.source), &remote, chunk_bytes).await
        }
        (Some(_), Some(_)) => bail!("cannot copy between two sessions"),
        (None, None) => bail!("one of SRC/DST must be NAME:PATH"),
    }
}

/// `saladfingers session ls`
pub async fn ls(_cfg: Config, args: ReadArgs) -> Result<()> {
    let sessions: Vec<state::RunState> = state::list_runs()?
        .into_iter()
        .filter(|r| r.kind == "session")
        .collect();
    if let OutputFormat::Json = OutputFormat::from_json_flag(args.json) {
        crate::output::print_json(&sessions)?;
        return Ok(());
    }
    let mut t = table(&["name", "status", "gpu", "created"]);
    for s in &sessions {
        t.add_row(vec![
            s.run_id.clone(),
            s.status.clone(),
            s.gpu_classes.first().cloned().unwrap_or_default(),
            s.created_at.format("%Y-%m-%d %H:%M").to_string(),
        ]);
    }
    println!("{t}");
    Ok(())
}

/// `saladfingers session logs NAME [EXEC]`
pub async fn logs(cfg: Config, args: SessionLogsArgs) -> Result<()> {
    match &args.exec_id {
        Some(exec_id) => {
            let client = cfg.client()?;
            let (agent, _) = resolve(&cfg, &client, &args.name).await?;
            // Replay the exec's retained output ring from the start.
            let page: OutputPage = agent
                .json::<(), _>(
                    Method::GET,
                    &format!(
                        "{}?cursor=0&wait_ms=0",
                        agent_api::route::exec_output(exec_id)
                    ),
                    None,
                )
                .await?;
            let mut out = std::io::stdout();
            for chunk in &page.chunks {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&chunk.data_b64)
                    .unwrap_or_default();
                out.write_all(&bytes)?;
            }
            out.flush()?;
            if page.truncated {
                eprintln!("saladfingers: note: earlier output was evicted from the ring");
            }
            Ok(())
        }
        // No exec id: fall back to the container's stdout via the org log query.
        None => logs_via_container(cfg, &args.name).await,
    }
}

async fn logs_via_container(cfg: Config, name: &str) -> Result<()> {
    crate::logs::logs(
        cfg,
        crate::cli::LogsArgs {
            run_id: name.to_string(),
            follow: false,
        },
    )
    .await
}

/// `saladfingers session stop NAME` — stop the group (billing ends when the instance dies).
pub async fn stop(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    client.stop_container_group(&args.name).await?;
    if let Some(mut run) = state::load_run(&args.name)? {
        run.status = "stopped".into();
        state::save_run(&run)?;
    }
    println!("stopped session {}", args.name);
    Ok(())
}

/// `saladfingers session rm NAME` — delete the group and forget the session.
pub async fn rm(cfg: Config, args: SessionNameArgs) -> Result<()> {
    let client = cfg.client()?;
    deploy::delete_group(&client, &args.name).await?;
    state::delete_run(&args.name)?;
    println!("removed session {}", args.name);
    Ok(())
}

// ---- agent client over the gateway ----

/// Talks to one session's `sf-agent serve` through the gateway. Every request carries
/// the Salad key (gateway `auth=true`) and the bearer token (agent auth).
struct AgentClient {
    http: reqwest::Client,
    base: String,
    api_key: String,
    token: String,
}

impl AgentClient {
    fn new(base: String, api_key: String, token: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(180))
            .build()?;
        Ok(Self {
            http,
            base,
            api_key,
            token,
        })
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .header("Salad-Api-Key", &self.api_key)
            .bearer_auth(&self.token)
    }

    /// Send an optional JSON body and decode a JSON response, erroring on non-2xx.
    async fn json<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let mut req = self.request(method, path);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!(
                "agent {status}: {}{}",
                text.trim(),
                gateway_503_hint(status)
            );
        }
        Ok(resp.json().await?)
    }

    async fn wait_healthy(&self, timeout: Duration) -> Result<Health> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let err = match self
                .request(Method::GET, agent_api::route::HEALTHZ)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => return Ok(r.json().await?),
                Ok(r) => format!("status {}{}", r.status(), gateway_503_hint(r.status())),
                Err(e) => e.to_string(),
            };
            if std::time::Instant::now() >= deadline {
                bail!("session agent did not become healthy: {err}");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// The classic gateway footgun, spelled out: a persistent 503 from the SaladCloud
/// gateway means nothing inside the container is listening on IPv6 `[::]` on the
/// gateway port (binding `0.0.0.0` is not enough), or the container is still booting.
fn gateway_503_hint(status: reqwest::StatusCode) -> &'static str {
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        " (a persistent gateway 503 usually means the process in the container is not \
         listening on IPv6 [::] on the gateway port, or the container is still starting)"
    } else {
        ""
    }
}

/// Look up a running session's gateway + token and build a client.
async fn resolve(
    cfg: &Config,
    client: &SaladClient,
    name: &str,
) -> Result<(AgentClient, state::RunState)> {
    let run = state::load_run(name)?
        .filter(|r| r.kind == "session")
        .with_context(|| format!("no such session {name}"))?;
    let token = run
        .agent_token
        .clone()
        .context("session has no agent token in local state")?;
    let agent = connect(cfg, client, name, &token).await?;
    Ok((agent, run))
}

/// Fetch the group's live gateway URL and build an [`AgentClient`].
async fn connect(
    cfg: &Config,
    client: &SaladClient,
    name: &str,
    token: &str,
) -> Result<AgentClient> {
    let group = client.get_container_group(name).await?;
    let gateway = group
        .gateway_url()
        .with_context(|| format!("session {name} exposes no gateway yet"))?;
    AgentClient::new(gateway, cfg.api_key.expose().to_string(), token.to_string())
}

/// The live agent's `boot_id` via the gateway healthz, or `None` when unreachable (no
/// gateway yet, box mid-relaunch, transient error). The session reaper compares
/// successive values: a changed `boot_id` means the container relaunched and every
/// exec/upload the user had is gone — the box only bills from then on.
pub(crate) async fn probe_boot_id(
    cfg: &Config,
    client: &SaladClient,
    name: &str,
) -> Option<String> {
    let group = client.get_container_group(name).await.ok()?;
    let gateway = group.gateway_url()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .ok()?;
    let resp = http
        .get(format!("{gateway}{}", agent_api::route::HEALTHZ))
        .header("Salad-Api-Key", cfg.api_key.expose())
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Health>().await.ok().map(|h| h.boot_id)
}

// ---- file transfer ----

async fn upload(agent: &AgentClient, local: &Path, remote: &str, chunk_bytes: u64) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    // Stream from disk one chunk at a time — never the whole file in memory. A dev-box
    // `cp` routinely moves multi-GB checkpoints/weights; buffering them whole would OOM
    // the CLI while the transfer protocol is chunked anyway.
    let mut file =
        std::fs::File::open(local).with_context(|| format!("opening {}", local.display()))?;
    let size = file.metadata()?.len();
    let sha = sha256_reader(&mut file).with_context(|| format!("hashing {}", local.display()))?;
    let init: UploadInitResponse = agent
        .json(
            Method::POST,
            agent_api::route::FILES_UPLOAD,
            Some(&UploadInit {
                path: remote.to_string(),
                size,
                sha256: sha,
                chunk_bytes,
            }),
        )
        .await
        .context("initialising upload")?;

    // Resume: skip any chunks the agent already holds for this upload id.
    let already: BTreeSet<u32> = agent
        .json::<(), UploadStatus>(
            Method::GET,
            &agent_api::route::upload_status(&init.upload_id),
            None,
        )
        .await
        .map(|s| s.received.into_iter().collect())
        .unwrap_or_default();

    let total_chunks = size.div_ceil(chunk_bytes).max(1);
    let mut buf = vec![0u8; usize::try_from(chunk_bytes).context("chunk size too large")?];
    for index in 0..total_chunks {
        if already.contains(&(index as u32)) {
            continue;
        }
        let offset = index * chunk_bytes;
        let len = chunk_bytes.min(size - offset) as usize;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf[..len])
            .with_context(|| format!("reading chunk {index} of {}", local.display()))?;
        put_chunk_with_retry(agent, &init.upload_id, index as u32, &buf[..len]).await?;
    }

    let stat: FileStat = agent
        .json::<(), _>(
            Method::POST,
            &agent_api::route::upload_complete(&init.upload_id),
            None,
        )
        .await
        .context("finalising upload (sha256 verified server-side)")?;
    println!("uploaded {} bytes → {remote}", stat.size);
    Ok(())
}

async fn put_chunk_with_retry(
    agent: &AgentClient,
    upload_id: &str,
    index: u32,
    bytes: &[u8],
) -> Result<()> {
    let path = agent_api::route::upload_chunk(upload_id, index);
    for attempt in 0..3u64 {
        let err = match agent
            .request(Method::PUT, &path)
            .body(bytes.to_vec())
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => format!("status {}", r.status()),
            Err(e) => e.to_string(),
        };
        if attempt == 2 {
            bail!("chunk {index} failed after retries: {err}");
        }
        tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
    }
    unreachable!("loop returns or bails")
}

async fn download(agent: &AgentClient, remote: &str, local: &Path, chunk_bytes: u64) -> Result<()> {
    let stat: FileStat = agent
        .json::<(), _>(
            Method::GET,
            &format!(
                "{}?path={}",
                agent_api::route::FILES_STAT,
                urlencode(remote)
            ),
            None,
        )
        .await
        .with_context(|| format!("stat {remote}"))?;

    // Resume from whatever is already on disk.
    let mut offset = std::fs::metadata(local)
        .map(|m| m.len())
        .unwrap_or(0)
        .min(stat.size);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // keep any partial bytes; set_len below trims to the resume point
        .open(local)
        .with_context(|| format!("opening {}", local.display()))?;
    file.set_len(offset)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;

    while offset < stat.size {
        let len = chunk_bytes.min(stat.size - offset);
        let path = format!(
            "{}?path={}&offset={offset}&len={len}",
            agent_api::route::FILES_DOWNLOAD,
            urlencode(remote)
        );
        let bytes = download_chunk_with_retry(agent, &path).await?;
        if bytes.is_empty() {
            break;
        }
        file.write_all(&bytes)?;
        offset += bytes.len() as u64;
    }
    file.flush()?;
    println!("downloaded {offset} bytes → {}", local.display());
    Ok(())
}

async fn download_chunk_with_retry(agent: &AgentClient, path: &str) -> Result<Vec<u8>> {
    for attempt in 0..3u64 {
        let err = match agent.request(Method::GET, path).send().await {
            Ok(r) if r.status().is_success() => return Ok(r.bytes().await?.to_vec()),
            Ok(r) => format!("status {}", r.status()),
            Err(e) => e.to_string(),
        };
        if attempt == 2 {
            bail!("download chunk failed after retries: {err}");
        }
        tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
    }
    unreachable!("loop returns or bails")
}

// ---- helpers ----

/// Split `NAME:PATH` into its parts, or `None` for a plain local path. A leading
/// segment with no `/` and a non-empty remainder marks a remote spec.
fn parse_remote(spec: &str) -> Option<(&str, String)> {
    let (head, tail) = spec.split_once(':')?;
    if head.is_empty() || head.contains('/') || tail.is_empty() {
        return None;
    }
    Some((head, tail.to_string()))
}

/// Parse a size like `32M`, `512K`, `1G`, or a plain byte count.
fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1024),
        Some('M' | 'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G' | 'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    let n: u64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid size {s:?}"))?;
    Ok((n * mult).max(1))
}

/// Streaming lowercase-hex SHA-256 of an open file, rewound to the start afterwards.
fn sha256_reader(file: &mut std::fs::File) -> Result<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// 256-bit random bearer token as lowercase hex.
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("os rng");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Minimal percent-encoding for a path in a query string.
fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_distinguishes_sessions_from_local_paths() {
        assert_eq!(
            parse_remote("dev:/work/out"),
            Some(("dev", "/work/out".to_string()))
        );
        assert_eq!(
            parse_remote("sf-abc123:data.bin"),
            Some(("sf-abc123", "data.bin".to_string()))
        );
        // Local paths (no colon, or a colon after a slash) are not remote.
        assert_eq!(parse_remote("/home/me/out"), None);
        assert_eq!(parse_remote("./rel/path"), None);
        assert_eq!(parse_remote("/abs/a:b"), None); // colon after a slash → local
        assert_eq!(parse_remote("dev:"), None); // empty remote path
    }

    #[test]
    fn parse_size_handles_suffixes() {
        assert_eq!(parse_size("32M").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_size("512K").unwrap(), 512 * 1024);
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("nope").is_err());
    }
}
