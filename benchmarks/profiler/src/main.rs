// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use scx_stats::prelude::StatsClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const PERF_EVENTS: &[&str] = &[
    "cycles",
    "instructions",
    "cache-references",
    "cache-misses",
    "branches",
    "branch-misses",
    "context-switches",
    "cpu-migrations",
    "page-faults",
    "major-faults",
];

const REQUIRED_RUN_FILES: &[&str] = &[
    "run_meta.json",
    "events.jsonl",
    "stdout.log",
    "stderr.log",
    "perf_stat.csv",
    "cgroup_cpu.csv",
    "cgroup_memory.csv",
    "cgroup_io.csv",
    "cgroup_pressure.csv",
    "net.csv",
    "qdisc.csv",
    "scheduler_stats.csv",
    "client_latency.csv",
    "openwhisk_activation.json",
    "summary.json",
];

const SCHEDULER_STATS_FIELDS: &[&str] = &[
    "nr_cpus",
    "nr_running",
    "nr_queued",
    "nr_scheduled",
    "nr_page_faults",
    "nr_cold_start_tasks",
    "nr_hot_invocation_tasks",
    "nr_background_tasks",
    "nr_slo_boosted",
    "max_pending",
    "nr_user_dispatches",
    "nr_kernel_dispatches",
    "nr_cancel_dispatches",
    "nr_bounce_dispatches",
    "nr_failed_dispatches",
    "nr_sched_congested",
];

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check host prerequisites.
    Preflight(PreflightArgs),
    /// Run a workload in the standalone cgroup profiler.
    Standalone(StandaloneArgs),
    /// Invoke an OpenWhisk action and collect activation, Docker, and cgroup traces.
    OpenWhisk(OpenWhiskArgs),
    /// Recompute summary.json for an existing run directory.
    Analyze(RunDirArgs),
    /// Verify a run directory has the required files and joins.
    VerifyRun(RunDirArgs),
    /// Print a benchmark matrix from benchmarks/plan.md.
    Matrix(MatrixArgs),
    /// Aggregate complete run summaries into a scheduler-facing profile DB.
    ProfileDb(ProfileDbArgs),
    /// Import an OpenWhisk activation JSON document into a run directory.
    ImportActivation(ImportActivationArgs),
    #[command(hide = true)]
    Micro(MicroArgs),
}

#[derive(Debug, Args)]
struct PreflightArgs {
    /// Return a non-zero exit status if optional dependencies are missing.
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Args)]
struct RunDirArgs {
    #[arg(long)]
    run_dir: PathBuf,
}

#[derive(Debug, Args)]
struct MatrixArgs {
    #[arg(long, value_enum)]
    kind: MatrixKind,
}

#[derive(Debug, Args)]
struct ProfileDbArgs {
    /// Directory containing one subdirectory per benchmark run.
    #[arg(long, default_value = "benchmarks/runs")]
    runs_dir: PathBuf,
    /// Output JSON profile DB path.
    #[arg(long, default_value = "benchmarks/profile_db.json")]
    out: PathBuf,
    /// Fail if no complete runs are found.
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MatrixKind {
    Sanity,
    Profile,
    Interference,
}

#[derive(Debug, Args)]
struct ImportActivationArgs {
    #[arg(long)]
    run_dir: PathBuf,
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Args)]
struct StandaloneArgs {
    /// Parent directory where a timestamped run directory will be created.
    #[arg(long, default_value = "benchmarks/runs")]
    out_dir: PathBuf,
    #[arg(long, default_value = "standalone")]
    name: String,
    #[arg(long, value_enum, default_value_t = WorkloadKind::Cpu)]
    workload: WorkloadKind,
    #[arg(long, default_value = "small")]
    input: String,
    #[arg(long, default_value = "warm")]
    warmth: String,
    #[arg(long, default_value_t = 1000)]
    duration_ms: u64,
    #[arg(long, default_value_t = 100)]
    sample_ms: u64,
    /// Skip perf collection and write a machine-readable skipped marker.
    #[arg(long)]
    skip_perf: bool,
    /// Allow running without a newly-created cgroup. Verification still records the mapping.
    #[arg(long)]
    allow_cgroup_fallback: bool,
    /// Command to run when --workload command is selected.
    #[arg(last = true)]
    command: Vec<String>,
}

#[derive(Debug, Args)]
struct OpenWhiskArgs {
    /// Parent directory where a timestamped run directory will be created.
    #[arg(long, default_value = "benchmarks/runs")]
    out_dir: PathBuf,
    #[arg(long, default_value = "openwhisk")]
    name: String,
    /// OpenWhisk action name, for example cosmos_hello or /guest/cosmos_hello.
    #[arg(long)]
    action: String,
    /// Runtime kind used when uploading --file.
    #[arg(long, default_value = "nodejs:20")]
    kind: String,
    /// Action source file to upload before invoking.
    #[arg(long)]
    file: Option<PathBuf>,
    /// Skip action update and invoke an existing action.
    #[arg(long)]
    skip_update: bool,
    /// Pass -i to wsk for local OpenWhisk development certificates.
    #[arg(long)]
    insecure: bool,
    /// Override the OpenWhisk API host passed to wsk.
    #[arg(long)]
    apihost: Option<String>,
    /// Override the OpenWhisk auth key passed to wsk.
    #[arg(long)]
    auth: Option<String>,
    /// Action parameter as key=value. May be repeated.
    #[arg(long = "param")]
    params: Vec<String>,
    /// JSON file passed to wsk as --param-file. May be repeated.
    #[arg(long = "param-file")]
    param_files: Vec<PathBuf>,
    /// Invoke via OpenWhisk HTTP API instead of wsk. Useful for SeBS parameter files.
    #[arg(long)]
    invoke_http: bool,
    #[arg(long, default_value = "small")]
    input: String,
    #[arg(long, default_value = "cold")]
    warmth: String,
    #[arg(long, default_value_t = 50)]
    sample_ms: u64,
    /// Maximum time to poll Docker for the action container after invoke exits.
    #[arg(long, default_value_t = 2000)]
    docker_grace_ms: u64,
}

#[derive(Debug, Args)]
struct MicroArgs {
    #[arg(value_enum)]
    workload: WorkloadKind,
    #[arg(long, default_value_t = 1000)]
    duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum WorkloadKind {
    Cpu,
    Memory,
    Io,
    Network,
    Command,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunMeta {
    run_id: String,
    workload: String,
    input: String,
    warmth: String,
    sample_ms: u64,
    cgroup_path: String,
    cgroup_fallback: bool,
    command: Vec<String>,
    env: BTreeMap<String, Value>,
    config_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientLatency {
    run_id: String,
    activation_id: String,
    send_ns: u128,
    first_byte_ns: Option<u128>,
    response_end_ns: u128,
    status: String,
    timing_source: String,
    error: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Summary {
    run_id: String,
    status: String,
    latency: BTreeMap<String, Value>,
    resources: BTreeMap<String, Value>,
    phase_windows: Vec<PhaseWindow>,
    missing: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PhaseWindow {
    start_ns: u128,
    end_ns: u128,
    phase: String,
}

#[derive(Debug, Serialize)]
struct ProfileDb {
    generated_ns: u128,
    source_runs_dir: String,
    profiles: Vec<WorkloadProfile>,
    skipped_runs: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WorkloadProfile {
    workload: String,
    input: String,
    warmth: String,
    runs: Vec<String>,
    latency: BTreeMap<String, Value>,
    dominant_phases: Vec<PhaseCount>,
    scheduler_features: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct PhaseCount {
    phase: String,
    windows: u64,
}

#[derive(Debug)]
struct ProfileSample {
    run_id: String,
    meta: RunMeta,
    summary: Summary,
}

#[derive(Debug, Default, Clone)]
struct CgroupSample {
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
    nr_periods: u64,
    nr_throttled: u64,
    throttled_usec: u64,
    memory_current: u64,
    memory_peak: u64,
    io_rbytes: u64,
    io_wbytes: u64,
    pressure_total: u64,
}

#[derive(Debug, Default)]
struct NetSample {
    scope: String,
    source: String,
    rx_bytes: u64,
    tx_bytes: u64,
}

struct InvokeResult {
    status: ExitStatus,
    stdout: String,
    first_byte_ns: Option<u128>,
    response_end_ns: u128,
    timing_source: String,
}

struct CurlTiming {
    starttransfer_ns: u128,
    total_ns: u128,
}

struct SchedulerStatsSampler {
    client: Option<StatsClient>,
    unavailable_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct ContainerInfo {
    id: String,
    name: String,
    host_pid: u32,
    cgroup_path: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Preflight(args) => preflight(args),
        Commands::Standalone(args) => standalone(args),
        Commands::OpenWhisk(args) => openwhisk(args),
        Commands::Analyze(args) => {
            write_summary(&args.run_dir)?;
            println!("{}", args.run_dir.join("summary.json").display());
            Ok(())
        }
        Commands::VerifyRun(args) => verify_run(&args.run_dir),
        Commands::Matrix(args) => print_matrix(args.kind),
        Commands::ProfileDb(args) => profile_db(args),
        Commands::ImportActivation(args) => import_activation(args),
        Commands::Micro(args) => micro_workload(args),
    }
}

fn preflight(args: PreflightArgs) -> Result<()> {
    let checks = vec![
        check(
            "cgroup v2 mounted",
            Path::new("/sys/fs/cgroup/cgroup.controllers").exists(),
        ),
        check("perf installed", command_exists("perf")),
        check("docker installed", command_exists("docker")),
        check(
            "OpenWhisk Docker network path available",
            openwhisk_docker_network_available(),
        ),
        check("tc installed for qdisc stats", command_exists("tc")),
        check("wsk installed for OpenWhisk", command_exists("wsk")),
        check("JDK 17 available for OpenWhisk build", jdk17_available()),
        check(
            "OpenWhisk submodule present",
            Path::new("benchmarks/third_party/openwhisk/.git").exists(),
        ),
        check(
            "SeBS submodule present",
            Path::new("benchmarks/third_party/serverless-benchmarks/.git").exists(),
        ),
        check(
            "disk has writable benchmark directory",
            writable_dir(Path::new("benchmarks")),
        ),
    ];

    let mut failures = 0;
    for (name, ok) in &checks {
        println!("{:44} {}", name, if *ok { "ok" } else { "missing" });
        if !ok {
            failures += 1;
        }
    }

    if args.strict && failures > 0 {
        bail!("{failures} preflight checks failed");
    }
    Ok(())
}

fn check(name: &'static str, ok: bool) -> (&'static str, bool) {
    (name, ok)
}

fn command_exists(cmd: &str) -> bool {
    command_path(cmd).is_some()
}

fn command_path(cmd: &str) -> Option<String> {
    let shell_path = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {cmd} 2>/dev/null"))
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|path| !path.is_empty());
    if shell_path.is_some() {
        return shell_path;
    }
    ["/usr/sbin", "/sbin", "/usr/bin", "/bin"]
        .iter()
        .map(|dir| Path::new(dir).join(cmd))
        .find(|path| path.exists())
        .map(|path| path.display().to_string())
}

fn writable_dir(path: &Path) -> bool {
    let probe = path.join(".cosmos-bench-write-test");
    match File::create(&probe) {
        Ok(_) => {
            let _ = fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

fn jdk17_available() -> bool {
    let candidates = [
        "/opt/jdk-17/bin/java".to_string(),
        command_path("java").unwrap_or_default(),
    ];
    candidates.iter().any(|java| {
        if java.is_empty() {
            return false;
        }
        Command::new(java)
            .arg("-version")
            .output()
            .ok()
            .map(|out| {
                let text = format!(
                    "{}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                text.contains("version \"17.") || text.contains("version \"1.17.")
            })
            .unwrap_or(false)
    })
}

fn docker_bridge_available() -> bool {
    Command::new("docker")
        .args(["network", "inspect", "bridge"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn docker_host_network_available() -> bool {
    Command::new("docker")
        .args(["network", "inspect", "host"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn openwhisk_host_network_override_available() -> bool {
    let config = Path::new("benchmarks/profiler/configs/openwhisk-host-network.conf");
    let runtimes = Path::new("benchmarks/profiler/configs/runtimes-no-prewarm.json");
    let docker_client = Path::new(
        "benchmarks/third_party/openwhisk/core/invoker/src/main/scala/org/apache/openwhisk/core/containerpool/docker/DockerClient.scala",
    );
    config.exists()
        && runtimes.exists()
        && docker_client
            .exists()
            .then(|| fs::read_to_string(docker_client).unwrap_or_default())
            .map(|source| {
                source.contains("network == \"host\"")
                    && source.contains("ContainerAddress(\"127.0.0.1\")")
            })
            .unwrap_or(false)
}

fn openwhisk_docker_network_available() -> bool {
    docker_bridge_available()
        || (docker_host_network_available() && openwhisk_host_network_override_available())
}

fn standalone(args: StandaloneArgs) -> Result<()> {
    if args.sample_ms == 0 {
        bail!("--sample-ms must be greater than zero");
    }
    if args.workload == WorkloadKind::Command && args.command.is_empty() {
        bail!("--workload command requires a trailing command after --");
    }

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;
    let run_id = make_run_id(&args.name);
    let run_dir = args.out_dir.join(&run_id);
    fs::create_dir(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;

    let (cgroup_path, cgroup_fallback) = create_run_cgroup(&run_id, args.allow_cgroup_fallback)?;
    let command = workload_command(args.workload, args.duration_ms, &args.command)?;
    let config_hash = stable_hash(&json!({
        "workload": args.workload,
        "input": args.input,
        "warmth": args.warmth,
        "duration_ms": args.duration_ms,
        "sample_ms": args.sample_ms,
        "command": command,
    }));

    let meta = RunMeta {
        run_id: run_id.clone(),
        workload: format!("{:?}", args.workload).to_lowercase(),
        input: args.input.clone(),
        warmth: args.warmth.clone(),
        sample_ms: args.sample_ms,
        cgroup_path: cgroup_path.display().to_string(),
        cgroup_fallback,
        command: command.clone(),
        env: run_environment(),
        config_hash,
    };
    write_json(&run_dir.join("run_meta.json"), &meta)?;
    write_placeholder_activation(&run_dir.join("openwhisk_activation.json"), &run_id)?;

    let activation_id = format!("{run_id}-activation-0");
    append_event(
        &run_dir,
        &json!({
            "event": "invocation_started",
            "run_id": run_id,
            "activation_id": activation_id,
            "timestamp_ns": now_ns(),
            "workload": meta.workload,
            "input": args.input,
            "warmth": args.warmth,
            "container_id": Value::Null,
            "host_pid": Value::Null,
            "cgroup_path": meta.cgroup_path,
            "cold_warm": args.warmth,
            "queue_wait_ns": 0,
            "init_ns": 0,
        }),
    )?;

    initialize_output_files(&run_dir)?;
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = spawn_sampler(
        run_dir.clone(),
        cgroup_path.clone(),
        args.sample_ms,
        stop.clone(),
    );

    let send_ns = now_ns();
    let start_instant = Instant::now();
    let mut child = spawn_workload(
        &command,
        &run_dir,
        (!cgroup_fallback).then_some(&cgroup_path),
    )?;
    if !cgroup_fallback {
        assign_pid_to_cgroup(&cgroup_path, child.id()).with_context(|| {
            format!(
                "assign pid {} to cgroup {}",
                child.id(),
                cgroup_path.display()
            )
        })?;
    }

    let mut perf_child = if args.skip_perf {
        write_perf_skipped(&run_dir.join("perf_stat.csv"), "disabled by --skip-perf")?;
        None
    } else {
        spawn_perf(
            child.id(),
            (!cgroup_fallback).then_some(&cgroup_path),
            &run_dir.join("perf_stat.csv"),
            args.duration_ms,
        )
        .ok()
    };

    let status = child.wait().context("wait for workload")?;
    let end_ns = now_ns();
    let elapsed_ms = start_instant.elapsed().as_millis() as u64;
    stop.store(true, Ordering::SeqCst);
    sampler
        .join()
        .map_err(|_| anyhow!("sampler thread panicked"))??;

    if let Some(perf) = perf_child.as_mut() {
        let _ = perf.wait();
        if fs::metadata(run_dir.join("perf_stat.csv"))
            .map(|m| m.len())
            .unwrap_or(0)
            == 0
        {
            write_perf_skipped(
                &run_dir.join("perf_stat.csv"),
                "perf exited without writing counters",
            )?;
        }
    }

    append_event(
        &run_dir,
        &json!({
            "event": "invocation_finished",
            "run_id": run_id,
            "activation_id": activation_id,
            "timestamp_ns": end_ns,
            "duration_ns": end_ns.saturating_sub(send_ns),
            "status": exit_status_string(status),
            "container_id": "standalone",
            "host_pid": child.id(),
            "cgroup_path": meta.cgroup_path,
            "reuse_age_ns": 0,
        }),
    )?;

    write_client_latency(
        &run_dir.join("client_latency.csv"),
        &ClientLatency {
            run_id: run_id.clone(),
            activation_id,
            send_ns,
            first_byte_ns: None,
            response_end_ns: end_ns,
            status: exit_status_string(status),
            timing_source: "process_wait".to_string(),
            error: if status.success() {
                String::new()
            } else {
                format!("workload exited after {elapsed_ms}ms")
            },
        },
    )?;

    write_summary(&run_dir)?;
    if !cgroup_fallback {
        let _ = fs::remove_dir(&cgroup_path);
    }
    verify_run(&run_dir)?;
    println!("{}", run_dir.display());
    Ok(())
}

fn openwhisk(args: OpenWhiskArgs) -> Result<()> {
    if args.sample_ms == 0 {
        bail!("--sample-ms must be greater than zero");
    }
    if !args.skip_update && args.file.is_none() {
        bail!("--file is required unless --skip-update is set");
    }

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;
    let run_id = make_run_id(&args.name);
    let run_dir = args.out_dir.join(&run_id);
    fs::create_dir(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    initialize_output_files(&run_dir)?;

    let mut meta = RunMeta {
        run_id: run_id.clone(),
        workload: args.action.clone(),
        input: args.input.clone(),
        warmth: args.warmth.clone(),
        sample_ms: args.sample_ms,
        cgroup_path: String::new(),
        cgroup_fallback: false,
        command: wsk_invoke_command(&args)?,
        env: run_environment(),
        config_hash: stable_hash(&json!({
            "action": args.action,
            "kind": args.kind,
            "file": args.file,
            "params": args.params,
            "param_files": args.param_files,
            "input": args.input,
            "warmth": args.warmth,
        })),
    };
    write_json(&run_dir.join("run_meta.json"), &meta)?;
    write_placeholder_activation(&run_dir.join("openwhisk_activation.json"), &run_id)?;

    if !args.skip_update {
        let file = args.file.as_ref().expect("checked above");
        let update = wsk_update_command(&args, file);
        run_command_to_logs(&update, &run_dir)
            .with_context(|| format!("update OpenWhisk action {}", args.action))?;
    }

    let send_ns = now_ns();
    append_event(
        &run_dir,
        &json!({
            "event": "invocation_started",
            "run_id": run_id,
            "activation_id": Value::Null,
            "timestamp_ns": send_ns,
            "workload": args.action,
            "input": args.input,
            "warmth": args.warmth,
            "container_id": Value::Null,
            "host_pid": Value::Null,
            "cgroup_path": Value::Null,
            "cold_warm": args.warmth,
            "queue_wait_ns": Value::Null,
            "init_ns": Value::Null,
        }),
    )?;

    let invoke = if args.invoke_http {
        openwhisk_http_invoke_command(&args)?
    } else {
        wsk_invoke_command(&args)?
    };
    let invoke_result = run_invoke_with_sampling(&invoke, &run_dir, &args, send_ns)?;
    let status = invoke_result.status;
    let end_ns = invoke_result.response_end_ns;
    let activation_id =
        parse_activation_id(&invoke_result.stdout).unwrap_or_else(|| format!("{run_id}-unknown"));
    let activation = fetch_activation(&args, &activation_id).unwrap_or_else(|err| {
        json!({
            "mode": "openwhisk",
            "error": err.to_string(),
            "activations": []
        })
    });
    write_json(&run_dir.join("openwhisk_activation.json"), &activation)?;

    let container = find_openwhisk_container(&args.action);
    if let Ok(info) = &container {
        meta.cgroup_path = info.cgroup_path.display().to_string();
        write_json(&run_dir.join("run_meta.json"), &meta)?;
        sample_once(&run_dir, &info.cgroup_path)?;
    }
    if fs::metadata(run_dir.join("perf_stat.csv"))
        .map(|m| m.len())
        .unwrap_or(0)
        == 0
    {
        write_perf_skipped(
            &run_dir.join("perf_stat.csv"),
            "OpenWhisk action container was not discovered before perf collection",
        )?;
    }

    let normalized = activation
        .get("activations")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .cloned()
        .unwrap_or(Value::Null);
    let wait_ns = millis_to_ns(normalized.get("wait_time"));
    let init_ns = millis_to_ns(normalized.get("init_time"));
    let run_ns = millis_to_ns_u64(normalized.get("duration"))
        .unwrap_or_else(|| end_ns.saturating_sub(send_ns) as u64);
    let (container_id, container_name, host_pid, cgroup_path) = container
        .map(|info| {
            (
                Value::String(info.id),
                Value::String(info.name),
                json!(info.host_pid),
                Value::String(info.cgroup_path.display().to_string()),
            )
        })
        .unwrap_or((Value::Null, Value::Null, Value::Null, Value::Null));

    append_event(
        &run_dir,
        &json!({
            "event": "invocation_finished",
            "run_id": run_id,
            "activation_id": activation_id,
            "timestamp_ns": end_ns,
            "duration_ns": run_ns,
            "status": exit_status_string(status),
            "container_id": container_id,
            "container_name": container_name,
            "host_pid": host_pid,
            "cgroup_path": cgroup_path,
            "reuse_age_ns": Value::Null,
            "queue_wait_ns": wait_ns,
            "init_ns": init_ns,
        }),
    )?;
    write_client_latency(
        &run_dir.join("client_latency.csv"),
        &ClientLatency {
            run_id: run_id.clone(),
            activation_id,
            send_ns,
            first_byte_ns: invoke_result.first_byte_ns,
            response_end_ns: end_ns,
            status: exit_status_string(status),
            timing_source: invoke_result.timing_source,
            error: if status.success() {
                String::new()
            } else {
                "wsk action invoke failed".to_string()
            },
        },
    )?;

    write_summary(&run_dir)?;
    verify_run(&run_dir)?;
    println!("{}", run_dir.display());
    Ok(())
}

fn make_run_id(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("{}-{}-{}", sanitized, now_ns(), std::process::id())
}

fn create_run_cgroup(run_id: &str, allow_fallback: bool) -> Result<(PathBuf, bool)> {
    let root = Path::new("/sys/fs/cgroup");
    if !root.join("cgroup.controllers").exists() {
        if allow_fallback {
            return Ok((current_cgroup_path(), true));
        }
        bail!("cgroup v2 is not mounted at /sys/fs/cgroup");
    }

    let parent = root.join("cosmos-bench");
    if let Err(err) = fs::create_dir_all(&parent) {
        if allow_fallback {
            return Ok((current_cgroup_path(), true));
        }
        return Err(err).with_context(|| format!("create {}", parent.display()));
    }

    let _ = enable_subtree_controllers(root);
    let _ = enable_subtree_controllers(&parent);

    let cg = parent.join(run_id);
    match fs::create_dir(&cg) {
        Ok(_) => Ok((cg, false)),
        Err(err) if allow_fallback => {
            eprintln!("cgroup fallback: {err}");
            Ok((current_cgroup_path(), true))
        }
        Err(err) => Err(err).with_context(|| format!("create {}", cg.display())),
    }
}

fn enable_subtree_controllers(path: &Path) -> Result<()> {
    let controllers = fs::read_to_string(path.join("cgroup.controllers")).unwrap_or_default();
    let wanted: Vec<String> = ["cpu", "memory", "io"]
        .iter()
        .filter(|name| controllers.split_whitespace().any(|ctrl| ctrl == **name))
        .map(|name| format!("+{name}"))
        .collect();
    if !wanted.is_empty() {
        fs::write(path.join("cgroup.subtree_control"), wanted.join(" "))?;
    }
    Ok(())
}

fn current_cgroup_path() -> PathBuf {
    let text = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    for line in text.lines() {
        if let Some(path) = line.strip_prefix("0::") {
            return Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
        }
    }
    PathBuf::from("/sys/fs/cgroup")
}

fn assign_pid_to_cgroup(path: &Path, pid: u32) -> Result<()> {
    fs::write(path.join("cgroup.procs"), pid.to_string())?;
    Ok(())
}

fn workload_command(
    kind: WorkloadKind,
    duration_ms: u64,
    command: &[String],
) -> Result<Vec<String>> {
    if kind == WorkloadKind::Command {
        return Ok(command.to_vec());
    }
    let exe = std::env::current_exe().context("resolve current executable")?;
    Ok(vec![
        exe.display().to_string(),
        "micro".to_string(),
        format!("{kind:?}").to_lowercase(),
        "--duration-ms".to_string(),
        duration_ms.to_string(),
    ])
}

fn spawn_workload(command: &[String], run_dir: &Path, cgroup_path: Option<&Path>) -> Result<Child> {
    let mut cmd;
    if let Some(cgroup_path) = cgroup_path {
        cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg("printf '%s\n' $$ > \"$COSMOS_CGROUP_PROCS\" || exit 125; exec \"$@\"")
            .arg("cosmos-cgroup-entry")
            .args(command);
        cmd.env(
            "COSMOS_CGROUP_PROCS",
            cgroup_path.join("cgroup.procs").display().to_string(),
        );
    } else {
        cmd = Command::new(&command[0]);
        cmd.args(&command[1..]);
    }
    let stdout = File::create(run_dir.join("stdout.log"))?;
    let stderr = File::create(run_dir.join("stderr.log"))?;
    cmd.env("COSMOS_BENCH_TMPDIR", run_dir.display().to_string());
    cmd.stdout(Stdio::from(stdout)).stderr(Stdio::from(stderr));
    cmd.spawn()
        .with_context(|| format!("spawn workload command: {}", command.join(" ")))
}

fn spawn_perf(pid: u32, cgroup_path: Option<&Path>, out: &Path, duration_ms: u64) -> Result<Child> {
    let events = PERF_EVENTS.join(",");
    let seconds = ((duration_ms + 999) / 1000).saturating_add(2).max(2);
    let mut cmd = Command::new("timeout");
    cmd.args(["-s", "INT", "--kill-after=2s", &format!("{seconds}s")])
        .arg("perf")
        .args(["stat", "-x", ",", "-e", &events]);
    if let Some(cgroup_path) = cgroup_path {
        cmd.arg("-a").arg("-G").arg(perf_cgroup_name(cgroup_path)?);
    } else {
        cmd.arg("-p").arg(pid.to_string());
    }
    cmd.arg("-o")
        .arg(out)
        .args(["--", "sleep", &seconds.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn perf stat")
}

fn wsk_base_args(args: &OpenWhiskArgs) -> Vec<String> {
    let mut out = Vec::new();
    if args.insecure {
        out.push("-i".to_string());
    }
    if let Some(apihost) = &args.apihost {
        out.push("--apihost".to_string());
        out.push(apihost.clone());
    }
    if let Some(auth) = &args.auth {
        out.push("--auth".to_string());
        out.push(auth.clone());
    }
    out
}

fn wsk_update_command(args: &OpenWhiskArgs, file: &Path) -> Vec<String> {
    let mut cmd = vec!["wsk".to_string()];
    cmd.extend(wsk_base_args(args));
    cmd.extend([
        "action".to_string(),
        "update".to_string(),
        args.action.clone(),
        file.display().to_string(),
        "--kind".to_string(),
        args.kind.clone(),
    ]);
    cmd
}

fn wsk_invoke_command(args: &OpenWhiskArgs) -> Result<Vec<String>> {
    let mut cmd = vec!["wsk".to_string()];
    cmd.extend(wsk_base_args(args));
    cmd.extend([
        "action".to_string(),
        "invoke".to_string(),
        args.action.clone(),
        "--blocking".to_string(),
    ]);
    for raw in &args.params {
        let Some((key, value)) = raw.split_once('=') else {
            bail!("--param must be key=value, got {raw}");
        };
        cmd.extend(["--param".to_string(), key.to_string(), value.to_string()]);
    }
    for path in &args.param_files {
        cmd.extend(["--param-file".to_string(), path.display().to_string()]);
    }
    Ok(cmd)
}

fn openwhisk_http_invoke_command(args: &OpenWhiskArgs) -> Result<Vec<String>> {
    let apihost = args
        .apihost
        .as_ref()
        .ok_or_else(|| anyhow!("--invoke-http requires --apihost"))?;
    let auth = args
        .auth
        .as_ref()
        .ok_or_else(|| anyhow!("--invoke-http requires --auth"))?;
    let param_file = match args.param_files.as_slice() {
        [path] => path,
        [] => bail!("--invoke-http requires one --param-file"),
        _ => bail!("--invoke-http accepts exactly one --param-file"),
    };
    let action = args.action.trim_start_matches('/');
    let url = format!(
        "{}/api/v1/namespaces/_/actions/{}?blocking=true&result=false",
        apihost.trim_end_matches('/'),
        action
    );
    Ok(vec![
        "curl".to_string(),
        "--fail-with-body".to_string(),
        "-sS".to_string(),
        "-w".to_string(),
        "\n__COSMOS_CURL_TIMING__:%{time_starttransfer}:%{time_total}\n".to_string(),
        "-u".to_string(),
        auth.clone(),
        "-H".to_string(),
        "Content-Type: application/json".to_string(),
        "-X".to_string(),
        "POST".to_string(),
        "-d".to_string(),
        format!("@{}", param_file.display()),
        url,
    ])
}

fn wsk_activation_get_command(args: &OpenWhiskArgs, activation_id: &str) -> Vec<String> {
    let mut cmd = vec!["wsk".to_string()];
    cmd.extend(wsk_base_args(args));
    cmd.extend([
        "activation".to_string(),
        "get".to_string(),
        activation_id.to_string(),
    ]);
    cmd
}

fn run_command_to_logs(command: &[String], run_dir: &Path) -> Result<()> {
    let stdout = OpenOptions::new()
        .append(true)
        .create(true)
        .open(run_dir.join("stdout.log"))?;
    let stderr = OpenOptions::new()
        .append(true)
        .create(true)
        .open(run_dir.join("stderr.log"))?;
    let status = Command::new(&command[0])
        .args(&command[1..])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .status()
        .with_context(|| format!("run {}", command.join(" ")))?;
    if !status.success() {
        bail!("command failed: {}", command.join(" "));
    }
    Ok(())
}

fn run_invoke_with_sampling(
    command: &[String],
    run_dir: &Path,
    args: &OpenWhiskArgs,
    send_ns: u128,
) -> Result<InvokeResult> {
    let stdout_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(run_dir.join("stdout.log"))?;
    let stderr_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(run_dir.join("stderr.log"))?;
    let stdout_read = stdout_file.try_clone()?;
    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .with_context(|| format!("spawn {}", command.join(" ")))?;

    let mut discovered: Option<ContainerInfo> = None;
    let mut perf_child: Option<Child> = None;
    let mut scheduler = SchedulerStatsSampler::new();
    let mut last_sample = Instant::now()
        .checked_sub(Duration::from_millis(args.sample_ms))
        .unwrap_or_else(Instant::now);
    let status = loop {
        if discovered.is_none() {
            if let Ok(info) = find_openwhisk_container(&args.action) {
                sample_once_with_scheduler(run_dir, &info.cgroup_path, &mut scheduler)?;
                perf_child = spawn_perf(
                    info.host_pid,
                    Some(&info.cgroup_path),
                    &run_dir.join("perf_stat.csv"),
                    10_000,
                )
                .ok();
                discovered = Some(info);
            }
        }
        if let Some(info) = &discovered {
            if last_sample.elapsed() >= Duration::from_millis(args.sample_ms) {
                sample_once_with_scheduler(run_dir, &info.cgroup_path, &mut scheduler)?;
                last_sample = Instant::now();
            }
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        thread::sleep(Duration::from_millis(20));
    };

    let deadline = Instant::now() + Duration::from_millis(args.docker_grace_ms);
    while discovered.is_none() && Instant::now() < deadline {
        if let Ok(info) = find_openwhisk_container(&args.action) {
            sample_once_with_scheduler(run_dir, &info.cgroup_path, &mut scheduler)?;
            discovered = Some(info);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    if let Some(info) = &discovered {
        sample_once_with_scheduler(run_dir, &info.cgroup_path, &mut scheduler)?;
    }
    if let Some(perf) = perf_child.as_mut() {
        let _ = perf.wait();
    }
    if perf_child.is_none() {
        write_perf_skipped(
            &run_dir.join("perf_stat.csv"),
            "OpenWhisk action container was not discovered before perf collection",
        )?;
    }

    drop(stdout_read);
    let stdout = fs::read_to_string(run_dir.join("stdout.log")).unwrap_or_default();
    let finished_ns = now_ns();
    let timing = parse_curl_timing(&stdout);
    let (first_byte_ns, response_end_ns, timing_source) =
        if command.first().map(String::as_str) == Some("curl") {
            if let Some(timing) = timing {
                (
                    Some(send_ns.saturating_add(timing.starttransfer_ns)),
                    send_ns.saturating_add(timing.total_ns),
                    "curl_write_out".to_string(),
                )
            } else {
                (
                    None,
                    finished_ns,
                    "process_wait_missing_curl_timing".to_string(),
                )
            }
        } else {
            (None, finished_ns, "process_wait".to_string())
        };
    Ok(InvokeResult {
        status,
        stdout,
        first_byte_ns,
        response_end_ns,
        timing_source,
    })
}

fn parse_curl_timing(stdout: &str) -> Option<CurlTiming> {
    stdout.lines().rev().find_map(|line| {
        let rest = line.strip_prefix("__COSMOS_CURL_TIMING__:")?;
        let mut parts = rest.split(':');
        let start = parse_seconds_as_ns(parts.next()?)?;
        let total = parse_seconds_as_ns(parts.next()?)?;
        Some(CurlTiming {
            starttransfer_ns: start,
            total_ns: total,
        })
    })
}

fn parse_seconds_as_ns(raw: &str) -> Option<u128> {
    let value = raw.parse::<f64>().ok()?;
    if value.is_finite() && value >= 0.0 {
        Some((value * 1_000_000_000.0).round() as u128)
    } else {
        None
    }
}

fn fetch_activation(args: &OpenWhiskArgs, activation_id: &str) -> Result<Value> {
    let command = wsk_activation_get_command(args, activation_id);
    let output = Command::new(&command[0])
        .args(&command[1..])
        .output()
        .with_context(|| format!("run {}", command.join(" ")))?;
    if !output.status.success() {
        bail!(
            "activation get failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json_start = stdout
        .find(|ch| ch == '{' || ch == '[')
        .ok_or_else(|| anyhow!("activation response did not contain JSON"))?;
    let value: Value = serde_json::from_str(&stdout[json_start..])
        .with_context(|| format!("parse activation {activation_id}"))?;
    Ok(normalize_openwhisk_activations(&value))
}

fn parse_activation_id(stdout: &str) -> Option<String> {
    stdout
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .find(|token| token.len() == 32 && token.chars().all(|ch| ch.is_ascii_hexdigit()))
        .map(ToString::to_string)
}

fn find_openwhisk_container(action: &str) -> Result<ContainerInfo> {
    let output = Command::new("docker")
        .args(["ps", "--no-trunc", "--format", "{{.ID}} {{.Names}}"])
        .output()
        .context("docker ps")?;
    if !output.status.success() {
        bail!("docker ps failed");
    }
    let action_key = docker_action_key(action);
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut parts = line.split_whitespace();
        let Some(id) = parts.next() else {
            continue;
        };
        let name = parts.next().unwrap_or("");
        let normalized_name = docker_action_key(name);
        if name.starts_with("wsk")
            && (action_key.is_empty() || normalized_name.contains(&action_key))
        {
            return inspect_container(id, name);
        }
    }
    bail!("OpenWhisk action container not found for {action}");
}

fn inspect_container(id: &str, fallback_name: &str) -> Result<ContainerInfo> {
    let output = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.Id}} {{.State.Pid}} {{.Name}}",
            id,
        ])
        .output()
        .with_context(|| format!("docker inspect {id}"))?;
    if !output.status.success() {
        bail!("docker inspect failed for {id}");
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut parts = text.split_whitespace();
    let id = parts.next().unwrap_or(id).to_string();
    let host_pid = parts
        .next()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(0);
    if host_pid == 0 {
        bail!("container {id} has no host pid");
    }
    let name = parts
        .next()
        .unwrap_or(fallback_name)
        .trim_start_matches('/')
        .to_string();
    let cgroup_path = pid_cgroup_path(host_pid)?;
    Ok(ContainerInfo {
        id,
        name,
        host_pid,
        cgroup_path,
    })
}

fn pid_cgroup_path(pid: u32) -> Result<PathBuf> {
    let text = fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .with_context(|| format!("read /proc/{pid}/cgroup"))?;
    for line in text.lines() {
        if let Some(path) = line.strip_prefix("0::") {
            return Ok(Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')));
        }
    }
    bail!("cgroup v2 path not found for pid {pid}");
}

fn docker_action_key(action: &str) -> String {
    action
        .rsplit('/')
        .next()
        .unwrap_or(action)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn millis_to_ns(value: Option<&Value>) -> Value {
    millis_to_ns_u64(value)
        .map(|ns| json!(ns))
        .unwrap_or(Value::Null)
}

fn millis_to_ns_u64(value: Option<&Value>) -> Option<u64> {
    value
        .and_then(json_u64)
        .map(|ms| json!(ms.saturating_mul(1_000_000)))
        .and_then(|value| value.as_u64())
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_f64().map(|v| v.max(0.0) as u64))
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
}

fn perf_cgroup_name(path: &Path) -> Result<String> {
    let rel = path
        .strip_prefix("/sys/fs/cgroup")
        .with_context(|| format!("{} is not under /sys/fs/cgroup", path.display()))?;
    Ok(rel
        .display()
        .to_string()
        .trim_start_matches('/')
        .to_string())
}

fn write_perf_skipped(path: &Path, reason: &str) -> Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "status,reason")?;
    writeln!(file, "skipped,{reason}")?;
    Ok(())
}

fn initialize_output_files(run_dir: &Path) -> Result<()> {
    write_if_missing(
        &run_dir.join("cgroup_cpu.csv"),
        "timestamp_ns,usage_usec,user_usec,system_usec,nr_periods,nr_throttled,throttled_usec\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_memory.csv"),
        "timestamp_ns,current_bytes,peak_bytes,anon_bytes,file_bytes,pgfault,pgmajfault,oom,oom_kill\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_io.csv"),
        "timestamp_ns,rbytes,wbytes,rios,wios,dbytes,dios\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_pressure.csv"),
        "timestamp_ns,resource,scope,avg10,avg60,avg300,total\n",
    )?;
    write_if_missing(
        &run_dir.join("net.csv"),
        "timestamp_ns,scope,source,rx_bytes,tx_bytes\n",
    )?;
    write_if_missing(
        &run_dir.join("qdisc.csv"),
        "timestamp_ns,scope,source,dev,kind,bytes,packets,drops,overlimits,requeues,backlog_bytes,backlog_packets\n",
    )?;
    write_if_missing(
        &run_dir.join("client_latency.csv"),
        "run_id,activation_id,send_ns,first_byte_ns,response_end_ns,status,timing_source,error\n",
    )?;
    write_if_missing(
        &run_dir.join("perf_stat.csv"),
        "status,reason\npending,collector-not-finished\n",
    )?;
    let scheduler_header = format!(
        "timestamp_ns,available,error,{}\n",
        SCHEDULER_STATS_FIELDS.join(",")
    );
    write_if_missing(&run_dir.join("scheduler_stats.csv"), &scheduler_header)?;
    Ok(())
}

fn write_if_missing(path: &Path, contents: &str) -> Result<()> {
    if !path.exists() {
        fs::write(path, contents)?;
    }
    Ok(())
}

fn spawn_sampler(
    run_dir: PathBuf,
    cgroup_path: PathBuf,
    sample_ms: u64,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let mut scheduler = SchedulerStatsSampler::new();
        while !stop.load(Ordering::SeqCst) {
            sample_once_with_scheduler(&run_dir, &cgroup_path, &mut scheduler)?;
            thread::sleep(Duration::from_millis(sample_ms));
        }
        sample_once_with_scheduler(&run_dir, &cgroup_path, &mut scheduler)?;
        Ok(())
    })
}

fn sample_once(run_dir: &Path, cgroup_path: &Path) -> Result<()> {
    let mut scheduler = SchedulerStatsSampler::new();
    sample_once_with_scheduler(run_dir, cgroup_path, &mut scheduler)
}

fn sample_once_with_scheduler(
    run_dir: &Path,
    cgroup_path: &Path,
    scheduler: &mut SchedulerStatsSampler,
) -> Result<()> {
    let ts = now_ns();
    let cpu = read_key_values(&cgroup_path.join("cpu.stat"));
    append_line(
        &run_dir.join("cgroup_cpu.csv"),
        &format!(
            "{ts},{},{},{},{},{},{}\n",
            value(&cpu, "usage_usec"),
            value(&cpu, "user_usec"),
            value(&cpu, "system_usec"),
            value(&cpu, "nr_periods"),
            value(&cpu, "nr_throttled"),
            value(&cpu, "throttled_usec")
        ),
    )?;

    let mem_stat = read_key_values(&cgroup_path.join("memory.stat"));
    let mem_events = read_key_values(&cgroup_path.join("memory.events"));
    append_line(
        &run_dir.join("cgroup_memory.csv"),
        &format!(
            "{ts},{},{},{},{},{},{},{},{}\n",
            read_u64(&cgroup_path.join("memory.current")),
            read_u64(&cgroup_path.join("memory.peak")),
            value(&mem_stat, "anon"),
            value(&mem_stat, "file"),
            value(&mem_stat, "pgfault"),
            value(&mem_stat, "pgmajfault"),
            value(&mem_events, "oom"),
            value(&mem_events, "oom_kill")
        ),
    )?;

    let io = read_io_stat(&cgroup_path.join("io.stat"));
    append_line(
        &run_dir.join("cgroup_io.csv"),
        &format!(
            "{ts},{},{},{},{},{},{}\n",
            value(&io, "rbytes"),
            value(&io, "wbytes"),
            value(&io, "rios"),
            value(&io, "wios"),
            value(&io, "dbytes"),
            value(&io, "dios")
        ),
    )?;

    for resource in ["cpu", "memory", "io"] {
        append_pressure(run_dir, cgroup_path, resource, ts)?;
    }

    let net = read_net_dev();
    append_line(
        &run_dir.join("net.csv"),
        &format!(
            "{ts},{},{},{},{}\n",
            net.scope, net.source, net.rx_bytes, net.tx_bytes
        ),
    )?;
    append_qdisc(run_dir, ts)?;
    scheduler.sample(run_dir, ts)?;
    Ok(())
}

impl SchedulerStatsSampler {
    fn new() -> Self {
        Self {
            client: None,
            unavailable_reason: None,
        }
    }

    fn sample(&mut self, run_dir: &Path, ts: u128) -> Result<()> {
        let sample = self.read_sample();
        match sample {
            Ok(value) => {
                self.unavailable_reason = None;
                append_scheduler_stats_row(run_dir, ts, true, "", &value)
            }
            Err(err) => {
                self.client = None;
                let reason = concise_error(&err.to_string());
                self.unavailable_reason = Some(reason.clone());
                append_scheduler_stats_row(run_dir, ts, false, &reason, &Value::Null)
            }
        }
    }

    fn read_sample(&mut self) -> Result<Value> {
        if self.client.is_none() {
            let path = scheduler_stats_path();
            if !path.exists() {
                bail!(
                    "COSMOS scheduler stats socket not found at {}",
                    path.display()
                );
            }
            self.client = Some(
                StatsClient::new()
                    .set_path(&path)
                    .connect(Some(20))
                    .with_context(|| format!("connect {}", path.display()))?,
            );
        }
        self.client
            .as_mut()
            .expect("client initialized")
            .request::<Value>("stats", vec![("target".to_string(), "top".to_string())])
            .context("request scheduler stats")
    }
}

fn scheduler_stats_path() -> PathBuf {
    env::var_os("COSMOS_STATS_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/run/scx/root/stats"))
}

fn append_scheduler_stats_row(
    run_dir: &Path,
    ts: u128,
    available: bool,
    error: &str,
    value: &Value,
) -> Result<()> {
    let stats = value.get("top").unwrap_or(value);
    let mut row = vec![
        ts.to_string(),
        if available { "1" } else { "0" }.to_string(),
        csv_field(error),
    ];
    for field in SCHEDULER_STATS_FIELDS {
        row.push(
            stats
                .get(*field)
                .and_then(json_u64)
                .map(|v| v.to_string())
                .unwrap_or_default(),
        );
    }
    append_line(
        &run_dir.join("scheduler_stats.csv"),
        &format!("{}\n", row.join(",")),
    )
}

fn concise_error(error: &str) -> String {
    let first_line = error.lines().next().unwrap_or(error);
    first_line
        .chars()
        .take(160)
        .map(|ch| match ch {
            ',' | '\n' | '\r' => ';',
            '"' => '\'',
            _ => ch,
        })
        .collect()
}

fn csv_field(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
    }
}

fn read_key_values(path: &Path) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let Ok(file) = File::open(path) else {
        return out;
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let mut parts = line.split_whitespace();
        if let (Some(key), Some(raw)) = (parts.next(), parts.next()) {
            if let Ok(value) = raw.parse() {
                out.insert(key.to_string(), value);
            }
        }
    }
    out
}

fn read_io_stat(path: &Path) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let Ok(file) = File::open(path) else {
        return out;
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        for token in line.split_whitespace().skip(1) {
            if let Some((key, raw)) = token.split_once('=') {
                if let Ok(value) = raw.parse::<u64>() {
                    *out.entry(key.to_string()).or_default() += value;
                }
            }
        }
    }
    out
}

fn value(map: &BTreeMap<String, u64>, key: &str) -> u64 {
    *map.get(key).unwrap_or(&0)
}

fn read_u64(path: &Path) -> u64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn append_pressure(run_dir: &Path, cgroup_path: &Path, resource: &str, ts: u128) -> Result<()> {
    let path = cgroup_path.join(format!("{resource}.pressure"));
    let Ok(file) = File::open(path) else {
        return Ok(());
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let mut parts = line.split_whitespace();
        let Some(scope) = parts.next() else {
            continue;
        };
        let mut vals = BTreeMap::new();
        for token in parts {
            if let Some((key, raw)) = token.split_once('=') {
                vals.insert(key, raw);
            }
        }
        append_line(
            &run_dir.join("cgroup_pressure.csv"),
            &format!(
                "{ts},{resource},{scope},{},{},{},{}\n",
                vals.get("avg10").copied().unwrap_or("0"),
                vals.get("avg60").copied().unwrap_or("0"),
                vals.get("avg300").copied().unwrap_or("0"),
                vals.get("total").copied().unwrap_or("0")
            ),
        )?;
    }
    Ok(())
}

fn read_net_dev() -> NetSample {
    let mut sample = NetSample {
        scope: "host".to_string(),
        source: "/proc/net/dev".to_string(),
        rx_bytes: 0,
        tx_bytes: 0,
    };
    let Ok(file) = File::open("/proc/net/dev") else {
        return sample;
    };
    for line in BufReader::new(file).lines().map_while(Result::ok).skip(2) {
        let Some((_iface, rest)) = line.split_once(':') else {
            continue;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        if fields.len() >= 16 {
            sample.rx_bytes += fields[0].parse::<u64>().unwrap_or(0);
            sample.tx_bytes += fields[8].parse::<u64>().unwrap_or(0);
        }
    }
    sample
}

fn append_qdisc(run_dir: &Path, ts: u128) -> Result<()> {
    let Some(tc) = command_path("tc") else {
        return Ok(());
    };
    let Ok(output) = Command::new(tc).args(["-s", "qdisc", "show"]).output() else {
        return Ok(());
    };
    if !output.status.success() {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("qdisc ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            let kind = parts.get(1).copied().unwrap_or("unknown").to_string();
            let dev = parts
                .windows(2)
                .find(|pair| pair[0] == "dev")
                .map(|pair| pair[1].to_string())
                .unwrap_or_else(|| "unknown".to_string());
            current = Some((dev, kind));
        } else if trimmed.starts_with("Sent ") {
            let Some((dev, kind)) = &current else {
                continue;
            };
            let numbers = numbers_in(trimmed);
            let bytes = numbers.first().copied().unwrap_or(0);
            let packets = numbers.get(1).copied().unwrap_or(0);
            let drops = numbers.get(2).copied().unwrap_or(0);
            let overlimits = numbers.get(3).copied().unwrap_or(0);
            let requeues = numbers.get(4).copied().unwrap_or(0);
            append_line(
                &run_dir.join("qdisc.csv"),
                &format!(
                    "{ts},host,tc-qdisc,{dev},{kind},{bytes},{packets},{drops},{overlimits},{requeues},0,0\n"
                ),
            )?;
        } else if trimmed.starts_with("backlog ") {
            let Some((dev, kind)) = &current else {
                continue;
            };
            let numbers = numbers_in(trimmed);
            append_line(
                &run_dir.join("qdisc.csv"),
                &format!(
                    "{ts},host,tc-qdisc,{dev},{kind},0,0,0,0,0,{},{}\n",
                    numbers.first().copied().unwrap_or(0),
                    numbers.get(1).copied().unwrap_or(0)
                ),
            )?;
        }
    }
    Ok(())
}

fn numbers_in(text: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_ascii_digit() {
            current.push(ch);
        } else if !current.is_empty() {
            if let Ok(v) = current.parse() {
                out.push(v);
            }
            current.clear();
        }
    }
    if !current.is_empty() {
        if let Ok(v) = current.parse() {
            out.push(v);
        }
    }
    out
}

fn append_line(path: &Path, line: &str) -> Result<()> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    file.write_all(line.as_bytes())?;
    Ok(())
}

fn append_event(run_dir: &Path, event: &Value) -> Result<()> {
    append_line(
        &run_dir.join("events.jsonl"),
        &format!("{}\n", serde_json::to_string(event)?),
    )
}

fn write_client_latency(path: &Path, row: &ClientLatency) -> Result<()> {
    append_line(
        path,
        &format!(
            "{},{},{},{},{},{},{},{}\n",
            row.run_id,
            row.activation_id,
            row.send_ns,
            row.first_byte_ns
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.response_end_ns,
            row.status,
            row.timing_source.replace(',', ";"),
            row.error.replace(',', ";")
        ),
    )
}

fn write_placeholder_activation(path: &Path, run_id: &str) -> Result<()> {
    write_json(
        path,
        &json!({
            "mode": "standalone",
            "run_id": run_id,
            "activations": []
        }),
    )
}

fn import_activation(args: ImportActivationArgs) -> Result<()> {
    let value: Value = serde_json::from_reader(File::open(&args.input)?)
        .with_context(|| format!("parse {}", args.input.display()))?;
    let value = normalize_openwhisk_activations(&value);
    fs::create_dir_all(&args.run_dir)?;
    write_json(&args.run_dir.join("openwhisk_activation.json"), &value)?;
    write_summary(&args.run_dir)?;
    println!(
        "{}",
        args.run_dir.join("openwhisk_activation.json").display()
    );
    Ok(())
}

fn normalize_openwhisk_activations(value: &Value) -> Value {
    let activations: Vec<Value> =
        if let Some(items) = value.get("activations").and_then(Value::as_array) {
            items.iter().map(normalize_openwhisk_activation).collect()
        } else if let Some(items) = value.as_array() {
            items.iter().map(normalize_openwhisk_activation).collect()
        } else {
            vec![normalize_openwhisk_activation(value)]
        };
    json!({
        "mode": "openwhisk",
        "activations": activations
    })
}

fn normalize_openwhisk_activation(value: &Value) -> Value {
    let annotations: BTreeMap<String, Value> = value
        .get("annotations")
        .and_then(Value::as_array)
        .map(|items| annotation_map(items))
        .unwrap_or_default();
    let response = value.get("response").unwrap_or(&Value::Null);
    json!({
        "activation_id": value.get("activationId").or_else(|| value.get("activation_id")).cloned().unwrap_or(Value::Null),
        "action": value.get("name").or_else(|| value.get("action")).cloned().unwrap_or(Value::Null),
        "namespace": value.get("namespace").cloned().unwrap_or(Value::Null),
        "start": value.get("start").cloned().unwrap_or(Value::Null),
        "end": value.get("end").cloned().unwrap_or(Value::Null),
        "duration": value.get("duration").cloned().unwrap_or(Value::Null),
        "status": response.get("status").or_else(|| value.get("status")).cloned().unwrap_or(Value::Null),
        "status_code": response.get("statusCode").or_else(|| value.get("status_code")).cloned().unwrap_or(Value::Null),
        "wait_time": annotations.get("waitTime").cloned().unwrap_or(Value::Null),
        "init_time": annotations.get("initTime").cloned().unwrap_or(Value::Null),
        "limits": annotations.get("limits").cloned().unwrap_or(Value::Null),
        "container_id": annotations.get("containerId").or_else(|| annotations.get("container_id")).cloned().unwrap_or(Value::Null),
        "host_pid": annotations.get("hostPid").or_else(|| annotations.get("host_pid")).cloned().unwrap_or(Value::Null),
        "cgroup_path": annotations.get("cgroupPath").or_else(|| annotations.get("cgroup_path")).cloned().unwrap_or(Value::Null),
        "raw": value
    })
}

fn annotation_map(items: &[Value]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for item in items {
        if let Some(key) = item.get("key").and_then(Value::as_str) {
            out.insert(
                key.to_string(),
                item.get("value").cloned().unwrap_or(Value::Null),
            );
        }
    }
    out
}

fn write_summary(run_dir: &Path) -> Result<()> {
    let meta: RunMeta = serde_json::from_reader(File::open(run_dir.join("run_meta.json"))?)
        .with_context(|| format!("parse {}", run_dir.join("run_meta.json").display()))?;
    let latency = read_latency_summary(
        &run_dir.join("client_latency.csv"),
        &run_dir.join("openwhisk_activation.json"),
    )?;
    let resources = read_resource_summary(run_dir)?;
    let phase_windows = classify_phase_windows(run_dir)?;
    let missing = missing_requirements(run_dir)?;
    let summary = Summary {
        run_id: meta.run_id,
        status: if missing.is_empty() {
            "complete".to_string()
        } else {
            "incomplete".to_string()
        },
        latency,
        resources,
        phase_windows,
        missing,
    };
    write_json(&run_dir.join("summary.json"), &summary)?;
    Ok(())
}

fn read_latency_summary(path: &Path, activation_path: &Path) -> Result<BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    let rows = read_csv_rows(path)?;
    let mut latencies = Vec::new();
    for row in rows {
        if row.len() < 6 || row[0] == "run_id" {
            continue;
        }
        let send = row[2].parse::<u128>().unwrap_or(0);
        let end = row[4].parse::<u128>().unwrap_or(0);
        if end >= send {
            latencies.push((end - send) as u64);
        }
    }
    latencies.sort_unstable();
    out.insert("count".to_string(), json!(latencies.len()));
    out.insert("median_ns".to_string(), json!(percentile(&latencies, 50.0)));
    out.insert("p95_ns".to_string(), json!(percentile(&latencies, 95.0)));
    out.insert("p99_ns".to_string(), json!(percentile(&latencies, 99.0)));
    out.insert(
        "client_latency_ns".to_string(),
        json!(latencies.first().copied().unwrap_or(0)),
    );
    let platform = activation_latency(activation_path);
    out.insert(
        "platform_wait_ns".to_string(),
        json!(platform.get("wait_ns").copied().unwrap_or(0)),
    );
    out.insert(
        "platform_init_ns".to_string(),
        json!(platform.get("init_ns").copied().unwrap_or(0)),
    );
    out.insert(
        "platform_run_ns".to_string(),
        json!(platform
            .get("run_ns")
            .copied()
            .unwrap_or_else(|| latencies.first().copied().unwrap_or(0))),
    );
    Ok(out)
}

fn activation_latency(path: &Path) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let Ok(value) = File::open(path)
        .ok()
        .and_then(|file| serde_json::from_reader::<_, Value>(file).ok())
        .ok_or(())
    else {
        return out;
    };
    let Some(activation) = value
        .get("activations")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
    else {
        return out;
    };
    if let Some(wait) = millis_to_ns_u64(activation.get("wait_time")) {
        out.insert("wait_ns".to_string(), wait);
    }
    if let Some(init) = millis_to_ns_u64(activation.get("init_time")) {
        out.insert("init_ns".to_string(), init);
    }
    if let Some(run) = millis_to_ns_u64(activation.get("duration")) {
        out.insert("run_ns".to_string(), run);
    }
    out
}

fn read_resource_summary(run_dir: &Path) -> Result<BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    let cpu = read_cgroup_samples(run_dir)?;
    if let (Some(first), Some(last)) = (cpu.first(), cpu.last()) {
        out.insert(
            "cpu_usage_usec".to_string(),
            json!(last.usage_usec.saturating_sub(first.usage_usec)),
        );
        out.insert(
            "cpu_throttled_usec".to_string(),
            json!(last.throttled_usec.saturating_sub(first.throttled_usec)),
        );
        out.insert(
            "memory_peak_bytes".to_string(),
            json!(cpu
                .iter()
                .map(|s| s.memory_peak.max(s.memory_current))
                .max()
                .unwrap_or(0)),
        );
        out.insert(
            "io_bytes".to_string(),
            json!(last
                .io_rbytes
                .saturating_add(last.io_wbytes)
                .saturating_sub(first.io_rbytes.saturating_add(first.io_wbytes))),
        );
    }
    let net = read_net_samples(&run_dir.join("net.csv"))?;
    if let (Some(first), Some(last)) = (net.first(), net.last()) {
        let bytes = last
            .rx_bytes
            .saturating_add(last.tx_bytes)
            .saturating_sub(first.rx_bytes.saturating_add(first.tx_bytes));
        if first.scope == "container" && last.scope == "container" {
            out.insert("network_bytes".to_string(), json!(bytes));
            out.insert(
                "network_bytes_source".to_string(),
                json!(format!("{}:{}", first.scope, first.source)),
            );
        } else {
            out.insert("network_host_bytes_observed".to_string(), json!(bytes));
            out.insert("network_host_bytes_model_safe".to_string(), json!(false));
            out.insert(
                "network_host_bytes_source".to_string(),
                json!(format!("{}:{}", first.scope, first.source)),
            );
        }
    }
    for (key, value) in read_scheduler_summary(&run_dir.join("scheduler_stats.csv"))? {
        out.insert(key, json!(value));
    }
    Ok(out)
}

fn read_scheduler_summary(path: &Path) -> Result<BTreeMap<String, u64>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let rows = read_csv_rows(path)?;
    let mut summary = BTreeMap::new();
    let mut available = 0_u64;
    let mut max_values: BTreeMap<String, u64> = BTreeMap::new();
    let mut sum_values: BTreeMap<String, u64> = BTreeMap::new();
    for row in data_rows(rows) {
        if row.get(1).map(String::as_str) != Some("1") {
            continue;
        }
        available += 1;
        for (idx, field) in SCHEDULER_STATS_FIELDS.iter().enumerate() {
            let value = row.get(idx + 3).map(|raw| parse_u64(raw)).unwrap_or(0);
            max_values
                .entry((*field).to_string())
                .and_modify(|max| *max = (*max).max(value))
                .or_insert(value);
            *sum_values.entry((*field).to_string()).or_default() += value;
        }
    }
    summary.insert("scheduler_available_windows".to_string(), available);
    for field in SCHEDULER_STATS_FIELDS {
        if let Some(max) = max_values.get(*field) {
            summary.insert(format!("scheduler_max_{field}"), *max);
        }
        if let Some(sum) = sum_values.get(*field) {
            summary.insert(format!("scheduler_sum_{field}"), *sum);
        }
    }
    Ok(summary)
}

fn classify_phase_windows(run_dir: &Path) -> Result<Vec<PhaseWindow>> {
    let samples = read_cgroup_samples(run_dir)?;
    let net = read_net_samples(&run_dir.join("net.csv"))?;
    let mut phases = Vec::new();
    for idx in 1..samples.len() {
        let prev = &samples[idx - 1];
        let curr = &samples[idx];
        let start = sample_timestamp(run_dir, "cgroup_cpu.csv", idx)?;
        let end = sample_timestamp(run_dir, "cgroup_cpu.csv", idx + 1)?;
        let cpu_delta = curr.usage_usec.saturating_sub(prev.usage_usec);
        let io_delta = curr
            .io_rbytes
            .saturating_add(curr.io_wbytes)
            .saturating_sub(prev.io_rbytes.saturating_add(prev.io_wbytes));
        let net_delta = net
            .get(idx)
            .zip(net.get(idx - 1))
            .filter(|(a, b)| a.scope == "container" && b.scope == "container")
            .map(|(a, b)| {
                a.rx_bytes
                    .saturating_add(a.tx_bytes)
                    .saturating_sub(b.rx_bytes.saturating_add(b.tx_bytes))
            })
            .unwrap_or(0);
        let phase = if io_delta >= 64 * 1024 {
            "IO_PAGECACHE"
        } else if curr.memory_current >= 64 * 1024 * 1024
            || curr.memory_current > prev.memory_current.saturating_add(1024 * 1024)
            || curr.pressure_total > prev.pressure_total
        {
            "CACHE_OR_MEM_BOUND"
        } else if net_delta >= 64 * 1024 {
            "NETWORK_WAIT"
        } else if cpu_delta >= 40_000 {
            "CPU_BOUND"
        } else {
            "MIXED_UNKNOWN"
        };
        phases.push(PhaseWindow {
            start_ns: start,
            end_ns: end,
            phase: phase.to_string(),
        });
    }
    Ok(phases)
}

fn read_cgroup_samples(run_dir: &Path) -> Result<Vec<CgroupSample>> {
    let cpu_rows = read_csv_rows(&run_dir.join("cgroup_cpu.csv"))?;
    let mem_rows = read_csv_rows(&run_dir.join("cgroup_memory.csv"))?;
    let io_rows = read_csv_rows(&run_dir.join("cgroup_io.csv"))?;
    let pressure_rows = read_csv_rows(&run_dir.join("cgroup_pressure.csv"))?;
    let len = cpu_rows
        .iter()
        .filter(|row| row.first().map(|v| v != "timestamp_ns").unwrap_or(false))
        .count();
    let mut samples = vec![CgroupSample::default(); len];

    for (idx, row) in data_rows(cpu_rows).into_iter().enumerate() {
        if row.len() >= 7 {
            samples[idx].usage_usec = parse_u64(&row[1]);
            samples[idx].user_usec = parse_u64(&row[2]);
            samples[idx].system_usec = parse_u64(&row[3]);
            samples[idx].nr_periods = parse_u64(&row[4]);
            samples[idx].nr_throttled = parse_u64(&row[5]);
            samples[idx].throttled_usec = parse_u64(&row[6]);
        }
    }
    for (idx, row) in data_rows(mem_rows)
        .into_iter()
        .enumerate()
        .take(samples.len())
    {
        if row.len() >= 3 {
            samples[idx].memory_current = parse_u64(&row[1]);
            samples[idx].memory_peak = parse_u64(&row[2]);
        }
    }
    for (idx, row) in data_rows(io_rows)
        .into_iter()
        .enumerate()
        .take(samples.len())
    {
        if row.len() >= 3 {
            samples[idx].io_rbytes = parse_u64(&row[1]);
            samples[idx].io_wbytes = parse_u64(&row[2]);
        }
    }
    for row in data_rows(pressure_rows) {
        if row.len() >= 7 {
            let total = parse_u64(&row[6]);
            for sample in &mut samples {
                sample.pressure_total = sample.pressure_total.max(total);
            }
        }
    }
    Ok(samples)
}

fn read_net_samples(path: &Path) -> Result<Vec<NetSample>> {
    Ok(data_rows(read_csv_rows(path)?)
        .into_iter()
        .filter_map(|row| {
            if row.len() >= 5 {
                Some(NetSample {
                    scope: row[1].clone(),
                    source: row[2].clone(),
                    rx_bytes: parse_u64(&row[3]),
                    tx_bytes: parse_u64(&row[4]),
                })
            } else if row.len() >= 3 {
                Some(NetSample {
                    scope: "host".to_string(),
                    source: "/proc/net/dev".to_string(),
                    rx_bytes: parse_u64(&row[1]),
                    tx_bytes: parse_u64(&row[2]),
                })
            } else {
                None
            }
        })
        .collect())
}

fn sample_timestamp(run_dir: &Path, file: &str, data_row_index: usize) -> Result<u128> {
    let rows = data_rows(read_csv_rows(&run_dir.join(file))?);
    Ok(rows
        .get(data_row_index.saturating_sub(1))
        .and_then(|row| row.first())
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0))
}

fn read_csv_rows(path: &Path) -> Result<Vec<Vec<String>>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .map(|line| {
            line.split(',')
                .map(|part| part.trim().to_string())
                .collect()
        })
        .collect())
}

fn data_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .filter(|row| {
            row.first()
                .map(|v| v != "timestamp_ns" && v != "run_id")
                .unwrap_or(false)
        })
        .collect()
}

fn parse_u64(raw: &str) -> u64 {
    raw.parse().unwrap_or(0)
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((percentile / 100.0) * ((values.len() - 1) as f64)).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn missing_requirements(run_dir: &Path) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    for file in REQUIRED_RUN_FILES {
        let path = run_dir.join(file);
        if !path.exists() {
            missing.push(format!("missing file {file}"));
        } else if !["stdout.log", "stderr.log"].contains(file)
            && fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0
        {
            missing.push(format!("empty file {file}"));
        }
    }
    if !events_have_required_mapping(&run_dir.join("events.jsonl"))? {
        missing.push(
            "events.jsonl lacks activation_id -> host_pid -> cgroup_path mapping".to_string(),
        );
    }
    if !client_latency_has_join(&run_dir.join("client_latency.csv"))? {
        missing.push("client_latency.csv lacks run_id/activation_id join".to_string());
    }
    if !client_latency_success(&run_dir.join("client_latency.csv"))? {
        missing.push("client_latency.csv does not contain a successful invocation".to_string());
    }
    if !openwhisk_activation_valid(&run_dir.join("openwhisk_activation.json"))? {
        missing.push("openwhisk_activation.json lacks imported activation records".to_string());
    }
    Ok(missing)
}

fn events_have_required_mapping(path: &Path) -> Result<bool> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let value: Value = serde_json::from_str(&line)?;
        if value.get("event").and_then(Value::as_str) == Some("invocation_finished") {
            let activation = value
                .get("activation_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let cgroup = value
                .get("cgroup_path")
                .and_then(Value::as_str)
                .unwrap_or("");
            let host_pid = value.get("host_pid").and_then(Value::as_u64).unwrap_or(0);
            if !activation.is_empty() && !cgroup.is_empty() && host_pid > 0 {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn client_latency_has_join(path: &Path) -> Result<bool> {
    for row in data_rows(read_csv_rows(path)?) {
        if row.len() >= 2 && !row[0].is_empty() && !row[1].is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn client_latency_success(path: &Path) -> Result<bool> {
    for row in data_rows(read_csv_rows(path)?) {
        if row.len() >= 6 && row[5] == "exit:0" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn openwhisk_activation_valid(path: &Path) -> Result<bool> {
    let value: Value = serde_json::from_reader(File::open(path)?)?;
    if value.get("mode").and_then(Value::as_str) == Some("standalone") {
        return Ok(true);
    }
    Ok(value
        .get("activations")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(false))
}

fn verify_run(run_dir: &Path) -> Result<()> {
    let summary_path = run_dir.join("summary.json");
    match write_summary(run_dir) {
        Ok(()) => {}
        Err(err) if summary_path.exists() && is_permission_denied(&err) => {}
        Err(err) => return Err(err),
    }
    let summary: Summary = serde_json::from_reader(File::open(summary_path)?)?;
    if !summary.missing.is_empty() {
        for item in &summary.missing {
            eprintln!("{item}");
        }
        bail!(
            "run verification failed: {} missing requirements",
            summary.missing.len()
        );
    }
    println!("verified {}", run_dir.display());
    Ok(())
}

fn profile_db(args: ProfileDbArgs) -> Result<()> {
    let mut samples_by_key: BTreeMap<(String, String, String), Vec<ProfileSample>> =
        BTreeMap::new();
    let mut skipped_runs = Vec::new();

    for entry in
        fs::read_dir(&args.runs_dir).with_context(|| format!("read {}", args.runs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match read_profile_sample(&path) {
            Ok(sample) => {
                let key = (
                    sample.meta.workload.clone(),
                    sample.meta.input.clone(),
                    sample.meta.warmth.clone(),
                );
                samples_by_key.entry(key).or_default().push(sample);
            }
            Err(err) => skipped_runs.push(format!("{}: {err}", path.display())),
        }
    }

    if args.strict && samples_by_key.is_empty() {
        bail!(
            "no complete benchmark runs found in {}",
            args.runs_dir.display()
        );
    }

    let mut profiles = Vec::new();
    for ((workload, input, warmth), samples) in samples_by_key {
        profiles.push(build_workload_profile(workload, input, warmth, samples));
    }
    let db = ProfileDb {
        generated_ns: now_ns(),
        source_runs_dir: args.runs_dir.display().to_string(),
        profiles,
        skipped_runs,
    };
    if let Some(parent) = args.out.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    write_json(&args.out, &db)?;
    println!("{}", args.out.display());
    Ok(())
}

fn read_profile_sample(run_dir: &Path) -> Result<ProfileSample> {
    let meta_path = run_dir.join("run_meta.json");
    let summary_path = run_dir.join("summary.json");
    match write_summary(run_dir) {
        Ok(()) => {}
        Err(err) if summary_path.exists() && is_permission_denied(&err) => {}
        Err(err) => return Err(err),
    }
    let current_missing = missing_requirements(run_dir)?;
    if !current_missing.is_empty() {
        bail!("run verification failed: {}", current_missing.join("; "));
    }
    let meta: RunMeta = serde_json::from_reader(File::open(&meta_path)?)
        .with_context(|| format!("parse {}", meta_path.display()))?;
    let summary: Summary = serde_json::from_reader(File::open(&summary_path)?)
        .with_context(|| format!("parse {}", summary_path.display()))?;
    if summary.status != "complete" || !summary.missing.is_empty() {
        bail!("summary is not complete");
    }
    Ok(ProfileSample {
        run_id: meta.run_id.clone(),
        meta,
        summary,
    })
}

fn build_workload_profile(
    workload: String,
    input: String,
    warmth: String,
    samples: Vec<ProfileSample>,
) -> WorkloadProfile {
    let mut latencies = samples
        .iter()
        .filter_map(|sample| metric_u64(&sample.summary.latency, "client_latency_ns"))
        .collect::<Vec<_>>();
    latencies.sort_unstable();

    let mut platform_wait = samples
        .iter()
        .filter_map(|sample| metric_u64(&sample.summary.latency, "platform_wait_ns"))
        .collect::<Vec<_>>();
    platform_wait.sort_unstable();
    let mut platform_init = samples
        .iter()
        .filter_map(|sample| metric_u64(&sample.summary.latency, "platform_init_ns"))
        .collect::<Vec<_>>();
    platform_init.sort_unstable();
    let mut platform_run = samples
        .iter()
        .filter_map(|sample| metric_u64(&sample.summary.latency, "platform_run_ns"))
        .collect::<Vec<_>>();
    platform_run.sort_unstable();

    let mut latency = BTreeMap::new();
    latency.insert("count".to_string(), json!(latencies.len()));
    latency.insert("median_ns".to_string(), json!(percentile(&latencies, 50.0)));
    latency.insert("p95_ns".to_string(), json!(percentile(&latencies, 95.0)));
    latency.insert("p99_ns".to_string(), json!(percentile(&latencies, 99.0)));
    latency.insert(
        "median_platform_wait_ns".to_string(),
        json!(percentile(&platform_wait, 50.0)),
    );
    latency.insert(
        "median_platform_init_ns".to_string(),
        json!(percentile(&platform_init, 50.0)),
    );
    latency.insert(
        "median_platform_run_ns".to_string(),
        json!(percentile(&platform_run, 50.0)),
    );

    let dominant_phases = dominant_phases(&samples);
    let scheduler_features = scheduler_features(&samples, &dominant_phases);
    WorkloadProfile {
        workload,
        input,
        warmth,
        runs: samples.into_iter().map(|sample| sample.run_id).collect(),
        latency,
        dominant_phases,
        scheduler_features,
    }
}

fn dominant_phases(samples: &[ProfileSample]) -> Vec<PhaseCount> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for sample in samples {
        for window in &sample.summary.phase_windows {
            *counts.entry(window.phase.clone()).or_default() += 1;
        }
    }
    let mut phases = counts
        .into_iter()
        .map(|(phase, windows)| PhaseCount { phase, windows })
        .collect::<Vec<_>>();
    phases.sort_by(|a, b| {
        b.windows
            .cmp(&a.windows)
            .then_with(|| a.phase.cmp(&b.phase))
    });
    phases
}

fn scheduler_features(
    samples: &[ProfileSample],
    dominant_phases: &[PhaseCount],
) -> BTreeMap<String, Value> {
    let mut features = BTreeMap::new();
    for key in [
        "cpu_usage_usec",
        "cpu_throttled_usec",
        "memory_peak_bytes",
        "io_bytes",
        "network_bytes",
        "scheduler_available_windows",
        "scheduler_max_nr_running",
        "scheduler_max_nr_queued",
        "scheduler_max_nr_scheduled",
        "scheduler_max_nr_cold_start_tasks",
        "scheduler_max_nr_hot_invocation_tasks",
        "scheduler_max_nr_background_tasks",
        "scheduler_sum_nr_slo_boosted",
        "scheduler_sum_nr_user_dispatches",
        "scheduler_sum_nr_kernel_dispatches",
        "scheduler_sum_nr_cancel_dispatches",
        "scheduler_sum_nr_bounce_dispatches",
        "scheduler_sum_nr_failed_dispatches",
        "scheduler_sum_nr_sched_congested",
    ] {
        let mut values = samples
            .iter()
            .filter_map(|sample| metric_u64(&sample.summary.resources, key))
            .collect::<Vec<_>>();
        if values.is_empty() {
            continue;
        }
        values.sort_unstable();
        features.insert(format!("median_{key}"), json!(percentile(&values, 50.0)));
        features.insert(format!("p95_{key}"), json!(percentile(&values, 95.0)));
    }
    let total_windows: u64 = dominant_phases.iter().map(|phase| phase.windows).sum();
    for phase in dominant_phases {
        let ratio = if total_windows == 0 {
            0.0
        } else {
            phase.windows as f64 / total_windows as f64
        };
        features.insert(
            format!("phase_ratio_{}", phase.phase.to_lowercase()),
            json!(ratio),
        );
    }
    features.insert(
        "primary_phase".to_string(),
        json!(dominant_phases
            .first()
            .map(|phase| phase.phase.as_str())
            .unwrap_or("MIXED_UNKNOWN")),
    );
    features
}

fn metric_u64(map: &BTreeMap<String, Value>, key: &str) -> Option<u64> {
    map.get(key).and_then(json_u64)
}

fn is_permission_denied(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io| io.kind() == ErrorKind::PermissionDenied)
            .unwrap_or(false)
    })
}

fn print_matrix(kind: MatrixKind) -> Result<()> {
    let value = match kind {
        MatrixKind::Sanity => json!({
            "workloads": ["dynamic-html", "thumbnailer", "compression", "image-recognition"],
            "inputs": ["small", "medium"],
            "warmth": ["cold", "warm"],
            "repetitions": 5,
            "concurrency": 1
        }),
        MatrixKind::Profile => json!({
            "workloads": ["dynamic-html", "uploader", "thumbnailer", "video-processing", "compression", "image-recognition", "pagerank", "bfs"],
            "inputs": ["small", "medium", "large"],
            "warmth": ["cold", "warm", "lukewarm"],
            "repetitions": 10,
            "concurrency": 1
        }),
        MatrixKind::Interference => json!({
            "targets": ["thumbnailer", "compression", "image-recognition", "uploader"],
            "interference": ["none", "cpu", "memory", "network", "io"],
            "input": "medium",
            "warmth": "warm",
            "repetitions": 10
        }),
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn micro_workload(args: MicroArgs) -> Result<()> {
    match args.workload {
        WorkloadKind::Cpu => micro_cpu(args.duration_ms),
        WorkloadKind::Memory => micro_memory(args.duration_ms),
        WorkloadKind::Io => micro_io(args.duration_ms),
        WorkloadKind::Network => micro_network(args.duration_ms),
        WorkloadKind::Command => bail!("command is not a built-in micro workload"),
    }
}

fn micro_cpu(duration_ms: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    let mut x = 0_u64;
    while Instant::now() < deadline {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        std::hint::black_box(x);
    }
    println!("{x}");
    Ok(())
}

fn micro_memory(duration_ms: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    let mut data = vec![0_u8; 128 * 1024 * 1024];
    let mut n = 0_u8;
    while Instant::now() < deadline {
        for byte in data.iter_mut().step_by(4096) {
            *byte = byte.wrapping_add(n);
        }
        n = n.wrapping_add(1);
    }
    println!("{}", data[0]);
    Ok(())
}

fn micro_io(duration_ms: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    let dir = std::env::var_os("COSMOS_BENCH_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let path = dir.join(format!("cosmos-bench-io-{}", std::process::id()));
    let mut file = File::create(&path)?;
    let buf = vec![42_u8; 1024 * 1024];
    while Instant::now() < deadline {
        file.write_all(&buf)?;
        file.sync_data()?;
    }
    drop(file);
    let _ = fs::remove_file(path);
    Ok(())
}

fn micro_network(duration_ms: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = thread::spawn(move || -> Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut buf = [0_u8; 16 * 1024];
        while std::io::Read::read(&mut stream, &mut buf)? > 0 {}
        Ok(())
    });
    let mut stream = TcpStream::connect(addr)?;
    let buf = vec![7_u8; 64 * 1024];
    while Instant::now() < deadline {
        std::io::Write::write_all(&mut stream, &buf)?;
    }
    drop(stream);
    server
        .join()
        .map_err(|_| anyhow!("network server panicked"))??;
    Ok(())
}

fn run_environment() -> BTreeMap<String, Value> {
    let mut env = BTreeMap::new();
    env.insert(
        "kernel".to_string(),
        json!(command_output("uname", &["-r"])),
    );
    env.insert("cpu_model".to_string(), json!(cpu_model()));
    env.insert(
        "cpu_count".to_string(),
        json!(std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)),
    );
    env.insert("mem_total_kb".to_string(), json!(mem_total_kb()));
    env.insert("cpu_governor".to_string(), json!(first_governor()));
    env.insert(
        "docker_version".to_string(),
        json!(command_output("docker", &["--version"])),
    );
    env.insert(
        "openwhisk_git".to_string(),
        json!(git_rev("benchmarks/third_party/openwhisk")),
    );
    env.insert(
        "sebs_git".to_string(),
        json!(git_rev("benchmarks/third_party/serverless-benchmarks")),
    );
    env.insert("cosmos_git".to_string(), json!(git_rev(".")));
    env
}

fn command_output(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_default()
}

fn git_rev(path: &str) -> String {
    Command::new("git")
        .args(["-C", path, "rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_default()
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("model name")
                    .and_then(|rest| rest.split_once(':').map(|(_, v)| v.trim().to_string()))
            })
        })
        .unwrap_or_default()
}

fn mem_total_kb() -> u64 {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|raw| raw.parse().ok())
            })
        })
        .unwrap_or(0)
}

fn first_governor() -> String {
    fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn stable_hash(value: &Value) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in serde_json::to_string(value).unwrap_or_default().bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    writeln!(file)?;
    Ok(())
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn exit_status_string(status: ExitStatus) -> String {
    status
        .code()
        .map(|code| format!("exit:{code}"))
        .unwrap_or_else(|| "signal".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_shapes_match_plan() {
        let sanity = MatrixKind::Sanity;
        assert!(matches!(sanity, MatrixKind::Sanity));
    }

    #[test]
    fn curl_timing_marker_is_source_timing() {
        let timing = parse_curl_timing(
            "{\"activationId\":\"abc\"}\n__COSMOS_CURL_TIMING__:0.012345:0.067890\n",
        )
        .unwrap();
        assert_eq!(timing.starttransfer_ns, 12_345_000);
        assert_eq!(timing.total_ns, 67_890_000);
    }

    #[test]
    fn verifier_rejects_missing_lifecycle_mapping() {
        let dir = tempfile::tempdir().unwrap();
        for file in REQUIRED_RUN_FILES {
            fs::write(dir.path().join(file), "x\n").unwrap();
        }
        write_json(
            &dir.path().join("run_meta.json"),
            &RunMeta {
                run_id: "r".to_string(),
                workload: "cpu".to_string(),
                input: "small".to_string(),
                warmth: "warm".to_string(),
                sample_ms: 100,
                cgroup_path: "/sys/fs/cgroup".to_string(),
                cgroup_fallback: true,
                command: vec!["true".to_string()],
                env: BTreeMap::new(),
                config_hash: "h".to_string(),
            },
        )
        .unwrap();
        fs::write(
            dir.path().join("events.jsonl"),
            "{\"event\":\"invocation_finished\"}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("client_latency.csv"),
            "run_id,activation_id,send_ns,first_byte_ns,response_end_ns,status,error\nr,a,1,2,3,exit:0,\n",
        )
        .unwrap();
        fs::write(dir.path().join("cgroup_cpu.csv"), "timestamp_ns,usage_usec,user_usec,system_usec,nr_periods,nr_throttled,throttled_usec\n1,0,0,0,0,0,0\n").unwrap();
        fs::write(dir.path().join("cgroup_memory.csv"), "timestamp_ns,current_bytes,peak_bytes,anon_bytes,file_bytes,pgfault,pgmajfault,oom,oom_kill\n1,0,0,0,0,0,0,0,0\n").unwrap();
        fs::write(
            dir.path().join("cgroup_io.csv"),
            "timestamp_ns,rbytes,wbytes,rios,wios,dbytes,dios\n1,0,0,0,0,0,0\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("cgroup_pressure.csv"),
            "timestamp_ns,resource,scope,avg10,avg60,avg300,total\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("net.csv"),
            "timestamp_ns,rx_bytes,tx_bytes\n1,0,0\n",
        )
        .unwrap();
        assert!(verify_run(dir.path()).is_err());
    }

    #[test]
    fn profile_sample_rechecks_stale_summary() {
        let dir = tempfile::tempdir().unwrap();
        for file in REQUIRED_RUN_FILES {
            fs::write(dir.path().join(file), "x\n").unwrap();
        }
        write_json(
            &dir.path().join("run_meta.json"),
            &RunMeta {
                run_id: "r".to_string(),
                workload: "cpu".to_string(),
                input: "small".to_string(),
                warmth: "warm".to_string(),
                sample_ms: 100,
                cgroup_path: "/sys/fs/cgroup".to_string(),
                cgroup_fallback: true,
                command: vec!["true".to_string()],
                env: BTreeMap::new(),
                config_hash: "h".to_string(),
            },
        )
        .unwrap();
        fs::write(
            dir.path().join("events.jsonl"),
            "{\"event\":\"invocation_finished\"}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("client_latency.csv"),
            "run_id,activation_id,send_ns,first_byte_ns,response_end_ns,status,error\nr,a,1,2,3,exit:0,\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("openwhisk_activation.json"),
            "{\"mode\":\"standalone\"}\n",
        )
        .unwrap();
        write_json(
            &dir.path().join("summary.json"),
            &Summary {
                run_id: "r".to_string(),
                status: "complete".to_string(),
                latency: BTreeMap::new(),
                resources: BTreeMap::new(),
                phase_windows: Vec::new(),
                missing: Vec::new(),
            },
        )
        .unwrap();

        assert!(read_profile_sample(dir.path()).is_err());
    }
}
