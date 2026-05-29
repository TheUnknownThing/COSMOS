// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use cosmos_metadata_model::{PhaseSequenceEntry, ProfileCatalogFile, ProfileHints};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Learn profile hints and phase sequences from scheduler runtime traces.
    Analyze(AnalyzeArgs),
}

#[derive(Debug, Parser)]
struct AnalyzeArgs {
    /// Root directory containing per-profile runtime trace directories.
    #[arg(long)]
    trace_root: PathBuf,
    /// Optional base catalog to merge learned hints into.
    #[arg(long)]
    base_catalog: Option<PathBuf>,
    /// Output profile catalog JSON path.
    #[arg(long)]
    out: PathBuf,
    /// Output human/audit report JSON path.
    #[arg(long)]
    report_out: PathBuf,
    /// Minimum complete warm traces required before writing hints for a profile.
    #[arg(long, default_value_t = 5)]
    min_warm_traces: u64,
    /// Fail if no profile reaches --min-warm-traces.
    #[arg(long)]
    strict: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RuntimeTraceMeta {
    profile_id: String,
    invocation_id: u64,
    tgid: u32,
    created_at_ns: u64,
    deadline_ns: u64,
    estimated_duration_ns: u64,
    warmth: String,
    cgroup_path: String,
}

#[derive(Debug)]
struct RuntimeTraceSample {
    meta: RuntimeTraceMeta,
    samples: Vec<CgroupSample>,
    phase_windows: Vec<PhaseWindow>,
}

#[derive(Debug, Default, Clone)]
struct CgroupSample {
    usage_usec: u64,
    memory_current: u64,
    memory_peak: u64,
    io_rbytes: u64,
    io_wbytes: u64,
    pressure_total: u64,
}

#[derive(Debug, Clone, Serialize)]
struct PhaseWindow {
    start_ns: u128,
    end_ns: u128,
    phase: String,
}

#[derive(Debug, Default, Clone, Serialize)]
struct PhaseSummary {
    traces: u64,
    primary_phase: String,
    phase_ratios: BTreeMap<String, f64>,
    median_duration_ns: u64,
    p95_duration_ns: u64,
    median_memory_current_bytes: u64,
    p95_memory_peak_bytes: u64,
    p95_io_bytes_per_sec: u64,
    phase_sequence: Vec<PhaseSequenceEntry>,
}

#[derive(Debug, Default, Serialize)]
struct AnalysisReport {
    generated_ns: u128,
    trace_root: String,
    min_warm_traces: u64,
    profiles: Vec<ProfileReport>,
    skipped: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProfileReport {
    profile_id: String,
    warm: PhaseSummary,
    cold: PhaseSummary,
    learned: Option<ProfileHints>,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Commands::Analyze(args) => analyze(args),
    }
}

fn analyze(args: AnalyzeArgs) -> Result<()> {
    let mut samples_by_profile: BTreeMap<String, Vec<RuntimeTraceSample>> = BTreeMap::new();
    let mut skipped = Vec::new();

    for profile_entry in fs::read_dir(&args.trace_root)
        .with_context(|| format!("read {}", args.trace_root.display()))?
    {
        let profile_entry = profile_entry?;
        let profile_path = profile_entry.path();
        if !profile_path.is_dir() {
            continue;
        }
        for run_entry in fs::read_dir(&profile_path)
            .with_context(|| format!("read {}", profile_path.display()))?
        {
            let run_entry = run_entry?;
            let run_path = run_entry.path();
            if !run_path.is_dir() {
                continue;
            }
            match read_runtime_trace_sample(&run_path) {
                Ok(sample) => {
                    samples_by_profile
                        .entry(sample.meta.profile_id.clone())
                        .or_default()
                        .push(sample);
                }
                Err(err) => skipped.push(format!("{}: {err:#}", run_path.display())),
            }
        }
    }

    let mut catalog = load_catalog_file(args.base_catalog.as_deref())?;
    let mut report = AnalysisReport {
        generated_ns: now_ns(),
        trace_root: args.trace_root.display().to_string(),
        min_warm_traces: args.min_warm_traces,
        skipped,
        ..AnalysisReport::default()
    };

    let mut learned_profiles = 0usize;
    for (profile_id, samples) in samples_by_profile {
        let warm = samples
            .iter()
            .filter(|sample| sample.meta.warmth != "cold")
            .collect::<Vec<_>>();
        let cold = samples
            .iter()
            .filter(|sample| sample.meta.warmth == "cold")
            .collect::<Vec<_>>();
        let warm_summary = summarize_phase_samples(&warm);
        let cold_summary = summarize_phase_samples(&cold);

        let learned = if warm_summary.traces >= args.min_warm_traces {
            let learned = learned_profile_hints(&warm_summary, &cold_summary);
            let entry = catalog.profiles.entry(profile_id.clone()).or_default();
            entry.overlay(&learned);
            learned_profiles += 1;
            Some(learned)
        } else {
            report.skipped.push(format!(
                "{}: only {} complete warm traces, need {}",
                profile_id, warm_summary.traces, args.min_warm_traces
            ));
            None
        };

        report.profiles.push(ProfileReport {
            profile_id,
            warm: warm_summary,
            cold: cold_summary,
            learned,
        });
    }

    if args.strict && learned_profiles == 0 {
        bail!(
            "no profiles with at least {} complete warm traces found under {}",
            args.min_warm_traces,
            args.trace_root.display()
        );
    }

    write_json(&args.out, &catalog)?;
    write_json(&args.report_out, &report)?;
    println!(
        "learned_profiles={} catalog={} report={}",
        learned_profiles,
        args.out.display(),
        args.report_out.display()
    );
    Ok(())
}

fn load_catalog_file(path: Option<&Path>) -> Result<ProfileCatalogFile> {
    let Some(path) = path else {
        return Ok(ProfileCatalogFile {
            version: 1,
            profiles: BTreeMap::new(),
        });
    };
    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))
}

fn read_runtime_trace_sample(run_dir: &Path) -> Result<RuntimeTraceSample> {
    let meta_path = run_dir.join("run_meta.json");
    let meta: RuntimeTraceMeta = serde_json::from_reader(File::open(&meta_path)?)
        .with_context(|| format!("parse {}", meta_path.display()))?;
    if meta.profile_id.is_empty() {
        bail!("missing profile_id");
    }
    if !runtime_trace_complete(&run_dir.join("events.jsonl"))? {
        bail!("trace is incomplete");
    }
    let (samples, timestamps) = read_runtime_cgroup_samples(run_dir)?;
    if samples.len() < 2 {
        bail!("need at least two cgroup samples");
    }
    let phase_windows = classify_runtime_phase_windows(&samples, &timestamps);
    Ok(RuntimeTraceSample {
        meta,
        samples,
        phase_windows,
    })
}

fn runtime_trace_complete(path: &Path) -> Result<bool> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let value: Value = serde_json::from_str(&line)?;
        if value.get("event").and_then(Value::as_str) == Some("invocation_finished") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_runtime_cgroup_samples(run_dir: &Path) -> Result<(Vec<CgroupSample>, Vec<u128>)> {
    let cpu_rows = data_rows(read_csv_rows(&run_dir.join("cgroup_cpu.csv"))?);
    let mem_rows = data_rows(read_csv_rows(&run_dir.join("cgroup_memory.csv"))?);
    let io_rows = data_rows(read_csv_rows(&run_dir.join("cgroup_io.csv"))?);
    let pressure_rows = data_rows(read_csv_rows(&run_dir.join("cgroup_pressure.csv"))?);

    let len = cpu_rows.len();
    let mut samples = vec![CgroupSample::default(); len];
    let mut timestamps = vec![0u128; len];

    for (idx, row) in cpu_rows.into_iter().enumerate() {
        if row.len() >= 7 {
            timestamps[idx] = row[0].parse().unwrap_or(0);
            samples[idx].usage_usec = parse_u64(&row[1]);
        }
    }
    for (idx, row) in mem_rows.into_iter().enumerate().take(samples.len()) {
        if row.len() >= 3 {
            samples[idx].memory_current = parse_u64(&row[1]);
            samples[idx].memory_peak = parse_u64(&row[2]);
        }
    }
    for (idx, row) in io_rows.into_iter().enumerate().take(samples.len()) {
        if row.len() >= 3 {
            samples[idx].io_rbytes = parse_u64(&row[1]);
            samples[idx].io_wbytes = parse_u64(&row[2]);
        }
    }

    let mut pressure_by_ts = BTreeMap::<u128, u64>::new();
    for row in pressure_rows {
        if row.len() >= 7 {
            let timestamp = row[0].parse().unwrap_or(0);
            let total = parse_u64(&row[6]);
            let entry = pressure_by_ts.entry(timestamp).or_insert(0);
            *entry = (*entry).max(total);
        }
    }
    for (idx, timestamp) in timestamps.iter().copied().enumerate() {
        samples[idx].pressure_total = pressure_by_ts.get(&timestamp).copied().unwrap_or(0);
    }

    Ok((samples, timestamps))
}

fn classify_runtime_phase_windows(
    samples: &[CgroupSample],
    timestamps: &[u128],
) -> Vec<PhaseWindow> {
    let mut phases = Vec::new();
    for idx in 1..samples.len() {
        let prev = &samples[idx - 1];
        let curr = &samples[idx];
        let cpu_delta = curr.usage_usec.saturating_sub(prev.usage_usec);
        let io_delta = curr
            .io_rbytes
            .saturating_add(curr.io_wbytes)
            .saturating_sub(prev.io_rbytes.saturating_add(prev.io_wbytes));
        let phase = if io_delta >= 64 * 1024 {
            "io_bound"
        } else if curr.memory_current >= 64 * 1024 * 1024
            || curr.memory_current > prev.memory_current.saturating_add(1024 * 1024)
            || curr.pressure_total > prev.pressure_total
        {
            "memory_bound"
        } else if cpu_delta >= 40_000 {
            "cpu_bound"
        } else {
            "mixed"
        };
        phases.push(PhaseWindow {
            start_ns: timestamps.get(idx - 1).copied().unwrap_or(0),
            end_ns: timestamps.get(idx).copied().unwrap_or(0),
            phase: phase.to_string(),
        });
    }
    phases
}

fn summarize_phase_samples(samples: &[&RuntimeTraceSample]) -> PhaseSummary {
    let mut counts = BTreeMap::<String, u64>::new();
    let mut durations = Vec::new();
    let mut memory_current = Vec::new();
    let mut memory_peak = Vec::new();
    let mut io_throughput = Vec::new();
    let mut normalized_windows = Vec::new();

    for sample in samples {
        if let (Some(first), Some(last)) = (sample.phase_windows.first(), sample.phase_windows.last()) {
            let trace_start = first.start_ns;
            let trace_duration = last.end_ns.saturating_sub(trace_start);
            durations.push(trace_duration as u64);
            if trace_duration > 0 {
                for window in &sample.phase_windows {
                    let start_pct =
                        window.start_ns.saturating_sub(trace_start).saturating_mul(1000)
                            / trace_duration;
                    let end_pct =
                        window.end_ns.saturating_sub(trace_start).saturating_mul(1000)
                            / trace_duration;
                    normalized_windows.push(NormalizedWindow {
                        start_millipct: start_pct.min(1000) as u32,
                        end_millipct: end_pct.min(1000) as u32,
                        phase: window.phase.clone(),
                    });
                }
            }
        } else if sample.meta.estimated_duration_ns > 0 {
            durations.push(sample.meta.estimated_duration_ns);
        }
        for window in &sample.phase_windows {
            *counts.entry(window.phase.clone()).or_default() += 1;
        }
        for point in &sample.samples {
            memory_current.push(point.memory_current);
            memory_peak.push(point.memory_peak);
        }
        for idx in 1..sample.samples.len() {
            let prev = &sample.samples[idx - 1];
            let curr = &sample.samples[idx];
            let start_ns = sample
                .phase_windows
                .get(idx - 1)
                .map(|w| w.start_ns)
                .unwrap_or(0);
            let end_ns = sample
                .phase_windows
                .get(idx - 1)
                .map(|w| w.end_ns)
                .unwrap_or(0);
            let dt_ns = end_ns.saturating_sub(start_ns);
            if dt_ns == 0 {
                continue;
            }
            let io_bytes = curr
                .io_rbytes
                .saturating_add(curr.io_wbytes)
                .saturating_sub(prev.io_rbytes.saturating_add(prev.io_wbytes));
            io_throughput.push(io_bytes.saturating_mul(1_000_000_000) / dt_ns as u64);
        }
    }

    durations.sort_unstable();
    memory_current.sort_unstable();
    memory_peak.sort_unstable();
    io_throughput.sort_unstable();

    let total_windows: u64 = counts.values().copied().sum();
    let mut phase_ratios = BTreeMap::new();
    for (phase, windows) in &counts {
        let ratio = if total_windows == 0 {
            0.0
        } else {
            *windows as f64 / total_windows as f64
        };
        phase_ratios.insert(phase.clone(), ratio);
    }
    let primary_phase = counts
        .iter()
        .max_by(|(phase_a, count_a), (phase_b, count_b)| {
            count_a.cmp(count_b).then_with(|| phase_b.cmp(phase_a))
        })
        .map(|(phase, _)| phase.clone())
        .unwrap_or_else(|| "mixed".to_string());

    PhaseSummary {
        traces: samples.len() as u64,
        primary_phase,
        phase_ratios,
        median_duration_ns: percentile(&durations, 50.0),
        p95_duration_ns: percentile(&durations, 95.0),
        median_memory_current_bytes: percentile(&memory_current, 50.0),
        p95_memory_peak_bytes: percentile(&memory_peak, 95.0),
        p95_io_bytes_per_sec: percentile(&io_throughput, 95.0),
        phase_sequence: build_phase_sequence(&normalized_windows),
    }
}

#[derive(Debug)]
struct NormalizedWindow {
    start_millipct: u32,
    end_millipct: u32,
    phase: String,
}

fn build_phase_sequence(windows: &[NormalizedWindow]) -> Vec<PhaseSequenceEntry> {
    if windows.is_empty() {
        return Vec::new();
    }
    let mut bins = Vec::with_capacity(100);
    for bin in 0..100_u32 {
        let start = bin * 10;
        let end = start + 10;
        let phase = dominant_phase_for_bin(windows, start, end);
        bins.push(phase);
    }
    compress_phase_bins(&bins)
}

fn dominant_phase_for_bin(windows: &[NormalizedWindow], start: u32, end: u32) -> String {
    let mut scores = BTreeMap::<String, u32>::new();
    for window in windows {
        let overlap = window
            .end_millipct
            .min(end)
            .saturating_sub(window.start_millipct.max(start));
        if overlap > 0 {
            *scores.entry(window.phase.clone()).or_default() += overlap;
        }
    }
    scores
        .into_iter()
        .max_by(|(phase_a, score_a), (phase_b, score_b)| {
            score_a.cmp(score_b).then_with(|| phase_b.cmp(phase_a))
        })
        .map(|(phase, _)| phase)
        .unwrap_or_else(|| "mixed".to_string())
}

fn compress_phase_bins(bins: &[String]) -> Vec<PhaseSequenceEntry> {
    let mut entries: Vec<PhaseSequenceEntry> = Vec::new();
    for phase in bins {
        if let Some(last) = entries.last_mut() {
            if last.kind == *phase {
                last.duration_pct += 1;
                continue;
            }
        }
        entries.push(PhaseSequenceEntry {
            kind: phase.clone(),
            duration_pct: 1,
        });
    }
    entries
}

fn learned_profile_hints(warm: &PhaseSummary, cold: &PhaseSummary) -> ProfileHints {
    let cpu_bound = warm.phase_ratios.get("cpu_bound").copied().unwrap_or(0.0);
    let mixed = warm.phase_ratios.get("mixed").copied().unwrap_or(0.0);
    let io_ratio = warm.phase_ratios.get("io_bound").copied().unwrap_or(0.0);
    let cpu_intensity = (cpu_bound + 0.5 * mixed).clamp(0.0, 1.0);
    let working_set = round_up_16_mib(warm.median_memory_current_bytes);
    let memory_bytes = round_up_16_mib(warm.p95_memory_peak_bytes.max(working_set));
    let io_weight = match warm.primary_phase.as_str() {
        "io_bound" => 700,
        "memory_bound" => 350,
        "cpu_bound" => 400,
        _ => 500,
    };
    ProfileHints {
        cpu_intensity: Some(cpu_intensity),
        memory_bytes: Some(memory_bytes),
        working_set_bytes: Some(working_set),
        io_weight: Some(io_weight),
        io_bandwidth_bytes_per_sec: (io_ratio >= 0.20 && warm.p95_io_bytes_per_sec > 0)
            .then_some(warm.p95_io_bytes_per_sec),
        network_bandwidth_bytes_per_sec: None,
        phase_sequence: (!warm.phase_sequence.is_empty()).then_some(warm.phase_sequence.clone()),
        estimated_duration_ns: (warm.median_duration_ns > 0).then_some(warm.median_duration_ns),
        cold_load_penalty_ns: cold
            .median_duration_ns
            .checked_sub(warm.median_duration_ns)
            .filter(|value| *value > 0),
    }
}

fn read_csv_rows(path: &Path) -> Result<Vec<Vec<String>>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut rows = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        rows.push(line.split(',').map(|value| value.trim().to_string()).collect());
    }
    Ok(rows)
}

fn data_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .filter(|row| {
            row.first()
                .map(|value| value != "timestamp_ns")
                .unwrap_or(false)
        })
        .collect()
}

fn parse_u64(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(0)
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn round_up_16_mib(value: u64) -> u64 {
    const CHUNK: u64 = 16 * 1024 * 1024;
    if value == 0 {
        0
    } else {
        value.saturating_add(CHUNK - 1) / CHUNK * CHUNK
    }
}

fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", path.display()))
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learned_profile_uses_warm_phase_summary_rules() {
        let mut ratios = BTreeMap::new();
        ratios.insert("cpu_bound".to_string(), 0.6);
        ratios.insert("mixed".to_string(), 0.2);
        ratios.insert("io_bound".to_string(), 0.2);
        let warm = PhaseSummary {
            traces: 6,
            primary_phase: "io_bound".to_string(),
            phase_ratios: ratios,
            median_duration_ns: 100,
            p95_duration_ns: 120,
            median_memory_current_bytes: 20 * 1024 * 1024,
            p95_memory_peak_bytes: 70 * 1024 * 1024,
            p95_io_bytes_per_sec: 123_456_789,
            phase_sequence: vec![PhaseSequenceEntry {
                kind: "cpu_bound".to_string(),
                duration_pct: 100,
            }],
        };
        let cold = PhaseSummary {
            median_duration_ns: 160,
            ..PhaseSummary::default()
        };

        let learned = learned_profile_hints(&warm, &cold);

        assert_eq!(learned.cpu_intensity, Some(0.7));
        assert_eq!(learned.working_set_bytes, Some(32 * 1024 * 1024));
        assert_eq!(learned.memory_bytes, Some(80 * 1024 * 1024));
        assert_eq!(learned.io_weight, Some(700));
        assert_eq!(learned.io_bandwidth_bytes_per_sec, Some(123_456_789));
        assert_eq!(learned.estimated_duration_ns, Some(100));
        assert_eq!(learned.cold_load_penalty_ns, Some(60));
        assert_eq!(
            learned.phase_sequence,
            Some(vec![PhaseSequenceEntry {
                kind: "cpu_bound".to_string(),
                duration_pct: 100,
            }])
        );
    }
}
