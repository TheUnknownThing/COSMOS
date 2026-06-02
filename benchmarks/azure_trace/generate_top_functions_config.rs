use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(about = "Generate a top-functions config from Azure 2019 CPU trace CSVs.")]
struct Args {
    #[arg(long, value_name = "DIR")]
    dataset_dir: PathBuf,

    #[arg(long, value_name = "PATH")]
    output: PathBuf,

    #[arg(long, default_value_t = 20)]
    top_functions: usize,

    #[arg(long)]
    frequency_top_functions: Option<usize>,

    #[arg(long)]
    time_top_functions: Option<usize>,

    #[arg(long, default_value = "p75", value_parser = ["p25", "p50", "p75", "p99", "mean"])]
    expected_time_percentile: String,
}

#[derive(Debug, Clone)]
struct FunctionStats {
    app: String,
    function: String,
    trigger: String,
    invocation_count: u64,
    cpu_time_ms: CpuTimeStats,
}

#[derive(Debug)]
struct TopFunctions {
    functions: Vec<FunctionStats>,
    total_dataset_invocations: u64,
    total_profile_cpu_time_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct CpuTimeStats {
    min: f64,
    p25: f64,
    p50: f64,
    p75: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

#[derive(Debug, Serialize)]
struct Config {
    version: u32,
    schema: &'static str,
    source: Source,
    summary: Summary,
    functions: Vec<FunctionConfig>,
}

#[derive(Debug, Serialize)]
struct Source {
    dataset_dir: String,
    top_n: usize,
    selection: &'static str,
    frequency_top_functions: usize,
    time_top_functions: usize,
    expected_time_percentile: String,
}

#[derive(Debug, Serialize)]
struct Summary {
    function_count: usize,
    total_invocations: u64,
    total_cpu_time_ms: f64,
    cpu_time_coverage: f64,
    coverage: f64,
}

#[derive(Debug, Serialize)]
struct FunctionConfig {
    function_id: String,
    function_index: usize,
    frequency: f64,
    expected_time_ms: f64,
    time_distribution: TimeDistribution,
    metadata: Metadata,
}

#[derive(Debug, Serialize)]
struct TimeDistribution {
    min: f64,
    p25: f64,
    p50: f64,
    p75: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct Metadata {
    invocation_count: u64,
    total_cpu_time_ms: f64,
    frequency_rank: usize,
    total_cpu_time_rank: usize,
    selection_reasons: Vec<&'static str>,
    trigger: String,
}

#[derive(Debug, Clone)]
struct SelectedFunction {
    stats: FunctionStats,
    frequency_rank: usize,
    total_cpu_time_rank: usize,
    selection_reasons: Vec<&'static str>,
}

impl FunctionStats {
    fn function_id(&self) -> String {
        format!("{}:{}", short_hash(&self.app), short_hash(&self.function))
    }

    fn total_cpu_time_ms(&self) -> f64 {
        self.invocation_count as f64 * self.cpu_time_ms.mean
    }
}

fn select_functions(
    mut functions: Vec<FunctionStats>,
    frequency_count: usize,
    time_count: usize,
    target_count: usize,
) -> Vec<SelectedFunction> {
    functions.sort_by(|a, b| {
        b.invocation_count
            .cmp(&a.invocation_count)
            .then_with(|| a.app.cmp(&b.app))
            .then_with(|| a.function.cmp(&b.function))
    });
    let by_frequency = functions;

    let mut frequency_ranks = HashMap::new();
    for (idx, item) in by_frequency.iter().enumerate() {
        frequency_ranks.insert(item.function_id(), idx + 1);
    }

    let mut by_time = by_frequency.clone();
    by_time.sort_by(|a, b| {
        b.total_cpu_time_ms()
            .partial_cmp(&a.total_cpu_time_ms())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.invocation_count.cmp(&a.invocation_count))
            .then_with(|| a.app.cmp(&b.app))
            .then_with(|| a.function.cmp(&b.function))
    });

    let mut time_ranks = HashMap::new();
    for (idx, item) in by_time.iter().enumerate() {
        time_ranks.insert(item.function_id(), idx + 1);
    }

    let mut selected_ids = HashSet::new();
    let mut selected = Vec::new();
    let mut reasons: HashMap<String, Vec<&'static str>> = HashMap::new();

    for item in by_frequency.iter().take(frequency_count) {
        let id = item.function_id();
        selected_ids.insert(id.clone());
        reasons.entry(id).or_default().push("frequency");
    }
    for item in by_time.iter().take(time_count) {
        let id = item.function_id();
        selected_ids.insert(id.clone());
        reasons.entry(id).or_default().push("total_cpu_time");
    }

    let target_count = target_count.min(by_frequency.len());
    let mut fill_from_frequency = true;
    let mut frequency_idx = frequency_count;
    let mut time_idx = time_count;
    while selected_ids.len() < target_count {
        let source = if fill_from_frequency {
            "frequency_fill"
        } else {
            "total_cpu_time_fill"
        };

        if fill_from_frequency {
            while frequency_idx < by_frequency.len()
                && selected_ids.contains(&by_frequency[frequency_idx].function_id())
            {
                frequency_idx += 1;
            }
            if let Some(item) = by_frequency.get(frequency_idx) {
                let id = item.function_id();
                selected_ids.insert(id.clone());
                reasons.entry(id).or_default().push(source);
                frequency_idx += 1;
            }
        } else {
            while time_idx < by_time.len()
                && selected_ids.contains(&by_time[time_idx].function_id())
            {
                time_idx += 1;
            }
            if let Some(item) = by_time.get(time_idx) {
                let id = item.function_id();
                selected_ids.insert(id.clone());
                reasons.entry(id).or_default().push(source);
                time_idx += 1;
            }
        }

        fill_from_frequency = !fill_from_frequency;
        if frequency_idx >= by_frequency.len() && time_idx >= by_time.len() {
            break;
        }
    }

    for item in by_frequency {
        let id = item.function_id();
        if selected_ids.contains(&id) {
            selected.push(SelectedFunction {
                stats: item,
                frequency_rank: *frequency_ranks.get(&id).unwrap_or(&usize::MAX),
                total_cpu_time_rank: *time_ranks.get(&id).unwrap_or(&usize::MAX),
                selection_reasons: reasons.remove(&id).unwrap_or_default(),
            });
        }
    }

    selected.sort_by(|a, b| {
        let a_frequency_selected = a.selection_reasons.contains(&"frequency");
        let b_frequency_selected = b.selection_reasons.contains(&"frequency");
        b_frequency_selected
            .cmp(&a_frequency_selected)
            .then_with(|| {
                a.frequency_rank
                    .min(a.total_cpu_time_rank)
                    .cmp(&b.frequency_rank.min(b.total_cpu_time_rank))
            })
            .then_with(|| a.frequency_rank.cmp(&b.frequency_rank))
            .then_with(|| a.stats.app.cmp(&b.stats.app))
            .then_with(|| a.stats.function.cmp(&b.stats.function))
    });

    selected
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.top_functions == 0 {
        anyhow::bail!("--top-functions must be at least 1");
    }
    let top = load_top_functions(&args.dataset_dir)?;
    if top.functions.is_empty() {
        anyhow::bail!("no top functions found in {}", args.dataset_dir.display());
    }

    let frequency_count = args
        .frequency_top_functions
        .unwrap_or((args.top_functions + 1) / 2);
    let time_count = args.time_top_functions.unwrap_or(args.top_functions / 2);
    if frequency_count == 0 && time_count == 0 {
        anyhow::bail!("at least one selection count must be positive");
    }

    let selected = select_functions(
        top.functions,
        frequency_count,
        time_count,
        args.top_functions,
    );
    if selected.is_empty() {
        anyhow::bail!("requested mixed selection is empty");
    }

    let total_invocations: u64 = selected.iter().map(|f| f.stats.invocation_count).sum();
    if total_invocations == 0 {
        anyhow::bail!("selected functions have zero total invocation count");
    }
    let total_cpu_time_ms: f64 = selected.iter().map(|f| f.stats.total_cpu_time_ms()).sum();

    let functions: Vec<FunctionConfig> = selected
        .into_iter()
        .enumerate()
        .map(|(index, selected)| {
            let item = selected.stats;
            let expected_time_ms = match args.expected_time_percentile.as_str() {
                "p25" => item.cpu_time_ms.p25,
                "p50" => item.cpu_time_ms.p50,
                "p75" => item.cpu_time_ms.p75,
                "p99" => item.cpu_time_ms.p99,
                "mean" => item.cpu_time_ms.mean,
                _ => unreachable!(),
            };
            FunctionConfig {
                function_id: format!("{}:{}", short_hash(&item.app), short_hash(&item.function)),
                function_index: index,
                frequency: item.invocation_count as f64 / total_invocations as f64,
                expected_time_ms,
                time_distribution: TimeDistribution {
                    min: item.cpu_time_ms.min,
                    p25: item.cpu_time_ms.p25,
                    p50: item.cpu_time_ms.p50,
                    p75: item.cpu_time_ms.p75,
                    p99: item.cpu_time_ms.p99,
                    max: item.cpu_time_ms.max,
                },
                metadata: Metadata {
                    invocation_count: item.invocation_count,
                    total_cpu_time_ms: item.total_cpu_time_ms(),
                    frequency_rank: selected.frequency_rank,
                    total_cpu_time_rank: selected.total_cpu_time_rank,
                    selection_reasons: selected.selection_reasons,
                    trigger: item.trigger,
                },
            }
        })
        .collect();

    let config = Config {
        version: 1,
        schema: "cosmos.azure.top-functions-config",
        source: Source {
            dataset_dir: args.dataset_dir.display().to_string(),
            top_n: args.top_functions,
            selection: "frequency_and_total_cpu_time",
            frequency_top_functions: frequency_count,
            time_top_functions: time_count,
            expected_time_percentile: args.expected_time_percentile.clone(),
        },
        summary: Summary {
            function_count: functions.len(),
            total_invocations,
            total_cpu_time_ms,
            cpu_time_coverage: if top.total_profile_cpu_time_ms > 0.0 {
                total_cpu_time_ms / top.total_profile_cpu_time_ms
            } else {
                0.0
            },
            coverage: total_invocations as f64 / top.total_dataset_invocations as f64,
        },
        functions,
    };

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::write(&args.output, serde_json::to_string_pretty(&config)? + "\n")
        .with_context(|| format!("write {}", args.output.display()))?;

    println!(
        "Generated config with {} functions",
        config.summary.function_count
    );
    println!(
        "  Selection: top {} by frequency + top {} by total CPU time",
        frequency_count, time_count
    );
    println!(
        "  Coverage: {:.1}% of trace invocations",
        config.summary.coverage * 100.0
    );
    println!("  Expected time: {}", args.expected_time_percentile);
    println!("  Output: {}", args.output.display());

    Ok(())
}

fn load_top_functions(dataset_dir: &Path) -> Result<TopFunctions> {
    let invocation_files = collect_csv_files(dataset_dir, "invocations_per_function_md.anon.d")?;
    let duration_files = collect_csv_files(dataset_dir, "function_durations_percentiles.anon.d")?;

    let mut invocations: HashMap<(String, String, String), (u64, String)> = HashMap::new();
    let mut total_dataset_invocations = 0_u64;
    for path in invocation_files {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut lines = BufReader::new(file).lines();
        validate_invocation_header(
            lines
                .next()
                .transpose()?
                .ok_or_else(|| anyhow::anyhow!("missing header in {}", path.display()))?
                .as_str(),
        )?;
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let (owner, app, function, trigger, count) = parse_invocation_row(&line)?;
            total_dataset_invocations = total_dataset_invocations.saturating_add(count);
            let entry = invocations
                .entry((owner, app, function))
                .or_insert((0, trigger.clone()));
            entry.0 = entry.0.saturating_add(count);
            if entry.1.is_empty() {
                entry.1 = trigger;
            }
        }
    }

    let mut durations: HashMap<(String, String, String), CpuTimeStats> = HashMap::new();
    for path in duration_files {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut lines = BufReader::new(file).lines();
        let headers = parse_csv_header(
            lines
                .next()
                .transpose()?
                .ok_or_else(|| anyhow::anyhow!("missing header in {}", path.display()))?
                .as_str(),
        );
        let idx_app = header_index(&headers, "HashApp")?;
        let idx_func = header_index(&headers, "HashFunction")?;
        let idx_avg = header_index(&headers, "Average")?;
        let idx_min = header_index(&headers, "Minimum")?;
        let idx_max = header_index(&headers, "Maximum")?;
        let idx_p25 = header_index(&headers, "percentile_Average_25")?;
        let idx_p50 = header_index(&headers, "percentile_Average_50")?;
        let idx_p75 = header_index(&headers, "percentile_Average_75")?;
        let idx_p99 = header_index(&headers, "percentile_Average_99")?;

        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let fields = split_csv_line(&line);
            let key = (
                field(&fields, header_index(&headers, "HashOwner")?),
                field(&fields, idx_app),
                field(&fields, idx_func),
            );
            if !invocations.contains_key(&key) {
                continue;
            }
            let stats = CpuTimeStats {
                min: parse_f64(&fields, idx_min)?.max(1.0),
                p25: parse_f64(&fields, idx_p25)?.max(1.0),
                p50: parse_f64(&fields, idx_p50)?.max(1.0),
                p75: parse_f64(&fields, idx_p75)?.max(1.0),
                p99: parse_f64(&fields, idx_p99)?.max(1.0),
                max: parse_f64(&fields, idx_max)?.max(1.0),
                mean: parse_f64(&fields, idx_avg)?.max(1.0),
            };
            durations.insert(key, monotonicize(stats));
        }
    }

    let mut functions = Vec::new();
    let mut total_profile_cpu_time_ms = 0.0_f64;
    for ((owner, app, function), (invocation_count, trigger)) in invocations {
        if let Some(cpu_time_ms) = durations.get(&(owner.clone(), app.clone(), function.clone())) {
            total_profile_cpu_time_ms += invocation_count as f64 * cpu_time_ms.mean;
            functions.push(FunctionStats {
                app,
                function,
                trigger,
                invocation_count,
                cpu_time_ms: *cpu_time_ms,
            });
        }
    }

    functions.sort_by(|a, b| {
        b.invocation_count
            .cmp(&a.invocation_count)
            .then_with(|| a.app.cmp(&b.app))
            .then_with(|| a.function.cmp(&b.function))
    });

    Ok(TopFunctions {
        functions,
        total_dataset_invocations,
        total_profile_cpu_time_ms,
    })
}

fn collect_csv_files(dataset_dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in
        std::fs::read_dir(dataset_dir).with_context(|| format!("read {}", dataset_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(prefix) && name.ends_with(".csv") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn parse_csv_header(line: &str) -> Vec<String> {
    split_csv_line(line)
}

fn split_csv_line(line: &str) -> Vec<String> {
    line.split(',')
        .map(|value| value.trim().to_string())
        .collect()
}

fn header_index(headers: &[String], name: &str) -> Result<usize> {
    headers
        .iter()
        .position(|header| header == name)
        .with_context(|| format!("missing CSV column {name}"))
}

fn field(fields: &[String], idx: usize) -> String {
    fields.get(idx).cloned().unwrap_or_default()
}

fn validate_invocation_header(line: &str) -> Result<()> {
    let mut fields = line.split(',');
    let expected = ["HashOwner", "HashApp", "HashFunction", "Trigger"];
    for name in expected {
        let actual = fields.next().unwrap_or_default().trim();
        if actual != name {
            anyhow::bail!("expected invocation CSV column {name}, got {actual}");
        }
    }
    Ok(())
}

fn parse_invocation_row(line: &str) -> Result<(String, String, String, String, u64)> {
    let mut parts = line.split(',');
    let owner = parts.next().unwrap_or_default().trim().to_string();
    let app = parts.next().unwrap_or_default().trim().to_string();
    let function = parts.next().unwrap_or_default().trim().to_string();
    let trigger = parts.next().unwrap_or("others").trim().to_string();
    let mut total = 0_u64;
    for value in parts {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        total = total.saturating_add(
            value
                .parse::<u64>()
                .with_context(|| format!("parse invocation count from {value}"))?,
        );
    }
    Ok((owner, app, function, trigger, total))
}

fn parse_f64(fields: &[String], idx: usize) -> Result<f64> {
    let value = fields
        .get(idx)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);
    Ok(value)
}

fn monotonicize(mut stats: CpuTimeStats) -> CpuTimeStats {
    stats.min = stats.min.max(1.0);
    stats.p25 = stats.p25.max(stats.min);
    stats.p50 = stats.p50.max(stats.p25);
    stats.p75 = stats.p75.max(stats.p50);
    stats.p99 = stats.p99.max(stats.p75);
    stats.max = stats.max.max(stats.p99);
    stats.mean = stats.mean.max(stats.min);
    stats
}

fn short_hash(text: &str) -> String {
    let mut acc: u64 = 0xcbf29ce484222325;
    for byte in text.as_bytes() {
        acc ^= u64::from(*byte);
        acc = acc.wrapping_mul(0x100000001b3);
    }
    format!("{:08x}", acc as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generates_top_functions_config_from_fixture() -> Result<()> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("azure-top-functions-{stamp}"));
        let dataset = root.join("azurefunctions-dataset2019");
        std::fs::create_dir_all(&dataset)?;

        std::fs::write(
            dataset.join("invocations_per_function_md.anon.d01.csv"),
            "HashOwner,HashApp,HashFunction,Trigger,1,2,3\n\
o1,a1,f1,http,10,20,30\n\
o2,a2,f2,timer,5,5,5\n\
o3,a3,f3,queue,1,0,0\n",
        )?;
        std::fs::write(
            dataset.join("function_durations_percentiles.anon.d01.csv"),
            "HashOwner,HashApp,HashFunction,Average,Count,Minimum,Maximum,percentile_Average_0,percentile_Average_1,percentile_Average_25,percentile_Average_50,percentile_Average_75,percentile_Average_99,percentile_Average_100\n\
o1,a1,f1,12,60,1,30,1,1,5,10,15,25,30\n\
o2,a2,f2,7,15,1,20,1,1,3,6,8,18,20\n\
o3,a3,f3,1000,1,800,1400,800,800,900,1000,1100,1300,1400\n",
        )?;

        let output = root.join("out.json");
        let args = Args {
            dataset_dir: dataset.clone(),
            output: output.clone(),
            top_functions: 2,
            frequency_top_functions: None,
            time_top_functions: None,
            expected_time_percentile: "p75".to_string(),
        };

        let top = load_top_functions(&args.dataset_dir)?;
        assert_eq!(top.functions.len(), 3);
        assert_eq!(top.total_dataset_invocations, 76);

        let frequency_count = (args.top_functions + 1) / 2;
        let time_count = args.top_functions / 2;
        let selected = select_functions(
            top.functions,
            frequency_count,
            time_count,
            args.top_functions,
        );
        let total_invocations: u64 = selected.iter().map(|f| f.stats.invocation_count).sum();
        let total_cpu_time_ms: f64 = selected.iter().map(|f| f.stats.total_cpu_time_ms()).sum();
        let functions: Vec<FunctionConfig> = selected
            .into_iter()
            .enumerate()
            .map(|(index, selected)| {
                let item = selected.stats;
                FunctionConfig {
                    function_id: item.function_id(),
                    function_index: index,
                    frequency: item.invocation_count as f64 / total_invocations as f64,
                    expected_time_ms: item.cpu_time_ms.p75,
                    time_distribution: TimeDistribution {
                        min: item.cpu_time_ms.min,
                        p25: item.cpu_time_ms.p25,
                        p50: item.cpu_time_ms.p50,
                        p75: item.cpu_time_ms.p75,
                        p99: item.cpu_time_ms.p99,
                        max: item.cpu_time_ms.max,
                    },
                    metadata: Metadata {
                        invocation_count: item.invocation_count,
                        total_cpu_time_ms: item.total_cpu_time_ms(),
                        frequency_rank: selected.frequency_rank,
                        total_cpu_time_rank: selected.total_cpu_time_rank,
                        selection_reasons: selected.selection_reasons,
                        trigger: item.trigger,
                    },
                }
            })
            .collect();

        assert_eq!(functions.len(), 2);
        assert_eq!(functions[0].metadata.invocation_count, 60);
        assert_eq!(functions[0].time_distribution.p99, 25.0);
        assert_eq!(functions[1].metadata.invocation_count, 1);
        assert_eq!(
            functions[1].metadata.selection_reasons,
            vec!["total_cpu_time"]
        );

        let config = Config {
            version: 1,
            schema: "cosmos.azure.top-functions-config",
            source: Source {
                dataset_dir: args.dataset_dir.display().to_string(),
                top_n: args.top_functions,
                selection: "frequency_and_total_cpu_time",
                frequency_top_functions: frequency_count,
                time_top_functions: time_count,
                expected_time_percentile: args.expected_time_percentile.clone(),
            },
            summary: Summary {
                function_count: functions.len(),
                total_invocations,
                total_cpu_time_ms,
                cpu_time_coverage: total_cpu_time_ms / top.total_profile_cpu_time_ms,
                coverage: total_invocations as f64 / top.total_dataset_invocations as f64,
            },
            functions,
        };

        std::fs::write(&output, serde_json::to_string_pretty(&config)? + "\n")?;
        let written = std::fs::read_to_string(&output)?;
        assert!(written.contains("cosmos.azure.top-functions-config"));
        assert!(written.contains("\"function_count\": 2"));
        assert!(written.contains("total_cpu_time"));
        Ok(())
    }
}
