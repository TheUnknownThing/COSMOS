use std::collections::VecDeque;
use std::env;
use std::fs::{remove_file, File};
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_DURATION_MS: u64 = 250;
const WALL_TIME_SAMPLE_FLOOR_MS: u64 = 8;
const CPU_TIME_SAMPLE_FLOOR_MS: u64 = 8;
const MAX_SAMPLE_ITERS: u64 = 256;
const MEMORY_HEAVY_BYTES: usize = 8 * 1024 * 1024;

type AppResult<T> = Result<T, String>;

#[derive(Clone, Debug)]
struct Args {
    workload: String,
    duration_ms: u64,
    warm_hold_ms: u64,
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    }
}

fn run() -> AppResult<()> {
    let args = parse_args(env::args().skip(1))?;
    maybe_enter_sched_ext()?;
    let target = Duration::from_millis(args.duration_ms);
    match args.workload.as_str() {
        "cpu_burst" => run_cpu_burst(target),
        "sleep_short" => run_sleep_short(target),
        "io_mixed" => run_io_mixed(target),
        "memory_heavy" => run_memory_heavy(target, Duration::from_millis(args.warm_hold_ms)),
        "network_heavy" => run_network_heavy(target),
        "pipeline" => run_pipeline(target),
        "compression_mixed" => run_compression_mixed(target),
        "graph_bfs" => run_graph_bfs(target),
        other => Err(format!("unknown workload: {other}")),
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> AppResult<Args> {
    let mut workload = None;
    let mut duration_ms = DEFAULT_DURATION_MS;
    let mut warm_hold_ms = 0;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--workload" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--workload requires a value".to_string())?;
                workload = Some(value);
            }
            "--duration-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--duration-ms requires a value".to_string())?;
                duration_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid duration-ms: {value}"))?;
            }
            "--warm-hold-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--warm-hold-ms requires a value".to_string())?;
                warm_hold_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid warm-hold-ms: {value}"))?;
            }
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let workload = workload.ok_or_else(|| "--workload is required".to_string())?;
    if duration_ms == 0 {
        return Err("--duration-ms must be greater than zero".to_string());
    }

    Ok(Args {
        workload,
        duration_ms,
        warm_hold_ms,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: cosmos-benchmark-workload --workload <name> [--duration-ms <ms>] [--warm-hold-ms <ms>]"
    );
}

fn maybe_enter_sched_ext() -> AppResult<()> {
    if env::var_os("COSMOS_BENCH_SCHED_EXT").is_none() {
        return Ok(());
    }

    let param = SchedParam { sched_priority: 0 };
    let rc = unsafe { sched_setscheduler(0, SCHED_EXT, &param) };
    if rc != 0 {
        return Err(format!(
            "sched_setscheduler(SCHED_EXT) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn execute_calibrated_wall_time<F>(target: Duration, mut unit: F) -> AppResult<()>
where
    F: FnMut() -> AppResult<()>,
{
    let sample_floor = Duration::from_millis(WALL_TIME_SAMPLE_FLOOR_MS);
    let (sample_iters, sample_elapsed) = run_sample(
        || Ok::<Instant, String>(Instant::now()),
        |start| Ok::<Duration, String>(start.elapsed()),
        sample_floor,
        &mut unit,
    )?;
    let total_iters = estimate_total_iters(target, sample_elapsed, sample_iters);
    for _ in sample_iters..total_iters {
        unit()?;
    }
    Ok(())
}

fn execute_calibrated_cpu_time<F>(target: Duration, mut unit: F) -> AppResult<()>
where
    F: FnMut() -> AppResult<()>,
{
    let sample_floor = Duration::from_millis(CPU_TIME_SAMPLE_FLOOR_MS);
    let (sample_iters, sample_elapsed) = run_sample(
        process_cpu_time,
        |start| {
            let end = process_cpu_time()?;
            end.checked_sub(start)
                .ok_or_else(|| "process CPU clock moved backwards".to_string())
        },
        sample_floor,
        &mut unit,
    )?;
    let total_iters = estimate_total_iters(target, sample_elapsed, sample_iters);
    for _ in sample_iters..total_iters {
        unit()?;
    }
    Ok(())
}

fn run_sample<S, Begin, End, F>(
    begin: Begin,
    end: End,
    sample_floor: Duration,
    unit: &mut F,
) -> AppResult<(u64, Duration)>
where
    Begin: Fn() -> AppResult<S>,
    End: Fn(S) -> AppResult<Duration>,
    F: FnMut() -> AppResult<()>,
{
    let mut sample_iters = 1_u64;
    loop {
        let start = begin()?;
        for _ in 0..sample_iters {
            unit()?;
        }
        let elapsed = end(start)?;
        if elapsed >= sample_floor || sample_iters >= MAX_SAMPLE_ITERS {
            return Ok((sample_iters, elapsed));
        }
        sample_iters *= 2;
    }
}

fn estimate_total_iters(target: Duration, sample_elapsed: Duration, sample_iters: u64) -> u64 {
    let sample_nanos = sample_elapsed.as_nanos().max(1);
    let target_nanos = target.as_nanos().max(1);
    let total_iters = (target_nanos * sample_iters as u128).div_ceil(sample_nanos);
    total_iters.max(sample_iters as u128).min(u64::MAX as u128) as u64
}

fn split_pipeline_duration(target: Duration) -> (Duration, Duration, Duration) {
    let total_ns = target.as_nanos();
    let fetch_ns = total_ns / 3;
    let compute_ns = total_ns / 3;
    let upload_ns = total_ns.saturating_sub(fetch_ns).saturating_sub(compute_ns);
    (
        duration_from_nanos(fetch_ns),
        duration_from_nanos(compute_ns),
        duration_from_nanos(upload_ns),
    )
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

fn run_cpu_burst(target: Duration) -> AppResult<()> {
    let mut job = CpuBurstJob::new(128);
    execute_calibrated_cpu_time(target, || {
        job.run_once();
        Ok(())
    })
}

fn run_sleep_short(target: Duration) -> AppResult<()> {
    thread::sleep(target);
    Ok(())
}

fn run_io_mixed(target: Duration) -> AppResult<()> {
    let mut job = IoMixedJob::new()?;
    let result = execute_calibrated_wall_time(target, || job.run_once());
    job.cleanup();
    result
}

fn run_memory_heavy(target: Duration, warm_hold: Duration) -> AppResult<()> {
    let mut job = MemoryHeavyJob::new(MEMORY_HEAVY_BYTES);
    let result = execute_calibrated_wall_time(target, || {
        job.run_once();
        Ok(())
    });
    if result.is_ok() && !warm_hold.is_zero() {
        thread::sleep(warm_hold);
    }
    result
}

fn run_network_heavy(target: Duration) -> AppResult<()> {
    let mut job = NetworkHeavyJob::new(256 * 1024)?;
    let result = execute_calibrated_wall_time(target, || job.run_once());
    job.shutdown();
    result
}

fn run_pipeline(target: Duration) -> AppResult<()> {
    let (fetch_duration, compute_duration, upload_duration) = split_pipeline_duration(target);
    let mut network = NetworkHeavyJob::new(128 * 1024)?;
    let mut compute = CpuBurstJob::new(96);
    let mut upload = IoMixedJob::new()?;

    let result = (|| {
        execute_calibrated_wall_time(fetch_duration, || network.run_once())?;
        execute_calibrated_cpu_time(compute_duration, || {
            compute.run_once();
            Ok(())
        })?;
        execute_calibrated_wall_time(upload_duration, || upload.run_once())
    })();

    network.shutdown();
    upload.cleanup();
    result
}

fn run_compression_mixed(target: Duration) -> AppResult<()> {
    let mut job = CompressionMixedJob::new(512 * 1024);
    execute_calibrated_wall_time(target, || job.run_once())
}

fn run_graph_bfs(target: Duration) -> AppResult<()> {
    let mut job = GraphBfsJob::new(8_192, 8);
    execute_calibrated_wall_time(target, || {
        job.run_once();
        Ok(())
    })
}

fn process_cpu_time() -> AppResult<Duration> {
    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return Err(format!(
            "clock_gettime(CLOCK_PROCESS_CPUTIME_ID) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    let secs = u64::try_from(ts.tv_sec).map_err(|_| "negative cpu time".to_string())?;
    let nanos = u32::try_from(ts.tv_nsec)
        .map_err(|_| "invalid nanoseconds from clock_gettime".to_string())?;
    Ok(Duration::new(secs, nanos))
}

struct CpuBurstJob {
    size: usize,
    a: Vec<f64>,
    b: Vec<f64>,
    c: Vec<f64>,
    checksum: f64,
}

impl CpuBurstJob {
    fn new(size: usize) -> Self {
        let len = size * size;
        let a = (0..len)
            .map(|idx| ((idx % 97) as f64 + 1.0) / 97.0)
            .collect();
        let b = (0..len)
            .map(|idx| (((idx * 7) % 89) as f64 + 1.0) / 89.0)
            .collect();
        Self {
            size,
            a,
            b,
            c: vec![0.0; len],
            checksum: 0.0,
        }
    }

    fn run_once(&mut self) {
        self.c.fill(0.0);
        let n = self.size;
        let block = 16;
        for ii in (0..n).step_by(block) {
            for kk in (0..n).step_by(block) {
                for jj in (0..n).step_by(block) {
                    let i_max = (ii + block).min(n);
                    let k_max = (kk + block).min(n);
                    let j_max = (jj + block).min(n);
                    for i in ii..i_max {
                        let row = i * n;
                        for k in kk..k_max {
                            let a_ik = self.a[row + k];
                            let b_row = k * n;
                            for j in jj..j_max {
                                self.c[row + j] += a_ik * self.b[b_row + j];
                            }
                        }
                    }
                }
            }
        }

        let mut fold = 0.0;
        for idx in (0..self.c.len()).step_by(97) {
            fold += self.c[idx];
        }
        self.checksum += fold;
        black_box(self.checksum);
    }
}

struct MemoryHeavyJob {
    buf: Vec<u8>,
    stride: usize,
    checksum: u64,
}

impl MemoryHeavyJob {
    fn new(size: usize) -> Self {
        let buf = (0..size).map(|idx| ((idx * 7) % 251) as u8).collect();
        Self {
            buf,
            stride: 64,
            checksum: 0,
        }
    }

    fn run_once(&mut self) {
        for offset in (0..self.buf.len()).step_by(self.stride) {
            let update = (((offset / self.stride) * 13) & 0xff) as u8;
            let cell = &mut self.buf[offset];
            *cell = cell.wrapping_add(update);
            self.checksum = self.checksum.wrapping_add(u64::from(*cell));
        }

        for chunk in self.buf.chunks(4096).take(128) {
            let mut local = 0_u64;
            for byte in chunk.iter().step_by(31) {
                local = local.wrapping_add(u64::from(*byte));
            }
            self.checksum = self.checksum.wrapping_add(local);
        }
        black_box(self.checksum);
    }
}

struct IoMixedJob {
    path: PathBuf,
    payload: Vec<u8>,
    readback: Vec<u8>,
    checksum: u64,
}

impl IoMixedJob {
    fn new() -> AppResult<Self> {
        let pid = process::id();
        let epoch_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| format!("system clock error: {err}"))?
            .as_nanos();
        let unique = format!("cosmos-io-mixed-{pid}-{epoch_nanos}.bin",);
        let path = env::temp_dir().join(unique);
        let payload = (0..256 * 1024)
            .map(|idx| ((idx * 17 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        let readback = vec![0_u8; payload.len()];
        Ok(Self {
            path,
            payload,
            readback,
            checksum: 0,
        })
    }

    fn run_once(&mut self) -> AppResult<()> {
        self.scramble_payload();

        let mut file = File::create(&self.path)
            .map_err(|err| format!("create {}: {err}", self.path.display()))?;
        file.write_all(&self.payload)
            .map_err(|err| format!("write {}: {err}", self.path.display()))?;
        file.sync_all()
            .map_err(|err| format!("sync {}: {err}", self.path.display()))?;
        drop(file);

        let mut file =
            File::open(&self.path).map_err(|err| format!("open {}: {err}", self.path.display()))?;
        file.read_exact(&mut self.readback)
            .map_err(|err| format!("read {}: {err}", self.path.display()))?;

        for byte in self.readback.iter().step_by(257) {
            self.checksum = self.checksum.wrapping_add(u64::from(*byte));
        }
        black_box(self.checksum);
        Ok(())
    }

    fn scramble_payload(&mut self) {
        for idx in (0..self.payload.len()).step_by(64) {
            self.payload[idx] = self.payload[idx].wrapping_add(((idx / 64) % 251) as u8);
        }
    }

    fn cleanup(&self) {
        let _ = remove_file(&self.path);
    }
}

struct NetworkHeavyJob {
    client: TcpStream,
    payload: Vec<u8>,
    recv: Vec<u8>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl NetworkHeavyJob {
    fn new(chunk_bytes: usize) -> AppResult<Self> {
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).map_err(|err| format!("bind listener: {err}"))?;
        let port = listener
            .local_addr()
            .map_err(|err| format!("get listener address: {err}"))?
            .port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_worker = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let Ok((mut conn, _)) = listener.accept() else {
                return;
            };
            let mut buf = vec![0_u8; chunk_bytes];
            while !stop_worker.load(Ordering::Relaxed) {
                if conn.read_exact(&mut buf).is_err() {
                    break;
                }
                if conn.write_all(&buf).is_err() {
                    break;
                }
            }
        });

        let client = TcpStream::connect(("127.0.0.1", port))
            .map_err(|err| format!("connect client: {err}"))?;
        let payload = vec![b'x'; chunk_bytes];
        let recv = vec![0_u8; chunk_bytes];
        Ok(Self {
            client,
            payload,
            recv,
            stop,
            worker: Some(worker),
        })
    }

    fn run_once(&mut self) -> AppResult<()> {
        self.client
            .write_all(&self.payload)
            .map_err(|err| format!("network write failed: {err}"))?;
        self.client
            .read_exact(&mut self.recv)
            .map_err(|err| format!("network read failed: {err}"))?;
        black_box(&self.recv);
        Ok(())
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.client.shutdown(Shutdown::Both);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct CompressionMixedJob {
    payload: Vec<u8>,
    compressed: Vec<u8>,
    decompressed: Vec<u8>,
}

impl CompressionMixedJob {
    fn new(payload_bytes: usize) -> Self {
        let mut payload = Vec::with_capacity(payload_bytes);
        let blocks = payload_bytes / 256;
        for block in 0..blocks.max(1) {
            let value = ((block * 19) % 251) as u8;
            payload.extend(std::iter::repeat_n(value, 192));
            payload.extend((0..64).map(|offset| value.wrapping_add(offset as u8)));
        }
        payload.truncate(payload_bytes);
        if payload.is_empty() {
            payload.push(0);
        }
        Self {
            payload,
            compressed: Vec::with_capacity(payload_bytes),
            decompressed: Vec::with_capacity(payload_bytes),
        }
    }

    fn run_once(&mut self) -> AppResult<()> {
        rle_compress(&self.payload, &mut self.compressed);
        rle_decompress(&self.compressed, &mut self.decompressed)?;
        if self.decompressed != self.payload {
            return Err("compression roundtrip mismatch".to_string());
        }
        black_box(&self.decompressed);
        Ok(())
    }
}

fn rle_compress(input: &[u8], output: &mut Vec<u8>) {
    output.clear();
    let mut idx = 0;
    while idx < input.len() {
        let byte = input[idx];
        let mut run = 1_u8;
        while idx + usize::from(run) < input.len()
            && input[idx + usize::from(run)] == byte
            && run < u8::MAX
        {
            run += 1;
        }
        output.push(run);
        output.push(byte);
        idx += usize::from(run);
    }
}

fn rle_decompress(input: &[u8], output: &mut Vec<u8>) -> AppResult<()> {
    if input.len() % 2 != 0 {
        return Err("invalid compressed payload".to_string());
    }

    output.clear();
    let mut idx = 0;
    while idx < input.len() {
        let run = usize::from(input[idx]);
        let byte = input[idx + 1];
        output.extend(std::iter::repeat_n(byte, run));
        idx += 2;
    }
    Ok(())
}

struct GraphBfsJob {
    graph: Vec<Vec<usize>>,
    visited: Vec<u8>,
    queue: VecDeque<usize>,
    next_start: usize,
    checksum: usize,
}

impl GraphBfsJob {
    fn new(nodes: usize, degree: usize) -> Self {
        let graph = build_graph(nodes, degree);
        Self {
            graph,
            visited: vec![0; nodes],
            queue: VecDeque::with_capacity(nodes),
            next_start: 0,
            checksum: 0,
        }
    }

    fn run_once(&mut self) {
        self.visited.fill(0);
        self.queue.clear();
        self.queue.push_back(self.next_start);
        self.visited[self.next_start] = 1;

        let mut seen = 0_usize;
        while let Some(node) = self.queue.pop_front() {
            seen += 1;
            for &neighbor in &self.graph[node] {
                if self.visited[neighbor] == 0 {
                    self.visited[neighbor] = 1;
                    self.queue.push_back(neighbor);
                }
            }
        }

        self.checksum ^= seen;
        self.next_start = (self.next_start + 97) % self.graph.len();
        black_box(self.checksum);
    }
}

fn build_graph(nodes: usize, degree: usize) -> Vec<Vec<usize>> {
    let mut graph = Vec::with_capacity(nodes);
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    for node in 0..nodes {
        let mut neighbors = Vec::with_capacity(degree);
        for edge in 0..degree {
            seed ^= seed << 7;
            seed ^= seed >> 9;
            seed ^= seed << 8;
            let next = ((seed as usize) + node + edge * 17) % nodes;
            neighbors.push(next);
        }
        graph.push(neighbors);
    }
    graph
}

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

#[repr(C)]
struct SchedParam {
    sched_priority: i32,
}

const CLOCK_PROCESS_CPUTIME_ID: i32 = 2;
const SCHED_EXT: i32 = 7;

unsafe extern "C" {
    fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
    fn sched_setscheduler(pid: i32, policy: i32, param: *const SchedParam) -> i32;
}
