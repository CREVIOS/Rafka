//! End-to-end Kafka-protocol benchmark for Rafka.
//!
//! Unlike perf_harness (in-process API calls), this binary exercises the full
//! stack: TCP accept → Kafka wire-protocol decode → storage write/read →
//! Kafka wire-protocol encode → TCP send.  Latency is measured from the first
//! byte written by the client to the last byte read from the server, matching
//! how Kafka's own benchmark tools measure producer/consumer latency.
//!
//! Usage:
//!   e2e_bench [--mode produce|fetch|both] [--duration-secs N]
//!             [--target-rates R1,R2,...] [--payload-bytes B]
//!             [--workers N] [--sync] [--partitions N] [--pipeline-depth N]
//!
//! --sync             call sync_data() after every write batch (tests fdatasync overhead)
//! --partitions N     use AsyncTransportServerSharded with N partitions shared across all
//!                    workers; worker i writes to partition i%N.  Compare N=1 (contended)
//!                    vs N=workers (sharded) to measure Phase-4 per-partition concurrency gain.
//! --pipeline-depth N keep N requests in-flight per connection (sliding window).  Default=1
//!                    (ping-pong).  Set >1 to exercise Phase-1 server pipelining and to get
//!                    throughput numbers comparable to Kafka's async producer benchmarks.
//! --records-per-request N  pack N records into each ProduceRequest using the Rafka batch
//!                    encoding.  Default=1 (one record per request).  Set to 64 to match
//!                    Kafka's default producer batch size and measure Priority-1 gain.
//!
//! Output: same CSV columns as perf_harness.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rafka_broker::{async_transport::AsyncTransportServerSharded, PersistentLogConfig, TransportServer};
use rafka_protocol::messages::{
    ProduceRequest, ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData,
    VersionedCodec,
};

// ── Kafka API constants ───────────────────────────────────────────────────────
const API_KEY_PRODUCE: i16 = 0;
const API_KEY_FETCH: i16 = 1;

const PRODUCE_API_VERSION: i16 = 3; // non-flexible, simple, well-tested
const FETCH_API_VERSION: i16 = 4; // non-flexible, has LSO + aborted ranges

const BENCH_TOPIC: &str = "e2e-bench-topic";
const BENCH_PARTITION: i32 = 0;

// ── Throttler constants (mirrors Kafka's ThroughputThrottler) ─────────────────
const THROTTLE_MIN_SLEEP_NS: u64 = 2_000_000; // 2 ms minimum sleep quantum
const FETCH_PRELOAD_COUNT: u64 = 1_000; // fixed pre-produced records for fetch (wrap-around)

const DEFAULT_TARGET_RATES: &[u64] = &[1_000, 10_000, 100_000];
const DEFAULT_DURATION_SECS: u64 = 10;
const DEFAULT_WARMUP_SECS: u64 = 1;
const DEFAULT_PAYLOAD_BYTES: usize = 1024;
const DEFAULT_MAX_WORKERS: usize = 16;

/// Magic prefix for the Rafka multi-record batch wire format.
/// Must match `BATCH_MAGIC` in `crates/broker/src/lib.rs`.
const BENCH_BATCH_MAGIC: u32 = 0x5242_4348; // b"RBCH"

/// Encode `count` copies of `record` into a single batch payload.
/// Returns the raw bytes to place in `ProduceRequestPartitionProduceData::records`.
/// When `count <= 1` returns an unmodified copy of `record` for backwards compatibility.
fn encode_bench_batch(record: &[u8], count: usize) -> Vec<u8> {
    if count <= 1 {
        return record.to_vec();
    }
    let mut out = Vec::with_capacity(12 + record.len() * count);
    out.extend_from_slice(&BENCH_BATCH_MAGIC.to_be_bytes());
    out.extend_from_slice(&(count as u32).to_be_bytes());
    out.extend_from_slice(&(record.len() as u32).to_be_bytes());
    for _ in 0..count {
        out.extend_from_slice(record);
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    Produce,
    Fetch,
    Both,
}

impl BenchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Fetch => "fetch",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone)]
struct BenchConfig {
    mode: BenchMode,
    target_rates: Vec<u64>,
    duration_secs: u64,
    warmup_secs: u64,
    payload_bytes: usize,
    workers: usize,
    data_root: PathBuf,
    /// Call `sync_data()` after every append batch (`--sync`).
    sync: bool,
    /// When > 0, use `AsyncTransportServerSharded` shared by all workers;
    /// worker `i` writes to partition `i % partitions`. Enables Phase-4 scaling test.
    partitions: usize,
    /// Number of requests kept in-flight simultaneously (sliding window).
    /// 1 = classic ping-pong (default).  >1 exercises Phase-1 server pipelining
    /// and is required for a fair Kafka throughput comparison.
    pipeline_depth: usize,
    /// Number of records packed into each ProduceRequest (Priority 1 batching).
    /// 1 = one record per request (default). 64 matches Kafka's default batch size.
    records_per_request: usize,
}

#[derive(Debug, Clone)]
struct RunStats {
    total_ops: u64,
    #[allow(dead_code)]
    elapsed: Duration,
    achieved_msgs_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    total_ops: u64,
    elapsed: Duration,
    latencies_ns: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct RunTiming {
    duration_secs: u64,
    warmup_secs: u64,
}

// ── Pacer (identical to perf_harness) ────────────────────────────────────────

struct TargetPacer {
    start: Instant,
    target_rate: u64,
    sleep_time_ns: u64,
    sleep_deficit_ns: u64,
}

impl TargetPacer {
    fn new(target_rate: u64) -> Self {
        let sleep_time_ns = 1_000_000_000u64.saturating_div(target_rate).max(1);
        Self {
            start: Instant::now(),
            target_rate,
            sleep_time_ns,
            sleep_deficit_ns: 0,
        }
    }

    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    fn throttle(&mut self, ops_so_far: u64) {
        let elapsed_ns = self.start.elapsed().as_nanos() as u64;
        let expected_ns = ops_so_far
            .saturating_mul(1_000_000_000)
            .saturating_div(self.target_rate);
        if elapsed_ns >= expected_ns {
            return;
        }

        self.sleep_deficit_ns = self.sleep_deficit_ns.saturating_add(self.sleep_time_ns);
        if self.sleep_deficit_ns < THROTTLE_MIN_SLEEP_NS {
            return;
        }
        let sleep_ns = self.sleep_deficit_ns;
        self.sleep_deficit_ns = 0;
        thread::sleep(Duration::from_nanos(sleep_ns));
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<(), String> {
    let config = parse_args(env::args().skip(1).collect())?;
    fs::create_dir_all(&config.data_root)
        .map_err(|e| format!("create {}: {e}", config.data_root.display()))?;

    println!(
        "mode,target_msgs_per_sec,duration_secs,total_ops,\
         achieved_msgs_per_sec,target_achievement_pct,p50_us,p95_us,p99_us,max_us"
    );

    match config.mode {
        BenchMode::Produce => run_produce_matrix(&config)?,
        BenchMode::Fetch => run_fetch_matrix(&config)?,
        BenchMode::Both => {
            run_produce_matrix(&config)?;
            run_fetch_matrix(&config)?;
        }
    }
    Ok(())
}

// ── Produce matrix ────────────────────────────────────────────────────────────

fn run_produce_matrix(config: &BenchConfig) -> Result<(), String> {
    for &rate in &config.target_rates {
        let dir = scenario_dir(&config.data_root, "produce", rate);
        reset_dir(&dir)?;
        let stats = if config.partitions > 0 {
            run_produce_shared_scenario(config, rate, &dir)?
        } else {
            run_produce_scenario(config, rate, &dir)?
        };
        print_stats(BenchMode::Produce.as_str(), rate, config.duration_secs, &stats);
    }
    Ok(())
}

fn run_produce_scenario(
    config: &BenchConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate(target_rate, config.workers);
    let active = worker_rates.iter().filter(|&&r| r > 0).count();
    if active == 0 {
        return Ok(zero_stats());
    }

    let barrier = Arc::new(Barrier::new(active + 1));
    let mut handles = Vec::with_capacity(active);

    for (id, rate) in worker_rates.into_iter().enumerate() {
        if rate == 0 {
            continue;
        }
        let worker_dir = data_dir.join(format!("w{id}"));
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let payload_bytes = config.payload_bytes;
        let sync = config.sync;
        let pipeline_depth = config.pipeline_depth;
        let records_per_request = config.records_per_request;
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            run_produce_worker(id, rate, timing, payload_bytes, &worker_dir, b, sync, pipeline_depth, records_per_request)
        }));
    }

    barrier.wait();
    collect_stats(handles)
}

fn run_produce_worker(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    data_dir: &Path,
    start_barrier: Arc<Barrier>,
    sync: bool,
    pipeline_depth: usize,
    records_per_request: usize,
) -> Result<WorkerStats, String> {
    reset_dir(data_dir)?;

    // Each worker has its own server + connection — no shared state.
    let mut server = TransportServer::bind("127.0.0.1:0", data_dir, bench_log_config(sync))
        .map_err(|e| format!("worker {worker_id} bind: {e:?}"))?;
    let addr = server
        .local_addr()
        .map_err(|e| format!("worker {worker_id} local_addr: {e:?}"))?;

    let server_handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("worker {worker_id} connect: {e}"))?;
    // TCP_NODELAY prevents Nagle batching from adding latency jitter.
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set_nodelay: {e}"))?;

    // Pre-encode the produce frame body once; only correlation_id changes.
    // Priority 1: pack `records_per_request` records into each request.
    let rpr = records_per_request.max(1);
    let single_record: Vec<u8> = vec![0x6a; payload_bytes];
    let batch_payload = encode_bench_batch(&single_record, rpr);
    let produce_body = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(BENCH_TOPIC.to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: BENCH_PARTITION,
                records: Some(batch_payload),
            }],
        }],
    }
    .encode(PRODUCE_API_VERSION)
    .map_err(|e| format!("encode produce body: {e:?}"))?;

    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline + measured_deadline;

    let capacity = usize::try_from(target_rate.saturating_mul(timing.duration_secs).min(4_000_000))
        .unwrap_or(4_000_000);
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(capacity);

    start_barrier.wait();
    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut measured_ops: u64 = 0;
    let mut corr_id: i32 = 1;
    let mut response_buf = [0u8; 4];

    if pipeline_depth <= 1 {
        // ── Ping-pong: send one request, wait for response ───────────────────
        loop {
            if pacer.elapsed() >= wall_deadline {
                break;
            }
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            let t0 = Instant::now();
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            read_response_frame(&mut stream, &mut response_buf)
                .map_err(|e| format!("worker {worker_id} produce read: {e}"))?;
            let elapsed_ns = duration_ns(t0.elapsed());
            total_ops += rpr as u64;
            corr_id = corr_id.wrapping_add(1);
            if pacer.elapsed() >= warmup_deadline {
                measured_ops += rpr as u64;
                latencies_ns.push(elapsed_ns);
            }
            pacer.throttle(total_ops);
        }
    } else {
        // ── Sliding-window pipeline: keep `pipeline_depth` requests in-flight ─
        // We track send-time per slot so we can record latency as the window
        // slides.  The pacer is bypassed — pipelined mode always runs full-speed.
        let depth = pipeline_depth;
        let mut in_flight: VecDeque<Instant> = VecDeque::with_capacity(depth);

        // Fill the initial window.
        while in_flight.len() < depth {
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            in_flight.push_back(Instant::now());
            corr_id = corr_id.wrapping_add(1);
        }

        loop {
            // Read the oldest outstanding response.
            read_response_frame(&mut stream, &mut response_buf)
                .map_err(|e| format!("worker {worker_id} produce read: {e}"))?;
            let t0 = in_flight.pop_front().expect("in_flight not empty");
            let elapsed_ns = duration_ns(t0.elapsed());

            if pacer.elapsed() >= warmup_deadline {
                measured_ops += rpr as u64;
                latencies_ns.push(elapsed_ns);
            }

            if pacer.elapsed() >= wall_deadline {
                break;
            }

            // Send the next request to keep the window full.
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            in_flight.push_back(Instant::now());
            corr_id = corr_id.wrapping_add(1);
        }

        // Drain remaining in-flight responses.
        while let Some(_t0) = in_flight.pop_front() {
            let _ = read_response_frame(&mut stream, &mut response_buf);
        }
    }

    stream
        .shutdown(Shutdown::Both)
        .map_err(|e| format!("worker {worker_id} shutdown: {e}"))?;
    let _ = server_handle.join();

    Ok(WorkerStats {
        total_ops: measured_ops,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

// ── Fetch matrix ──────────────────────────────────────────────────────────────

fn run_fetch_matrix(config: &BenchConfig) -> Result<(), String> {
    for &rate in &config.target_rates {
        let dir = scenario_dir(&config.data_root, "fetch", rate);
        reset_dir(&dir)?;
        let stats = if config.partitions > 0 {
            run_fetch_shared_scenario(config, rate, &dir)?
        } else {
            run_fetch_scenario(config, rate, &dir)?
        };
        print_stats(BenchMode::Fetch.as_str(), rate, config.duration_secs, &stats);
    }
    Ok(())
}

fn run_fetch_scenario(
    config: &BenchConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate(target_rate, config.workers);
    let active = worker_rates.iter().filter(|&&r| r > 0).count();
    if active == 0 {
        return Ok(zero_stats());
    }

    let barrier = Arc::new(Barrier::new(active + 1));
    let mut handles = Vec::with_capacity(active);

    for (id, rate) in worker_rates.into_iter().enumerate() {
        if rate == 0 {
            continue;
        }
        let worker_dir = data_dir.join(format!("w{id}"));
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let payload_bytes = config.payload_bytes;
        let sync = config.sync;
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            run_fetch_worker(id, rate, timing, payload_bytes, &worker_dir, b, sync)
        }));
    }

    barrier.wait();
    collect_stats(handles)
}

fn run_fetch_worker(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    data_dir: &Path,
    start_barrier: Arc<Barrier>,
    sync: bool,
) -> Result<WorkerStats, String> {
    reset_dir(data_dir)?;

    let mut server = TransportServer::bind("127.0.0.1:0", data_dir, bench_log_config(sync))
        .map_err(|e| format!("worker {worker_id} bind: {e:?}"))?;
    let addr = server
        .local_addr()
        .map_err(|e| format!("worker {worker_id} local_addr: {e:?}"))?;
    let server_handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("worker {worker_id} connect: {e}"))?;
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set_nodelay: {e}"))?;

    // ── Pre-load: produce a fixed set of records before the timed loop.
    // We keep the count small so preloading completes quickly; the fetch loop
    // wraps around (next_offset %= preload_limit) so we still measure steady-state.
    let preload_count = FETCH_PRELOAD_COUNT;
    let preload_body = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(BENCH_TOPIC.to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: BENCH_PARTITION,
                records: Some(vec![0x42; payload_bytes]),
            }],
        }],
    }
    .encode(PRODUCE_API_VERSION)
    .map_err(|e| format!("encode preload body: {e:?}"))?;

    let mut preload_corr: i32 = -1_000_000;
    let mut response_buf = [0u8; 4];

    for _ in 0..preload_count {
        let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, preload_corr, &preload_body);
        stream
            .write_all(&frame)
            .map_err(|e| format!("worker {worker_id} preload write: {e}"))?;
        read_response_frame(&mut stream, &mut response_buf)
            .map_err(|e| format!("worker {worker_id} preload read: {e}"))?;
        preload_corr = preload_corr.wrapping_add(1);
    }

    // ── Timed fetch loop ──────────────────────────────────────────────────────
    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline + measured_deadline;

    let capacity = usize::try_from(target_rate.saturating_mul(timing.duration_secs).min(4_000_000))
        .unwrap_or(4_000_000);
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(capacity);

    let preload_limit = i64::try_from(preload_count).unwrap_or(i64::MAX);

    start_barrier.wait();
    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut measured_ops: u64 = 0;
    let mut fetch_corr: i32 = 1;
    let mut next_offset: i64 = 0;

    loop {
        if pacer.elapsed() >= wall_deadline {
            break;
        }

        // Limit to roughly one record to measure per-record fetch RTT.
        let one_record_bytes = i32::try_from(payload_bytes + 256).unwrap_or(i32::MAX);
        let fetch_body = encode_fetch_v4_body(BENCH_TOPIC, BENCH_PARTITION, next_offset, one_record_bytes);
        let frame = build_request_frame(API_KEY_FETCH, FETCH_API_VERSION, fetch_corr, &fetch_body);

        let t0 = Instant::now();
        stream
            .write_all(&frame)
            .map_err(|e| format!("worker {worker_id} fetch write: {e}"))?;
        read_response_frame(&mut stream, &mut response_buf)
            .map_err(|e| format!("worker {worker_id} fetch read: {e}"))?;
        let elapsed_ns = duration_ns(t0.elapsed());

        total_ops += 1;
        fetch_corr = fetch_corr.wrapping_add(1);
        next_offset += 1;
        if next_offset >= preload_limit {
            next_offset = 0;
        }

        if pacer.elapsed() >= warmup_deadline {
            measured_ops += 1;
            latencies_ns.push(elapsed_ns);
        }
        pacer.throttle(total_ops);
    }

    stream
        .shutdown(Shutdown::Both)
        .map_err(|e| format!("worker {worker_id} shutdown: {e}"))?;
    let _ = server_handle.join();

    Ok(WorkerStats {
        total_ops: measured_ops,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

// ── Shared-server scenarios (Phase 4: --partitions N) ────────────────────────
//
// All workers share one `AsyncTransportServerSharded`.  Worker `i` writes to
// partition `i % partitions`.  With `--partitions 1` all workers contend on
// partition 0; with `--partitions N` (N == workers) each has its own shard.
// Comparing the two reveals the Phase-4 per-partition concurrency gain.

fn run_produce_shared_scenario(
    config: &BenchConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate(target_rate, config.workers);
    let active = worker_rates.iter().filter(|&&r| r > 0).count();
    if active == 0 {
        return Ok(zero_stats());
    }

    // One tokio runtime drives the shared async server.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;

    let log_config = bench_log_config(config.sync);
    let server = rt
        .block_on(AsyncTransportServerSharded::bind("127.0.0.1:0", data_dir, log_config))
        .map_err(|e| format!("shared server bind: {e:?}"))?;
    let addr = server.local_addr().map_err(|e| format!("local_addr: {e:?}"))?;

    // Run the server on a background thread so it processes requests
    // while workers are running.  Workers signal via TCP disconnect when done.
    let server_thread = thread::spawn(move || {
        rt.block_on(server.serve_n_connections_concurrent(active))
            .map_err(|e| format!("shared server error: {e:?}"))
    });

    let barrier = Arc::new(Barrier::new(active + 1));
    let partitions = config.partitions;
    let payload_bytes = config.payload_bytes;
    let mut handles = Vec::with_capacity(active);

    let pipeline_depth = config.pipeline_depth;
    let records_per_request = config.records_per_request;
    for (id, rate) in worker_rates.into_iter().enumerate() {
        if rate == 0 {
            continue;
        }
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            run_produce_worker_shared(id, rate, timing, payload_bytes, partitions, addr, b, pipeline_depth, records_per_request)
        }));
    }

    barrier.wait(); // start all workers simultaneously
    let stats = collect_stats(handles)?;

    server_thread
        .join()
        .map_err(|_| "server thread panicked".to_string())??;

    Ok(stats)
}

fn run_produce_worker_shared(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    partitions: usize,
    addr: std::net::SocketAddr,
    start_barrier: Arc<Barrier>,
    pipeline_depth: usize,
    records_per_request: usize,
) -> Result<WorkerStats, String> {
    let partition = worker_id as i32 % partitions.max(1) as i32;
    let rpr = records_per_request.max(1);

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("worker {worker_id} connect: {e}"))?;
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set_nodelay: {e}"))?;

    let single_record: Vec<u8> = vec![0x6a; payload_bytes];
    let batch_payload = encode_bench_batch(&single_record, rpr);
    let produce_body = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(BENCH_TOPIC.to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: partition,
                records: Some(batch_payload),
            }],
        }],
    }
    .encode(PRODUCE_API_VERSION)
    .map_err(|e| format!("encode produce body: {e:?}"))?;

    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline + measured_deadline;

    let capacity = usize::try_from(target_rate.saturating_mul(timing.duration_secs).min(4_000_000))
        .unwrap_or(4_000_000);
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(capacity);

    start_barrier.wait();
    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut measured_ops: u64 = 0;
    let mut corr_id: i32 = 1;
    let mut response_buf = [0u8; 4];

    if pipeline_depth <= 1 {
        loop {
            if pacer.elapsed() >= wall_deadline { break; }
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            let t0 = Instant::now();
            stream.write_all(&frame).map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            read_response_frame(&mut stream, &mut response_buf).map_err(|e| format!("worker {worker_id} produce read: {e}"))?;
            let elapsed_ns = duration_ns(t0.elapsed());
            total_ops += rpr as u64;
            corr_id = corr_id.wrapping_add(1);
            if pacer.elapsed() >= warmup_deadline { measured_ops += rpr as u64; latencies_ns.push(elapsed_ns); }
            pacer.throttle(total_ops);
        }
    } else {
        let depth = pipeline_depth;
        let mut in_flight: VecDeque<Instant> = VecDeque::with_capacity(depth);
        while in_flight.len() < depth {
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            stream.write_all(&frame).map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            in_flight.push_back(Instant::now());
            corr_id = corr_id.wrapping_add(1);
        }
        loop {
            read_response_frame(&mut stream, &mut response_buf).map_err(|e| format!("worker {worker_id} produce read: {e}"))?;
            let t0 = in_flight.pop_front().expect("in_flight");
            let elapsed_ns = duration_ns(t0.elapsed());
            if pacer.elapsed() >= warmup_deadline { measured_ops += rpr as u64; latencies_ns.push(elapsed_ns); }
            if pacer.elapsed() >= wall_deadline { break; }
            let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, corr_id, &produce_body);
            stream.write_all(&frame).map_err(|e| format!("worker {worker_id} produce write: {e}"))?;
            in_flight.push_back(Instant::now());
            corr_id = corr_id.wrapping_add(1);
        }
        while let Some(_) = in_flight.pop_front() {
            let _ = read_response_frame(&mut stream, &mut response_buf);
        }
    }

    stream
        .shutdown(Shutdown::Both)
        .map_err(|e| format!("worker {worker_id} shutdown: {e}"))?;

    Ok(WorkerStats {
        total_ops: measured_ops,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

fn run_fetch_shared_scenario(
    config: &BenchConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate(target_rate, config.workers);
    let active = worker_rates.iter().filter(|&&r| r > 0).count();
    if active == 0 {
        return Ok(zero_stats());
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;

    let log_config = bench_log_config(config.sync);
    let server = rt
        .block_on(AsyncTransportServerSharded::bind("127.0.0.1:0", data_dir, log_config))
        .map_err(|e| format!("shared server bind: {e:?}"))?;
    let addr = server.local_addr().map_err(|e| format!("local_addr: {e:?}"))?;

    // Server runs in background so it can process pre-load produce requests
    // before the timed fetch loop begins.
    let server_thread = thread::spawn(move || {
        rt.block_on(server.serve_n_connections_concurrent(active))
            .map_err(|e| format!("shared server error: {e:?}"))
    });

    // Barrier fires AFTER connection but BEFORE pre-loading: ensures the
    // server has started accepting before any requests arrive.
    let barrier = Arc::new(Barrier::new(active + 1));
    let partitions = config.partitions;
    let payload_bytes = config.payload_bytes;
    let mut handles = Vec::with_capacity(active);

    for (id, rate) in worker_rates.into_iter().enumerate() {
        if rate == 0 {
            continue;
        }
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let pipeline_depth = config.pipeline_depth;
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            run_fetch_worker_shared(id, rate, timing, payload_bytes, partitions, addr, b, pipeline_depth)
        }));
    }

    barrier.wait(); // releases workers to connect + pre-load + fetch
    let stats = collect_stats(handles)?;

    server_thread
        .join()
        .map_err(|_| "server thread panicked".to_string())??;

    Ok(stats)
}

fn run_fetch_worker_shared(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    partitions: usize,
    addr: std::net::SocketAddr,
    start_barrier: Arc<Barrier>,
    pipeline_depth: usize,
) -> Result<WorkerStats, String> {
    let partition = worker_id as i32 % partitions.max(1) as i32;

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("worker {worker_id} connect: {e}"))?;
    stream
        .set_nodelay(true)
        .map_err(|e| format!("set_nodelay: {e}"))?;

    // Wait until all workers have connected AND the server background thread
    // has started accepting.  Pre-loading runs after the barrier so the server
    // is guaranteed to be processing requests.
    start_barrier.wait();

    // ── Pre-load records for this worker's partition before the timed loop. ──
    let preload_body = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(BENCH_TOPIC.to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: partition,
                records: Some(vec![0x42; payload_bytes]),
            }],
        }],
    }
    .encode(PRODUCE_API_VERSION)
    .map_err(|e| format!("encode preload body: {e:?}"))?;

    let mut preload_corr: i32 = -1_000_000;
    let mut response_buf = [0u8; 4];

    for _ in 0..FETCH_PRELOAD_COUNT {
        let frame = build_request_frame(API_KEY_PRODUCE, PRODUCE_API_VERSION, preload_corr, &preload_body);
        stream
            .write_all(&frame)
            .map_err(|e| format!("worker {worker_id} preload write: {e}"))?;
        read_response_frame(&mut stream, &mut response_buf)
            .map_err(|e| format!("worker {worker_id} preload read: {e}"))?;
        preload_corr = preload_corr.wrapping_add(1);
    }

    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline + measured_deadline;

    let capacity = usize::try_from(target_rate.saturating_mul(timing.duration_secs).min(4_000_000))
        .unwrap_or(4_000_000);
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(capacity);

    let preload_limit = i64::try_from(FETCH_PRELOAD_COUNT).unwrap_or(i64::MAX);

    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut measured_ops: u64 = 0;
    let mut fetch_corr: i32 = 1;
    let mut next_offset: i64 = 0;
    let one_record_bytes = i32::try_from(payload_bytes + 256).unwrap_or(i32::MAX);

    if pipeline_depth <= 1 {
        // ── Ping-pong: one request in-flight at a time ───────────────────────
        loop {
            if pacer.elapsed() >= wall_deadline {
                break;
            }

            let fetch_body =
                encode_fetch_v4_body(BENCH_TOPIC, partition, next_offset, one_record_bytes);
            let frame = build_request_frame(API_KEY_FETCH, FETCH_API_VERSION, fetch_corr, &fetch_body);

            let t0 = Instant::now();
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} fetch write: {e}"))?;
            read_response_frame(&mut stream, &mut response_buf)
                .map_err(|e| format!("worker {worker_id} fetch read: {e}"))?;
            let elapsed_ns = duration_ns(t0.elapsed());

            total_ops += 1;
            fetch_corr = fetch_corr.wrapping_add(1);
            next_offset += 1;
            if next_offset >= preload_limit {
                next_offset = 0;
            }

            if pacer.elapsed() >= warmup_deadline {
                measured_ops += 1;
                latencies_ns.push(elapsed_ns);
            }
            pacer.throttle(total_ops);
        }
    } else {
        // ── Sliding-window pipeline: keep `pipeline_depth` requests in-flight ─
        let depth = pipeline_depth;
        let mut in_flight: VecDeque<Instant> = VecDeque::with_capacity(depth);

        // Fill the initial window.
        while in_flight.len() < depth {
            let fetch_body =
                encode_fetch_v4_body(BENCH_TOPIC, partition, next_offset, one_record_bytes);
            let frame = build_request_frame(API_KEY_FETCH, FETCH_API_VERSION, fetch_corr, &fetch_body);
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} fetch pipeline write: {e}"))?;
            in_flight.push_back(Instant::now());
            fetch_corr = fetch_corr.wrapping_add(1);
            next_offset += 1;
            if next_offset >= preload_limit {
                next_offset = 0;
            }
        }

        loop {
            // Drain oldest response.
            let t0 = in_flight.pop_front().expect("in_flight non-empty");
            read_response_frame(&mut stream, &mut response_buf)
                .map_err(|e| format!("worker {worker_id} fetch pipeline read: {e}"))?;
            let elapsed_ns = duration_ns(t0.elapsed());

            total_ops += 1;
            if pacer.elapsed() >= warmup_deadline {
                measured_ops += 1;
                latencies_ns.push(elapsed_ns);
            }

            if pacer.elapsed() >= wall_deadline {
                break;
            }

            // Refill the slot.
            let fetch_body =
                encode_fetch_v4_body(BENCH_TOPIC, partition, next_offset, one_record_bytes);
            let frame = build_request_frame(API_KEY_FETCH, FETCH_API_VERSION, fetch_corr, &fetch_body);
            stream
                .write_all(&frame)
                .map_err(|e| format!("worker {worker_id} fetch pipeline write: {e}"))?;
            in_flight.push_back(Instant::now());
            fetch_corr = fetch_corr.wrapping_add(1);
            next_offset += 1;
            if next_offset >= preload_limit {
                next_offset = 0;
            }
        }

        // Drain responses for any requests still in-flight so the server can
        // finish its write loop before we send RST via shutdown(Both).
        while in_flight.pop_front().is_some() {
            let _ = read_response_frame(&mut stream, &mut response_buf);
        }
    }

    stream
        .shutdown(Shutdown::Both)
        .map_err(|e| format!("worker {worker_id} shutdown: {e}"))?;

    Ok(WorkerStats {
        total_ops: measured_ops,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

// ── Wire-protocol helpers ─────────────────────────────────────────────────────

/// Build a complete Kafka request frame:
/// [4-byte frame_len][api_key][api_version][correlation_id][null client_id][body]
fn build_request_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    // header_version: Produce v3 → 1 (legacy), Fetch v4 → 1 (legacy)
    // null client_id = -1 as i16
    let header: [u8; 10] = {
        let mut h = [0u8; 10];
        h[0..2].copy_from_slice(&api_key.to_be_bytes());
        h[2..4].copy_from_slice(&api_version.to_be_bytes());
        h[4..8].copy_from_slice(&correlation_id.to_be_bytes());
        h[8..10].copy_from_slice(&(-1_i16).to_be_bytes()); // null client_id
        h
    };

    let frame_len = i32::try_from(header.len() + body.len()).expect("frame fits i32");
    let mut out = Vec::with_capacity(4 + header.len() + body.len());
    out.extend_from_slice(&frame_len.to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(body);
    out
}

/// Encode a FetchRequest v4 body.
/// v4 layout (non-flexible):
///   replica_id(i32) max_wait_ms(i32) min_bytes(i32) max_bytes(i32) isolation_level(i8)
///   topics[]: topic_name(str16) partitions[]: partition(i32) fetch_offset(i64) partition_max_bytes(i32)
/// Note: log_start_offset was added in v5 — do NOT include it here.
fn encode_fetch_v4_body(
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let topic_len = i16::try_from(topic_bytes.len()).expect("topic name fits i16");

    let mut out = Vec::with_capacity(64 + topic_bytes.len());
    out.extend_from_slice(&(-1_i32).to_be_bytes()); // replica_id
    out.extend_from_slice(&(0_i32).to_be_bytes()); // max_wait_ms=0 → return immediately
    out.extend_from_slice(&(1_i32).to_be_bytes()); // min_bytes
    out.extend_from_slice(&(1_048_576_i32).to_be_bytes()); // max_bytes (v≥3)
    out.push(0u8); // isolation_level (v≥4, READ_UNCOMMITTED)

    // topics array (non-flexible: i32 count)
    out.extend_from_slice(&(1_i32).to_be_bytes()); // 1 topic
    out.extend_from_slice(&topic_len.to_be_bytes());
    out.extend_from_slice(topic_bytes);

    // partitions array
    out.extend_from_slice(&(1_i32).to_be_bytes()); // 1 partition
    out.extend_from_slice(&partition.to_be_bytes());
    out.extend_from_slice(&fetch_offset.to_be_bytes());
    // log_start_offset was added in v5 — omitted for v4
    out.extend_from_slice(&partition_max_bytes.to_be_bytes());
    out
}

/// Read one response frame: [4-byte length][payload].
/// Discards the payload bytes — we only need to know the round-trip is complete.
fn read_response_frame(stream: &mut TcpStream, len_buf: &mut [u8; 4]) -> std::io::Result<()> {
    stream.read_exact(len_buf)?;
    let frame_len = i32::from_be_bytes(*len_buf);
    debug_assert!(frame_len >= 0, "negative frame length from server");
    let remaining = frame_len.max(0) as usize;
    if remaining == 0 {
        return Ok(());
    }
    // Drain the payload without allocating a buffer for every call.
    let mut drain = stream.take(remaining as u64);
    std::io::copy(&mut drain, &mut std::io::sink())?;
    Ok(())
}

// ── Statistics helpers (identical to perf_harness) ────────────────────────────

fn collect_stats(
    handles: Vec<thread::JoinHandle<Result<WorkerStats, String>>>,
) -> Result<RunStats, String> {
    let mut total_ops = 0_u64;
    let mut elapsed = Duration::ZERO;
    let mut latencies_ns: Vec<u64> = Vec::new();

    for h in handles {
        let w = h
            .join()
            .map_err(|_| "worker thread panicked".to_string())??;
        total_ops = total_ops.saturating_add(w.total_ops);
        elapsed = elapsed.max(w.elapsed);
        latencies_ns.extend(w.latencies_ns);
    }

    Ok(summarize(total_ops, elapsed, latencies_ns))
}

fn summarize(total_ops: u64, elapsed: Duration, mut latencies_ns: Vec<u64>) -> RunStats {
    latencies_ns.sort_unstable();
    let p50 = percentile_ns(&latencies_ns, 50.0);
    let p95 = percentile_ns(&latencies_ns, 95.0);
    let p99 = percentile_ns(&latencies_ns, 99.0);
    let max = latencies_ns.last().copied().unwrap_or(0);
    let secs = elapsed.as_secs_f64();
    let achieved = if secs <= f64::EPSILON {
        0.0
    } else {
        total_ops as f64 / secs
    };
    RunStats {
        total_ops,
        elapsed,
        achieved_msgs_per_sec: achieved,
        p50_us: p50 / 1_000,
        p95_us: p95 / 1_000,
        p99_us: p99 / 1_000,
        max_us: max / 1_000,
    }
}

fn percentile_ns(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let upper = sorted.len() - 1;
    let idx = (((pct / 100.0) * upper as f64).round() as usize).min(upper);
    sorted[idx]
}

fn print_stats(mode: &str, target_rate: u64, duration_secs: u64, stats: &RunStats) {
    let achievement = if target_rate == 0 {
        0.0
    } else {
        (stats.achieved_msgs_per_sec / target_rate as f64) * 100.0
    };
    println!(
        "{mode},{target_rate},{duration_secs},{},{:.2},{:.2},{},{},{},{}",
        stats.total_ops,
        stats.achieved_msgs_per_sec,
        achievement,
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
        stats.max_us,
    );
}

fn zero_stats() -> RunStats {
    RunStats {
        total_ops: 0,
        elapsed: Duration::ZERO,
        achieved_msgs_per_sec: 0.0,
        p50_us: 0,
        p95_us: 0,
        p99_us: 0,
        max_us: 0,
    }
}

// ── Utility ───────────────────────────────────────────────────────────────────

fn bench_log_config(sync: bool) -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 1_000_000, // one segment for entire bench run
        sync_on_append: sync,
    }
}

fn scenario_dir(root: &Path, mode: &str, rate: u64) -> PathBuf {
    root.join(format!("{mode}-{rate}"))
}

fn reset_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    fs::create_dir_all(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    Ok(())
}

fn split_rate(total: u64, max_workers: usize) -> Vec<u64> {
    let workers = (max_workers as u64).min(total).max(1) as usize;
    let base = total / workers as u64;
    let remainder = total % workers as u64;
    (0..workers)
        .map(|i| base + if (i as u64) < remainder { 1 } else { 0 })
        .collect()
}

fn duration_ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

// ── Argument parsing ──────────────────────────────────────────────────────────

fn parse_args(args: Vec<String>) -> Result<BenchConfig, String> {
    let mut mode = BenchMode::Both;
    let mut target_rates: Vec<u64> = DEFAULT_TARGET_RATES.to_vec();
    let mut duration_secs = DEFAULT_DURATION_SECS;
    let mut warmup_secs = DEFAULT_WARMUP_SECS;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut workers = DEFAULT_MAX_WORKERS;
    let mut data_root = std::env::temp_dir().join(format!("rafka-e2e-bench-{}", now_ms()));
    let mut sync = false;
    let mut partitions: usize = 0;
    let mut pipeline_depth: usize = 1;
    let mut records_per_request: usize = 1;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                i += 1;
                mode = match args.get(i).map(String::as_str) {
                    Some("produce") => BenchMode::Produce,
                    Some("fetch") => BenchMode::Fetch,
                    Some("both") => BenchMode::Both,
                    other => return Err(format!("unknown mode: {other:?}")),
                };
            }
            "--duration-secs" => {
                i += 1;
                duration_secs = args.get(i).ok_or("missing duration")?.parse().map_err(|e| format!("{e}"))?;
            }
            "--warmup-secs" => {
                i += 1;
                warmup_secs = args.get(i).ok_or("missing warmup")?.parse().map_err(|e| format!("{e}"))?;
            }
            "--target-rates" => {
                i += 1;
                target_rates = args.get(i)
                    .ok_or("missing rates")?
                    .split(',')
                    .map(|s| s.trim().parse::<u64>().map_err(|e| format!("{e}")))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--payload-bytes" => {
                i += 1;
                payload_bytes = args.get(i).ok_or("missing bytes")?.parse().map_err(|e| format!("{e}"))?;
            }
            "--workers" => {
                i += 1;
                workers = args.get(i).ok_or("missing workers")?.parse().map_err(|e| format!("{e}"))?;
            }
            "--data-dir" => {
                i += 1;
                data_root = PathBuf::from(args.get(i).ok_or("missing data dir")?);
            }
            "--sync" => {
                sync = true;
            }
            "--partitions" => {
                i += 1;
                partitions = args.get(i).ok_or("missing partitions")?.parse().map_err(|e| format!("{e}"))?;
            }
            "--pipeline-depth" => {
                i += 1;
                pipeline_depth = args.get(i).ok_or("missing pipeline-depth")?.parse().map_err(|e| format!("{e}"))?;
                if pipeline_depth == 0 { pipeline_depth = 1; }
            }
            "--records-per-request" => {
                i += 1;
                records_per_request = args.get(i).ok_or("missing records-per-request")?.parse().map_err(|e| format!("{e}"))?;
                if records_per_request == 0 { records_per_request = 1; }
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }

    Ok(BenchConfig {
        mode,
        target_rates,
        duration_secs,
        warmup_secs,
        payload_bytes,
        workers,
        data_root,
        sync,
        partitions,
        pipeline_depth,
        records_per_request,
    })
}
