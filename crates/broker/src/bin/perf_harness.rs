#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rafka_broker::{PartitionedBroker, PersistentLogConfig};

const DEFAULT_TARGET_RATES: &[u64] = &[1_000, 10_000, 100_000];
const DEFAULT_DURATION_SECS: u64 = 10;
const DEFAULT_WARMUP_SECS: u64 = 1;
const DEFAULT_PAYLOAD_BYTES: usize = 512;
const DEFAULT_FETCH_MAX_RECORDS: usize = 1;
const DEFAULT_MAX_WORKERS: usize = 16;
const BENCH_TOPIC: &str = "perf-topic";
const BENCH_PARTITION: i32 = 0;

/// Kafka ThroughputThrottler uses a 2ms minimum sleep quantum to avoid
/// per-message sleep jitter while still enforcing average target throughput.
const THROTTLE_MIN_SLEEP_NS: u64 = 2_000_000;

/// Cap on the number of records pre-produced before a fetch scenario.
/// Avoids unbounded preload time when target_rate is very high.
const FETCH_PRELOAD_CAP: u64 = 200_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HarnessMode {
    Produce,
    Fetch,
    Both,
}

impl HarnessMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Produce => "produce",
            Self::Fetch => "fetch",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone)]
struct HarnessConfig {
    mode: HarnessMode,
    target_rates: Vec<u64>,
    duration_secs: u64,
    warmup_secs: u64,
    payload_bytes: usize,
    fetch_max_records: usize,
    workers: usize,
    data_root: PathBuf,
}

#[derive(Debug, Clone)]
struct RunStats {
    total_ops: u64,
    elapsed: Duration,
    achieved_msgs_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Clone)]
struct WorkerRunStats {
    total_ops: u64,
    total_messages: u64,
    elapsed: Duration,
    latencies_ns: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct RunTiming {
    duration_secs: u64,
    warmup_secs: u64,
}

#[derive(Debug, Clone)]
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

    fn throttle(&mut self, amount_so_far: u64) {
        if !should_throttle(amount_so_far, self.target_rate, self.start.elapsed()) {
            return;
        }

        self.sleep_deficit_ns = self.sleep_deficit_ns.saturating_add(self.sleep_time_ns);
        if self.sleep_deficit_ns < THROTTLE_MIN_SLEEP_NS {
            return;
        }

        let sleep_ns = self.sleep_deficit_ns;
        self.sleep_deficit_ns = 0;
        std::thread::sleep(Duration::from_nanos(sleep_ns));
    }
}

fn main() -> Result<(), String> {
    let config = parse_args(env::args().skip(1).collect())?;
    fs::create_dir_all(&config.data_root)
        .map_err(|err| format!("failed to create {}: {err}", config.data_root.display()))?;

    println!(
        "mode,target_msgs_per_sec,duration_secs,total_ops,achieved_msgs_per_sec,target_achievement_pct,p50_us,p95_us,p99_us,max_us"
    );

    match config.mode {
        HarnessMode::Produce => run_produce_matrix(&config)?,
        HarnessMode::Fetch => run_fetch_matrix(&config)?,
        HarnessMode::Both => {
            run_produce_matrix(&config)?;
            run_fetch_matrix(&config)?;
        }
    }

    Ok(())
}

fn run_produce_matrix(config: &HarnessConfig) -> Result<(), String> {
    for &target_rate in &config.target_rates {
        let run_dir = scenario_dir(&config.data_root, "produce", target_rate);
        reset_dir(&run_dir)?;
        let stats = run_produce_scenario(config, target_rate, &run_dir)?;
        print_stats(
            HarnessMode::Produce.as_str(),
            target_rate,
            config.duration_secs,
            &stats,
        );
    }
    Ok(())
}

fn run_fetch_matrix(config: &HarnessConfig) -> Result<(), String> {
    for &target_rate in &config.target_rates {
        let run_dir = scenario_dir(&config.data_root, "fetch", target_rate);
        reset_dir(&run_dir)?;
        let stats = run_fetch_scenario(config, target_rate, &run_dir)?;
        print_stats(
            HarnessMode::Fetch.as_str(),
            target_rate,
            config.duration_secs,
            &stats,
        );
    }
    Ok(())
}

fn run_produce_scenario(
    config: &HarnessConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate_across_workers(target_rate, config.workers);
    let active_workers = worker_rates.iter().filter(|rate| **rate > 0).count();
    if active_workers == 0 {
        return Ok(summarize(0, 0, Duration::ZERO, Vec::new()));
    }
    let start_barrier = Arc::new(Barrier::new(active_workers + 1));
    let mut handles = Vec::with_capacity(worker_rates.len());

    for (worker_id, worker_rate) in worker_rates.into_iter().enumerate() {
        if worker_rate == 0 {
            continue;
        }
        let worker_dir = data_dir.join(format!("produce-worker-{worker_id}"));
        let payload_bytes = config.payload_bytes;
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let worker_start_barrier = Arc::clone(&start_barrier);
        handles.push(thread::spawn(move || {
            run_produce_worker(
                worker_id,
                worker_rate,
                timing,
                payload_bytes,
                &worker_dir,
                worker_start_barrier,
            )
        }));
    }

    start_barrier.wait();
    collect_worker_stats(handles)
}

fn run_fetch_scenario(
    config: &HarnessConfig,
    target_rate: u64,
    data_dir: &Path,
) -> Result<RunStats, String> {
    let worker_rates = split_rate_across_workers(target_rate, config.workers);
    let active_workers = worker_rates.iter().filter(|rate| **rate > 0).count();
    if active_workers == 0 {
        return Ok(summarize(0, 0, Duration::ZERO, Vec::new()));
    }
    let start_barrier = Arc::new(Barrier::new(active_workers + 1));
    let mut handles = Vec::with_capacity(worker_rates.len());

    for (worker_id, worker_rate) in worker_rates.into_iter().enumerate() {
        if worker_rate == 0 {
            continue;
        }
        let worker_dir = data_dir.join(format!("fetch-worker-{worker_id}"));
        let payload_bytes = config.payload_bytes;
        let timing = RunTiming {
            duration_secs: config.duration_secs,
            warmup_secs: config.warmup_secs,
        };
        let fetch_max_records = config.fetch_max_records;
        let worker_start_barrier = Arc::clone(&start_barrier);
        handles.push(thread::spawn(move || {
            run_fetch_worker(
                worker_id,
                worker_rate,
                timing,
                payload_bytes,
                fetch_max_records,
                &worker_dir,
                worker_start_barrier,
            )
        }));
    }

    start_barrier.wait();
    collect_worker_stats(handles)
}

fn run_produce_worker(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    data_dir: &Path,
    start_barrier: Arc<Barrier>,
) -> Result<WorkerRunStats, String> {
    reset_dir(data_dir)?;
    let mut broker = open_broker(data_dir)?;
    let payload = vec![0x6a; payload_bytes];
    let timestamp_ms = now_ms();
    let capacity = usize::try_from(
        target_rate
            .saturating_mul(timing.duration_secs)
            .min(4_000_000),
    )
    .unwrap_or(4_000_000);
    let mut latencies_ns = Vec::with_capacity(capacity);
    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline.saturating_add(measured_deadline);
    start_barrier.wait();
    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut measured_ops: u64 = 0;

    loop {
        if pacer.elapsed() >= wall_deadline {
            break;
        }
        let op_start = Instant::now();
        let offset = broker
            .produce_to(
                BENCH_TOPIC,
                BENCH_PARTITION,
                Vec::new(),
                payload.clone(),
                timestamp_ms,
            )
            .map_err(|err| {
                format!("produce worker {worker_id} failure at op {total_ops}: {err:?}")
            })?;
        std::hint::black_box(offset);
        total_ops += 1;
        if pacer.elapsed() >= warmup_deadline {
            measured_ops += 1;
            latencies_ns.push(duration_to_u64_ns(op_start.elapsed()));
        }
        pacer.throttle(total_ops);
    }

    Ok(WorkerRunStats {
        total_ops: measured_ops,
        total_messages: measured_ops,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

fn run_fetch_worker(
    worker_id: usize,
    target_rate: u64,
    timing: RunTiming,
    payload_bytes: usize,
    fetch_max_records: usize,
    data_dir: &Path,
    start_barrier: Arc<Barrier>,
) -> Result<WorkerRunStats, String> {
    reset_dir(data_dir)?;
    let mut broker = open_broker(data_dir)?;

    // Pre-produce records before the timed loop. Cap the preload to avoid
    // long setup when target_rate is very high; the fetch loop wraps around.
    let preload_records = target_rate
        .saturating_mul(timing.duration_secs)
        .saturating_add(u64::try_from(fetch_max_records).unwrap_or(u64::MAX))
        .min(FETCH_PRELOAD_CAP);
    let payload = vec![0x42; payload_bytes];
    let preload_ts = now_ms();
    for _ in 0..preload_records {
        broker
            .produce_to(
                BENCH_TOPIC,
                BENCH_PARTITION,
                Vec::new(),
                payload.clone(),
                preload_ts,
            )
            .map_err(|err| format!("fetch worker {worker_id} preload failed: {err:?}"))?;
    }

    let mut next_offset = 0_i64;
    let preload_limit = i64::try_from(preload_records).unwrap_or(i64::MAX);
    let capacity = usize::try_from(
        target_rate
            .saturating_mul(timing.duration_secs)
            .min(4_000_000),
    )
    .unwrap_or(4_000_000);
    let mut latencies_ns = Vec::with_capacity(capacity);
    let warmup_deadline = Duration::from_secs(timing.warmup_secs);
    let measured_deadline = Duration::from_secs(timing.duration_secs);
    let wall_deadline = warmup_deadline.saturating_add(measured_deadline);
    start_barrier.wait();
    let mut pacer = TargetPacer::new(target_rate);
    let mut total_ops: u64 = 0;
    let mut total_messages: u64 = 0;
    let mut measured_ops: u64 = 0;
    let mut measured_messages: u64 = 0;

    loop {
        if pacer.elapsed() >= wall_deadline {
            break;
        }
        let op_start = Instant::now();
        let records = broker
            .fetch_from_partition(BENCH_TOPIC, BENCH_PARTITION, next_offset, fetch_max_records)
            .map_err(|err| {
                format!("fetch worker {worker_id} failure at op {total_ops}: {err:?}")
            })?;
        let fetched_len = records.len();
        std::hint::black_box(fetched_len);
        if let Some(last) = records.last() {
            next_offset = last.offset.saturating_add(1);
        } else {
            next_offset = 0;
        }
        if next_offset >= preload_limit {
            next_offset = 0;
        }

        total_ops += 1;
        let fetched_messages = u64::try_from(fetched_len).unwrap_or(u64::MAX);
        total_messages = total_messages.saturating_add(fetched_messages);
        if pacer.elapsed() >= warmup_deadline {
            measured_ops += 1;
            measured_messages = measured_messages.saturating_add(fetched_messages);
            latencies_ns.push(duration_to_u64_ns(op_start.elapsed()));
        }
        pacer.throttle(total_messages);
    }

    Ok(WorkerRunStats {
        total_ops: measured_ops,
        total_messages: measured_messages,
        elapsed: pacer.elapsed().saturating_sub(warmup_deadline),
        latencies_ns,
    })
}

fn collect_worker_stats(
    handles: Vec<thread::JoinHandle<Result<WorkerRunStats, String>>>,
) -> Result<RunStats, String> {
    let mut total_ops = 0_u64;
    let mut total_messages = 0_u64;
    let mut elapsed = Duration::ZERO;
    let mut latencies_ns = Vec::new();

    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| "worker thread panicked".to_string())??;
        total_ops = total_ops.saturating_add(worker.total_ops);
        total_messages = total_messages.saturating_add(worker.total_messages);
        elapsed = elapsed.max(worker.elapsed);
        latencies_ns.extend(worker.latencies_ns);
    }

    Ok(summarize(total_ops, total_messages, elapsed, latencies_ns))
}

fn open_broker(path: &Path) -> Result<PartitionedBroker, String> {
    PartitionedBroker::open(path, log_config())
        .map_err(|err| format!("failed to open broker at {}: {err:?}", path.display()))
}

fn log_config() -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset: 0,
        // Keep a large active segment during short perf runs to avoid
        // artificial roll overhead in the hot path.
        segment_max_records: 1_000_000,
        sync_on_append: false,
    }
}

fn summarize(
    total_ops: u64,
    total_messages: u64,
    elapsed: Duration,
    mut latencies_ns: Vec<u64>,
) -> RunStats {
    latencies_ns.sort_unstable();
    let p50 = percentile_ns(&latencies_ns, 50.0);
    let p95 = percentile_ns(&latencies_ns, 95.0);
    let p99 = percentile_ns(&latencies_ns, 99.0);
    let max = latencies_ns.last().copied().unwrap_or(0);

    let elapsed_secs = elapsed.as_secs_f64();
    let achieved_msgs_per_sec = if elapsed_secs <= f64::EPSILON {
        0.0
    } else {
        total_messages as f64 / elapsed_secs
    };

    RunStats {
        total_ops,
        elapsed,
        achieved_msgs_per_sec,
        p50_us: p50 / 1_000,
        p95_us: p95 / 1_000,
        p99_us: p99 / 1_000,
        max_us: max / 1_000,
    }
}

fn percentile_ns(sorted_ns: &[u64], percentile: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let upper = sorted_ns.len() - 1;
    let position = ((percentile / 100.0) * upper as f64).round();
    let index = usize::try_from(position as i64).unwrap_or(upper).min(upper);
    sorted_ns[index]
}

fn print_stats(mode: &str, target_rate: u64, duration_secs: u64, stats: &RunStats) {
    std::hint::black_box(stats.elapsed);
    let target_achievement_pct = if target_rate == 0 {
        0.0
    } else {
        (stats.achieved_msgs_per_sec / target_rate as f64) * 100.0
    };
    println!(
        "{mode},{target_rate},{duration_secs},{},{:.2},{:.2},{},{},{},{}",
        stats.total_ops,
        stats.achieved_msgs_per_sec,
        target_achievement_pct,
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
        stats.max_us
    );
}

fn split_rate_across_workers(total_rate: u64, workers: usize) -> Vec<u64> {
    let workers = workers.max(1);
    let workers_u64 = u64::try_from(workers).unwrap_or(u64::MAX);
    let base = total_rate / workers_u64;
    let remainder = total_rate % workers_u64;
    (0..workers)
        .map(|worker| {
            if u64::try_from(worker).unwrap_or(u64::MAX) < remainder {
                base.saturating_add(1)
            } else {
                base
            }
        })
        .collect()
}

fn scenario_dir(root: &Path, mode: &str, rate: u64) -> PathBuf {
    root.join(format!("{mode}-{rate}"))
}

fn reset_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|err| format!("failed to clean {}: {err}", path.display()))?;
    }
    fs::create_dir_all(path).map_err(|err| format!("failed to create {}: {err}", path.display()))
}

fn should_throttle(amount_so_far: u64, target_rate: u64, elapsed: Duration) -> bool {
    if target_rate == 0 || amount_so_far == 0 {
        return false;
    }
    let elapsed_ns = elapsed.as_nanos();
    if elapsed_ns == 0 {
        return false;
    }
    let produced_per_sec = u128::from(amount_so_far).saturating_mul(1_000_000_000);
    let allowed_per_sec = u128::from(target_rate).saturating_mul(elapsed_ns);
    produced_per_sec > allowed_per_sec
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn parse_args(args: Vec<String>) -> Result<HarnessConfig, String> {
    let mut mode = HarnessMode::Both;
    let mut target_rates = DEFAULT_TARGET_RATES.to_vec();
    let mut duration_secs = DEFAULT_DURATION_SECS;
    let mut warmup_secs = DEFAULT_WARMUP_SECS;
    let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
    let mut fetch_max_records = DEFAULT_FETCH_MAX_RECORDS;
    let mut workers = default_worker_count();
    let mut data_root: Option<PathBuf> = None;

    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        match flag.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--mode" => {
                let value = next_arg(&args, &mut index, "--mode")?;
                mode = parse_mode(value)?;
            }
            "--target-rates" => {
                let value = next_arg(&args, &mut index, "--target-rates")?;
                target_rates = parse_target_rates(value)?;
            }
            "--duration-secs" => {
                let value = next_arg(&args, &mut index, "--duration-secs")?;
                duration_secs = value
                    .parse::<u64>()
                    .map_err(|err| format!("invalid --duration-secs `{value}`: {err}"))?;
            }
            "--warmup-secs" => {
                let value = next_arg(&args, &mut index, "--warmup-secs")?;
                warmup_secs = value
                    .parse::<u64>()
                    .map_err(|err| format!("invalid --warmup-secs `{value}`: {err}"))?;
            }
            "--payload-bytes" => {
                let value = next_arg(&args, &mut index, "--payload-bytes")?;
                payload_bytes = value
                    .parse::<usize>()
                    .map_err(|err| format!("invalid --payload-bytes `{value}`: {err}"))?;
            }
            "--fetch-max-records" => {
                let value = next_arg(&args, &mut index, "--fetch-max-records")?;
                fetch_max_records = value
                    .parse::<usize>()
                    .map_err(|err| format!("invalid --fetch-max-records `{value}`: {err}"))?;
            }
            "--workers" => {
                let value = next_arg(&args, &mut index, "--workers")?;
                workers = value
                    .parse::<usize>()
                    .map_err(|err| format!("invalid --workers `{value}`: {err}"))?;
            }
            "--data-root" => {
                let value = next_arg(&args, &mut index, "--data-root")?;
                data_root = Some(PathBuf::from(value));
            }
            other => {
                return Err(format!("unknown argument `{other}`"));
            }
        }
        index += 1;
    }

    let data_root = data_root.unwrap_or_else(default_data_root);
    if fetch_max_records == 0 {
        return Err("--fetch-max-records must be > 0".to_string());
    }
    if workers == 0 {
        return Err("--workers must be > 0".to_string());
    }

    Ok(HarnessConfig {
        mode,
        target_rates,
        duration_secs,
        warmup_secs,
        payload_bytes,
        fetch_max_records,
        workers,
        data_root,
    })
}

fn next_arg<'a>(args: &'a [String], index: &mut usize, flag: &str) -> Result<&'a str, String> {
    *index += 1;
    args.get(*index)
        .map(String::as_str)
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_mode(value: &str) -> Result<HarnessMode, String> {
    match value {
        "produce" => Ok(HarnessMode::Produce),
        "fetch" => Ok(HarnessMode::Fetch),
        "both" => Ok(HarnessMode::Both),
        _ => Err(format!(
            "invalid --mode `{value}`; expected one of: produce, fetch, both"
        )),
    }
}

fn parse_target_rates(value: &str) -> Result<Vec<u64>, String> {
    let mut rates = Vec::new();
    for part in value.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return Err("invalid --target-rates: empty component".to_string());
        }
        let parsed = trimmed
            .parse::<u64>()
            .map_err(|err| format!("invalid target rate `{trimmed}`: {err}"))?;
        if parsed == 0 {
            return Err("target rates must be > 0".to_string());
        }
        rates.push(parsed);
    }
    if rates.is_empty() {
        return Err("at least one target rate is required".to_string());
    }
    Ok(rates)
}

fn duration_to_u64_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn default_data_root() -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!("rafka-perf-{}-{millis}", std::process::id()))
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .clamp(1, DEFAULT_MAX_WORKERS)
}

fn print_usage() {
    println!(
        "rafka perf harness\n\
         \n\
         Usage:\n\
           cargo run --release -p rafka-broker --bin perf_harness -- [options]\n\
         \n\
         Options:\n\
           --mode <produce|fetch|both>           Benchmark mode (default: both)\n\
           --target-rates <r1,r2,...>            Target msg/sec classes (default: 1000,10000,100000)\n\
           --duration-secs <seconds>             Scenario wall-time in seconds (default: 10)\n\
           --warmup-secs <seconds>               Warmup time excluded from metrics (default: 1)\n\
           --payload-bytes <bytes>               Produce/fetch payload bytes (default: 512)\n\
           --fetch-max-records <n>               Records per fetch call (default: 1)\n\
           --workers <n>                         Worker threads (default: min(cpu_count,16))\n\
           --data-root <path>                    Scenario working directory root\n\
           -h, --help                            Show this help\n\
         \n\
         Notes:\n\
           Throughput is throttled with a Kafka-style average-rate pacer.\n\
           target_achievement_pct near 100 %% indicates accurate target tracking.\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_rates_rejects_empty_component() {
        let err = parse_target_rates("1000,,10000").expect_err("expected parse error");
        assert!(err.contains("empty component"));
    }

    #[test]
    fn parse_target_rates_accepts_valid_csv() {
        let parsed = parse_target_rates("1000,10000,100000").expect("parse rates");
        assert_eq!(parsed, vec![1_000, 10_000, 100_000]);
    }

    #[test]
    fn percentile_picks_expected_value() {
        let values = vec![10, 20, 30, 40, 50];
        assert_eq!(percentile_ns(&values, 50.0), 30);
        assert_eq!(percentile_ns(&values, 95.0), 50);
    }

    #[test]
    fn parse_mode_rejects_invalid() {
        let err = parse_mode("invalid").expect_err("expected parse error");
        assert!(err.contains("invalid --mode"));
    }

    #[test]
    fn parse_args_defaults_to_both_mode() {
        let config = parse_args(Vec::new()).expect("parse defaults");
        assert_eq!(config.mode, HarnessMode::Both);
        assert_eq!(config.target_rates, DEFAULT_TARGET_RATES);
        assert_eq!(config.duration_secs, DEFAULT_DURATION_SECS);
        assert_eq!(config.warmup_secs, DEFAULT_WARMUP_SECS);
        assert!(config.workers >= 1);
    }

    #[test]
    fn parse_args_overrides_values() {
        let args = vec![
            "--mode".to_string(),
            "produce".to_string(),
            "--target-rates".to_string(),
            "2000,4000".to_string(),
            "--duration-secs".to_string(),
            "5".to_string(),
            "--warmup-secs".to_string(),
            "2".to_string(),
            "--payload-bytes".to_string(),
            "128".to_string(),
            "--fetch-max-records".to_string(),
            "3".to_string(),
            "--workers".to_string(),
            "4".to_string(),
            "--data-root".to_string(),
            "/tmp/rafka-bench".to_string(),
        ];
        let config = parse_args(args).expect("parse args");
        assert_eq!(config.mode, HarnessMode::Produce);
        assert_eq!(config.target_rates, vec![2_000, 4_000]);
        assert_eq!(config.duration_secs, 5);
        assert_eq!(config.warmup_secs, 2);
        assert_eq!(config.payload_bytes, 128);
        assert_eq!(config.fetch_max_records, 3);
        assert_eq!(config.workers, 4);
        assert_eq!(config.data_root, PathBuf::from("/tmp/rafka-bench"));
    }

    #[test]
    fn mode_as_str_values() {
        assert_eq!(HarnessMode::Produce.as_str(), "produce");
        assert_eq!(HarnessMode::Fetch.as_str(), "fetch");
        assert_eq!(HarnessMode::Both.as_str(), "both");
    }

    #[test]
    fn should_throttle_is_false_below_target() {
        assert!(!should_throttle(900, 1_000, Duration::from_secs(1)));
    }

    #[test]
    fn should_throttle_is_true_above_target() {
        assert!(should_throttle(1_100, 1_000, Duration::from_secs(1)));
    }

    #[test]
    fn should_throttle_is_false_at_exact_target() {
        assert!(!should_throttle(1_000, 1_000, Duration::from_secs(1)));
    }

    #[test]
    fn should_throttle_handles_large_values_without_overflow() {
        let elapsed = Duration::from_secs(1_000_000);
        assert!(!should_throttle(u64::MAX, u64::MAX, elapsed));
    }

    #[test]
    fn throttle_min_sleep_constant_is_sane() {
        assert_eq!(THROTTLE_MIN_SLEEP_NS, 2_000_000);
    }

    #[test]
    fn split_rate_across_workers_distributes_remainder() {
        let rates = split_rate_across_workers(10, 3);
        assert_eq!(rates, vec![4, 3, 3]);
        assert_eq!(rates.iter().sum::<u64>(), 10);
    }

    #[test]
    fn split_rate_across_workers_handles_more_workers_than_rate() {
        let rates = split_rate_across_workers(3, 5);
        assert_eq!(rates, vec![1, 1, 1, 0, 0]);
        assert_eq!(rates.iter().sum::<u64>(), 3);
    }

    #[test]
    fn target_pacer_sleep_interval_has_nanosecond_floor() {
        let pacer = TargetPacer::new(2_000_000_000);
        assert_eq!(pacer.sleep_time_ns, 1);
    }
}
