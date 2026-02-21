use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_storage::{PersistentLogConfig, PersistentSegmentLog, StorageError};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-storage-overflow-{label}-{millis}-{}-{counter}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn cfg(base_offset: i64, segment_max: usize) -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset,
        segment_max_records: segment_max,
        sync_on_append: false,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-zero base_offset: appended records start at base_offset
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn non_zero_base_offset_first_record_at_base() {
    let temp = TempDir::new("nonzero-base");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(1_000, 8)).expect("open");

    let offset = log
        .append(b"key".to_vec(), b"value".to_vec(), 12345)
        .expect("append");
    assert_eq!(offset, 1_000, "first record must be at base_offset 1000");
    assert_eq!(log.next_offset(), 1_001);
}

// ──────────────────────────────────────────────────────────────────────────────
// fetch_from before base_offset → OffsetOutOfRange
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_before_base_offset_returns_out_of_range() {
    let temp = TempDir::new("fetch-before-base");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(500, 8)).expect("open");
    log.append(b"k".to_vec(), b"v".to_vec(), 0).expect("append");

    let err = log.fetch_from(499, 1).expect_err("below base must fail");
    assert!(
        matches!(err, StorageError::OffsetOutOfRange { requested: 499, .. }),
        "expected OffsetOutOfRange for offset 499, got {err:?}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// fetch_from at next_offset returns empty (valid high-watermark fetch)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_at_next_offset_returns_empty() {
    let temp = TempDir::new("fetch-at-hwm");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("open");
    log.append(b"k".to_vec(), b"v".to_vec(), 0).expect("append");

    let records = log
        .fetch_from(1, 10)
        .expect("fetch at next_offset must succeed");
    assert!(
        records.is_empty(),
        "fetching at next_offset must return nothing"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// fetch_from with max_records=0 returns empty regardless of offset
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_with_zero_max_records_is_always_empty() {
    let temp = TempDir::new("fetch-zero-max");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("open");
    for i in 0..5 {
        log.append(vec![i], vec![i], i as i64).expect("append");
    }

    let records = log.fetch_from(0, 0).expect("fetch with max=0 must succeed");
    assert!(
        records.is_empty(),
        "max_records=0 must always return empty slice"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// fetch_from way beyond next_offset → OffsetOutOfRange
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_far_beyond_next_offset_returns_out_of_range() {
    let temp = TempDir::new("fetch-far-beyond");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("open");
    log.append(b"k".to_vec(), b"v".to_vec(), 0).expect("append");
    // next_offset = 1; fetching at 1000 is out of range
    let err = log.fetch_from(1_000, 1).expect_err("far beyond must fail");
    assert!(
        matches!(
            err,
            StorageError::OffsetOutOfRange {
                requested: 1_000,
                ..
            }
        ),
        "expected OffsetOutOfRange for 1000, got {err:?}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Empty key and empty value are stored and retrieved correctly
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_key_and_empty_value_roundtrip() {
    let temp = TempDir::new("empty-kv");
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("open");

    let offset = log
        .append(Vec::new(), Vec::new(), 0)
        .expect("append empty key+value");
    assert_eq!(offset, 0);

    drop(log);
    let log2 = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("reopen");
    let records = log2.fetch_from(0, 1).expect("fetch");
    assert_eq!(records.len(), 1);
    assert!(
        records[0].key.is_empty(),
        "key must be empty after roundtrip"
    );
    assert!(
        records[0].value.is_empty(),
        "value must be empty after roundtrip"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Binary data including null bytes and 0xFF survives roundtrip
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn binary_data_with_null_bytes_roundtrip() {
    let temp = TempDir::new("binary-null");
    let key: Vec<u8> = (0u8..=255).collect(); // all byte values 0x00-0xFF
    let value: Vec<u8> = vec![0x00, 0xFF, 0x00, 0x01, 0x7F, 0x80];

    {
        let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("open");
        log.append(key.clone(), value.clone(), i64::MAX / 2)
            .expect("append binary data");
    }

    let log2 = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("reopen");
    let records = log2.fetch_from(0, 1).expect("fetch binary");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key, key, "key with all byte values must survive");
    assert_eq!(
        records[0].value, value,
        "value with null bytes must survive"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// All-zeros key and value roundtrip
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn all_zeros_key_and_value_roundtrip() {
    let temp = TempDir::new("all-zeros");
    let key = vec![0u8; 128];
    let value = vec![0u8; 512];

    {
        let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("open");
        log.append(key.clone(), value.clone(), 0)
            .expect("append zeros");
    }

    let log2 = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("reopen");
    let records = log2.fetch_from(0, 1).expect("fetch zeros");
    assert_eq!(records[0].key, key);
    assert_eq!(records[0].value, value);
}

// ──────────────────────────────────────────────────────────────────────────────
// Many segments (50 segments, 1 record each): all readable after reopen
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fifty_segments_all_records_readable_after_reopen() {
    let temp = TempDir::new("fifty-segments");
    const N: i64 = 50;

    {
        let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 1)).expect("open");
        for i in 0..N {
            let offset = log
                .append(vec![i as u8], vec![i as u8 + 1], i * 10)
                .expect("append");
            assert_eq!(offset, i);
        }
    }

    let log2 = PersistentSegmentLog::open(temp.path(), cfg(0, 1)).expect("reopen");
    assert_eq!(log2.next_offset(), N);

    let records = log2.fetch_from(0, N as usize).expect("fetch all");
    assert_eq!(records.len(), N as usize, "all 50 records must be readable");
    for (idx, rec) in records.iter().enumerate() {
        assert_eq!(rec.offset, idx as i64);
        assert_eq!(rec.key, vec![idx as u8]);
        assert_eq!(rec.value, vec![idx as u8 + 1]);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-zero base_offset: all records are accessible at correct offsets
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn non_zero_base_offset_all_records_at_correct_offsets() {
    let temp = TempDir::new("nonzero-all-offsets");
    const BASE: i64 = 10_000;
    const N: usize = 20;

    {
        let mut log = PersistentSegmentLog::open(temp.path(), cfg(BASE, 4)).expect("open");
        for i in 0..N {
            let offset = log
                .append(vec![i as u8], vec![i as u8], i as i64)
                .expect("append");
            assert_eq!(offset, BASE + i as i64);
        }
    }

    let log2 = PersistentSegmentLog::open(temp.path(), cfg(BASE, 4)).expect("reopen");
    assert_eq!(log2.next_offset(), BASE + N as i64);

    let records = log2.fetch_from(BASE, N).expect("fetch from base");
    assert_eq!(records.len(), N);
    assert_eq!(records[0].offset, BASE);
    assert_eq!(records[N - 1].offset, BASE + N as i64 - 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// Fetch partial range: skip first k records and read n after that
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_partial_range_from_middle_of_log() {
    let temp = TempDir::new("fetch-partial-range");
    const N: i64 = 30;
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("open");
    for i in 0..N {
        log.append(vec![i as u8], vec![i as u8], i * 100)
            .expect("append");
    }

    // Fetch 5 records starting from offset 10
    let records = log.fetch_from(10, 5).expect("fetch partial range");
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].offset, 10);
    assert_eq!(records[4].offset, 14);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.key, vec![(10 + i) as u8]);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Fetch that spans two consecutive segment boundaries
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_spanning_two_segment_boundaries() {
    let temp = TempDir::new("cross-segment-fetch");
    // segment_max_records = 4, so records 0-3 in seg0, 4-7 in seg1, 8-11 in seg2
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 4)).expect("open");
    for i in 0..12i64 {
        log.append(vec![i as u8], vec![i as u8], i).expect("append");
    }

    // Fetch from offset 3 for 6 records → crosses seg0→seg1→seg2 boundary
    let records = log.fetch_from(3, 6).expect("cross-segment fetch");
    assert_eq!(records.len(), 6);
    assert_eq!(records[0].offset, 3);
    assert_eq!(records[5].offset, 8);
}

// ──────────────────────────────────────────────────────────────────────────────
// next_offset after many appends and rolls tracks correctly
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn next_offset_after_many_rolls_is_correct() {
    let temp = TempDir::new("next-offset-rolls");
    const N: i64 = 100;
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 3)).expect("open");
    for i in 0..N {
        log.append(vec![], vec![i as u8], i).expect("append");
    }

    assert_eq!(log.next_offset(), N);
    let snaps = log.segment_snapshots();
    // With segment_max=3 and 100 records: ceil(100/3) = 34 segments
    // (last segment may have 1 record)
    assert!(snaps.len() >= 33, "must have at least 33 segment snapshots");
}

// ──────────────────────────────────────────────────────────────────────────────
// Large number of records: sequential bulk append then full fetch is consistent
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn bulk_append_then_bulk_fetch_all_consistent() {
    let temp = TempDir::new("bulk-append-fetch");
    const N: usize = 200;
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 32)).expect("open");

    for i in 0..N {
        let offset = log
            .append(
                format!("key-{i}").into_bytes(),
                format!("val-{i}").into_bytes(),
                i as i64 * 10,
            )
            .expect("bulk append");
        assert_eq!(offset, i as i64);
    }

    let records = log.fetch_from(0, N).expect("bulk fetch");
    assert_eq!(records.len(), N, "all {N} records must be readable");
    for (idx, rec) in records.iter().enumerate() {
        assert_eq!(rec.offset, idx as i64);
        assert_eq!(rec.key, format!("key-{idx}").into_bytes());
        assert_eq!(rec.value, format!("val-{idx}").into_bytes());
        assert_eq!(rec.timestamp_ms, idx as i64 * 10);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Exactly at segment_max_records boundary: next append rolls the segment
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn segment_rolls_when_max_records_reached() {
    let temp = TempDir::new("roll-boundary");
    const MAX: usize = 5;
    let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, MAX)).expect("open");

    // Append exactly MAX records → fills the first segment
    for i in 0..MAX {
        log.append(vec![i as u8], vec![], i as i64)
            .expect("append to first segment");
    }

    let snaps_before = log.segment_snapshots().len();

    // One more append → must roll to a new segment
    log.append(b"new-key".to_vec(), vec![], MAX as i64)
        .expect("append to new segment");

    let snaps_after = log.segment_snapshots().len();
    assert_eq!(
        snaps_after,
        snaps_before + 1,
        "segment count must increase by 1 after roll"
    );

    // All records must still be readable
    let records = log
        .fetch_from(0, MAX + 1)
        .expect("fetch across roll boundary");
    assert_eq!(records.len(), MAX + 1);
    assert_eq!(records[MAX].offset, MAX as i64);
}

// ──────────────────────────────────────────────────────────────────────────────
// Timestamp values: zero, large positive, and i64::MAX / 2
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn extreme_timestamp_values_survive_roundtrip() {
    let temp = TempDir::new("extreme-timestamps");

    let timestamps = [0i64, 1, 1_000_000_000_000i64, i64::MAX / 2];

    {
        let mut log = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("open");
        for &ts in &timestamps {
            log.append(ts.to_be_bytes().to_vec(), vec![1], ts)
                .expect("append with extreme timestamp");
        }
    }

    let log2 = PersistentSegmentLog::open(temp.path(), cfg(0, 8)).expect("reopen");
    let records = log2
        .fetch_from(0, timestamps.len())
        .expect("fetch timestamps");
    assert_eq!(records.len(), timestamps.len());
    for (idx, &ts) in timestamps.iter().enumerate() {
        assert_eq!(
            records[idx].timestamp_ms, ts,
            "timestamp {ts} at index {idx} must survive roundtrip"
        );
    }
}
