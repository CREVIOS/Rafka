use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_storage::{PersistentLogConfig, PersistentSegmentLog, StorageError};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const INDEX_ENTRY_BYTES: usize = 16;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-storage-edge-{label}-{millis}-{}-{counter}",
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

fn config(base_offset: i64, segment_max_records: usize) -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset,
        segment_max_records,
        sync_on_append: false,
    }
}

fn filename_prefix(base_offset: i64) -> String {
    format!("{base_offset:020}")
}

fn find_single_file_with_suffix(dir: &Path, suffix: &str) -> PathBuf {
    let mut matches = fs::read_dir(dir)
        .expect("read_dir")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect::<Vec<_>>();
    matches.sort();
    assert_eq!(matches.len(), 1, "expected exactly one file with {suffix}");
    matches.pop().expect("single match should exist")
}

fn read_index_entries(path: &Path) -> Vec<(i64, u64)> {
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(path)
        .expect("open index")
        .read_to_end(&mut bytes)
        .expect("read index bytes");

    assert_eq!(bytes.len() % INDEX_ENTRY_BYTES, 0);
    bytes
        .chunks_exact(INDEX_ENTRY_BYTES)
        .map(|chunk| {
            let offset = i64::from_be_bytes(chunk[0..8].try_into().expect("offset bytes"));
            let position = u64::from_be_bytes(chunk[8..16].try_into().expect("position bytes"));
            (offset, position)
        })
        .collect()
}

#[test]
fn open_rejects_invalid_configuration() {
    let temp = TempDir::new("invalid-config");

    let negative_base =
        PersistentSegmentLog::open(temp.path(), config(-1, 1)).expect_err("negative base offset");
    assert_eq!(
        negative_base,
        StorageError::InvalidConfiguration {
            message: "base_offset must be >= 0".to_string(),
        }
    );

    let zero_segment =
        PersistentSegmentLog::open(temp.path(), config(0, 0)).expect_err("zero segment size");
    assert_eq!(
        zero_segment,
        StorageError::InvalidConfiguration {
            message: "segment_max_records must be > 0".to_string(),
        }
    );
}

#[test]
fn creates_kafka_style_padded_segment_filenames() {
    let temp = TempDir::new("padded-filenames");
    let base_offset = 42;

    let _log = PersistentSegmentLog::open(temp.path(), config(base_offset, 4)).expect("open log");

    let mut names = fs::read_dir(temp.path())
        .expect("read_dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .into_string()
                .expect("utf-8 file name")
        })
        .collect::<Vec<_>>();
    names.sort();

    let prefix = filename_prefix(base_offset);
    assert_eq!(
        names,
        vec![format!("{prefix}.index"), format!("{prefix}.log")]
    );
}

#[test]
fn recovery_truncates_from_middle_record_corruption() {
    let temp = TempDir::new("middle-corruption");

    {
        let mut log = PersistentSegmentLog::open(temp.path(), config(0, 16)).expect("open log");
        for i in 0..3 {
            log.append(
                format!("k-{i}").into_bytes(),
                format!("v-{i}").into_bytes(),
                1_000 + i,
            )
            .expect("append record");
        }
    }

    let log_path = find_single_file_with_suffix(temp.path(), ".log");
    let index_path = find_single_file_with_suffix(temp.path(), ".index");
    let entries = read_index_entries(&index_path);
    assert_eq!(entries.len(), 3);

    let second_record_pos = entries[1].1;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .expect("open log for corruption");
    file.seek(SeekFrom::Start(second_record_pos + 12))
        .expect("seek into second record payload");
    file.write_all(&[0xEE]).expect("flip one byte");

    let mut reopened = PersistentSegmentLog::open(temp.path(), config(0, 16)).expect("reopen log");
    assert_eq!(reopened.next_offset(), 1);

    let recovered = reopened.fetch_from(0, 10).expect("fetch recovered records");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].offset, 0);

    let appended = reopened
        .append(b"k-new".to_vec(), b"v-new".to_vec(), 2_000)
        .expect("append after truncation");
    assert_eq!(appended, 1);
}

#[test]
fn recovery_errors_when_segment_gap_exists() {
    let temp = TempDir::new("segment-gap");

    {
        let mut log = PersistentSegmentLog::open(temp.path(), config(0, 1)).expect("open log");
        for i in 0..3 {
            log.append(vec![i as u8], vec![42], i as i64)
                .expect("append record");
        }
    }

    let from_prefix = filename_prefix(1);
    let to_prefix = filename_prefix(5);

    fs::rename(
        temp.path().join(format!("{from_prefix}.log")),
        temp.path().join(format!("{to_prefix}.log")),
    )
    .expect("rename log file");
    fs::rename(
        temp.path().join(format!("{from_prefix}.index")),
        temp.path().join(format!("{to_prefix}.index")),
    )
    .expect("rename index file");

    let err = PersistentSegmentLog::open(temp.path(), config(0, 1)).expect_err("open should fail");
    assert!(
        matches!(err, StorageError::CorruptData { .. }),
        "expected corruption error, got {err:?}"
    );
}

#[test]
fn index_garbage_is_overwritten_during_recovery() {
    let temp = TempDir::new("index-garbage");

    {
        let mut log = PersistentSegmentLog::open(temp.path(), config(0, 10)).expect("open log");
        for i in 0..4 {
            log.append(
                format!("k-{i}").into_bytes(),
                format!("v-{i}").into_bytes(),
                10 + i,
            )
            .expect("append record");
        }
    }

    let index_path = find_single_file_with_suffix(temp.path(), ".index");
    let expected_len = fs::metadata(&index_path).expect("index metadata").len();

    let mut file = OpenOptions::new()
        .append(true)
        .open(&index_path)
        .expect("open index for garbage append");
    file.write_all(&[9, 8, 7, 6, 5, 4, 3, 2, 1])
        .expect("append garbage");

    let reopened = PersistentSegmentLog::open(temp.path(), config(0, 10)).expect("reopen log");
    let recovered_len = fs::metadata(&index_path)
        .expect("recovered index metadata")
        .len();
    assert_eq!(recovered_len, expected_len);

    let all = reopened
        .fetch_from(0, 100)
        .expect("fetch all after recovery");
    assert_eq!(all.len(), 4);
    assert_eq!(all[0].offset, 0);
    assert_eq!(all[3].offset, 3);
}

#[test]
fn restart_cycles_preserve_monotonic_offsets() {
    let temp = TempDir::new("restart-cycles");

    for i in 0..150_i64 {
        let mut log = PersistentSegmentLog::open(temp.path(), config(0, 8)).expect("open log");
        let offset = log
            .append(format!("k-{i}").into_bytes(), vec![1, 2, 3], i)
            .expect("append across restart cycle");
        assert_eq!(offset, i);
    }

    let reopened = PersistentSegmentLog::open(temp.path(), config(0, 8)).expect("reopen final log");
    assert_eq!(reopened.next_offset(), 150);

    let tail = reopened.fetch_from(145, 10).expect("fetch tail");
    assert_eq!(tail.len(), 5);
    assert_eq!(tail[0].offset, 145);
    assert_eq!(tail[4].offset, 149);
}

#[test]
fn large_payload_roundtrip_survives_recovery() {
    let temp = TempDir::new("large-payload");

    let key = vec![0xAB; 64 * 1024];
    let value = vec![0xCD; 256 * 1024];

    {
        let mut log = PersistentSegmentLog::open(temp.path(), config(0, 4)).expect("open log");
        let offset = log
            .append(key.clone(), value.clone(), 9_999)
            .expect("append large payload");
        assert_eq!(offset, 0);
    }

    let reopened = PersistentSegmentLog::open(temp.path(), config(0, 4)).expect("reopen log");
    let records = reopened.fetch_from(0, 1).expect("fetch large payload");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key.len(), key.len());
    assert_eq!(records[0].value.len(), value.len());
    assert_eq!(records[0].key, key);
    assert_eq!(records[0].value, value);
}

#[test]
fn segment_rolls_keep_offsets_contiguous_and_appendable() {
    let temp = TempDir::new("segment-roll-offset-contiguous");
    let mut log = PersistentSegmentLog::open(temp.path(), config(0, 1)).expect("open log");

    for i in 0..3_i64 {
        let offset = log
            .append(
                format!("k-{i}").into_bytes(),
                format!("v-{i}").into_bytes(),
                i,
            )
            .expect("append record with roll-per-record");
        assert_eq!(offset, i);
    }

    let snapshots = log.segment_snapshots();
    assert_eq!(snapshots.len(), 3);
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.base_offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.next_offset)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let records = log.fetch_from(0, 10).expect("fetch rolled records");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[2].offset, 2);

    drop(log);
    let mut reopened = PersistentSegmentLog::open(temp.path(), config(0, 1)).expect("reopen log");
    let next = reopened
        .append(b"k-3".to_vec(), b"v-3".to_vec(), 3)
        .expect("append after reopen");
    assert_eq!(next, 3);
}
