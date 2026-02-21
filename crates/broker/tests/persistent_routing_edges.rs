use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{
    PartitionedBroker, PartitionedBrokerError, PersistentLogConfig, TopicPartition,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
            "rafka-broker-edge-{label}-{millis}-{}-{counter}",
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

fn log_config() -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 8,
        sync_on_append: false,
    }
}

fn as_set(partitions: Vec<TopicPartition>) -> HashSet<(String, i32)> {
    partitions
        .into_iter()
        .map(|tp| (tp.topic, tp.partition))
        .collect()
}

#[test]
fn topic_names_with_dashes_recover_correctly() {
    let temp = TempDir::new("topic-name-with-dashes");

    {
        let mut broker = PartitionedBroker::open(temp.path(), log_config()).expect("open broker");
        broker
            .produce_to(
                "orders-eu-west",
                12,
                b"key".to_vec(),
                b"value".to_vec(),
                1_000,
            )
            .expect("produce with dashed topic");
    }

    let mut reopened = PartitionedBroker::open(temp.path(), log_config()).expect("reopen broker");
    let records = reopened
        .fetch_from_partition("orders-eu-west", 12, 0, 10)
        .expect("fetch recovered partition");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].value, b"value".to_vec());
}

#[test]
fn startup_ignores_non_partition_files_and_directories() {
    let temp = TempDir::new("ignores-non-partition-paths");

    {
        let mut broker = PartitionedBroker::open(temp.path(), log_config()).expect("open broker");
        broker
            .produce_to("events", 0, b"k".to_vec(), b"v".to_vec(), 0)
            .expect("produce valid partition");
    }

    fs::create_dir_all(temp.path().join("tmp")).expect("create tmp directory");
    fs::create_dir_all(temp.path().join("events-")).expect("create invalid suffix directory");
    fs::create_dir_all(temp.path().join("-1")).expect("create invalid name directory");
    fs::create_dir_all(temp.path().join("events--1")).expect("create negative partition directory");
    File::create(temp.path().join("README.txt")).expect("create stray file");

    let mut reopened = PartitionedBroker::open(temp.path(), log_config()).expect("reopen broker");
    let partitions = as_set(reopened.known_partitions());

    assert_eq!(partitions.len(), 1);
    assert!(partitions.contains(&("events".to_string(), 0)));

    let records = reopened
        .fetch_from_partition("events", 0, 0, 10)
        .expect("fetch valid partition after reopen");
    assert_eq!(records.len(), 1);
}

#[test]
fn known_partitions_reports_all_created_partitions() {
    let temp = TempDir::new("known-partitions");
    let mut broker = PartitionedBroker::open(temp.path(), log_config()).expect("open broker");

    broker
        .produce_to("a", 0, b"k".to_vec(), b"1".to_vec(), 0)
        .expect("produce a-0");
    broker
        .produce_to("a", 1, b"k".to_vec(), b"2".to_vec(), 0)
        .expect("produce a-1");
    broker
        .produce_to("b", 0, b"k".to_vec(), b"3".to_vec(), 0)
        .expect("produce b-0");

    let partitions = as_set(broker.known_partitions());
    let expected = HashSet::from([
        ("a".to_string(), 0),
        ("a".to_string(), 1),
        ("b".to_string(), 0),
    ]);

    assert_eq!(partitions, expected);
}

#[test]
fn repeated_restarts_keep_partition_offsets_contiguous() {
    let temp = TempDir::new("restart-contiguous-offsets");

    for i in 0..120_i64 {
        let mut broker = PartitionedBroker::open(temp.path(), log_config()).expect("open broker");
        let offset = broker
            .produce_to(
                "metrics",
                3,
                format!("k-{i}").into_bytes(),
                vec![1, 2, 3],
                i,
            )
            .expect("produce across restart");
        assert_eq!(offset, i);
    }

    let mut reopened = PartitionedBroker::open(temp.path(), log_config()).expect("reopen broker");
    let records = reopened
        .fetch_from_partition("metrics", 3, 0, usize::MAX)
        .expect("fetch contiguous partition");

    assert_eq!(records.len(), 120);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[119].offset, 119);
}

#[test]
fn fetch_from_never_created_partition_still_returns_unknown_partition() {
    let temp = TempDir::new("unknown-after-noise");

    fs::create_dir_all(temp.path().join("noise-1")).expect("create noise dir");
    let mut broker = PartitionedBroker::open(temp.path(), log_config()).expect("open broker");

    let err = broker
        .fetch_from_partition("noise", 1, 0, 10)
        .expect_err("unknown partition expected");

    assert_eq!(
        err,
        PartitionedBrokerError::UnknownPartition {
            topic: "noise".to_string(),
            partition: 1,
        }
    );
}
