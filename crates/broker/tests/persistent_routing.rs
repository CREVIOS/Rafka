use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{PartitionedBroker, PartitionedBrokerError, PersistentLogConfig, StorageError};

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
            "rafka-broker-{label}-{millis}-{}-{counter}",
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
        segment_max_records: 4,
        sync_on_append: false,
    }
}

#[test]
fn routes_produce_fetch_by_topic_partition() {
    let temp = TempDir::new("routes-by-topic-partition");
    let mut broker =
        PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");

    let a0 = broker
        .produce_to("topic-a", 0, b"ka".to_vec(), b"va0".to_vec(), 100)
        .expect("produce topic-a partition 0");
    let a1 = broker
        .produce_to("topic-a", 1, b"kb".to_vec(), b"va1".to_vec(), 101)
        .expect("produce topic-a partition 1");
    let b0 = broker
        .produce_to("topic-b", 0, b"kc".to_vec(), b"vb0".to_vec(), 102)
        .expect("produce topic-b partition 0");

    assert_eq!(a0, 0);
    assert_eq!(a1, 0);
    assert_eq!(b0, 0);

    let topic_a_p0 = broker
        .fetch_from_partition("topic-a", 0, 0, 10)
        .expect("fetch topic-a partition 0");
    let topic_a_p1 = broker
        .fetch_from_partition("topic-a", 1, 0, 10)
        .expect("fetch topic-a partition 1");
    let topic_b_p0 = broker
        .fetch_from_partition("topic-b", 0, 0, 10)
        .expect("fetch topic-b partition 0");

    assert_eq!(topic_a_p0.len(), 1);
    assert_eq!(topic_a_p1.len(), 1);
    assert_eq!(topic_b_p0.len(), 1);

    assert_eq!(topic_a_p0[0].value, b"va0".to_vec());
    assert_eq!(topic_a_p1[0].value, b"va1".to_vec());
    assert_eq!(topic_b_p0[0].value, b"vb0".to_vec());
}

#[test]
fn unknown_partition_returns_explicit_error() {
    let temp = TempDir::new("unknown-partition");
    let mut broker =
        PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");

    let err = broker
        .fetch_from_partition("topic-missing", 7, 0, 10)
        .expect_err("unknown partition should fail");
    assert_eq!(
        err,
        PartitionedBrokerError::UnknownPartition {
            topic: "topic-missing".to_string(),
            partition: 7,
        }
    );
}

#[test]
fn recovers_existing_partition_logs_after_restart() {
    let temp = TempDir::new("recovery-after-restart");

    {
        let mut broker =
            PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");
        for i in 0..10 {
            broker
                .produce_to(
                    "orders",
                    0,
                    format!("k-{i}").into_bytes(),
                    format!("v-{i}").into_bytes(),
                    10_000 + i,
                )
                .expect("produce to orders-0");
        }
        for i in 0..5 {
            broker
                .produce_to(
                    "orders",
                    1,
                    format!("k1-{i}").into_bytes(),
                    format!("v1-{i}").into_bytes(),
                    20_000 + i,
                )
                .expect("produce to orders-1");
        }
    }

    let mut reopened =
        PartitionedBroker::open(temp.path(), log_config()).expect("reopen partitioned broker");

    let p0 = reopened
        .fetch_from_partition("orders", 0, 0, 20)
        .expect("fetch recovered orders-0");
    let p1 = reopened
        .fetch_from_partition("orders", 1, 0, 20)
        .expect("fetch recovered orders-1");

    assert_eq!(p0.len(), 10);
    assert_eq!(p1.len(), 5);
    assert_eq!(p0[0].offset, 0);
    assert_eq!(p0[9].offset, 9);
    assert_eq!(p1[0].offset, 0);
    assert_eq!(p1[4].offset, 4);

    let next = reopened
        .produce_to(
            "orders",
            0,
            b"tail".to_vec(),
            b"tail-value".to_vec(),
            30_000,
        )
        .expect("append after restart");
    assert_eq!(next, 10);
}

#[test]
fn stress_multi_partition_produce_fetch() {
    let temp = TempDir::new("stress-multi-partition");
    let mut broker =
        PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");

    let partitions = [0, 1, 2, 3];
    let total_per_partition = 1_000;

    for i in 0..total_per_partition {
        for partition in partitions {
            broker
                .produce_to(
                    "telemetry",
                    partition,
                    format!("k-{partition}-{i}").into_bytes(),
                    format!("v-{partition}-{i}").into_bytes(),
                    i64::from(i),
                )
                .expect("produce stress record");
        }
    }

    for partition in partitions {
        let records = broker
            .fetch_from_partition("telemetry", partition, 0, usize::MAX)
            .expect("fetch stress partition records");
        assert_eq!(records.len(), total_per_partition as usize);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[999].offset, 999);
        assert_eq!(
            records[999].value,
            format!("v-{partition}-999").into_bytes()
        );
    }
}

#[test]
fn stress_high_volume_multi_partition_survives_restart() {
    let temp = TempDir::new("stress-high-volume-restart");
    let high_volume_config = PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 1_024,
        sync_on_append: false,
    };
    let mut broker = PartitionedBroker::open(temp.path(), high_volume_config.clone())
        .expect("open partitioned broker");

    let partitions = [0, 1, 2, 3];
    let total_per_partition = 5_000;

    for i in 0..total_per_partition {
        for partition in partitions {
            broker
                .produce_to(
                    "telemetry-restart",
                    partition,
                    Vec::new(),
                    format!("payload-{partition}-{i}").into_bytes(),
                    i64::from(i),
                )
                .expect("produce high-volume record");
        }
    }

    let mut reopened = PartitionedBroker::open(temp.path(), high_volume_config)
        .expect("reopen partitioned broker");
    for partition in partitions {
        let tail = reopened
            .fetch_from_partition("telemetry-restart", partition, 4_990, 20)
            .expect("fetch tail from reopened broker");
        assert_eq!(tail.len(), 10);
        assert_eq!(tail[0].offset, 4_990);
        assert_eq!(tail[9].offset, 4_999);
        assert_eq!(
            tail[9].value,
            format!("payload-{partition}-4999").into_bytes()
        );
    }
}

#[test]
#[ignore = "long-running million-scale stress test"]
fn stress_one_million_records_across_partitions() {
    let temp = TempDir::new("stress-one-million");
    let scale_config = PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 8_192,
        sync_on_append: false,
    };
    let mut broker = PartitionedBroker::open(temp.path(), scale_config.clone())
        .expect("open partitioned broker");

    let partitions: [i32; 8] = [0, 1, 2, 3, 4, 5, 6, 7];
    let per_partition = 125_000_i64;

    for partition in partitions {
        for i in 0..per_partition {
            let offset = broker
                .produce_to("scale-million", partition, Vec::new(), Vec::new(), i)
                .expect("produce million-scale record");
            if i == 0 || i == per_partition - 1 {
                assert_eq!(offset, i);
            }
        }
    }

    let mut reopened =
        PartitionedBroker::open(temp.path(), scale_config).expect("reopen partitioned broker");
    for partition in partitions {
        let tail = reopened
            .fetch_from_partition("scale-million", partition, per_partition - 3, 10)
            .expect("fetch million-scale tail");
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[0].offset, per_partition - 3);
        assert_eq!(tail[2].offset, per_partition - 1);
    }
}

#[test]
fn invalid_partition_or_topic_are_rejected() {
    let temp = TempDir::new("invalid-topic-partition");
    let mut broker =
        PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");

    let invalid_partition = broker
        .produce_to("valid", -1, b"k".to_vec(), b"v".to_vec(), 0)
        .expect_err("negative partition should fail");
    assert_eq!(
        invalid_partition,
        PartitionedBrokerError::InvalidPartition { partition: -1 }
    );

    let invalid_topic = broker
        .produce_to("", 0, b"k".to_vec(), b"v".to_vec(), 0)
        .expect_err("empty topic should fail");
    assert_eq!(
        invalid_topic,
        PartitionedBrokerError::InvalidTopic {
            topic: "".to_string()
        }
    );
}

#[test]
fn partition_offset_out_of_range_is_preserved() {
    let temp = TempDir::new("partition-offset-out-of-range");
    let mut broker =
        PartitionedBroker::open(temp.path(), log_config()).expect("open partitioned broker");

    broker
        .produce_to("events", 0, b"a".to_vec(), b"1".to_vec(), 0)
        .expect("produce first record");

    let err = broker
        .fetch_from_partition("events", 0, 2, 10)
        .expect_err("offset greater than next offset should fail");

    assert_eq!(
        err,
        PartitionedBrokerError::Storage(StorageError::OffsetOutOfRange {
            requested: 2,
            earliest: 0,
            latest: 0,
        })
    );
}
