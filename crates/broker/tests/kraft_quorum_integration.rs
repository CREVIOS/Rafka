use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{KraftError, KraftMetadataQuorum, MetadataRecord};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
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
            "rafka-kraft-{label}-{millis}-{}-{counter}",
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

#[test]
fn kraft_quorum_requires_majority_for_commit() {
    let mut quorum =
        KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create 3-voter metadata quorum");

    let first = quorum
        .append_record(MetadataRecord::RegisterBroker {
            broker_id: 1,
            rack: Some("rack-a".to_string()),
        })
        .expect("commit first record with full quorum");
    assert_eq!(first, 0);

    quorum
        .set_controller_online(2, false)
        .expect("controller 2 offline");
    quorum
        .set_controller_online(3, false)
        .expect("controller 3 offline");

    let err = quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "orders".to_string(),
        })
        .expect_err("commit should fail without majority");
    assert!(matches!(
        err,
        KraftError::QuorumUnavailable {
            online_voters: 1,
            total_voters: 3
        }
    ));
    assert!(!quorum.image().topics.contains_key("orders"));

    quorum
        .set_controller_online(2, true)
        .expect("controller 2 online");
    let second = quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "orders".to_string(),
        })
        .expect("commit succeeds after majority restored");
    assert_eq!(second, 1);
    assert!(quorum.image().topics.contains_key("orders"));
}

#[test]
fn kraft_failover_preserves_metadata_and_bumps_epoch() {
    let mut quorum =
        KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create 3-voter metadata quorum");

    for broker_id in [1_i32, 2, 3] {
        quorum
            .append_record(MetadataRecord::RegisterBroker {
                broker_id,
                rack: None,
            })
            .expect("register broker");
    }
    quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "payments".to_string(),
        })
        .expect("create topic");
    quorum
        .append_record(MetadataRecord::UpsertPartition {
            topic: "payments".to_string(),
            partition: 0,
            leader_id: 1,
            leader_epoch: 0,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
        })
        .expect("upsert partition");

    assert_eq!(quorum.leader_id(), 1);
    assert_eq!(quorum.leader_epoch(), 0);

    quorum
        .elect_leader(2)
        .expect("elect controller 2 as new leader");
    assert_eq!(quorum.leader_id(), 2);
    assert_eq!(quorum.leader_epoch(), 1);

    quorum
        .append_record(MetadataRecord::UpsertPartition {
            topic: "payments".to_string(),
            partition: 0,
            leader_id: 2,
            leader_epoch: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
        })
        .expect("commit partition leadership update");

    let partition = quorum
        .image()
        .topics
        .get("payments")
        .expect("payments topic")
        .partitions
        .get(&0)
        .expect("payments-0 metadata");
    assert_eq!(partition.leader_id, 2);
    assert_eq!(partition.leader_epoch, 1);
    assert_eq!(partition.isr, vec![1, 2]);
}

#[test]
fn kraft_snapshot_roundtrip_preserves_image_and_offsets() {
    let temp = TempDir::new("snapshot-roundtrip");
    let snapshot_path = temp.path().join("metadata.snapshot");

    let mut writer =
        KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create writer metadata quorum");
    writer
        .append_record(MetadataRecord::RegisterBroker {
            broker_id: 1,
            rack: Some("rack-a".to_string()),
        })
        .expect("register broker 1");
    writer
        .append_record(MetadataRecord::CreateTopic {
            topic: "telemetry".to_string(),
        })
        .expect("create telemetry topic");
    writer
        .append_record(MetadataRecord::UpsertPartition {
            topic: "telemetry".to_string(),
            partition: 7,
            leader_id: 1,
            leader_epoch: 4,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
        })
        .expect("upsert telemetry partition");

    writer
        .write_snapshot(&snapshot_path)
        .expect("write snapshot file");
    let source_image = writer.image().clone();
    let source_committed = writer.committed_offset();
    let source_leader_id = writer.leader_id();
    let source_leader_epoch = writer.leader_epoch();

    let mut reader =
        KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create reader metadata quorum");
    reader
        .load_snapshot(&snapshot_path)
        .expect("load snapshot file");

    assert_eq!(reader.image(), &source_image);
    assert_eq!(reader.committed_offset(), source_committed);
    assert_eq!(reader.leader_id(), source_leader_id);
    assert_eq!(reader.leader_epoch(), source_leader_epoch);

    let next = reader
        .append_record(MetadataRecord::RegisterBroker {
            broker_id: 2,
            rack: Some("rack-b".to_string()),
        })
        .expect("append after snapshot load");
    let expected_next = source_committed.map_or(0, |offset| offset.saturating_add(1));
    assert_eq!(next, expected_next);
}

#[test]
fn kraft_realistic_streaming_metadata_scenario() {
    let mut quorum =
        KraftMetadataQuorum::new(vec![1, 2, 3, 4, 5], 1).expect("create 5-voter metadata quorum");

    for broker_id in 1..=5_i32 {
        quorum
            .append_record(MetadataRecord::RegisterBroker {
                broker_id,
                rack: Some(format!("rack-{}", broker_id % 2)),
            })
            .expect("register broker");
    }
    quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "iot-telemetry".to_string(),
        })
        .expect("create iot telemetry topic");
    quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "clickstream".to_string(),
        })
        .expect("create clickstream topic");

    for partition in 0..12_i32 {
        let leader_id = (partition % 5) + 1;
        let replicas = vec![
            leader_id,
            ((leader_id % 5) + 1),
            (((leader_id + 1) % 5) + 1),
        ];
        quorum
            .append_record(MetadataRecord::UpsertPartition {
                topic: "iot-telemetry".to_string(),
                partition,
                leader_id,
                leader_epoch: 0,
                replicas: replicas.clone(),
                isr: replicas,
            })
            .expect("upsert iot telemetry partition");
    }

    quorum.elect_leader(3).expect("elect controller 3");
    for partition in [2_i32, 5, 9] {
        quorum
            .append_record(MetadataRecord::UpsertPartition {
                topic: "iot-telemetry".to_string(),
                partition,
                leader_id: 3,
                leader_epoch: 1,
                replicas: vec![3, 4, 5],
                isr: vec![3, 4],
            })
            .expect("upsert post-failover partition");
    }

    assert_eq!(quorum.leader_id(), 3);
    assert_eq!(quorum.leader_epoch(), 1);
    assert_eq!(quorum.image().brokers.len(), 5);
    assert_eq!(
        quorum
            .image()
            .topics
            .get("iot-telemetry")
            .expect("iot-telemetry metadata")
            .partitions
            .len(),
        12
    );
    assert_eq!(
        quorum
            .image()
            .topics
            .get("clickstream")
            .expect("clickstream metadata")
            .partitions
            .len(),
        0
    );
}

#[test]
#[ignore = "long-running metadata update stress test"]
fn stress_kraft_metadata_hundred_thousand_partition_updates() {
    let mut quorum =
        KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create stress metadata quorum");
    quorum
        .append_record(MetadataRecord::CreateTopic {
            topic: "stress-topic".to_string(),
        })
        .expect("create stress topic");

    for i in 0..100_000_i32 {
        quorum
            .append_record(MetadataRecord::UpsertPartition {
                topic: "stress-topic".to_string(),
                partition: i % 64,
                leader_id: ((i % 3) + 1),
                leader_epoch: i / 64,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
            })
            .expect("stress metadata update");
    }

    assert_eq!(
        quorum
            .image()
            .topics
            .get("stress-topic")
            .expect("stress-topic")
            .partitions
            .len(),
        64
    );
}
