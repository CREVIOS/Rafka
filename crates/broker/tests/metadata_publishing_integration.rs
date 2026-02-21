use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{
    KraftError, KraftMetadataQuorum, PersistentLogConfig, ReplicationCluster, ReplicationConfig,
    ReplicationControlPlane, ReplicationControlPlaneError, ReplicationError,
};

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
            "rafka-metadata-publish-{label}-{millis}-{}-{counter}",
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
        segment_max_records: 64,
        sync_on_append: false,
    }
}

fn create_control_plane(temp: &TempDir, max_lag_records: i64) -> ReplicationControlPlane {
    let mut cluster = ReplicationCluster::new(ReplicationConfig { max_lag_records });
    for node_id in [1_i32, 2, 3] {
        let dir = temp.path().join(format!("node-{node_id}"));
        cluster
            .add_node(node_id, dir, log_config())
            .expect("add node to replication cluster");
    }

    let quorum = KraftMetadataQuorum::new(vec![1, 2, 3], 1).expect("create metadata quorum");
    ReplicationControlPlane::new(cluster, quorum)
}

fn tick_until_isr_size(
    control: &mut ReplicationControlPlane,
    topic: &str,
    partition: i32,
    target_isr_size: usize,
    max_ticks: usize,
) {
    for _ in 0..max_ticks {
        let _ = control
            .tick_replication(topic, partition, 8_192)
            .expect("tick replication");
        let state = control
            .partition_state(topic, partition)
            .expect("partition state");
        if state.isr.len() == target_isr_size {
            return;
        }
    }
    panic!("timed out waiting for ISR size {target_isr_size}");
}

fn assert_quorum_partition_matches_cluster(
    control: &ReplicationControlPlane,
    topic: &str,
    partition: i32,
) {
    let cluster_state = control
        .partition_state(topic, partition)
        .expect("cluster partition state");
    let topic_metadata = control
        .quorum()
        .image()
        .topics
        .get(topic)
        .expect("topic metadata in quorum image");
    let partition_metadata = topic_metadata
        .partitions
        .get(&partition)
        .expect("partition metadata in quorum image");

    assert_eq!(partition_metadata.leader_id, cluster_state.leader_id);
    assert_eq!(partition_metadata.leader_epoch, cluster_state.leader_epoch);
    assert_eq!(partition_metadata.replicas, cluster_state.replicas);
    assert_eq!(partition_metadata.isr, cluster_state.isr);
}

#[test]
fn metadata_publishing_tracks_isr_and_epoch_changes() {
    let temp = TempDir::new("tracks-isr-and-epoch");
    let mut control = create_control_plane(&temp, 0);
    control
        .create_partition("orders-meta", 0, 1, vec![1, 2, 3])
        .expect("create partition");

    assert_quorum_partition_matches_cluster(&control, "orders-meta", 0);

    for i in 0..80_i64 {
        control
            .produce(
                "orders-meta",
                0,
                Vec::new(),
                format!("before-offline-{i}").into_bytes(),
                i,
            )
            .expect("produce before taking follower offline");
        if i % 20 == 0 {
            let _ = control.tick_all(8_192).expect("tick all");
        }
    }
    tick_until_isr_size(&mut control, "orders-meta", 0, 3, 256);
    assert_quorum_partition_matches_cluster(&control, "orders-meta", 0);

    control
        .set_node_online(3, false)
        .expect("take follower 3 offline");
    for i in 0..24_i64 {
        control
            .produce(
                "orders-meta",
                0,
                Vec::new(),
                format!("offline-phase-{i}").into_bytes(),
                10_000 + i,
            )
            .expect("produce while follower offline");
    }
    tick_until_isr_size(&mut control, "orders-meta", 0, 2, 256);

    let state_after_offline = control
        .partition_state("orders-meta", 0)
        .expect("state after follower offline");
    assert_eq!(state_after_offline.isr, vec![1, 2]);
    assert_quorum_partition_matches_cluster(&control, "orders-meta", 0);

    control
        .failover("orders-meta", 0, 2)
        .expect("fail over to node 2");
    let post_failover = control
        .partition_state("orders-meta", 0)
        .expect("post failover state");
    assert_eq!(post_failover.leader_id, 2);
    assert_eq!(post_failover.leader_epoch, 1);
    assert_quorum_partition_matches_cluster(&control, "orders-meta", 0);

    control
        .set_node_online(3, true)
        .expect("bring follower 3 online");
    tick_until_isr_size(&mut control, "orders-meta", 0, 3, 512);
    assert_quorum_partition_matches_cluster(&control, "orders-meta", 0);

    let node_one_next = control
        .node_next_offset(1, "orders-meta", 0)
        .expect("node 1 next offset");
    let node_two_next = control
        .node_next_offset(2, "orders-meta", 0)
        .expect("node 2 next offset");
    let node_three_next = control
        .node_next_offset(3, "orders-meta", 0)
        .expect("node 3 next offset");
    assert_eq!(node_one_next, node_two_next);
    assert_eq!(node_two_next, node_three_next);
}

#[test]
fn metadata_publishing_rejects_partition_creation_without_quorum_majority() {
    let temp = TempDir::new("rejects-without-majority");
    let mut control = create_control_plane(&temp, 0);
    control
        .quorum_mut()
        .set_controller_online(2, false)
        .expect("set controller 2 offline");
    control
        .quorum_mut()
        .set_controller_online(3, false)
        .expect("set controller 3 offline");

    let err = control
        .create_partition("blocked", 0, 1, vec![1, 2, 3])
        .expect_err("partition creation should fail without quorum majority");
    assert!(matches!(
        err,
        ReplicationControlPlaneError::Kraft(KraftError::QuorumUnavailable { .. })
    ));

    let missing_partition_err = control
        .partition_state("blocked", 0)
        .expect_err("partition should not exist when metadata commit is blocked");
    assert!(matches!(
        missing_partition_err,
        ReplicationControlPlaneError::Replication(ReplicationError::PartitionNotFound { .. })
    ));
}

#[test]
fn realistic_multi_topic_workload_keeps_metadata_image_in_sync() {
    let temp = TempDir::new("realistic-multi-topic");
    let mut control = create_control_plane(&temp, 0);

    for partition in 0..6_i32 {
        control
            .create_partition("telemetry", partition, 1, vec![1, 2, 3])
            .expect("create telemetry partition");
    }
    for partition in 0..2_i32 {
        control
            .create_partition("payments", partition, 1, vec![1, 2, 3])
            .expect("create payments partition");
    }

    let total_records = 12_000_i64;
    for i in 0..total_records {
        if i == 2_500 {
            control
                .set_node_online(3, false)
                .expect("take follower 3 offline");
        }
        if i == 5_000 {
            tick_until_isr_size(&mut control, "telemetry", 0, 2, 256);
            control
                .failover("telemetry", 0, 2)
                .expect("fail over telemetry hot partition");
        }
        if i == 8_000 {
            control
                .set_node_online(3, true)
                .expect("bring follower 3 online");
        }

        let (topic, partition) = if i % 7 == 0 {
            (
                "payments",
                i32::try_from(i % 2).expect("payments partition in range"),
            )
        } else if i % 10 < 8 {
            ("telemetry", 0)
        } else {
            (
                "telemetry",
                i32::try_from((i % 5) + 1).expect("telemetry partition in range"),
            )
        };

        control
            .produce(
                topic,
                partition,
                Vec::new(),
                format!("topic={topic};partition={partition};seq={i}").into_bytes(),
                i,
            )
            .expect("produce workload record");

        if i % 250 == 0 {
            let _ = control.tick_all(16_384).expect("tick all replication");
        }
    }

    let keys = control.cluster().partition_keys();
    for (topic, partition) in &keys {
        tick_until_isr_size(&mut control, topic, *partition, 3, 1_024);
    }

    let metadata_changes = control
        .reconcile_metadata()
        .expect("reconcile metadata image to replication state");
    assert_eq!(metadata_changes, 0);

    for (topic, partition) in keys {
        assert_quorum_partition_matches_cluster(&control, &topic, partition);
    }
}

#[test]
#[ignore = "long-running metadata publication stress test"]
fn stress_metadata_publication_one_million_records_and_failovers() {
    let temp = TempDir::new("stress-one-million-metadata");
    let mut control = create_control_plane(&temp, 0);
    for partition in 0..12_i32 {
        control
            .create_partition("stress-stream", partition, 1, vec![1, 2, 3])
            .expect("create stress partition");
    }

    let total_records = 1_000_000_i64;
    for i in 0..total_records {
        if i == 300_000 {
            control
                .set_node_online(3, false)
                .expect("take follower 3 offline");
        }
        if i == 600_000 {
            tick_until_isr_size(&mut control, "stress-stream", 0, 2, 1_024);
            control
                .failover("stress-stream", 0, 2)
                .expect("fail over stress hot partition");
        }
        if i == 700_000 {
            control
                .set_node_online(3, true)
                .expect("bring follower 3 online");
        }

        let partition = if i % 100 < 80 {
            0
        } else {
            i32::try_from((i % 11) + 1).expect("partition in range")
        };

        control
            .produce("stress-stream", partition, Vec::new(), Vec::new(), i)
            .expect("produce stress record");

        if i % 5_000 == 0 {
            let _ = control.tick_all(32_768).expect("tick stress replication");
        }
    }

    let keys = control.cluster().partition_keys();
    for (topic, partition) in &keys {
        tick_until_isr_size(&mut control, topic, *partition, 3, 4_000);
    }

    let _ = control
        .reconcile_metadata()
        .expect("reconcile final metadata");
    for (topic, partition) in keys {
        assert_quorum_partition_matches_cluster(&control, &topic, partition);
    }

    let committed = control
        .quorum()
        .committed_offset()
        .expect("committed metadata offset");
    assert!(committed > 200);
}
