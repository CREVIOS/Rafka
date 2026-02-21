use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{
    PartitionedBrokerError, PersistentLogConfig, ReplicationCluster, ReplicationConfig,
    ReplicationError,
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
            "rafka-replication-{label}-{millis}-{}-{counter}",
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

fn create_three_node_cluster(temp: &TempDir, max_lag_records: i64) -> ReplicationCluster {
    let mut cluster = ReplicationCluster::new(ReplicationConfig { max_lag_records });
    for node_id in [1_i32, 2, 3] {
        let dir = temp.path().join(format!("node-{node_id}"));
        cluster
            .add_node(node_id, dir, log_config())
            .expect("add node to replication cluster");
    }
    cluster
}

fn fetch_all_values(
    cluster: &mut ReplicationCluster,
    node_id: i32,
    topic: &str,
    partition: i32,
) -> Vec<Vec<u8>> {
    match cluster.fetch_from_node(node_id, topic, partition, 0, usize::MAX) {
        Ok(records) => records.into_iter().map(|record| record.value).collect(),
        Err(ReplicationError::Storage(PartitionedBrokerError::UnknownPartition { .. })) => {
            Vec::new()
        }
        Err(err) => panic!("unexpected fetch error for node {node_id}: {err:?}"),
    }
}

fn tick_until_isr_size(
    cluster: &mut ReplicationCluster,
    topic: &str,
    partition: i32,
    target_isr_size: usize,
    max_ticks: usize,
) {
    for _ in 0..max_ticks {
        let _ = cluster
            .tick_replication(topic, partition, 4_096)
            .expect("tick replication");
        let state = cluster
            .partition_state(topic, partition)
            .expect("partition state");
        if state.isr.len() == target_isr_size {
            return;
        }
    }
    panic!("timed out waiting for ISR size {target_isr_size}");
}

fn assert_partition_replicas_equal(
    cluster: &mut ReplicationCluster,
    topic: &str,
    partition: i32,
    expected_len: usize,
) {
    let leader = fetch_all_values(cluster, 1, topic, partition);
    let follower_two = fetch_all_values(cluster, 2, topic, partition);
    let follower_three = fetch_all_values(cluster, 3, topic, partition);
    assert_eq!(leader.len(), expected_len);
    assert_eq!(follower_two, leader);
    assert_eq!(follower_three, leader);
}

#[test]
fn replication_leader_append_follower_catchup_and_isr_expand() {
    let temp = TempDir::new("leader-append-catchup");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("orders-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    for i in 0..50_i64 {
        cluster
            .produce(
                "orders-repl",
                0,
                Vec::new(),
                format!("value-{i}").into_bytes(),
                i,
            )
            .expect("produce to leader");
    }

    let state_after_produce = cluster
        .partition_state("orders-repl", 0)
        .expect("partition state after produce");
    assert_eq!(state_after_produce.isr, vec![1]);
    assert_eq!(state_after_produce.leader_next_offset, 50);

    tick_until_isr_size(&mut cluster, "orders-repl", 0, 3, 128);
    let synced_state = cluster
        .partition_state("orders-repl", 0)
        .expect("synced partition state");
    assert_eq!(synced_state.isr, vec![1, 2, 3]);

    let leader_values = fetch_all_values(&mut cluster, 1, "orders-repl", 0);
    let follower_two_values = fetch_all_values(&mut cluster, 2, "orders-repl", 0);
    let follower_three_values = fetch_all_values(&mut cluster, 3, "orders-repl", 0);
    assert_eq!(leader_values.len(), 50);
    assert_eq!(follower_two_values, leader_values);
    assert_eq!(follower_three_values, leader_values);
}

#[test]
fn replication_isr_shrinks_when_follower_offline_and_recovers_after_catchup() {
    let temp = TempDir::new("isr-shrink-recover");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("events-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    cluster
        .set_node_online(3, false)
        .expect("take follower 3 offline");
    for i in 0..25_i64 {
        cluster
            .produce(
                "events-repl",
                0,
                Vec::new(),
                format!("event-{i}").into_bytes(),
                i,
            )
            .expect("produce events");
    }
    tick_until_isr_size(&mut cluster, "events-repl", 0, 2, 64);

    let state_with_offline_follower = cluster
        .partition_state("events-repl", 0)
        .expect("state with offline follower");
    assert_eq!(state_with_offline_follower.isr, vec![1, 2]);

    cluster
        .set_node_online(3, true)
        .expect("bring follower 3 online");
    tick_until_isr_size(&mut cluster, "events-repl", 0, 3, 128);

    let recovered_state = cluster
        .partition_state("events-repl", 0)
        .expect("recovered state");
    assert_eq!(recovered_state.isr, vec![1, 2, 3]);
    assert_eq!(
        cluster
            .node_next_offset(3, "events-repl", 0)
            .expect("follower 3 next offset"),
        25
    );
}

#[test]
fn replication_failover_bumps_epoch_and_old_leader_catches_up() {
    let temp = TempDir::new("failover-catchup");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("failover-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    for i in 0..20_i64 {
        cluster
            .produce(
                "failover-repl",
                0,
                Vec::new(),
                format!("before-failover-{i}").into_bytes(),
                i,
            )
            .expect("produce before failover");
    }
    tick_until_isr_size(&mut cluster, "failover-repl", 0, 3, 128);

    cluster
        .failover("failover-repl", 0, 2)
        .expect("fail over to node 2");
    let after_failover = cluster
        .partition_state("failover-repl", 0)
        .expect("state after failover");
    assert_eq!(after_failover.leader_id, 2);
    assert_eq!(after_failover.leader_epoch, 1);

    for i in 0..15_i64 {
        cluster
            .produce(
                "failover-repl",
                0,
                Vec::new(),
                format!("after-failover-{i}").into_bytes(),
                1_000 + i,
            )
            .expect("produce after failover");
    }
    tick_until_isr_size(&mut cluster, "failover-repl", 0, 3, 128);

    let final_state = cluster
        .partition_state("failover-repl", 0)
        .expect("final state");
    assert_eq!(final_state.leader_id, 2);
    assert_eq!(final_state.leader_epoch, 1);
    assert_eq!(final_state.leader_next_offset, 35);
    assert_eq!(final_state.isr, vec![1, 2, 3]);

    let node1 = fetch_all_values(&mut cluster, 1, "failover-repl", 0);
    let node2 = fetch_all_values(&mut cluster, 2, "failover-repl", 0);
    let node3 = fetch_all_values(&mut cluster, 3, "failover-repl", 0);
    assert_eq!(node1, node2);
    assert_eq!(node2, node3);
    assert_eq!(node2.len(), 35);
}

#[test]
fn replication_large_payload_roundtrip() {
    let temp = TempDir::new("large-payload-roundtrip");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("large-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    let payload = vec![0x5a_u8; 1_024 * 1_024];
    for i in 0..8_i64 {
        cluster
            .produce("large-repl", 0, Vec::new(), payload.clone(), i)
            .expect("produce large payload");
    }
    tick_until_isr_size(&mut cluster, "large-repl", 0, 3, 128);

    for node_id in [1_i32, 2, 3] {
        let records = cluster
            .fetch_from_node(node_id, "large-repl", 0, 0, usize::MAX)
            .expect("fetch large payload records");
        assert_eq!(records.len(), 8);
        assert!(records
            .iter()
            .all(|record| record.value.len() == payload.len()));
    }
}

#[test]
fn replication_rejects_failover_to_non_isr_replica() {
    let temp = TempDir::new("failover-non-isr");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("non-isr-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    cluster
        .set_node_online(3, false)
        .expect("take follower 3 offline");
    for i in 0..10_i64 {
        cluster
            .produce("non-isr-repl", 0, Vec::new(), vec![i as u8], i)
            .expect("produce");
    }
    tick_until_isr_size(&mut cluster, "non-isr-repl", 0, 2, 64);

    let err = cluster
        .failover("non-isr-repl", 0, 3)
        .expect_err("failover to offline non-isr replica should fail");
    assert!(matches!(
        err,
        ReplicationError::FailoverTargetNotInIsr { node_id: 3 }
    ));
}

#[test]
fn workload_realtime_telemetry_multi_partition_replication() {
    let temp = TempDir::new("workload-telemetry-multi-partition");
    let mut cluster = create_three_node_cluster(&temp, 0);
    let partitions = 8_i32;

    for partition in 0..partitions {
        cluster
            .create_partition("telemetry", partition, 1, vec![1, 2, 3])
            .expect("create telemetry partition");
    }

    let total_records = 4_000_i64;
    for i in 0..total_records {
        let partition = i32::try_from(i % i64::from(partitions)).expect("partition in i32 range");
        let payload = format!("sensor={};seq={i}", i % 256).into_bytes();
        cluster
            .produce("telemetry", partition, Vec::new(), payload, i)
            .expect("produce telemetry record");
        if i % 250 == 0 {
            let _ = cluster.tick_all(4_096).expect("tick telemetry replication");
        }
    }

    for partition in 0..partitions {
        tick_until_isr_size(&mut cluster, "telemetry", partition, 3, 256);
        let expected_len = usize::try_from(total_records / i64::from(partitions))
            .expect("expected len fits usize");
        assert_partition_replicas_equal(&mut cluster, "telemetry", partition, expected_len);
    }
}

#[test]
fn workload_hot_partition_failover_and_recovery_consistency() {
    let temp = TempDir::new("workload-hot-partition-failover");
    let mut cluster = create_three_node_cluster(&temp, 0);
    let partitions = [0_i32, 1, 2, 3];
    for partition in partitions {
        cluster
            .create_partition("clickstream", partition, 1, vec![1, 2, 3])
            .expect("create clickstream partition");
    }

    let pre_failover_records = 2_000_i64;
    for i in 0..pre_failover_records {
        let partition = if i % 10 == 0 {
            i32::try_from((i / 10) % 4).expect("partition in range")
        } else {
            0
        };
        cluster
            .produce(
                "clickstream",
                partition,
                Vec::new(),
                format!("phase1-{i}-p{partition}").into_bytes(),
                i,
            )
            .expect("produce clickstream phase 1");

        if i == 1_000 {
            cluster
                .set_node_online(3, false)
                .expect("take follower 3 offline");
        }
        if i % 200 == 0 {
            let _ = cluster.tick_all(4_096).expect("tick clickstream phase 1");
        }
    }

    tick_until_isr_size(&mut cluster, "clickstream", 0, 2, 256);
    cluster
        .failover("clickstream", 0, 2)
        .expect("fail over hot partition leader to node 2");

    let post_failover_records = 600_i64;
    for i in 0..post_failover_records {
        let partition = if i % 6 == 0 { 1 } else { 0 };
        cluster
            .produce(
                "clickstream",
                partition,
                Vec::new(),
                format!("phase2-{i}-p{partition}").into_bytes(),
                10_000 + i,
            )
            .expect("produce clickstream phase 2");
        if i % 100 == 0 {
            let _ = cluster.tick_all(4_096).expect("tick clickstream phase 2");
        }
    }

    cluster
        .set_node_online(3, true)
        .expect("bring follower 3 online");
    for partition in partitions {
        tick_until_isr_size(&mut cluster, "clickstream", partition, 3, 512);
    }

    let partition_zero_state = cluster
        .partition_state("clickstream", 0)
        .expect("partition 0 state");
    assert_eq!(partition_zero_state.leader_id, 2);
    assert_eq!(partition_zero_state.leader_epoch, 1);

    for partition in partitions {
        let expected_len = match partition {
            0 => 2_350,
            1 => 150,
            2 => 50,
            3 => 50,
            _ => 0,
        };
        assert_partition_replicas_equal(&mut cluster, "clickstream", partition, expected_len);
    }
}

#[test]
#[ignore = "long-running replication stress test"]
fn stress_replication_half_million_records_three_nodes() {
    let temp = TempDir::new("stress-half-million");
    let mut cluster = create_three_node_cluster(&temp, 0);
    cluster
        .create_partition("stress-repl", 0, 1, vec![1, 2, 3])
        .expect("create replicated partition");

    let total_records = 500_000_i64;
    for i in 0..total_records {
        cluster
            .produce("stress-repl", 0, Vec::new(), Vec::new(), i)
            .expect("stress produce");
        if i % 5_000 == 0 {
            let _ = cluster.tick_all(16_384).expect("stress tick all");
        }
    }

    tick_until_isr_size(&mut cluster, "stress-repl", 0, 3, 2_000);
    for node_id in [1_i32, 2, 3] {
        assert_eq!(
            cluster
                .node_next_offset(node_id, "stress-repl", 0)
                .expect("node next offset"),
            total_records
        );
    }
}

#[test]
#[ignore = "long-running skewed hot partition replication stress test"]
fn stress_replication_two_million_hot_partition_skew() {
    let temp = TempDir::new("stress-two-million-hot-skew");
    let mut cluster = create_three_node_cluster(&temp, 0);
    for partition in [0_i32, 1, 2, 3] {
        cluster
            .create_partition("hot-skew", partition, 1, vec![1, 2, 3])
            .expect("create hot-skew partition");
    }

    let total_records = 2_000_000_i64;
    for i in 0..total_records {
        let partition = if i % 100 < 90 {
            0
        } else {
            i32::try_from((i % 3) + 1).expect("partition in range")
        };
        cluster
            .produce("hot-skew", partition, Vec::new(), Vec::new(), i)
            .expect("produce skewed stress record");
        if i % 10_000 == 0 {
            let _ = cluster.tick_all(32_768).expect("tick skewed stress");
        }
    }

    for partition in [0_i32, 1, 2, 3] {
        tick_until_isr_size(&mut cluster, "hot-skew", partition, 3, 4_000);
    }

    let partition_zero_next = cluster
        .node_next_offset(1, "hot-skew", 0)
        .expect("partition 0 leader offset");
    assert!(partition_zero_next > 1_700_000);
}
