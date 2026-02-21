//! Real-world usage patterns for Rafka coordinators.
//!
//! These tests simulate common Kafka use cases: e-commerce event pipelines,
//! exactly-once payment processing, multi-tenant consumer groups, offset
//! tracking, rolling upgrades, and more. Each test exercises the full
//! coordinator stack in a way that mirrors production workloads.

#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_coordinator::{
    ClassicGroupCoordinator, ClassicGroupStoreConfig, EndTxnInput, InitProducerInput,
    JoinGroupInput, JoinProtocol, LeaveGroupMemberInput, OffsetCommitInput, OffsetCoordinator,
    OffsetStoreConfig, OffsetTopic, SyncAssignment, SyncGroupInput, TransactionCoordinator,
    TransactionStoreConfig,
};
use rafka_storage::Record;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

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
            "rafka-rw-{label}-{millis}-{}-{counter}",
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

fn open_txn(dir: &Path) -> TransactionCoordinator {
    TransactionCoordinator::open(dir, TransactionStoreConfig::default())
        .expect("open transaction coordinator")
}

fn open_offset(dir: &Path) -> OffsetCoordinator {
    OffsetCoordinator::open(dir, OffsetStoreConfig::default()).expect("open offset coordinator")
}

fn open_group(dir: &Path) -> ClassicGroupCoordinator {
    ClassicGroupCoordinator::open(dir, ClassicGroupStoreConfig::default())
        .expect("open group coordinator")
}

fn join_input_v4(group_id: &str, member_id: &str) -> JoinGroupInput {
    JoinGroupInput {
        api_version: 4,
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinProtocol {
            name: "range".to_string(),
            metadata: vec![1],
        }],
    }
}

/// Two-step v4 join — returns (member_id, generation_id).
fn v4_join(c: &mut ClassicGroupCoordinator, group_id: &str, now_ms: u64) -> (String, i32) {
    use rafka_coordinator::ClassicGroupCoordinatorError;
    let err = c
        .join_group(join_input_v4(group_id, ""), now_ms)
        .expect_err("first v4 join must return MemberIdRequired");
    let member_id = match err {
        ClassicGroupCoordinatorError::MemberIdRequired(id) => id,
        other => panic!("expected MemberIdRequired, got {other:?}"),
    };
    let outcome = c
        .join_group(join_input_v4(group_id, &member_id), now_ms + 1)
        .expect("rejoin with assigned ID");
    (member_id, outcome.generation_id)
}

fn make_record(offset: i64) -> Record {
    Record {
        offset,
        timestamp_ms: offset * 1_000,
        key: format!("key-{offset}").into_bytes(),
        value: format!("val-{offset}").into_bytes(),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// E-commerce: orders → payments → fulfillment pipeline
//
// Simulates a producer writing orders. A consumer group reads orders,
// commits offsets as it processes them, and a second consumer group
// handles fulfillment separately. Both groups track independent offsets.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn ecommerce_order_to_payment_pipeline() {
    let temp = TempDir::new("ecommerce-pipeline");
    let mut offsets = open_offset(temp.path());

    // ── Two consumers join "order-processors" group ──
    let r1 = offsets
        .join_group("order-processors", "consumer-1", 0, 30_000)
        .expect("consumer-1 joins");
    let r2 = offsets
        .join_group("order-processors", "consumer-2", 0, 30_000)
        .expect("consumer-2 joins");
    assert_eq!(r1.generation, 1);
    assert_eq!(r2.generation, 2);
    assert_eq!(r2.leader_id, r1.leader_id, "leader is consistent");

    // ── Admin-style offset commits (member_id="", member_epoch=-1) ──
    // consumer-1 processes orders 0-4 on partition 0
    for offset in 0..5i64 {
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "order-processors",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("orders"),
                partition: 0,
                committed_offset: offset,
                committed_leader_epoch: 0,
                metadata: Some(format!("order-{offset}")),
                commit_timestamp_ms: offset * 1_000,
            })
            .expect("commit order offset");
    }

    // consumer-2 processes orders 0-2 on partition 1
    for offset in 0..3i64 {
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "order-processors",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("orders"),
                partition: 1,
                committed_offset: offset,
                committed_leader_epoch: 0,
                metadata: None,
                commit_timestamp_ms: offset * 500,
            })
            .expect("commit order partition 1 offset");
    }

    // ── Fetch offsets: consumer-1 is ahead of consumer-2 ──
    let p0_offset = offsets
        .fetch_offset(
            "order-processors",
            None,
            None,
            &OffsetTopic::by_name("orders"),
            0,
        )
        .expect("fetch p0");
    let p1_offset = offsets
        .fetch_offset(
            "order-processors",
            None,
            None,
            &OffsetTopic::by_name("orders"),
            1,
        )
        .expect("fetch p1");

    assert_eq!(p0_offset.as_ref().map(|o| o.committed_offset), Some(4));
    assert_eq!(p1_offset.as_ref().map(|o| o.committed_offset), Some(2));
    assert_eq!(
        p0_offset.as_ref().and_then(|o| o.metadata.as_deref()),
        Some("order-4")
    );

    // ── Fulfillment group tracks its own independent offsets ──
    offsets
        .join_group("fulfillment-workers", "worker-1", 100, 30_000)
        .expect("worker-1 joins fulfillment");

    offsets
        .commit_offset(OffsetCommitInput {
            api_version: 1,
            group_id: "fulfillment-workers",
            member_id: "",
            member_epoch: -1,
            topic: OffsetTopic::by_name("orders"),
            partition: 0,
            committed_offset: 2,
            committed_leader_epoch: 0,
            metadata: None,
            commit_timestamp_ms: 2_000,
        })
        .expect("commit fulfillment offset");

    let fulfillment_offset = offsets
        .fetch_offset(
            "fulfillment-workers",
            None,
            None,
            &OffsetTopic::by_name("orders"),
            0,
        )
        .expect("fetch fulfillment offset");
    assert_eq!(
        fulfillment_offset.map(|o| o.committed_offset),
        Some(2),
        "fulfillment group offset is independent of order-processors"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Exactly-once payment processing via transactions
//
// Producer processes a payment and uses an exactly-once transaction:
// 1. init_producer with transactional_id = "payment-processor"
// 2. record_produce to "payments" topic
// 3. end_txn with committed=true
// A second transaction with the same transactional_id uses bumped epoch,
// ensuring the previous producer is fenced.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn exactly_once_payment_processing() {
    let temp = TempDir::new("payment-eos");
    let mut c = open_txn(temp.path());

    // Payment producer initializes
    let p = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("payment-processor".to_string()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("payment producer init");

    // Records a payment to 3 partitions (e.g. currency-bucketed)
    for partition in 0..3i32 {
        c.record_produce(
            "payment-processor",
            "payments",
            partition,
            partition as i64 * 100,
        )
        .expect("record payment");
    }

    // Commits the transaction
    let result = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "payment-processor".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: true,
        })
        .expect("commit payment");
    assert_eq!(result.producer_id, p.producer_id);

    // Verify payments are visible across all partitions
    for partition in 0..3i32 {
        let base_offset = partition as i64 * 100;
        let records = vec![make_record(base_offset)];
        let read_result = c.read_committed("payments", partition, 0, base_offset + 1, &records);
        assert_eq!(
            read_result.visible_records.len(),
            1,
            "payment on partition {partition} must be visible after commit"
        );
    }

    // New epoch init — old producer epoch is now stale
    let p2 = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("payment-processor".to_string()),
            transaction_timeout_ms: 30_000,
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
        })
        .expect("next epoch init");
    assert_eq!(
        p2.producer_epoch,
        p.producer_epoch + 1,
        "epoch must increment"
    );

    // Attempting end_txn with old epoch fails
    c.record_produce("payment-processor", "payments", 0, 500)
        .expect("new epoch produce");
    let fence_err = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "payment-processor".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch, // OLD epoch
            committed: true,
        })
        .expect_err("old epoch must be fenced");
    assert_eq!(
        fence_err,
        rafka_coordinator::TransactionCoordinatorError::InvalidProducerEpoch
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Multi-tenant consumer group isolation
//
// Tenants "acme" and "globex" both read from the same "events" topic but
// track completely separate offsets. Neither group's commits affect the other.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_tenant_consumer_groups_have_isolated_offsets() {
    let temp = TempDir::new("multi-tenant");
    let mut offsets = open_offset(temp.path());

    // Both tenants join and commit offsets on the same topic/partition
    for (tenant, committed_offset) in [("acme", 100i64), ("globex", 55i64)] {
        offsets
            .join_group(tenant, "worker-0", 0, 30_000)
            .expect("tenant join");
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: tenant,
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("events"),
                partition: 0,
                committed_offset,
                committed_leader_epoch: 0,
                metadata: None,
                commit_timestamp_ms: committed_offset * 10,
            })
            .expect("tenant commit");
    }

    // Offsets must be independent
    let acme_off = offsets
        .fetch_offset("acme", None, None, &OffsetTopic::by_name("events"), 0)
        .expect("fetch acme")
        .map(|o| o.committed_offset);
    let globex_off = offsets
        .fetch_offset("globex", None, None, &OffsetTopic::by_name("events"), 0)
        .expect("fetch globex")
        .map(|o| o.committed_offset);

    assert_eq!(acme_off, Some(100));
    assert_eq!(globex_off, Some(55));
    assert_ne!(acme_off, globex_off, "tenant offsets must be independent");
}

// ──────────────────────────────────────────────────────────────────────────────
// Consumer group offset tracking cycle
//
// Simulates a consumer reading in batches: fetch, process, commit, repeat.
// The committed offset should always equal the last-processed offset.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn consumer_group_offset_tracking_cycle() {
    let temp = TempDir::new("offset-tracking-cycle");
    let mut offsets = open_offset(temp.path());

    offsets
        .join_group("analytics", "m1", 0, 30_000)
        .expect("join");

    // Process in 3 batches of 10
    for batch in 0..3usize {
        let batch_end = (batch + 1) * 10 - 1;
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "analytics",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("metrics"),
                partition: 0,
                committed_offset: batch_end as i64,
                committed_leader_epoch: 0,
                metadata: Some(format!("batch-{batch}")),
                commit_timestamp_ms: batch as i64 * 1_000,
            })
            .expect("commit batch");

        let fetched = offsets
            .fetch_offset("analytics", None, None, &OffsetTopic::by_name("metrics"), 0)
            .expect("fetch after commit")
            .expect("offset must exist");

        assert_eq!(
            fetched.committed_offset, batch_end as i64,
            "committed offset must reflect batch {batch} end"
        );
        assert_eq!(
            fetched.metadata.as_deref(),
            Some(format!("batch-{batch}").as_str())
        );
    }

    // Final state: offset 29 committed
    let final_off = offsets
        .fetch_offset("analytics", None, None, &OffsetTopic::by_name("metrics"), 0)
        .expect("fetch final")
        .expect("must exist");
    assert_eq!(final_off.committed_offset, 29);
}

// ──────────────────────────────────────────────────────────────────────────────
// Dead letter queue via transaction abort
//
// A consumer processes an event, encounters an error, aborts the transaction
// (simulating a DLQ write that fails), then re-reads the same offsets.
// After abort, no records should be visible from the failed transaction.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dead_letter_queue_pattern_via_transaction_abort() {
    let temp = TempDir::new("dlq-abort");
    let mut c = open_txn(temp.path());

    // Normal producer commits order-events
    let p_normal = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("order-writer".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("normal producer init");
    c.record_produce("order-writer", "order-events", 0, 0)
        .expect("produce order");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "order-writer".to_string(),
        producer_id: p_normal.producer_id,
        producer_epoch: p_normal.producer_epoch,
        committed: true,
    })
    .expect("commit normal order");

    // DLQ producer tries to write to DLQ but fails (simulated by abort)
    let p_dlq = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("dlq-writer".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("dlq producer init");
    c.record_produce("dlq-writer", "dead-letter-queue", 0, 0)
        .expect("produce to DLQ");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "dlq-writer".to_string(),
        producer_id: p_dlq.producer_id,
        producer_epoch: p_dlq.producer_epoch,
        committed: false, // abort — message not delivered to DLQ
    })
    .expect("abort DLQ write");

    // DLQ topic must show no visible records (message was not really dead-lettered)
    let dlq_records = vec![make_record(0)];
    let dlq_result = c.read_committed("dead-letter-queue", 0, 0, 1, &dlq_records);
    assert!(
        dlq_result.visible_records.is_empty(),
        "aborted DLQ write must not appear as visible"
    );
    assert_eq!(dlq_result.aborted_transactions.len(), 1);

    // Normal order-events are still visible
    let order_records = vec![make_record(0)];
    let order_result = c.read_committed("order-events", 0, 0, 1, &order_records);
    assert_eq!(
        order_result.visible_records.len(),
        1,
        "committed order record must remain visible"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Fan-out: multiple consumer groups read from the same topic
//
// Three consumer groups ("analytics", "alerts", "archive") all consume
// from "clickstream" but each tracks their own offset independently.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fan_out_multiple_consumer_groups_independent_offsets() {
    let temp = TempDir::new("fan-out");
    let mut offsets = open_offset(temp.path());

    let groups_and_offsets = [
        ("analytics", 1_000i64),
        ("alerts", 500i64),
        ("archive", 9_999i64),
    ];

    for (group, committed_off) in groups_and_offsets {
        offsets
            .join_group(group, "worker-0", 0, 30_000)
            .expect("join group");
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: group,
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("clickstream"),
                partition: 0,
                committed_offset: committed_off,
                committed_leader_epoch: 0,
                metadata: None,
                commit_timestamp_ms: committed_off,
            })
            .expect("commit offset");
    }

    // Verify each group tracks its own position
    for (group, expected) in groups_and_offsets {
        let off = offsets
            .fetch_offset(group, None, None, &OffsetTopic::by_name("clickstream"), 0)
            .expect("fetch")
            .map(|o| o.committed_offset);
        assert_eq!(
            off,
            Some(expected),
            "group {group} must be at offset {expected}"
        );
    }

    // Fetch all offsets for the "analytics" group
    let all_analytics = offsets
        .fetch_group_offsets("analytics", None, None)
        .expect("fetch group offsets");
    assert_eq!(
        all_analytics.len(),
        1,
        "analytics has 1 committed offset entry"
    );
    assert_eq!(all_analytics[0].committed_offset_value(), 1_000);
}

// ──────────────────────────────────────────────────────────────────────────────
// Rolling restart: one consumer leaves, another joins, offsets preserved
//
// Simulates a rolling upgrade where consumers are restarted one at a time.
// The committed offsets on stable partitions must be preserved through the
// membership changes.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn rolling_restart_offsets_preserved_through_membership_changes() {
    let temp = TempDir::new("rolling-restart");
    let mut offsets = open_offset(temp.path());
    let mut groups = open_group(temp.path());

    // Initial state: 2 consumers in group, each commits an offset
    let (m1, gen1) = v4_join(&mut groups, "my-group", 0);
    let (m2, _gen2) = v4_join(&mut groups, "my-group", 10);

    // Rejoin m1 so it's not in pending_rejoin state
    groups
        .join_group(join_input_v4("my-group", &m1), 15)
        .expect("m1 rejoin");

    // Leader syncs both members
    groups
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "my-group".to_string(),
            generation_id: gen1 + 1, // gen 2 after m2 joined
            member_id: m1.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![
                SyncAssignment {
                    member_id: m1.clone(),
                    assignment: vec![0],
                },
                SyncAssignment {
                    member_id: m2.clone(),
                    assignment: vec![1],
                },
            ],
        })
        .expect("sync gen 2");

    // Both consumers commit offsets
    for (member, partition, committed) in [("", 0i32, 100i64), ("", 1, 200i64)] {
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "my-group",
                member_id: member,
                member_epoch: -1,
                topic: OffsetTopic::by_name("events"),
                partition,
                committed_offset: committed,
                committed_leader_epoch: 0,
                metadata: None,
                commit_timestamp_ms: committed,
            })
            .expect("commit offset");
    }

    // Rolling restart: m2 leaves (being upgraded)
    groups
        .leave_group_members(
            "my-group",
            &[LeaveGroupMemberInput {
                member_id: m2.clone(),
                group_instance_id: None,
            }],
        )
        .expect("m2 leave");

    // Offsets must still be intact after the membership change
    let p0 = offsets
        .fetch_offset("my-group", None, None, &OffsetTopic::by_name("events"), 0)
        .expect("fetch p0 after rolling restart")
        .expect("must exist");
    let p1 = offsets
        .fetch_offset("my-group", None, None, &OffsetTopic::by_name("events"), 1)
        .expect("fetch p1 after rolling restart")
        .expect("must exist");

    assert_eq!(
        p0.committed_offset, 100,
        "p0 offset preserved through rolling restart"
    );
    assert_eq!(
        p1.committed_offset, 200,
        "p1 offset preserved through rolling restart"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Session timeout eviction triggers generation bump
//
// A consumer stops heartbeating. After the session timeout, evicting expired
// members bumps the group generation, signalling the surviving members to
// rebalance.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn session_timeout_eviction_bumps_generation() {
    let temp = TempDir::new("session-timeout");
    let mut offsets = open_offset(temp.path());

    // Two members join with a 1500ms session timeout.
    // live-member will heartbeat at t=800, so at eviction time t=2000 its
    // age is 1200ms < 1500ms (survives). zombie-member never heartbeats, so
    // at t=2000 its age is 2000ms > 1500ms (expires).
    let _r1 = offsets
        .join_group("lag-group", "live-member", 0, 1_500)
        .expect("live-member joins");
    let r2 = offsets
        .join_group("lag-group", "zombie-member", 0, 1_500)
        .expect("zombie-member joins");
    assert_eq!(r2.generation, 2);

    // live-member heartbeats at t=800ms (age at eviction: 1200ms < 1500ms)
    offsets
        .heartbeat_group("lag-group", "live-member", 800)
        .expect("live-member heartbeat");

    // zombie-member does NOT heartbeat. At t=2000ms, its age=2000 > 1500 → expired.
    let evicted = offsets
        .evict_expired_members("lag-group", 2_000)
        .expect("evict expired");
    assert_eq!(
        evicted,
        vec!["zombie-member"],
        "zombie-member must be evicted"
    );

    // live-member rejoins — generation must have bumped
    let r3 = offsets
        .join_group("lag-group", "live-member", 2_001, 1_500)
        .expect("live-member rejoin after eviction");
    assert_eq!(r3.generation, 3, "generation must increment after eviction");
}

// ──────────────────────────────────────────────────────────────────────────────
// Offset fetch by topic_id (UUID-based routing)
//
// Some Kafka APIs address topics by UUID rather than name. This test verifies
// that offsets committed under a topic_id key are correctly fetched back.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn offset_commit_and_fetch_by_topic_id() {
    let temp = TempDir::new("topic-id-offset");
    let mut offsets = open_offset(temp.path());

    let topic_id: [u8; 16] = [
        0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA,
        0xBB,
    ];

    offsets
        .join_group("id-group", "worker", 0, 30_000)
        .expect("join");

    offsets
        .commit_offset(OffsetCommitInput {
            api_version: 1,
            group_id: "id-group",
            member_id: "",
            member_epoch: -1,
            topic: OffsetTopic::by_id(topic_id),
            partition: 0,
            committed_offset: 42,
            committed_leader_epoch: 1,
            metadata: None,
            commit_timestamp_ms: 100_000,
        })
        .expect("commit by topic_id");

    let fetched = offsets
        .fetch_offset("id-group", None, None, &OffsetTopic::by_id(topic_id), 0)
        .expect("fetch by topic_id")
        .expect("must exist");

    assert_eq!(fetched.committed_offset, 42);
    assert_eq!(fetched.committed_leader_epoch, 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// Admin-style offset reset without group membership
//
// Kafka admin tools can reset consumer group offsets without being a member
// (member_id="", member_epoch=-1). This test verifies that works correctly.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn admin_style_offset_reset_without_membership() {
    let temp = TempDir::new("admin-reset");
    let mut offsets = open_offset(temp.path());

    // No join_group call — directly commit via admin API
    offsets
        .commit_offset(OffsetCommitInput {
            api_version: 1,
            group_id: "stale-group",
            member_id: "",
            member_epoch: -1,
            topic: OffsetTopic::by_name("topic-a"),
            partition: 0,
            committed_offset: 0,
            committed_leader_epoch: 0,
            metadata: Some("reset-to-beginning".to_string()),
            commit_timestamp_ms: 0,
        })
        .expect("admin reset must succeed without membership");

    let fetched = offsets
        .fetch_offset(
            "stale-group",
            None,
            None,
            &OffsetTopic::by_name("topic-a"),
            0,
        )
        .expect("fetch after admin reset")
        .expect("must exist");

    assert_eq!(fetched.committed_offset, 0);
    assert_eq!(fetched.metadata.as_deref(), Some("reset-to-beginning"));
}

// ──────────────────────────────────────────────────────────────────────────────
// Inventory management: multi-partition writes across SKUs
//
// Simulates an inventory service that shards products across 4 partitions.
// Each partition tracks the most recent inventory update offset.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn inventory_management_multi_partition_offset_tracking() {
    let temp = TempDir::new("inventory-mgmt");
    let mut offsets = open_offset(temp.path());

    offsets
        .join_group("inventory-consumers", "inv-worker", 0, 30_000)
        .expect("join");

    // 4 partitions representing product category shards
    let partition_offsets = [(0i32, 500i64), (1, 1_200), (2, 800), (3, 2_500)];

    for (partition, offset) in partition_offsets {
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "inventory-consumers",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("inventory-updates"),
                partition,
                committed_offset: offset,
                committed_leader_epoch: 0,
                metadata: Some(format!("shard-{partition}-latest")),
                commit_timestamp_ms: offset * 100,
            })
            .expect("commit inventory offset");
    }

    // Retrieve all offsets for the group at once
    let all_offsets = offsets
        .fetch_group_offsets("inventory-consumers", None, None)
        .expect("fetch all group offsets");

    assert_eq!(all_offsets.len(), 4, "must have 4 partition offset entries");

    // Each partition offset matches what was committed
    for entry in &all_offsets {
        let expected_offset = partition_offsets
            .iter()
            .find(|(p, _)| *p == entry.partition)
            .map(|(_, o)| *o)
            .expect("partition must be in expected list");
        assert_eq!(
            entry.committed_offset_value(),
            expected_offset,
            "partition {} offset must match committed value",
            entry.partition
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// High-volume audit log: commit and fetch 100 partitions
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn high_volume_audit_log_100_partitions() {
    let temp = TempDir::new("audit-log-100");
    let mut offsets = open_offset(temp.path());

    offsets
        .join_group("auditors", "audit-consumer", 0, 30_000)
        .expect("join");

    const PARTITIONS: i32 = 100;

    // Commit an offset for each of 100 partitions
    for p in 0..PARTITIONS {
        let committed = p as i64 * 1_000;
        offsets
            .commit_offset(OffsetCommitInput {
                api_version: 1,
                group_id: "auditors",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("audit-events"),
                partition: p,
                committed_offset: committed,
                committed_leader_epoch: 0,
                metadata: None,
                commit_timestamp_ms: committed,
            })
            .expect("commit audit partition offset");
    }

    // Verify random sampling of offsets
    for p in [0i32, 25, 50, 75, 99] {
        let expected = p as i64 * 1_000;
        let off = offsets
            .fetch_offset(
                "auditors",
                None,
                None,
                &OffsetTopic::by_name("audit-events"),
                p,
            )
            .expect("fetch audit offset")
            .map(|o| o.committed_offset);
        assert_eq!(
            off,
            Some(expected),
            "audit partition {p} must be at offset {expected}"
        );
    }

    // Fetch all at once — should return 100 entries
    let all = offsets
        .fetch_group_offsets("auditors", None, None)
        .expect("fetch all audit offsets");
    assert_eq!(all.len(), PARTITIONS as usize);
}

// ──────────────────────────────────────────────────────────────────────────────
// Helper accessor for OffsetEntry (avoids accessing private fields directly)
// ──────────────────────────────────────────────────────────────────────────────

trait CommittedOffsetValue {
    fn committed_offset_value(&self) -> i64;
}

impl CommittedOffsetValue for rafka_coordinator::OffsetEntry {
    fn committed_offset_value(&self) -> i64 {
        self.value.committed_offset
    }
}
