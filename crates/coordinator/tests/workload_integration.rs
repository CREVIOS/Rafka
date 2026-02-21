#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_coordinator::{
    OffsetCommitInput, OffsetCoordinator, OffsetCoordinatorError, OffsetStoreConfig, OffsetTopic,
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
            "rafka-coordinator-workload-{label}-{millis}-{}-{counter}",
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

fn commit_offset(
    coordinator: &mut OffsetCoordinator,
    input: OffsetCommitInput<'_>,
) -> Result<(), OffsetCoordinatorError> {
    coordinator.commit_offset(input)
}

#[test]
fn rebalance_member_churn_and_epoch_gates_match_kafka_semantics() {
    let temp = TempDir::new("rebalance-member-churn");
    let mut coordinator =
        OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");

    let first_join = coordinator
        .join_group("payments-cg", "member-a", 0, 1_000)
        .expect("join member-a");
    assert_eq!(first_join.generation, 1);

    commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-a",
            member_epoch: 1,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 100,
            committed_leader_epoch: -1,
            metadata: Some("initial".to_string()),
            commit_timestamp_ms: 10,
        },
    )
    .expect("initial commit");

    let second_join = coordinator
        .join_group("payments-cg", "member-b", 10, 1_000)
        .expect("join member-b");
    assert_eq!(second_join.generation, 2);

    let stale_err = commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-a",
            member_epoch: 1,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 120,
            committed_leader_epoch: -1,
            metadata: None,
            commit_timestamp_ms: 20,
        },
    )
    .expect_err("stale epoch should fail after rebalance");
    assert_eq!(stale_err, OffsetCoordinatorError::StaleMemberEpoch);

    commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-a",
            member_epoch: 2,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 130,
            committed_leader_epoch: -1,
            metadata: None,
            commit_timestamp_ms: 30,
        },
    )
    .expect("commit with current generation");

    coordinator
        .heartbeat_group("payments-cg", "member-b", 600)
        .expect("heartbeat member-b");
    let evicted = coordinator
        .evict_expired_members("payments-cg", 1_200)
        .expect("evict expired");
    assert_eq!(evicted, vec!["member-a".to_string()]);

    let unknown_member_err = commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-a",
            member_epoch: 2,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 140,
            committed_leader_epoch: -1,
            metadata: None,
            commit_timestamp_ms: 40,
        },
    )
    .expect_err("evicted member should not commit");
    assert_eq!(
        unknown_member_err,
        OffsetCoordinatorError::UnknownMemberId("member-a".to_string())
    );

    let stale_b_err = commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-b",
            member_epoch: 2,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 145,
            committed_leader_epoch: -1,
            metadata: None,
            commit_timestamp_ms: 50,
        },
    )
    .expect_err("member-b should need the post-eviction generation");
    assert_eq!(stale_b_err, OffsetCoordinatorError::StaleMemberEpoch);

    commit_offset(
        &mut coordinator,
        OffsetCommitInput {
            api_version: 9,
            group_id: "payments-cg",
            member_id: "member-b",
            member_epoch: 3,
            topic: OffsetTopic::by_name("payments"),
            partition: 0,
            committed_offset: 150,
            committed_leader_epoch: -1,
            metadata: Some("steady-state".to_string()),
            commit_timestamp_ms: 60,
        },
    )
    .expect("commit with refreshed generation");

    let fetched = coordinator
        .fetch_offset(
            "payments-cg",
            Some("member-b"),
            Some(3),
            &OffsetTopic::by_name("payments"),
            0,
        )
        .expect("member fetch")
        .expect("offset should exist");
    assert_eq!(fetched.committed_offset, 150);
    assert_eq!(fetched.metadata.as_deref(), Some("steady-state"));
}

#[test]
fn lag_catch_up_progress_is_monotonic_and_survives_restart() {
    let temp = TempDir::new("lag-catch-up");
    {
        let mut coordinator = OffsetCoordinator::open(
            temp.path(),
            OffsetStoreConfig {
                sync_on_commit: true,
            },
        )
        .expect("open");

        let join = coordinator
            .join_group("analytics-cg", "consumer-1", 0, 10_000)
            .expect("join");
        assert_eq!(join.generation, 1);

        let progress = [0_i64, 40, 250, 2_000, 9_500];
        for (step, offset) in progress.into_iter().enumerate() {
            commit_offset(
                &mut coordinator,
                OffsetCommitInput {
                    api_version: 9,
                    group_id: "analytics-cg",
                    member_id: "consumer-1",
                    member_epoch: 1,
                    topic: OffsetTopic::by_name("events"),
                    partition: 3,
                    committed_offset: offset,
                    committed_leader_epoch: -1,
                    metadata: Some(format!("lag-step-{step}")),
                    commit_timestamp_ms: i64::try_from(step).expect("step fits i64"),
                },
            )
            .expect("commit progress step");

            let fetched = coordinator
                .fetch_offset(
                    "analytics-cg",
                    None,
                    None,
                    &OffsetTopic::by_name("events"),
                    3,
                )
                .expect("fetch progress")
                .expect("progress offset present");
            assert_eq!(fetched.committed_offset, offset);
        }
    }

    let reopened =
        OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("reopen");
    let recovered = reopened
        .fetch_offset(
            "analytics-cg",
            None,
            None,
            &OffsetTopic::by_name("events"),
            3,
        )
        .expect("fetch recovered")
        .expect("recovered offset present");
    assert_eq!(recovered.committed_offset, 9_500);
    assert_eq!(recovered.metadata.as_deref(), Some("lag-step-4"));
}

#[test]
fn commit_storm_with_member_churn_recovers_latest_offsets() {
    let temp = TempDir::new("commit-storm-member-churn");
    let mut coordinator =
        OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");

    const PARTITIONS: usize = 64;
    const CHURN_ROUNDS: usize = 30;
    const COMMITS_PER_ROUND: usize = 300;
    let mut expected = vec![-1_i64; PARTITIONS];
    let mut now_ms = 0_u64;

    for round in 0..CHURN_ROUNDS {
        if round > 0 {
            now_ms = now_ms.saturating_add(35);
            let expired = coordinator
                .evict_expired_members("storm-cg", now_ms)
                .expect("evict previous generation");
            assert!(
                !expired.is_empty(),
                "a previous member should expire each churn round"
            );
        }

        let member_id = format!("member-{round}");
        let join = coordinator
            .join_group("storm-cg", &member_id, now_ms, 25)
            .expect("join current member");
        let epoch = i32::try_from(join.generation).expect("generation fits i32");

        if round > 0 {
            let old_member = format!("member-{}", round - 1);
            let old_err = commit_offset(
                &mut coordinator,
                OffsetCommitInput {
                    api_version: 9,
                    group_id: "storm-cg",
                    member_id: &old_member,
                    member_epoch: epoch,
                    topic: OffsetTopic::by_name("orders"),
                    partition: 0,
                    committed_offset: 0,
                    committed_leader_epoch: -1,
                    metadata: None,
                    commit_timestamp_ms: i64::try_from(now_ms).expect("timestamp fits i64"),
                },
            )
            .expect_err("evicted member should be fenced");
            assert_eq!(old_err, OffsetCoordinatorError::UnknownMemberId(old_member));
        }

        for i in 0..COMMITS_PER_ROUND {
            let partition = i32::try_from((round * 17 + i) % PARTITIONS).expect("partition fits");
            let offset = (i64::try_from(round).expect("round fits i64") * 1_000_000)
                + i64::try_from(i).expect("i fits i64");
            commit_offset(
                &mut coordinator,
                OffsetCommitInput {
                    api_version: 9,
                    group_id: "storm-cg",
                    member_id: &member_id,
                    member_epoch: epoch,
                    topic: OffsetTopic::by_name("orders"),
                    partition,
                    committed_offset: offset,
                    committed_leader_epoch: -1,
                    metadata: None,
                    commit_timestamp_ms: i64::try_from(now_ms).expect("timestamp fits i64"),
                },
            )
            .expect("storm commit");
            expected[usize::try_from(partition).expect("partition index fits usize")] = offset;
            if i % 60 == 0 {
                coordinator
                    .heartbeat_group("storm-cg", &member_id, now_ms)
                    .expect("heartbeat active member");
            }
            now_ms = now_ms.saturating_add(1);
        }
    }

    let entries = coordinator
        .fetch_group_offsets("storm-cg", None, None)
        .expect("fetch full storm group");
    let expected_non_empty = expected.iter().filter(|offset| **offset >= 0).count();
    assert_eq!(entries.len(), expected_non_empty);
    for entry in &entries {
        let idx = usize::try_from(entry.partition).expect("partition fits usize");
        assert_eq!(entry.value.committed_offset, expected[idx]);
    }

    let reopened =
        OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("reopen");
    for (partition, offset) in expected.into_iter().enumerate() {
        if offset < 0 {
            continue;
        }
        let fetched = reopened
            .fetch_offset(
                "storm-cg",
                None,
                None,
                &OffsetTopic::by_name("orders"),
                i32::try_from(partition).expect("partition fits i32"),
            )
            .expect("fetch persisted storm offset")
            .expect("persisted offset present");
        assert_eq!(fetched.committed_offset, offset);
    }
}

#[test]
#[ignore = "long-running million-commit stress test"]
fn stress_million_offset_commits_survive_restart() {
    let temp = TempDir::new("stress-million-offset-commits");
    const PARTITIONS: usize = 128;
    const TOTAL_COMMITS: usize = 1_000_000;
    let mut expected = BTreeMap::<i32, i64>::new();

    {
        let mut coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");
        for i in 0..TOTAL_COMMITS {
            let partition = i32::try_from(i % PARTITIONS).expect("partition fits i32");
            let offset = i64::try_from(i).expect("offset fits i64");
            commit_offset(
                &mut coordinator,
                OffsetCommitInput {
                    api_version: 10,
                    group_id: "stress-cg",
                    member_id: "",
                    member_epoch: -1,
                    topic: OffsetTopic::by_id([0x44_u8; 16]),
                    partition,
                    committed_offset: offset,
                    committed_leader_epoch: -1,
                    metadata: None,
                    commit_timestamp_ms: offset,
                },
            )
            .expect("stress commit");
            expected.insert(partition, offset);
        }
    }

    let reopened =
        OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("reopen");
    for (partition, offset) in expected {
        let fetched = reopened
            .fetch_offset(
                "stress-cg",
                None,
                None,
                &OffsetTopic::by_id([0x44_u8; 16]),
                partition,
            )
            .expect("fetch stress offset")
            .expect("stress offset exists");
        assert_eq!(fetched.committed_offset, offset);
    }
}
