#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_coordinator::{
    ClassicGroupCoordinator, ClassicGroupCoordinatorError, ClassicGroupStoreConfig, HeartbeatInput,
    JoinGroupInput, JoinProtocol, LeaveGroupMemberInput, SyncAssignment, SyncGroupInput,
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
            .expect("system time after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-classic-integration-{label}-{millis}-{}-{counter}",
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

fn join_input(version: i16, group_id: &str, member_id: &str) -> JoinGroupInput {
    JoinGroupInput {
        api_version: version,
        group_id: group_id.to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 10_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinProtocol {
            name: "consumer-range".to_string(),
            metadata: vec![1, 2, 3],
        }],
    }
}

/// Performs the two-step v4+ join (get assigned ID, then rejoin with it).
/// Returns the assigned member_id and the join generation.
fn v4_join(
    coordinator: &mut ClassicGroupCoordinator,
    group_id: &str,
    now_ms: u64,
) -> (String, i32) {
    let err = coordinator
        .join_group(join_input(4, group_id, ""), now_ms)
        .expect_err("v4 join must return MemberIdRequired on first attempt");
    let member_id = match err {
        ClassicGroupCoordinatorError::MemberIdRequired(id) => id,
        other => panic!("expected MemberIdRequired, got {other:?}"),
    };
    let outcome = coordinator
        .join_group(join_input(4, group_id, &member_id), now_ms + 1)
        .expect("rejoin with assigned member_id must succeed");
    (member_id, outcome.generation_id)
}

// ──────────────────────────────────────────────────────────────────────────────
// Basic join / sync / heartbeat / leave cycle
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn single_member_full_lifecycle_join_sync_heartbeat_leave() {
    let temp = TempDir::new("single-member-lifecycle");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    // ── join ──
    let (member_id, generation_id) = v4_join(&mut coordinator, "grp", 0);
    assert_eq!(generation_id, 1, "first member starts generation 1");

    // ── sync: leader assigns itself ──
    let synced = coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id,
            member_id: member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![SyncAssignment {
                member_id: member_id.clone(),
                assignment: vec![0x10, 0x20, 0x30],
            }],
        })
        .expect("sync");
    assert_eq!(synced.assignment, vec![0x10, 0x20, 0x30]);
    assert_eq!(synced.protocol_type.as_deref(), Some("consumer"));
    assert_eq!(synced.protocol_name.as_deref(), Some("consumer-range"));

    // ── heartbeat works after sync (no rebalance flag) ──
    coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id,
                member_id: member_id.clone(),
                group_instance_id: None,
            },
            100,
        )
        .expect("heartbeat after sync should succeed");

    // ── leave: last member removes group ──
    coordinator
        .leave_group_legacy("grp", &member_id)
        .expect("leave group");

    // After leave the group is gone; heartbeat should fail
    let hb_err = coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id,
                member_id: member_id.clone(),
                group_instance_id: None,
            },
            200,
        )
        .expect_err("heartbeat after leave must fail");
    assert_eq!(
        hb_err,
        ClassicGroupCoordinatorError::UnknownMemberId(member_id)
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Two-member rebalance: leader distributes partitions to follower
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn two_members_leader_distributes_assignments_to_follower() {
    let temp = TempDir::new("two-member-assign");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    // Leader joins first
    let (leader_id, gen1) = v4_join(&mut coordinator, "grp", 0);
    assert_eq!(gen1, 1);

    // Leader syncs alone in gen 1
    let leader_sync_1 = coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: leader_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![SyncAssignment {
                member_id: leader_id.clone(),
                assignment: vec![0, 1, 2],
            }],
        })
        .expect("leader sync gen 1");
    assert_eq!(leader_sync_1.assignment, vec![0, 1, 2]);

    // Follower joins → triggers rebalance, generation bumps to 2
    let (follower_id, gen2) = v4_join(&mut coordinator, "grp", 50);
    assert_eq!(gen2, 2, "adding a second member should bump generation");

    // Heartbeat during rebalance must signal rebalance-in-progress
    let hb_err = coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 2,
                member_id: leader_id.clone(),
                group_instance_id: None,
            },
            60,
        )
        .expect_err("heartbeat during rebalance must fail");
    assert_eq!(
        hb_err,
        ClassicGroupCoordinatorError::RebalanceInProgress,
        "should get RebalanceInProgress"
    );

    // Leader rejoins gen 2 (pending_rejoin=false after second join_group call)
    coordinator
        .join_group(join_input(4, "grp", &leader_id), 70)
        .expect("leader rejoin gen 2");

    // Leader syncs: assigns partition 0 to itself, partition 1 to follower
    let leader_sync_2 = coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 2,
            member_id: leader_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![
                SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: vec![0],
                },
                SyncAssignment {
                    member_id: follower_id.clone(),
                    assignment: vec![1],
                },
            ],
        })
        .expect("leader sync gen 2");
    assert_eq!(leader_sync_2.assignment, vec![0]);

    // Follower syncs and receives its assignment
    let follower_sync = coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 2,
            member_id: follower_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: Vec::new(), // follower never provides assignments
        })
        .expect("follower sync gen 2");
    assert_eq!(follower_sync.assignment, vec![1]);

    // Both members can heartbeat normally after sync clears the rebalance flag
    coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 2,
                member_id: leader_id.clone(),
                group_instance_id: None,
            },
            100,
        )
        .expect("leader heartbeat after sync");
    coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 2,
                member_id: follower_id.clone(),
                group_instance_id: None,
            },
            100,
        )
        .expect("follower heartbeat after sync");
}

// ──────────────────────────────────────────────────────────────────────────────
// Leave group triggers rebalance for remaining members
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn leave_group_bumps_generation_for_remaining_members() {
    let temp = TempDir::new("leave-rebalance");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    let (leader_id, _) = v4_join(&mut coordinator, "grp", 0);
    let (follower_id, gen2) = v4_join(&mut coordinator, "grp", 10);
    assert_eq!(gen2, 2);

    // Leader rejoins so rebalance flag clears
    coordinator
        .join_group(join_input(4, "grp", &leader_id), 15)
        .expect("leader rejoin");

    // Leader syncs to clear rebalance
    coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 2,
            member_id: leader_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![
                SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: vec![0],
                },
                SyncAssignment {
                    member_id: follower_id.clone(),
                    assignment: vec![1],
                },
            ],
        })
        .expect("sync gen 2");

    // Follower leaves
    coordinator
        .leave_group_legacy("grp", &follower_id)
        .expect("follower leave");

    // Leader's heartbeat must now see rebalance-in-progress
    let hb_err = coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 3,
                member_id: leader_id.clone(),
                group_instance_id: None,
            },
            50,
        )
        .expect_err("heartbeat must fail after leave-triggered rebalance");
    assert_eq!(hb_err, ClassicGroupCoordinatorError::RebalanceInProgress);
}

// ──────────────────────────────────────────────────────────────────────────────
// Illegal generation fences stale heartbeats
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn stale_generation_heartbeat_rejected() {
    let temp = TempDir::new("stale-generation");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    let (member_id, gen1) = v4_join(&mut coordinator, "grp", 0);
    assert_eq!(gen1, 1);

    // Sync to clear rebalance flag
    coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![SyncAssignment {
                member_id: member_id.clone(),
                assignment: vec![0],
            }],
        })
        .expect("sync");

    // Heartbeat with old generation 0 must be rejected
    let err = coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 0,
                member_id: member_id.clone(),
                group_instance_id: None,
            },
            10,
        )
        .expect_err("stale generation heartbeat must fail");
    assert_eq!(err, ClassicGroupCoordinatorError::IllegalGeneration);

    // Correct generation succeeds
    coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 1,
                member_id: member_id.clone(),
                group_instance_id: None,
            },
            10,
        )
        .expect("correct generation heartbeat");
}

// ──────────────────────────────────────────────────────────────────────────────
// Restart recovers group state: assignments and generation survive reopen
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn restart_recovers_group_state_assignments_and_generation() {
    let temp = TempDir::new("restart-recovery");

    let (member_id, generation_id) = {
        let mut coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");
        let (member_id, gen) = v4_join(&mut coordinator, "grp", 0);
        coordinator
            .sync_group(SyncGroupInput {
                api_version: 5,
                group_id: "grp".to_string(),
                generation_id: gen,
                member_id: member_id.clone(),
                group_instance_id: None,
                protocol_type: Some("consumer".to_string()),
                protocol_name: Some("consumer-range".to_string()),
                assignments: vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: vec![0xAA, 0xBB],
                }],
            })
            .expect("sync before shutdown");
        (member_id, gen)
    };

    // Reopen and verify assignment is still retrievable
    let mut reopened =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("reopen");

    let recovered_sync = reopened
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id,
            member_id: member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: Vec::new(),
        })
        .expect("sync after restart must return persisted assignment");
    assert_eq!(
        recovered_sync.assignment,
        vec![0xAA, 0xBB],
        "assignment must survive restart"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Group instance ID (static membership) fences mismatched member
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn group_instance_id_fences_mismatched_static_member() {
    let temp = TempDir::new("static-member-fence");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    // First static member joins with instance-A
    let err = coordinator
        .join_group(
            JoinGroupInput {
                group_instance_id: Some("instance-A".to_string()),
                ..join_input(4, "grp", "")
            },
            0,
        )
        .expect_err("member id required");
    let member_id = match err {
        ClassicGroupCoordinatorError::MemberIdRequired(id) => id,
        other => panic!("expected MemberIdRequired, got {other:?}"),
    };
    coordinator
        .join_group(
            JoinGroupInput {
                member_id: member_id.clone(),
                group_instance_id: Some("instance-A".to_string()),
                ..join_input(4, "grp", "")
            },
            1,
        )
        .expect("static member join");

    // Attempt to leave with a different member_id but same instance-A → FencedInstanceId
    let outcomes = coordinator
        .leave_group_members(
            "grp",
            &[LeaveGroupMemberInput {
                member_id: "impostor-member".to_string(),
                group_instance_id: Some("instance-A".to_string()),
            }],
        )
        .expect("leave_group_members returns outcomes not error");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0].error,
        Some(ClassicGroupCoordinatorError::FencedInstanceId)
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Three members: generation increments for each new joiner
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn three_member_join_sequence_increments_generation_each_time() {
    let temp = TempDir::new("three-member-gen");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    let (_, gen1) = v4_join(&mut coordinator, "grp", 0);
    assert_eq!(gen1, 1);

    let (_, gen2) = v4_join(&mut coordinator, "grp", 10);
    assert_eq!(gen2, 2);

    let (_, gen3) = v4_join(&mut coordinator, "grp", 20);
    assert_eq!(gen3, 3);
}

// ──────────────────────────────────────────────────────────────────────────────
// Sync with wrong generation is rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sync_with_wrong_generation_rejected() {
    let temp = TempDir::new("sync-wrong-gen");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    let (member_id, gen) = v4_join(&mut coordinator, "grp", 0);

    let err = coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: gen + 1, // wrong
            member_id: member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: Vec::new(),
        })
        .expect_err("sync with wrong generation must fail");
    assert_eq!(err, ClassicGroupCoordinatorError::IllegalGeneration);
}

// ──────────────────────────────────────────────────────────────────────────────
// Leave-members batch: successful removal triggers generation bump
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn leave_group_members_batch_triggers_rebalance_for_remainder() {
    let temp = TempDir::new("leave-members-batch");
    let mut coordinator =
        ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
            .expect("open");

    let (leader_id, _) = v4_join(&mut coordinator, "grp", 0);
    let (follower_a, _) = v4_join(&mut coordinator, "grp", 10);
    let (follower_b, _) = v4_join(&mut coordinator, "grp", 20);

    // Rejoin leader so pending_rejoin flags clear
    coordinator
        .join_group(join_input(4, "grp", &leader_id), 25)
        .expect("leader rejoin");

    // Leader syncs all three members
    coordinator
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: 3,
            member_id: leader_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("consumer-range".to_string()),
            assignments: vec![
                SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: vec![0],
                },
                SyncAssignment {
                    member_id: follower_a.clone(),
                    assignment: vec![1],
                },
                SyncAssignment {
                    member_id: follower_b.clone(),
                    assignment: vec![2],
                },
            ],
        })
        .expect("sync all three");

    // Batch remove both followers
    let outcomes = coordinator
        .leave_group_members(
            "grp",
            &[
                LeaveGroupMemberInput {
                    member_id: follower_a.clone(),
                    group_instance_id: None,
                },
                LeaveGroupMemberInput {
                    member_id: follower_b.clone(),
                    group_instance_id: None,
                },
            ],
        )
        .expect("batch leave");

    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes[0].error.is_none(),
        "follower-a leave should succeed"
    );
    assert!(
        outcomes[1].error.is_none(),
        "follower-b leave should succeed"
    );

    // Leader should now see rebalance in progress (generation 4)
    let hb_err = coordinator
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 4,
                member_id: leader_id.clone(),
                group_instance_id: None,
            },
            100,
        )
        .expect_err("rebalance must be in progress after batch leave");
    assert_eq!(hb_err, ClassicGroupCoordinatorError::RebalanceInProgress);
}
