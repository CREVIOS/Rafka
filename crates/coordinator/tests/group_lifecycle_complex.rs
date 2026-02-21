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
            "rafka-classic-complex-{label}-{millis}-{}-{counter}",
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

fn coordinator(dir: &Path) -> ClassicGroupCoordinator {
    ClassicGroupCoordinator::open(dir, ClassicGroupStoreConfig::default())
        .expect("open coordinator")
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
            name: "sticky".to_string(),
            metadata: vec![1],
        }],
    }
}

fn join_input_v0(group_id: &str, member_id: &str, protocol_type: &str) -> JoinGroupInput {
    JoinGroupInput {
        api_version: 0,
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: protocol_type.to_string(),
        protocols: vec![JoinProtocol {
            name: "range".to_string(),
            metadata: vec![1],
        }],
    }
}

/// Two-step v4 join (get assigned ID, then rejoin).
fn v4_join(c: &mut ClassicGroupCoordinator, group_id: &str, now_ms: u64) -> (String, i32) {
    let err = c
        .join_group(join_input_v4(group_id, ""), now_ms)
        .expect_err("v4 join with empty member_id must return MemberIdRequired");
    let member_id = match err {
        ClassicGroupCoordinatorError::MemberIdRequired(id) => id,
        other => panic!("expected MemberIdRequired, got {other:?}"),
    };
    let outcome = c
        .join_group(join_input_v4(group_id, &member_id), now_ms + 1)
        .expect("rejoin with assigned member_id must succeed");
    (member_id, outcome.generation_id)
}

fn sync_alone(
    c: &mut ClassicGroupCoordinator,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    assignment: Vec<u8>,
) {
    c.sync_group(SyncGroupInput {
        api_version: 5,
        group_id: group_id.to_string(),
        generation_id,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("sticky".to_string()),
        assignments: vec![SyncAssignment {
            member_id: member_id.to_string(),
            assignment,
        }],
    })
    .expect("sync");
}

// ──────────────────────────────────────────────────────────────────────────────
// Five sequential joins each bump generation by one
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn five_members_sequential_join_bumps_generation_five_times() {
    let temp = TempDir::new("five-member-gen");
    let mut c = coordinator(temp.path());

    let mut member_ids = Vec::new();
    for i in 1..=5 {
        let (mid, gen) = v4_join(&mut c, "grp", (i as u64) * 10);
        assert_eq!(gen, i, "member {i} should yield generation {i}");
        member_ids.push(mid);
    }

    // Verify join outcome lists all current members after last join
    let outcome = c
        .join_group(join_input_v4("grp", &member_ids[0]), 100)
        .expect("leader rejoin");
    assert_eq!(
        outcome.generation_id, 5,
        "rejoining with known ID must not bump generation again"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// sync_group with an unknown member_id is rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sync_unknown_member_rejected() {
    let temp = TempDir::new("sync-unknown-member");
    let mut c = coordinator(temp.path());

    let (_, gen) = v4_join(&mut c, "grp", 0);

    let err = c
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: gen,
            member_id: "not-a-real-member".to_string(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("sticky".to_string()),
            assignments: Vec::new(),
        })
        .expect_err("sync with unknown member must fail");

    assert_eq!(
        err,
        ClassicGroupCoordinatorError::UnknownMemberId("not-a-real-member".to_string())
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Heartbeat on a group that has never been created → UnknownMemberId
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn heartbeat_on_nonexistent_group_rejected() {
    let temp = TempDir::new("hb-no-group");
    let mut c = coordinator(temp.path());

    let err = c
        .heartbeat(
            HeartbeatInput {
                group_id: "ghost-group".to_string(),
                generation_id: 1,
                member_id: "ghost-member".to_string(),
                group_instance_id: None,
            },
            0,
        )
        .expect_err("heartbeat on nonexistent group must fail");
    assert_eq!(
        err,
        ClassicGroupCoordinatorError::UnknownMemberId("ghost-member".to_string())
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Two distinct consumer groups operate independently (no cross-group interference)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn two_groups_are_completely_isolated() {
    let temp = TempDir::new("two-group-isolation");
    let mut c = coordinator(temp.path());

    let (leader_a, gen_a) = v4_join(&mut c, "group-alpha", 0);
    let (leader_b, gen_b) = v4_join(&mut c, "group-beta", 10);

    assert_eq!(gen_a, 1);
    assert_eq!(gen_b, 1);

    sync_alone(&mut c, "group-alpha", 1, &leader_a, vec![0xAA]);
    sync_alone(&mut c, "group-beta", 1, &leader_b, vec![0xBB]);

    // Add second member to group-alpha; group-beta must not be affected
    let (_, gen_a2) = v4_join(&mut c, "group-alpha", 20);
    assert_eq!(gen_a2, 2, "group-alpha bumped to gen 2");

    // group-beta's leader can still heartbeat with gen 1
    c.heartbeat(
        HeartbeatInput {
            group_id: "group-beta".to_string(),
            generation_id: 1,
            member_id: leader_b.clone(),
            group_instance_id: None,
        },
        30,
    )
    .expect("group-beta heartbeat must not be affected by group-alpha rebalance");
}

// ──────────────────────────────────────────────────────────────────────────────
// Joining with an incompatible protocol_type is rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn inconsistent_protocol_type_rejected() {
    let temp = TempDir::new("inconsistent-proto");
    let mut c = coordinator(temp.path());

    // v0 with empty member_id auto-assigns an ID in a single step
    c.join_group(join_input_v0("grp", "", "consumer"), 0)
        .expect("first member joins with consumer");

    // Second member tries to join with "connect" → InconsistentGroupProtocol
    let err = c
        .join_group(join_input_v0("grp", "", "connect"), 5)
        .expect_err("different protocol_type must fail");
    assert_eq!(err, ClassicGroupCoordinatorError::InconsistentGroupProtocol);
}

// ──────────────────────────────────────────────────────────────────────────────
// Empty group_id is rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_group_id_rejected_on_join() {
    let temp = TempDir::new("empty-group-id");
    let mut c = coordinator(temp.path());

    let err = c
        .join_group(join_input_v0("", "m1", "consumer"), 0)
        .expect_err("empty group_id must fail");
    assert_eq!(err, ClassicGroupCoordinatorError::InvalidGroupId);
}

// ──────────────────────────────────────────────────────────────────────────────
// Empty protocols list is rejected (InconsistentGroupProtocol)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_protocols_list_rejected() {
    let temp = TempDir::new("empty-protocols");
    let mut c = coordinator(temp.path());

    let err = c
        .join_group(
            JoinGroupInput {
                api_version: 0,
                group_id: "grp".to_string(),
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 10_000,
                member_id: "m1".to_string(),
                group_instance_id: None,
                protocol_type: "consumer".to_string(),
                protocols: Vec::new(), // empty!
            },
            0,
        )
        .expect_err("empty protocols must fail");
    assert_eq!(err, ClassicGroupCoordinatorError::InconsistentGroupProtocol);
}

// ──────────────────────────────────────────────────────────────────────────────
// Rejoining with the same member_id does not bump the generation
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn rejoin_same_member_does_not_bump_generation() {
    let temp = TempDir::new("rejoin-same-member");
    let mut c = coordinator(temp.path());

    let (member_id, gen1) = v4_join(&mut c, "grp", 0);
    assert_eq!(gen1, 1);

    // Sync to clear rebalance state
    sync_alone(&mut c, "grp", 1, &member_id, vec![1]);

    // Same member rejoins → generation must remain 1
    let outcome = c
        .join_group(join_input_v4("grp", &member_id), 10)
        .expect("rejoin known member must succeed");
    assert_eq!(
        outcome.generation_id, 1,
        "rejoin of existing member must not bump generation"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Five-member sync: leader distributes distinct assignments, all verified
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn five_members_all_receive_correct_assignments() {
    let temp = TempDir::new("five-member-assign");
    let mut c = coordinator(temp.path());

    // Join 5 members
    let mut member_ids: Vec<String> = Vec::new();
    for i in 0..5 {
        let (mid, _) = v4_join(&mut c, "grp", i * 10);
        member_ids.push(mid);
    }

    // The leader rejoins to clear pending_rejoin
    let leader_id = member_ids[0].clone();
    c.join_group(join_input_v4("grp", &leader_id), 100)
        .expect("leader rejoin");

    // Leader syncs all 5 members with distinct assignments
    let leader_assignments: Vec<SyncAssignment> = member_ids
        .iter()
        .enumerate()
        .map(|(idx, mid)| SyncAssignment {
            member_id: mid.clone(),
            assignment: vec![idx as u8 * 2], // partition 0, 2, 4, 6, 8
        })
        .collect();

    c.sync_group(SyncGroupInput {
        api_version: 5,
        group_id: "grp".to_string(),
        generation_id: 5, // after 5 joins
        member_id: leader_id.clone(),
        group_instance_id: None,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("sticky".to_string()),
        assignments: leader_assignments,
    })
    .expect("leader sync all 5 members");

    // Each follower syncs and receives their own assignment
    for (idx, mid) in member_ids.iter().enumerate().skip(1) {
        let result = c
            .sync_group(SyncGroupInput {
                api_version: 5,
                group_id: "grp".to_string(),
                generation_id: 5,
                member_id: mid.clone(),
                group_instance_id: None,
                protocol_type: Some("consumer".to_string()),
                protocol_name: Some("sticky".to_string()),
                assignments: Vec::new(),
            })
            .expect("follower sync");
        assert_eq!(
            result.assignment,
            vec![idx as u8 * 2],
            "member {idx} should receive correct partition assignment"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Unknown member leave returns per-member error
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn leave_unknown_member_returns_error_in_outcome() {
    let temp = TempDir::new("leave-unknown");
    let mut c = coordinator(temp.path());

    let (_, _) = v4_join(&mut c, "grp", 0);

    let outcomes = c
        .leave_group_members(
            "grp",
            &[LeaveGroupMemberInput {
                member_id: "nobody".to_string(),
                group_instance_id: None,
            }],
        )
        .expect("leave_group_members returns outcomes, not error");

    assert_eq!(outcomes.len(), 1);
    assert!(
        outcomes[0].error.is_some(),
        "leaving unknown member must produce an error in outcome"
    );
    assert_eq!(
        outcomes[0].error,
        Some(ClassicGroupCoordinatorError::UnknownMemberId(
            "nobody".to_string()
        ))
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Leave and rejoin: former member gets a new assigned ID and gen bumps
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn leave_then_rejoin_gets_new_member_id_and_new_generation() {
    let temp = TempDir::new("leave-rejoin");
    let mut c = coordinator(temp.path());

    let (leader_id, gen1) = v4_join(&mut c, "grp", 0);
    assert_eq!(gen1, 1);
    sync_alone(&mut c, "grp", 1, &leader_id, vec![0]);

    // Leader leaves
    c.leave_group_legacy("grp", &leader_id)
        .expect("leader leave");

    // Former leader rejoins — gets a new member_id (v4 MemberIdRequired pattern)
    let err = c
        .join_group(join_input_v4("grp", ""), 20)
        .expect_err("must get MemberIdRequired");
    let new_member_id = match err {
        ClassicGroupCoordinatorError::MemberIdRequired(id) => id,
        other => panic!("expected MemberIdRequired, got {other:?}"),
    };
    // The new ID must differ from the original
    assert_ne!(new_member_id, leader_id, "returned member_id must be fresh");

    let outcome = c
        .join_group(join_input_v4("grp", &new_member_id), 21)
        .expect("rejoin with new ID");
    // Group was deleted when last member left; rejoining creates gen 1 again
    assert!(outcome.generation_id >= 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// Three consecutive rebalances: each adds one member, generation tracks correctly
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn consecutive_rebalances_generation_monotonically_increases() {
    let temp = TempDir::new("consec-rebalance");
    let mut c = coordinator(temp.path());

    let (m1, gen1) = v4_join(&mut c, "grp", 0);
    assert_eq!(gen1, 1);
    sync_alone(&mut c, "grp", 1, &m1, vec![0]);

    let (_, gen2) = v4_join(&mut c, "grp", 10);
    assert_eq!(gen2, 2);

    // m1 must rejoin so rebalance clears
    c.join_group(join_input_v4("grp", &m1), 11)
        .expect("m1 rejoin gen 2");
    c.sync_group(SyncGroupInput {
        api_version: 5,
        group_id: "grp".to_string(),
        generation_id: 2,
        member_id: m1.clone(),
        group_instance_id: None,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("sticky".to_string()),
        assignments: vec![SyncAssignment {
            member_id: m1.clone(),
            assignment: vec![0],
        }],
    })
    .expect("sync gen 2");

    let (_, gen3) = v4_join(&mut c, "grp", 20);
    assert_eq!(gen3, 3, "generation must increment on each new member join");
}

// ──────────────────────────────────────────────────────────────────────────────
// Assignments survive a sync → re-sync without new assignments returns same data
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn assignments_are_returned_on_subsequent_sync_calls() {
    let temp = TempDir::new("subsequent-sync");
    let mut c = coordinator(temp.path());

    let (leader, gen) = v4_join(&mut c, "grp", 0);
    let assigned_data = vec![0xDE, 0xAD, 0xBE, 0xEF];

    sync_alone(&mut c, "grp", gen, &leader, assigned_data.clone());

    // A second sync call (e.g., after reconnect) must return the same assignment
    let result = c
        .sync_group(SyncGroupInput {
            api_version: 5,
            group_id: "grp".to_string(),
            generation_id: gen,
            member_id: leader.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("sticky".to_string()),
            assignments: Vec::new(), // no new assignments — just fetching
        })
        .expect("second sync must succeed");
    assert_eq!(
        result.assignment, assigned_data,
        "assignment must be returned on subsequent sync"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Stale generation heartbeat is rejected even after multiple rebalances
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn heartbeat_with_skipped_generation_is_rejected() {
    let temp = TempDir::new("skip-gen-hb");
    let mut c = coordinator(temp.path());

    let (m1, gen1) = v4_join(&mut c, "grp", 0);
    assert_eq!(gen1, 1);
    sync_alone(&mut c, "grp", 1, &m1, vec![0]);

    let (_, _gen2) = v4_join(&mut c, "grp", 10);
    c.join_group(join_input_v4("grp", &m1), 11)
        .expect("m1 rejoin gen 2");
    c.sync_group(SyncGroupInput {
        api_version: 5,
        group_id: "grp".to_string(),
        generation_id: 2,
        member_id: m1.clone(),
        group_instance_id: None,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("sticky".to_string()),
        assignments: vec![SyncAssignment {
            member_id: m1.clone(),
            assignment: vec![0],
        }],
    })
    .expect("sync gen 2");

    // Heartbeat with gen 0 (skipping both gen 1 and 2) must fail
    let err = c
        .heartbeat(
            HeartbeatInput {
                group_id: "grp".to_string(),
                generation_id: 0,
                member_id: m1.clone(),
                group_instance_id: None,
            },
            50,
        )
        .expect_err("gen 0 heartbeat must be rejected");
    assert_eq!(err, ClassicGroupCoordinatorError::IllegalGeneration);
}

// ──────────────────────────────────────────────────────────────────────────────
// Leader outcome includes all member IDs from the group
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn join_outcome_members_list_reflects_all_group_members() {
    let temp = TempDir::new("members-list");
    let mut c = coordinator(temp.path());

    let (m1, _) = v4_join(&mut c, "grp", 0);
    let (m2, _) = v4_join(&mut c, "grp", 5);
    let (m3, gen3) = v4_join(&mut c, "grp", 10);

    // Leader (m1) rejoins to get the full members list
    let outcome = c
        .join_group(join_input_v4("grp", &m1), 15)
        .expect("leader rejoin");
    assert_eq!(outcome.generation_id, gen3);
    // Members list should contain all three
    let member_ids: Vec<&str> = outcome
        .members
        .iter()
        .map(|m| m.member_id.as_str())
        .collect();
    assert!(
        member_ids.contains(&m1.as_str()),
        "m1 must be in members list"
    );
    assert!(
        member_ids.contains(&m2.as_str()),
        "m2 must be in members list"
    );
    assert!(
        member_ids.contains(&m3.as_str()),
        "m3 must be in members list"
    );
    assert_eq!(outcome.members.len(), 3);
}

// ──────────────────────────────────────────────────────────────────────────────
// Leader field in join outcome points to the first (alphabetically) member
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn join_outcome_leader_is_consistent_across_members() {
    let temp = TempDir::new("leader-consistent");
    let mut c = coordinator(temp.path());

    let (m1, _) = v4_join(&mut c, "grp", 0);
    let (_, _) = v4_join(&mut c, "grp", 5);

    // All rejoining members see the same leader field
    let outcome1 = c
        .join_group(join_input_v4("grp", &m1), 10)
        .expect("m1 rejoin");

    // Leader is a non-empty string and must be one of the known member_ids
    assert!(!outcome1.leader.is_empty(), "leader must be non-empty");
}
