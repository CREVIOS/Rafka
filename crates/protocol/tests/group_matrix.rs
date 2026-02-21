#![forbid(unsafe_code)]

use rafka_protocol::messages::{
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupRequestJoinGroupRequestProtocol, JoinGroupResponse,
    JoinGroupResponseJoinGroupResponseMember, LeaveGroupRequest, LeaveGroupRequestMemberIdentity,
    LeaveGroupResponse, LeaveGroupResponseMemberResponse, SyncGroupRequest,
    SyncGroupRequestSyncGroupRequestAssignment, SyncGroupResponse, VersionedCodec,
    HEARTBEAT_FIRST_FLEXIBLE_VERSION, HEARTBEAT_MAX_VERSION, HEARTBEAT_MIN_VERSION,
    JOIN_GROUP_FIRST_FLEXIBLE_VERSION, JOIN_GROUP_MAX_VERSION, JOIN_GROUP_MIN_VERSION,
    LEAVE_GROUP_FIRST_FLEXIBLE_VERSION, LEAVE_GROUP_MAX_VERSION, LEAVE_GROUP_MIN_VERSION,
    SYNC_GROUP_FIRST_FLEXIBLE_VERSION, SYNC_GROUP_MAX_VERSION, SYNC_GROUP_MIN_VERSION,
};
use rafka_protocol::ProtocolError;

fn assert_roundtrip<T>(message: &T, version: i16)
where
    T: VersionedCodec + core::fmt::Debug + PartialEq,
{
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = T::decode(version, &encoded).expect("decode");
    assert_eq!(decoded, *message);
    assert_eq!(read, encoded.len());
}

fn sample_join_group_request(version: i16) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: "group-a".to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: (version >= 1).then_some(45_000),
        member_id: "member-a".to_string(),
        group_instance_id: (version >= 5).then_some("instance-a".to_string()),
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestJoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: vec![0x01, 0x02, 0x03],
        }],
        reason: (version >= 8).then_some("rolling update".to_string()),
    }
}

fn sample_join_group_response(version: i16) -> JoinGroupResponse {
    JoinGroupResponse {
        throttle_time_ms: (version >= 2).then_some(0),
        error_code: 0,
        generation_id: 5,
        protocol_type: (version >= 7).then_some("consumer".to_string()),
        protocol_name: Some("range".to_string()),
        leader: "member-a".to_string(),
        skip_assignment: (version >= 9).then_some(false),
        member_id: "member-a".to_string(),
        members: vec![JoinGroupResponseJoinGroupResponseMember {
            member_id: "member-a".to_string(),
            group_instance_id: (version >= 5).then_some("instance-a".to_string()),
            metadata: vec![0x01, 0x02],
        }],
    }
}

fn sample_sync_group_request(version: i16) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id: "group-a".to_string(),
        generation_id: 5,
        member_id: "member-a".to_string(),
        group_instance_id: (version >= 3).then_some("instance-a".to_string()),
        protocol_type: (version >= 5).then_some("consumer".to_string()),
        protocol_name: (version >= 5).then_some("range".to_string()),
        assignments: vec![SyncGroupRequestSyncGroupRequestAssignment {
            member_id: "member-a".to_string(),
            assignment: vec![0x0a, 0x0b],
        }],
    }
}

fn sample_sync_group_response(version: i16) -> SyncGroupResponse {
    SyncGroupResponse {
        throttle_time_ms: (version >= 1).then_some(0),
        error_code: 0,
        protocol_type: (version >= 5).then_some("consumer".to_string()),
        protocol_name: (version >= 5).then_some("range".to_string()),
        assignment: vec![0xaa, 0xbb],
    }
}

fn sample_heartbeat_request(version: i16) -> HeartbeatRequest {
    HeartbeatRequest {
        group_id: "group-a".to_string(),
        generation_id: 5,
        member_id: "member-a".to_string(),
        group_instance_id: (version >= 3).then_some("instance-a".to_string()),
    }
}

fn sample_heartbeat_response(version: i16) -> HeartbeatResponse {
    HeartbeatResponse {
        throttle_time_ms: (version >= 1).then_some(0),
        error_code: 0,
    }
}

fn sample_leave_group_request(version: i16) -> LeaveGroupRequest {
    if version <= 2 {
        return LeaveGroupRequest {
            group_id: "group-a".to_string(),
            member_id: Some("member-a".to_string()),
            members: None,
        };
    }

    LeaveGroupRequest {
        group_id: "group-a".to_string(),
        member_id: None,
        members: Some(vec![LeaveGroupRequestMemberIdentity {
            member_id: "member-a".to_string(),
            group_instance_id: Some("instance-a".to_string()),
            reason: (version >= 5).then_some("shutdown".to_string()),
        }]),
    }
}

fn sample_leave_group_response(version: i16) -> LeaveGroupResponse {
    LeaveGroupResponse {
        throttle_time_ms: (version >= 1).then_some(0),
        error_code: 0,
        members: (version >= 3).then_some(vec![LeaveGroupResponseMemberResponse {
            member_id: "member-a".to_string(),
            group_instance_id: Some("instance-a".to_string()),
            error_code: 0,
        }]),
    }
}

fn with_unknown_tag(mut encoded: Vec<u8>) -> Vec<u8> {
    assert_eq!(encoded.pop(), Some(0));
    encoded.extend([1, 31, 1, 0xaa]);
    encoded
}

#[test]
fn join_group_roundtrip_all_versions() {
    for version in JOIN_GROUP_MIN_VERSION..=JOIN_GROUP_MAX_VERSION {
        assert_roundtrip(&sample_join_group_request(version), version);
        assert_roundtrip(&sample_join_group_response(version), version);
    }
}

#[test]
fn sync_group_roundtrip_all_versions() {
    for version in SYNC_GROUP_MIN_VERSION..=SYNC_GROUP_MAX_VERSION {
        assert_roundtrip(&sample_sync_group_request(version), version);
        assert_roundtrip(&sample_sync_group_response(version), version);
    }
}

#[test]
fn heartbeat_roundtrip_all_versions() {
    for version in HEARTBEAT_MIN_VERSION..=HEARTBEAT_MAX_VERSION {
        assert_roundtrip(&sample_heartbeat_request(version), version);
        assert_roundtrip(&sample_heartbeat_response(version), version);
    }
}

#[test]
fn leave_group_roundtrip_all_versions() {
    for version in LEAVE_GROUP_MIN_VERSION..=LEAVE_GROUP_MAX_VERSION {
        assert_roundtrip(&sample_leave_group_request(version), version);
        assert_roundtrip(&sample_leave_group_response(version), version);
    }
}

#[test]
fn join_group_required_fields_fail_when_missing() {
    for version in 1..=JOIN_GROUP_MAX_VERSION {
        let mut request = sample_join_group_request(version);
        request.rebalance_timeout_ms = None;
        let err = request
            .encode(version)
            .expect_err("missing rebalance timeout must fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("JoinGroupRequest.rebalance_timeout_ms")
        );
    }

    for version in JOIN_GROUP_MIN_VERSION..=6 {
        let mut response = sample_join_group_response(version);
        response.protocol_name = None;
        let err = response
            .encode(version)
            .expect_err("missing protocol name must fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("JoinGroupResponse.protocol_name")
        );
    }
}

#[test]
fn leave_group_required_fields_fail_when_missing() {
    for version in LEAVE_GROUP_MIN_VERSION..=2 {
        let mut request = sample_leave_group_request(version);
        request.member_id = None;
        let err = request
            .encode(version)
            .expect_err("missing member id must fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("LeaveGroupRequest.member_id")
        );
    }

    for version in 3..=LEAVE_GROUP_MAX_VERSION {
        let mut request = sample_leave_group_request(version);
        request.members = None;
        let err = request
            .encode(version)
            .expect_err("missing members must fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("LeaveGroupRequest.members")
        );

        let mut response = sample_leave_group_response(version);
        response.members = None;
        let err = response
            .encode(version)
            .expect_err("missing response members must fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("LeaveGroupResponse.members")
        );
    }
}

#[test]
fn flexible_group_messages_skip_unknown_top_level_tags() {
    let join_req = sample_join_group_request(JOIN_GROUP_FIRST_FLEXIBLE_VERSION);
    let encoded = with_unknown_tag(
        join_req
            .encode(JOIN_GROUP_FIRST_FLEXIBLE_VERSION)
            .expect("encode"),
    );
    let (decoded, read) =
        JoinGroupRequest::decode(JOIN_GROUP_FIRST_FLEXIBLE_VERSION, &encoded).expect("decode");
    assert_eq!(decoded, join_req);
    assert_eq!(read, encoded.len());

    let sync_req = sample_sync_group_request(SYNC_GROUP_FIRST_FLEXIBLE_VERSION);
    let encoded = with_unknown_tag(
        sync_req
            .encode(SYNC_GROUP_FIRST_FLEXIBLE_VERSION)
            .expect("encode"),
    );
    let (decoded, read) =
        SyncGroupRequest::decode(SYNC_GROUP_FIRST_FLEXIBLE_VERSION, &encoded).expect("decode");
    assert_eq!(decoded, sync_req);
    assert_eq!(read, encoded.len());

    let heartbeat_req = sample_heartbeat_request(HEARTBEAT_FIRST_FLEXIBLE_VERSION);
    let encoded = with_unknown_tag(
        heartbeat_req
            .encode(HEARTBEAT_FIRST_FLEXIBLE_VERSION)
            .expect("encode"),
    );
    let (decoded, read) =
        HeartbeatRequest::decode(HEARTBEAT_FIRST_FLEXIBLE_VERSION, &encoded).expect("decode");
    assert_eq!(decoded, heartbeat_req);
    assert_eq!(read, encoded.len());

    let leave_req = sample_leave_group_request(LEAVE_GROUP_FIRST_FLEXIBLE_VERSION);
    let encoded = with_unknown_tag(
        leave_req
            .encode(LEAVE_GROUP_FIRST_FLEXIBLE_VERSION)
            .expect("encode"),
    );
    let (decoded, read) =
        LeaveGroupRequest::decode(LEAVE_GROUP_FIRST_FLEXIBLE_VERSION, &encoded).expect("decode");
    assert_eq!(decoded, leave_req);
    assert_eq!(read, encoded.len());
}

#[test]
fn group_decoders_reject_truncated_payloads() {
    let join = sample_join_group_request(0).encode(0).expect("encode");
    for cut in 0..join.len() {
        let err = JoinGroupRequest::decode(0, &join[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
    let sync = sample_sync_group_request(0).encode(0).expect("encode");
    for cut in 0..sync.len() {
        let err = SyncGroupRequest::decode(0, &sync[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
    let heartbeat = sample_heartbeat_request(0).encode(0).expect("encode");
    for cut in 0..heartbeat.len() {
        let err = HeartbeatRequest::decode(0, &heartbeat[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
    let leave = sample_leave_group_request(0).encode(0).expect("encode");
    for cut in 0..leave.len() {
        let err = LeaveGroupRequest::decode(0, &leave[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
}
