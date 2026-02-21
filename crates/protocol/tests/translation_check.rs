#![forbid(unsafe_code)]

use std::path::PathBuf;

use rafka_codegen::{load_message_specs, ProtocolMessageSpec};
use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, ProduceRequestPartitionProduceData,
    ProduceRequestTopicProduceData, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, SyncGroupRequest, SyncGroupResponse,
    VersionedCodec, API_VERSIONS_FIRST_FLEXIBLE_VERSION, HEARTBEAT_FIRST_FLEXIBLE_VERSION,
    JOIN_GROUP_FIRST_FLEXIBLE_VERSION, LEAVE_GROUP_FIRST_FLEXIBLE_VERSION,
    OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION, OFFSET_FETCH_FIRST_FLEXIBLE_VERSION,
    PRODUCE_FIRST_FLEXIBLE_VERSION, SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION,
    SYNC_GROUP_FIRST_FLEXIBLE_VERSION,
};
use rafka_protocol::ProtocolError;

fn specs_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../clients/src/main/resources/common/message")
}

fn find_spec<'a>(specs: &'a [ProtocolMessageSpec], name: &str) -> &'a ProtocolMessageSpec {
    specs
        .iter()
        .find(|spec| spec.name == name)
        .unwrap_or_else(|| panic!("missing spec: {name}"))
}

#[test]
fn translation_check_versions_match_kafka_specs() {
    let specs = load_message_specs(&specs_dir()).expect("load specs");

    let api_versions_request = find_spec(&specs, "ApiVersionsRequest");
    assert_eq!(
        ApiVersionsRequest::MIN_VERSION,
        api_versions_request.valid_versions.low
    );
    assert_eq!(
        ApiVersionsRequest::MAX_VERSION,
        api_versions_request.valid_versions.high
    );
    assert_eq!(
        API_VERSIONS_FIRST_FLEXIBLE_VERSION,
        api_versions_request.flexible_versions.low
    );

    let api_versions_response = find_spec(&specs, "ApiVersionsResponse");
    assert_eq!(
        ApiVersionsResponse::MIN_VERSION,
        api_versions_response.valid_versions.low
    );
    assert_eq!(
        ApiVersionsResponse::MAX_VERSION,
        api_versions_response.valid_versions.high
    );
    assert_eq!(
        API_VERSIONS_FIRST_FLEXIBLE_VERSION,
        api_versions_response.flexible_versions.low
    );

    let produce_request = find_spec(&specs, "ProduceRequest");
    assert_eq!(
        ProduceRequest::MIN_VERSION,
        produce_request.valid_versions.low
    );
    assert_eq!(
        ProduceRequest::MAX_VERSION,
        produce_request.valid_versions.high
    );
    assert_eq!(
        PRODUCE_FIRST_FLEXIBLE_VERSION,
        produce_request.flexible_versions.low
    );

    let offset_commit_request = find_spec(&specs, "OffsetCommitRequest");
    assert_eq!(
        OffsetCommitRequest::MIN_VERSION,
        offset_commit_request.valid_versions.low
    );
    assert_eq!(
        OffsetCommitRequest::MAX_VERSION,
        offset_commit_request.valid_versions.high
    );
    assert_eq!(
        OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION,
        offset_commit_request.flexible_versions.low
    );

    let offset_fetch_request = find_spec(&specs, "OffsetFetchRequest");
    assert_eq!(
        OffsetFetchRequest::MIN_VERSION,
        offset_fetch_request.valid_versions.low
    );
    assert_eq!(
        OffsetFetchRequest::MAX_VERSION,
        offset_fetch_request.valid_versions.high
    );
    assert_eq!(
        OFFSET_FETCH_FIRST_FLEXIBLE_VERSION,
        offset_fetch_request.flexible_versions.low
    );

    let join_group_request = find_spec(&specs, "JoinGroupRequest");
    assert_eq!(
        JoinGroupRequest::MIN_VERSION,
        join_group_request.valid_versions.low
    );
    assert_eq!(
        JoinGroupRequest::MAX_VERSION,
        join_group_request.valid_versions.high
    );
    assert_eq!(
        JOIN_GROUP_FIRST_FLEXIBLE_VERSION,
        join_group_request.flexible_versions.low
    );

    let join_group_response = find_spec(&specs, "JoinGroupResponse");
    assert_eq!(
        JoinGroupResponse::MIN_VERSION,
        join_group_response.valid_versions.low
    );
    assert_eq!(
        JoinGroupResponse::MAX_VERSION,
        join_group_response.valid_versions.high
    );
    assert_eq!(
        JOIN_GROUP_FIRST_FLEXIBLE_VERSION,
        join_group_response.flexible_versions.low
    );

    let sync_group_request = find_spec(&specs, "SyncGroupRequest");
    assert_eq!(
        SyncGroupRequest::MIN_VERSION,
        sync_group_request.valid_versions.low
    );
    assert_eq!(
        SyncGroupRequest::MAX_VERSION,
        sync_group_request.valid_versions.high
    );
    assert_eq!(
        SYNC_GROUP_FIRST_FLEXIBLE_VERSION,
        sync_group_request.flexible_versions.low
    );

    let sync_group_response = find_spec(&specs, "SyncGroupResponse");
    assert_eq!(
        SyncGroupResponse::MIN_VERSION,
        sync_group_response.valid_versions.low
    );
    assert_eq!(
        SyncGroupResponse::MAX_VERSION,
        sync_group_response.valid_versions.high
    );
    assert_eq!(
        SYNC_GROUP_FIRST_FLEXIBLE_VERSION,
        sync_group_response.flexible_versions.low
    );

    let heartbeat_request = find_spec(&specs, "HeartbeatRequest");
    assert_eq!(
        HeartbeatRequest::MIN_VERSION,
        heartbeat_request.valid_versions.low
    );
    assert_eq!(
        HeartbeatRequest::MAX_VERSION,
        heartbeat_request.valid_versions.high
    );
    assert_eq!(
        HEARTBEAT_FIRST_FLEXIBLE_VERSION,
        heartbeat_request.flexible_versions.low
    );

    let heartbeat_response = find_spec(&specs, "HeartbeatResponse");
    assert_eq!(
        HeartbeatResponse::MIN_VERSION,
        heartbeat_response.valid_versions.low
    );
    assert_eq!(
        HeartbeatResponse::MAX_VERSION,
        heartbeat_response.valid_versions.high
    );
    assert_eq!(
        HEARTBEAT_FIRST_FLEXIBLE_VERSION,
        heartbeat_response.flexible_versions.low
    );

    let leave_group_request = find_spec(&specs, "LeaveGroupRequest");
    assert_eq!(
        LeaveGroupRequest::MIN_VERSION,
        leave_group_request.valid_versions.low
    );
    assert_eq!(
        LeaveGroupRequest::MAX_VERSION,
        leave_group_request.valid_versions.high
    );
    assert_eq!(
        LEAVE_GROUP_FIRST_FLEXIBLE_VERSION,
        leave_group_request.flexible_versions.low
    );

    let leave_group_response = find_spec(&specs, "LeaveGroupResponse");
    assert_eq!(
        LeaveGroupResponse::MIN_VERSION,
        leave_group_response.valid_versions.low
    );
    assert_eq!(
        LeaveGroupResponse::MAX_VERSION,
        leave_group_response.valid_versions.high
    );
    assert_eq!(
        LEAVE_GROUP_FIRST_FLEXIBLE_VERSION,
        leave_group_response.flexible_versions.low
    );

    let sasl_handshake_request = find_spec(&specs, "SaslHandshakeRequest");
    assert_eq!(
        SaslHandshakeRequest::MIN_VERSION,
        sasl_handshake_request.valid_versions.low
    );
    assert_eq!(
        SaslHandshakeRequest::MAX_VERSION,
        sasl_handshake_request.valid_versions.high
    );
    assert!(sasl_handshake_request.flexible_versions.is_none());

    let sasl_handshake_response = find_spec(&specs, "SaslHandshakeResponse");
    assert_eq!(
        SaslHandshakeResponse::MIN_VERSION,
        sasl_handshake_response.valid_versions.low
    );
    assert_eq!(
        SaslHandshakeResponse::MAX_VERSION,
        sasl_handshake_response.valid_versions.high
    );
    assert!(sasl_handshake_response.flexible_versions.is_none());

    let sasl_authenticate_request = find_spec(&specs, "SaslAuthenticateRequest");
    assert_eq!(
        SaslAuthenticateRequest::MIN_VERSION,
        sasl_authenticate_request.valid_versions.low
    );
    assert_eq!(
        SaslAuthenticateRequest::MAX_VERSION,
        sasl_authenticate_request.valid_versions.high
    );
    assert_eq!(
        SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION,
        sasl_authenticate_request.flexible_versions.low
    );

    let sasl_authenticate_response = find_spec(&specs, "SaslAuthenticateResponse");
    assert_eq!(
        SaslAuthenticateResponse::MIN_VERSION,
        sasl_authenticate_response.valid_versions.low
    );
    assert_eq!(
        SaslAuthenticateResponse::MAX_VERSION,
        sasl_authenticate_response.valid_versions.high
    );
    assert_eq!(
        SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION,
        sasl_authenticate_response.flexible_versions.low
    );
}

#[test]
fn translation_check_produce_name_vs_topic_id_rules() {
    let base_partition = ProduceRequestPartitionProduceData {
        index: 0,
        records: None,
    };

    let v12_missing_name = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: None,
            topic_id: None,
            partition_data: vec![base_partition.clone()],
        }],
    };
    let err = v12_missing_name
        .encode(12)
        .expect_err("name is required for v12");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("ProduceRequest.topic_data.name")
    );

    let v13_missing_topic_id = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("topic-a".to_string()),
            topic_id: None,
            partition_data: vec![base_partition],
        }],
    };
    let err = v13_missing_topic_id
        .encode(13)
        .expect_err("topic id is required for v13");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("ProduceRequest.topic_data.topic_id")
    );
}

#[test]
fn translation_check_apiversions_throttle_field_rules() {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![],
        throttle_time_ms: None,
        supported_features: None,
        finalized_features_epoch: None,
        finalized_features: None,
        zk_migration_ready: None,
    };

    // In Kafka specs, throttle_time_ms exists in v1+.
    let err = response
        .encode(1)
        .expect_err("throttle_time_ms must be present at v1+");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("ApiVersionsResponse.throttle_time_ms")
    );

    // v0 remains valid without throttle_time_ms.
    let bytes = response.encode(0).expect("v0 encode");
    let (decoded, read) = ApiVersionsResponse::decode(0, &bytes).expect("v0 decode");
    assert_eq!(decoded, response);
    assert_eq!(read, bytes.len());
}

#[test]
fn translation_check_offset_commit_name_vs_topic_id_rules() {
    let v9_missing_name = OffsetCommitRequest {
        group_id: "group-a".to_string(),
        generation_id_or_member_epoch: -1,
        member_id: "".to_string(),
        group_instance_id: Some("instance-a".to_string()),
        retention_time_ms: None,
        topics: vec![
            rafka_protocol::messages::OffsetCommitRequestOffsetCommitRequestTopic {
                name: None,
                topic_id: None,
                partitions: vec![],
            },
        ],
    };
    let err = v9_missing_name
        .encode(9)
        .expect_err("name is required for v9");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("OffsetCommitRequest.topics.name")
    );

    let v10_missing_topic_id = OffsetCommitRequest {
        group_id: "group-a".to_string(),
        generation_id_or_member_epoch: -1,
        member_id: "".to_string(),
        group_instance_id: Some("instance-a".to_string()),
        retention_time_ms: None,
        topics: vec![
            rafka_protocol::messages::OffsetCommitRequestOffsetCommitRequestTopic {
                name: Some("topic-a".to_string()),
                topic_id: None,
                partitions: vec![],
            },
        ],
    };
    let err = v10_missing_topic_id
        .encode(10)
        .expect_err("topic_id is required for v10");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("OffsetCommitRequest.topics.topic_id")
    );
}
