//! Wire matrix tests for generated `VersionedCodec` implementations.
//!
//! Every test here exercises the codecs emitted by `rafka-codegen` from the
//! vendored Kafka specs, across the full valid version range for each API,
//! plus golden-byte vectors and negative paths.

use rafka_protocol::messages::CreateTopicsRequestCreatableReplicaAssignment;
use rafka_protocol::messages::CreateTopicsRequestCreatableTopic;
use rafka_protocol::messages::CreateTopicsRequestCreatableTopicConfig;
use rafka_protocol::messages::CreateTopicsResponseCreatableTopicConfigs;
use rafka_protocol::messages::DeleteGroupsResponseDeletableGroupResult;
use rafka_protocol::messages::DeleteTopicsRequestDeleteTopicState;
use rafka_protocol::messages::DeleteTopicsResponseDeletableTopicResult;
use rafka_protocol::messages::ListGroupsResponseListedGroup;
use rafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteGroupsRequest, DeleteGroupsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, ListGroupsRequest, ListGroupsResponse,
    VersionedCodec,
};
use rafka_protocol::ProtocolError;

fn roundtrip<M: VersionedCodec + PartialEq + std::fmt::Debug>(message: &M, version: i16) -> M {
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = M::decode(version, &encoded).expect("decode");
    assert_eq!(
        read,
        encoded.len(),
        "must consume exactly the encoded bytes"
    );
    assert_eq!(&decoded, message, "roundtrip must be lossless");
    decoded
}

fn sample_delete_groups_request() -> DeleteGroupsRequest {
    DeleteGroupsRequest {
        groups_names: vec!["a".to_string(), "bb".to_string()],
    }
}

fn sample_delete_groups_response() -> DeleteGroupsResponse {
    DeleteGroupsResponse {
        throttle_time_ms: 7,
        results: vec![
            DeleteGroupsResponseDeletableGroupResult {
                group_id: "g1".to_string(),
                error_code: 0,
            },
            DeleteGroupsResponseDeletableGroupResult {
                group_id: "g2".to_string(),
                error_code: 25,
            },
        ],
    }
}

fn sample_list_groups_request(version: i16) -> ListGroupsRequest {
    let mut request = ListGroupsRequest::default();
    if version >= 4 {
        request.states_filter = Some(vec!["Stable".to_string()]);
    }
    if version >= 5 {
        request.types_filter = Some(vec!["classic".to_string(), "consumer".to_string()]);
    }
    request
}

fn sample_list_groups_response() -> ListGroupsResponse {
    ListGroupsResponse {
        throttle_time_ms: Some(11),
        error_code: 0,
        groups: vec![ListGroupsResponseListedGroup {
            group_id: "g".to_string(),
            protocol_type: "consumer".to_string(),
            group_state: Some("Stable".to_string()),
            group_type: Some("classic".to_string()),
        }],
    }
}

// ---------------------------------------------------------------- DeleteGroups

#[test]
fn delete_groups_request_roundtrip_all_versions() {
    for version in DeleteGroupsRequest::MIN_VERSION..=DeleteGroupsRequest::MAX_VERSION {
        roundtrip(&sample_delete_groups_request(), version);
    }
}

#[test]
fn delete_groups_request_empty_roundtrip_all_versions() {
    for version in DeleteGroupsRequest::MIN_VERSION..=DeleteGroupsRequest::MAX_VERSION {
        roundtrip(&DeleteGroupsRequest::default(), version);
    }
}

#[test]
fn delete_groups_request_v0_known_bytes() {
    // int32 array length 2, then int16-prefixed strings "a" and "bb"
    let encoded = sample_delete_groups_request().encode(0).expect("encode");
    assert_eq!(encoded, vec![0, 0, 0, 2, 0, 1, b'a', 0, 2, b'b', b'b']);
}

#[test]
fn delete_groups_request_v2_known_bytes() {
    // flexible: compact array length (2+1), compact strings, tagged section 0
    let encoded = sample_delete_groups_request().encode(2).expect("encode");
    assert_eq!(encoded, vec![0x03, 0x02, b'a', 0x03, b'b', b'b', 0x00]);
}

#[test]
fn delete_groups_response_roundtrip_all_versions() {
    for version in DeleteGroupsResponse::MIN_VERSION..=DeleteGroupsResponse::MAX_VERSION {
        roundtrip(&sample_delete_groups_response(), version);
    }
}

#[test]
fn delete_groups_response_v2_known_bytes() {
    let encoded = sample_delete_groups_response().encode(2).expect("encode");
    assert_eq!(
        encoded,
        vec![
            0, 0, 0, 7,    // throttle_time_ms
            0x03, // compact array length 2+1
            0x03, b'g', b'1', // group_id "g1"
            0x00, 0x00, // error_code 0
            0x00, // element tagged section
            0x03, b'g', b'2', 0x00, 25, 0x00, // second element
            0x00, // top-level tagged section
        ]
    );
}

// ----------------------------------------------------------------- ListGroups

#[test]
fn list_groups_request_v0_is_empty() {
    let encoded = ListGroupsRequest::default().encode(0).expect("encode");
    assert!(encoded.is_empty());
}

#[test]
fn list_groups_request_v3_emits_only_tagged_section() {
    let encoded = ListGroupsRequest::default().encode(3).expect("encode");
    assert_eq!(encoded, vec![0x00]);
}

#[test]
fn list_groups_request_roundtrip_all_versions() {
    for version in ListGroupsRequest::MIN_VERSION..=ListGroupsRequest::MAX_VERSION {
        roundtrip(&sample_list_groups_request(version), version);
    }
}

#[test]
fn list_groups_request_v4_known_bytes() {
    let encoded = sample_list_groups_request(4).encode(4).expect("encode");
    // compact array ["Stable"] then tagged section
    let mut expected = vec![0x02, 0x07];
    expected.extend_from_slice(b"Stable");
    expected.push(0x00);
    assert_eq!(encoded, expected);
}

#[test]
fn list_groups_response_roundtrip_all_versions() {
    for version in ListGroupsResponse::MIN_VERSION..=ListGroupsResponse::MAX_VERSION {
        let mut expected = sample_list_groups_response();
        if version < 1 {
            expected.throttle_time_ms = None;
        }
        for group in &mut expected.groups {
            if version < 4 {
                group.group_state = None;
            }
            if version < 5 {
                group.group_type = None;
            }
        }
        let wire = expected.clone();
        roundtrip(&wire, version);
        assert_eq!(wire, expected);
    }
}

#[test]
fn list_groups_response_absent_fields_stay_none() {
    let response = sample_list_groups_response();
    let encoded = response.encode(0).expect("encode");
    let (decoded, read) = ListGroupsResponse::decode(0, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded.throttle_time_ms, None);
    assert_eq!(decoded.groups.len(), 1);
    assert_eq!(decoded.groups[0].group_state, None);
    assert_eq!(decoded.groups[0].group_type, None);
}

// --------------------------------------------------------------- CreateTopics

fn sample_create_topics_request() -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: vec![CreateTopicsRequestCreatableTopic {
            name: "t".to_string(),
            num_partitions: 1,
            replication_factor: 1,
            assignments: vec![CreateTopicsRequestCreatableReplicaAssignment {
                partition_index: 7,
                broker_ids: vec![4, 5],
            }],
            configs: vec![CreateTopicsRequestCreatableTopicConfig {
                name: "k".to_string(),
                value: Some("v".to_string()),
            }],
        }],
        timeout_ms: 9,
        validate_only: true,
    }
}

#[test]
fn create_topics_request_roundtrip_all_versions() {
    for version in CreateTopicsRequest::MIN_VERSION..=CreateTopicsRequest::MAX_VERSION {
        roundtrip(&sample_create_topics_request(), version);
    }
}

#[test]
fn create_topics_request_v4_known_bytes() {
    // legacy framing: int32 array lens, int16 string lens, no tagged sections
    let encoded = sample_create_topics_request().encode(4).expect("encode");
    assert_eq!(
        encoded,
        vec![
            0, 0, 0, 1, // topics len 1
            0, 1, b't', // name
            0, 0, 0, 1, // num_partitions
            0, 1, // replication_factor
            0, 0, 0, 1, // assignments len 1
            0, 0, 0, 7, // partition_index
            0, 0, 0, 2, // broker_ids len 2
            0, 0, 0, 4, 0, 0, 0, 5, // broker ids (scalar array of int32)
            0, 0, 0, 1, // configs len 1
            0, 1, b'k', // config name
            0, 1, b'v', // config value (nullable string, present)
            0, 0, 0, 9, // timeout_ms
            1, // validate_only
        ]
    );
}

#[test]
fn create_topics_request_v7_known_bytes() {
    // flexible framing: compact lens/strings + tagged section after every
    // struct level (config element, assignment element, topic element, top)
    let encoded = sample_create_topics_request().encode(7).expect("encode");
    assert_eq!(
        encoded,
        vec![
            0x02, // topics compact len 1+1
            0x02, b't', // name
            0, 0, 0, 1, // num_partitions
            0, 1,    // replication_factor
            0x02, // assignments compact len 1+1
            0, 0, 0, 7,    // partition_index
            0x03, // broker_ids compact len 2+1
            0, 0, 0, 4, 0, 0, 0, 5, 0x00, // ids + element tagged section
            0x02, // configs compact len 1+1
            0x02, b'k', // config name
            0x02, b'v', // config value
            0x00, // config element tagged section
            0x00, // topic element tagged section
            0, 0, 0, 9,    // timeout_ms
            1,    // validate_only
            0x00, // top-level tagged section
        ]
    );
}

fn sample_create_topics_response() -> CreateTopicsResponse {
    CreateTopicsResponse {
        throttle_time_ms: 3,
        topics: vec![
            rafka_protocol::messages::CreateTopicsResponseCreatableTopicResult {
                name: "t".to_string(),
                topic_id: Some([9_u8; 16]),
                error_code: 0,
                error_message: None,
                topic_config_error_code: Some(0),
                num_partitions: Some(1),
                replication_factor: Some(1),
                configs: Some(vec![CreateTopicsResponseCreatableTopicConfigs {
                    name: "k".to_string(),
                    value: None,
                    read_only: false,
                    config_source: 1,
                    is_sensitive: false,
                }]),
            },
        ],
    }
}

#[test]
fn create_topics_response_roundtrip_all_versions() {
    for version in CreateTopicsResponse::MIN_VERSION..=CreateTopicsResponse::MAX_VERSION {
        let mut expected = sample_create_topics_response();
        if version < 7 {
            expected.topics[0].topic_id = None;
        }
        if version < 5 {
            expected.topics[0].topic_config_error_code = None;
            expected.topics[0].num_partitions = None;
            expected.topics[0].replication_factor = None;
            expected.topics[0].configs = None;
        }
        let wire = expected.clone();
        roundtrip(&wire, version);
        assert_eq!(wire, expected);
    }
}

#[test]
fn create_topics_response_v7_known_bytes() {
    let encoded = sample_create_topics_response().encode(7).expect("encode");
    let mut expected = vec![
        0, 0, 0, 3,    // throttle_time_ms
        0x02, // topics compact len 1+1
        0x02, b't', // name
    ];
    expected.extend_from_slice(&[9_u8; 16]); // topic_id
    expected.extend_from_slice(&[
        0, 0,    // error_code
        0x00, // error_message null (compact nullable string)
        0, 0, 0, 1, // num_partitions
        0, 1,    // replication_factor
        0x02, // configs compact len 1+1
        0x02, b'k', // config name
        0x00, // config value null
        0x00, // read_only
        1,    // config_source (int8)
        0x00, // is_sensitive
        0x00, // config element tagged section
        0x01, // topic element tagged section count 1
        0x00, // tag 0: TopicConfigErrorCode (tagged from v5)
        0x02, // payload size 2
        0, 0,    // topic_config_error_code
        0x00, // top-level tagged section
    ]);
    assert_eq!(encoded, expected);
}

// --------------------------------------------------------------- DeleteTopics

fn sample_delete_topics_request(version: i16) -> DeleteTopicsRequest {
    let mut request = DeleteTopicsRequest {
        timeout_ms: 44,
        ..Default::default()
    };
    if version >= 6 {
        request.topics = Some(vec![DeleteTopicsRequestDeleteTopicState {
            name: Some("b".to_string()),
            topic_id: [7_u8; 16],
        }]);
    } else {
        request.topic_names = Some(vec!["a".to_string()]);
    }
    request
}

#[test]
fn delete_topics_request_roundtrip_all_versions() {
    for version in DeleteTopicsRequest::MIN_VERSION..=DeleteTopicsRequest::MAX_VERSION {
        roundtrip(&sample_delete_topics_request(version), version);
    }
}

#[test]
fn delete_topics_request_v1_known_bytes() {
    // legacy: TopicNames array of string + timeout
    let encoded = sample_delete_topics_request(1).encode(1).expect("encode");
    assert_eq!(encoded, vec![0, 0, 0, 1, 0, 1, b'a', 0, 0, 0, 44]);
}

#[test]
fn delete_topics_request_v6_known_bytes() {
    // flexible v6: Topics array of {nullable name, uuid} + timeout + tagged
    let encoded = sample_delete_topics_request(6).encode(6).expect("encode");
    let mut expected = vec![0x02, 0x02, b'b'];
    expected.extend_from_slice(&[7_u8; 16]);
    expected.extend_from_slice(&[0x00, 0, 0, 0, 44, 0x00]);
    assert_eq!(encoded, expected);
}

#[test]
fn delete_topics_request_v6_null_name() {
    let request = DeleteTopicsRequest {
        topics: Some(vec![DeleteTopicsRequestDeleteTopicState {
            name: None,
            topic_id: [1_u8; 16],
        }]),
        timeout_ms: 0,
        topic_names: None,
    };
    let encoded = request.encode(6).expect("encode");
    // compact nullable string null is a single 0x00 byte
    assert_eq!(encoded[0], 0x02);
    assert_eq!(encoded[1], 0x00);
    let (decoded, read) = DeleteTopicsRequest::decode(6, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded, request);
}

fn sample_delete_topics_response() -> DeleteTopicsResponse {
    DeleteTopicsResponse {
        throttle_time_ms: 6,
        responses: vec![DeleteTopicsResponseDeletableTopicResult {
            name: Some("x".to_string()),
            topic_id: Some([3_u8; 16]),
            error_code: 0,
            error_message: Some("boom".to_string()),
        }],
    }
}

#[test]
fn delete_topics_response_roundtrip_all_versions() {
    for version in DeleteTopicsResponse::MIN_VERSION..=DeleteTopicsResponse::MAX_VERSION {
        let mut expected = sample_delete_topics_response();
        if version < 6 {
            expected.responses[0].topic_id = None;
        }
        if version < 5 {
            expected.responses[0].error_message = None;
        }
        let wire = expected.clone();
        roundtrip(&wire, version);
        assert_eq!(wire, expected);
    }
}

#[test]
fn delete_topics_response_v6_known_bytes() {
    let encoded = sample_delete_topics_response().encode(6).expect("encode");
    let mut expected = vec![0, 0, 0, 6, 0x02, 0x02, b'x'];
    expected.extend_from_slice(&[3_u8; 16]);
    expected.extend_from_slice(&[
        0, 0, // error_code
        0x05, b'b', b'o', b'o', b'm', // error_message "boom" (len 4+1)
        0x00, // element tagged section
        0x00, // top-level tagged section
    ]);
    assert_eq!(encoded, expected);
}

// ------------------------------------------------------------------- negative

#[test]
fn generated_codec_rejects_out_of_range_version() {
    let err = sample_delete_groups_request()
        .encode(DeleteGroupsRequest::MAX_VERSION + 1)
        .expect_err("out of range");
    assert_eq!(
        err,
        ProtocolError::InvalidVersion {
            api: "DeleteGroupsRequest",
            version: DeleteGroupsRequest::MAX_VERSION + 1,
        }
    );
}

#[test]
fn generated_codec_rejects_truncated_input() {
    let encoded = sample_delete_groups_response().encode(2).expect("encode");
    let err =
        DeleteGroupsResponse::decode(2, &encoded[..encoded.len() - 1]).expect_err("truncated");
    assert_eq!(err, ProtocolError::Truncated);
}

#[test]
fn generated_codec_rejects_invalid_utf8_string() {
    let mut encoded = sample_delete_groups_request().encode(0).expect("encode");
    // overwrite the "a" payload byte (index 6: [len4][len2][strlen1][payload])
    encoded[6] = 0xff;
    let err = DeleteGroupsRequest::decode(0, &encoded).expect_err("invalid utf8");
    assert_eq!(err, ProtocolError::InvalidString);
}
