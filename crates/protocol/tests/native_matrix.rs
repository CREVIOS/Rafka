#![forbid(unsafe_code)]

use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, ApiVersionsResponseApiVersion,
    ApiVersionsResponseFinalizedFeatureKey, ApiVersionsResponseSupportedFeatureKey,
    OffsetCommitRequest, OffsetCommitRequestOffsetCommitRequestPartition,
    OffsetCommitRequestOffsetCommitRequestTopic, OffsetCommitResponse,
    OffsetCommitResponseOffsetCommitResponsePartition,
    OffsetCommitResponseOffsetCommitResponseTopic, OffsetFetchRequest,
    OffsetFetchRequestOffsetFetchRequestGroup, OffsetFetchRequestOffsetFetchRequestTopic,
    OffsetFetchRequestOffsetFetchRequestTopics, OffsetFetchResponse,
    OffsetFetchResponseOffsetFetchResponseGroup, OffsetFetchResponseOffsetFetchResponsePartition,
    OffsetFetchResponseOffsetFetchResponsePartitions, OffsetFetchResponseOffsetFetchResponseTopic,
    OffsetFetchResponseOffsetFetchResponseTopics, ProduceRequest,
    ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData, VersionedCodec,
    API_VERSIONS_MAX_VERSION, API_VERSIONS_MIN_VERSION, OFFSET_COMMIT_MAX_VERSION,
    OFFSET_COMMIT_MIN_VERSION, OFFSET_FETCH_MAX_VERSION, OFFSET_FETCH_MIN_VERSION,
    PRODUCE_MAX_VERSION, PRODUCE_MIN_VERSION,
};
use rafka_protocol::ProtocolError;

fn sample_api_versions_request(version: i16, filled: bool) -> ApiVersionsRequest {
    if filled && version >= 3 {
        ApiVersionsRequest {
            client_software_name: Some("rafka-rs".to_string()),
            client_software_version: Some("1.0.0".to_string()),
        }
    } else if version >= 3 {
        // Kafka generated classes default unset non-nullable compact strings to empty values.
        ApiVersionsRequest {
            client_software_name: Some(String::new()),
            client_software_version: Some(String::new()),
        }
    } else {
        ApiVersionsRequest::default()
    }
}

fn sample_api_versions_response(version: i16, tagged: bool) -> ApiVersionsResponse {
    let base = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![ApiVersionsResponseApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 4,
        }],
        throttle_time_ms: (version >= 1).then_some(123),
        supported_features: None,
        finalized_features_epoch: None,
        finalized_features: None,
        zk_migration_ready: None,
    };
    if version < 3 || !tagged {
        return base;
    }
    ApiVersionsResponse {
        supported_features: Some(vec![ApiVersionsResponseSupportedFeatureKey {
            name: "metadata.version".to_string(),
            min_version: 1,
            max_version: 20,
        }]),
        finalized_features_epoch: Some(7),
        finalized_features: Some(vec![ApiVersionsResponseFinalizedFeatureKey {
            name: "metadata.version".to_string(),
            max_version_level: 20,
            min_version_level: 1,
        }]),
        zk_migration_ready: Some(true),
        ..base
    }
}

fn sample_produce_request(version: i16, payload: bool, multi: bool) -> ProduceRequest {
    let topic_one = ProduceRequestTopicProduceData {
        name: (version <= 12).then_some("topic-a".to_string()),
        topic_id: (version >= 13).then_some({
            let mut id = [0_u8; 16];
            id[15] = 0x0a;
            id
        }),
        partition_data: vec![
            ProduceRequestPartitionProduceData {
                index: 0,
                records: if payload {
                    Some(vec![0, 1, 2, 3])
                } else {
                    None
                },
            },
            ProduceRequestPartitionProduceData {
                index: 1,
                records: Some(vec![]),
            },
        ],
    };

    let mut topics = vec![topic_one];
    if multi {
        topics.push(ProduceRequestTopicProduceData {
            name: (version <= 12).then_some("topic-b".to_string()),
            topic_id: (version >= 13).then_some({
                let mut id = [0_u8; 16];
                id[15] = 0x0b;
                id
            }),
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 5,
                records: if payload { Some(vec![9, 8, 7]) } else { None },
            }],
        });
    }

    ProduceRequest {
        transactional_id: if payload {
            Some("tx-1".to_string())
        } else {
            None
        },
        acks: if payload { -1 } else { 1 },
        timeout_ms: if payload { 9_000 } else { 5_000 },
        topic_data: topics,
    }
}

fn sample_offset_commit_request(version: i16) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: "group-a".to_string(),
        generation_id_or_member_epoch: 7,
        member_id: "member-a".to_string(),
        group_instance_id: (version >= 7).then_some("instance-a".to_string()),
        retention_time_ms: if (2..=4).contains(&version) {
            Some(-1)
        } else {
            None
        },
        topics: vec![OffsetCommitRequestOffsetCommitRequestTopic {
            name: (version <= 9).then_some("topic-a".to_string()),
            topic_id: (version >= 10).then_some({
                let mut id = [0_u8; 16];
                id[15] = 1;
                id
            }),
            partitions: vec![OffsetCommitRequestOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 123,
                committed_leader_epoch: (version >= 6).then_some(5),
                committed_metadata: Some("meta".to_string()),
            }],
        }],
    }
}

fn sample_offset_commit_response(version: i16) -> OffsetCommitResponse {
    OffsetCommitResponse {
        throttle_time_ms: (version >= 3).then_some(12),
        topics: vec![OffsetCommitResponseOffsetCommitResponseTopic {
            name: (version <= 9).then_some("topic-a".to_string()),
            topic_id: (version >= 10).then_some({
                let mut id = [0_u8; 16];
                id[14] = 2;
                id
            }),
            partitions: vec![OffsetCommitResponseOffsetCommitResponsePartition {
                partition_index: 0,
                error_code: 0,
            }],
        }],
    }
}

fn sample_offset_fetch_request(version: i16) -> OffsetFetchRequest {
    if version <= 7 {
        return OffsetFetchRequest {
            group_id: Some("group-a".to_string()),
            topics: if version >= 2 {
                Some(vec![OffsetFetchRequestOffsetFetchRequestTopic {
                    name: "topic-a".to_string(),
                    partition_indexes: vec![0, 1],
                }])
            } else {
                Some(vec![OffsetFetchRequestOffsetFetchRequestTopic {
                    name: "topic-a".to_string(),
                    partition_indexes: vec![0],
                }])
            },
            groups: None,
            require_stable: (version >= 7).then_some(true),
        };
    }

    OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: Some(vec![OffsetFetchRequestOffsetFetchRequestGroup {
            group_id: "group-a".to_string(),
            member_id: (version >= 9).then_some("member-a".to_string()),
            member_epoch: (version >= 9).then_some(9),
            topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                name: (version <= 9).then_some("topic-a".to_string()),
                topic_id: (version >= 10).then_some({
                    let mut id = [0_u8; 16];
                    id[13] = 3;
                    id
                }),
                partition_indexes: vec![2, 3],
            }]),
        }]),
        require_stable: (version >= 7).then_some(false),
    }
}

fn sample_offset_fetch_response(version: i16) -> OffsetFetchResponse {
    if version <= 7 {
        return OffsetFetchResponse {
            throttle_time_ms: (version >= 3).then_some(6),
            topics: Some(vec![OffsetFetchResponseOffsetFetchResponseTopic {
                name: "topic-a".to_string(),
                partitions: vec![OffsetFetchResponseOffsetFetchResponsePartition {
                    partition_index: 0,
                    committed_offset: 101,
                    committed_leader_epoch: (version >= 5).then_some(4),
                    metadata: Some("meta".to_string()),
                    error_code: 0,
                }],
            }]),
            error_code: (version >= 2).then_some(0),
            groups: None,
        };
    }

    OffsetFetchResponse {
        throttle_time_ms: Some(8),
        topics: None,
        error_code: None,
        groups: Some(vec![OffsetFetchResponseOffsetFetchResponseGroup {
            group_id: "group-a".to_string(),
            topics: vec![OffsetFetchResponseOffsetFetchResponseTopics {
                name: (version <= 9).then_some("topic-a".to_string()),
                topic_id: (version >= 10).then_some({
                    let mut id = [0_u8; 16];
                    id[12] = 4;
                    id
                }),
                partitions: vec![OffsetFetchResponseOffsetFetchResponsePartitions {
                    partition_index: 0,
                    committed_offset: 202,
                    committed_leader_epoch: -1,
                    metadata: Some("meta".to_string()),
                    error_code: 0,
                }],
            }],
            error_code: 0,
        }]),
    }
}

fn assert_roundtrip<T>(message: &T, version: i16)
where
    T: VersionedCodec + core::fmt::Debug + PartialEq,
{
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = T::decode(version, &encoded).expect("decode");
    assert_eq!(decoded, *message);
    assert_eq!(read, encoded.len());
}

#[test]
fn api_versions_request_roundtrip_all_versions() {
    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        let minimal = sample_api_versions_request(version, false);
        assert_roundtrip(&minimal, version);
        let filled = sample_api_versions_request(version, true);
        assert_roundtrip(&filled, version);
    }
}

#[test]
fn api_versions_request_skips_unknown_tagged_fields() {
    let request = sample_api_versions_request(3, true);
    let mut encoded = request.encode(3).expect("encode");
    assert_eq!(encoded.pop(), Some(0));
    encoded.extend([1, 9, 2, 0xaa, 0xbb]);
    let (decoded, read) = ApiVersionsRequest::decode(3, &encoded).expect("decode");
    assert_eq!(decoded, request);
    assert_eq!(read, encoded.len());
}

#[test]
fn api_versions_request_invalid_compact_string_length_is_rejected() {
    let bytes = vec![0x00, 0x02, b'a', 0x00];
    let err = ApiVersionsRequest::decode(3, &bytes).expect_err("invalid compact string length");
    assert_eq!(err, ProtocolError::InvalidCompactLength(0));
}

#[test]
fn api_versions_response_roundtrip_all_versions() {
    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        let minimal = sample_api_versions_response(version, false);
        assert_roundtrip(&minimal, version);
        let tagged = sample_api_versions_response(version, true);
        assert_roundtrip(&tagged, version);
    }
}

#[test]
fn api_versions_response_skips_unknown_top_level_tag() {
    let response = sample_api_versions_response(3, false);
    let mut encoded = response.encode(3).expect("encode");
    assert_eq!(encoded.pop(), Some(0));
    encoded.extend([1, 9, 3, 1, 2, 3]);
    let (decoded, read) = ApiVersionsResponse::decode(3, &encoded).expect("decode");
    assert_eq!(decoded, response);
    assert_eq!(read, encoded.len());
}

#[test]
fn api_versions_response_invalid_bool_tag_is_rejected() {
    let response = sample_api_versions_response(3, false);
    let mut encoded = response.encode(3).expect("encode");
    assert_eq!(encoded.pop(), Some(0));
    encoded.extend([1, 3, 1, 2]);
    let err = ApiVersionsResponse::decode(3, &encoded).expect_err("invalid bool");
    assert_eq!(err, ProtocolError::InvalidBoolean(2));
}

#[test]
fn api_versions_response_compact_array_zero_length_is_rejected() {
    let bytes = vec![0x00, 0x00, 0x00];
    let err = ApiVersionsResponse::decode(3, &bytes).expect_err("invalid compact array length");
    assert_eq!(err, ProtocolError::InvalidCompactLength(0));
}

#[test]
fn produce_request_roundtrip_all_versions_and_shapes() {
    for version in PRODUCE_MIN_VERSION..=PRODUCE_MAX_VERSION {
        let minimal = sample_produce_request(version, false, false);
        assert_roundtrip(&minimal, version);
        let payload = sample_produce_request(version, true, false);
        assert_roundtrip(&payload, version);
        let multi = sample_produce_request(version, true, true);
        assert_roundtrip(&multi, version);
    }
}

#[test]
fn offset_commit_roundtrip_all_versions() {
    for version in OFFSET_COMMIT_MIN_VERSION..=OFFSET_COMMIT_MAX_VERSION {
        let request = sample_offset_commit_request(version);
        assert_roundtrip(&request, version);

        let response = sample_offset_commit_response(version);
        assert_roundtrip(&response, version);
    }
}

#[test]
fn offset_fetch_roundtrip_all_versions() {
    for version in OFFSET_FETCH_MIN_VERSION..=OFFSET_FETCH_MAX_VERSION {
        let request = sample_offset_fetch_request(version);
        assert_roundtrip(&request, version);

        let response = sample_offset_fetch_response(version);
        assert_roundtrip(&response, version);
    }
}

#[test]
fn offset_fetch_request_supports_null_topics_all_partitions_queries() {
    for version in OFFSET_FETCH_MIN_VERSION..=7 {
        if version < 2 {
            continue;
        }
        let request = OffsetFetchRequest {
            group_id: Some("group-a".to_string()),
            topics: None,
            groups: None,
            require_stable: (version >= 7).then_some(true),
        };
        assert_roundtrip(&request, version);
    }

    for version in 8..=OFFSET_FETCH_MAX_VERSION {
        let request = OffsetFetchRequest {
            group_id: None,
            topics: None,
            groups: Some(vec![OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-a".to_string(),
                member_id: (version >= 9).then_some("member-a".to_string()),
                member_epoch: (version >= 9).then_some(1),
                topics: None,
            }]),
            require_stable: Some(false),
        };
        assert_roundtrip(&request, version);
    }
}

#[test]
fn offset_commit_request_skips_unknown_tags_for_flexible_versions() {
    for version in 8..=OFFSET_COMMIT_MAX_VERSION {
        let request = sample_offset_commit_request(version);
        let mut encoded = request.encode(version).expect("encode");
        assert_eq!(encoded.pop(), Some(0));
        encoded.extend([1, 11, 2, 0xde, 0xad]);
        let (decoded, read) = OffsetCommitRequest::decode(version, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }
}

#[test]
fn offset_fetch_response_skips_unknown_tags_for_flexible_versions() {
    for version in 6..=OFFSET_FETCH_MAX_VERSION {
        let response = sample_offset_fetch_response(version);
        let mut encoded = response.encode(version).expect("encode");
        assert_eq!(encoded.pop(), Some(0));
        encoded.extend([1, 7, 1, 0xff]);
        let (decoded, read) = OffsetFetchResponse::decode(version, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }
}

#[test]
fn produce_request_skips_unknown_top_level_tag_for_flexible_versions() {
    for version in 9..=PRODUCE_MAX_VERSION {
        let request = sample_produce_request(version, true, false);
        let mut encoded = request.encode(version).expect("encode");
        assert_eq!(encoded.pop(), Some(0));
        encoded.extend([1, 15, 2, 0xde, 0xad]);
        let (decoded, read) = ProduceRequest::decode(version, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }
}

#[test]
fn produce_request_invalid_non_flexible_array_length_is_rejected() {
    // transactional_id = null, acks = 1, timeout = 5000, topic_data length = -1
    let bytes = vec![
        0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x13, 0x88, 0xff, 0xff, 0xff, 0xff,
    ];
    let err = ProduceRequest::decode(3, &bytes).expect_err("invalid array length");
    assert_eq!(err, ProtocolError::InvalidLength(-1));
}

#[test]
fn produce_request_invalid_flexible_array_length_is_rejected() {
    // transactional_id = null (0), acks = 1, timeout = 5000, topic_data compact length = 0
    let bytes = vec![0x00, 0x00, 0x01, 0x00, 0x00, 0x13, 0x88, 0x00];
    let err = ProduceRequest::decode(9, &bytes).expect_err("invalid compact array length");
    assert_eq!(err, ProtocolError::InvalidCompactLength(0));
}

#[test]
fn offset_commit_request_missing_required_fields_fail() {
    for version in OFFSET_COMMIT_MIN_VERSION..=9 {
        let mut request = sample_offset_commit_request(version);
        request.topics[0].name = None;
        let err = request
            .encode(version)
            .expect_err("missing topic name should fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("OffsetCommitRequest.topics.name")
        );
    }

    let mut request = sample_offset_commit_request(10);
    request.topics[0].topic_id = None;
    let err = request
        .encode(10)
        .expect_err("missing topic id should fail");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("OffsetCommitRequest.topics.topic_id")
    );
}

#[test]
fn offset_fetch_request_missing_required_fields_fail() {
    let request_v1 = OffsetFetchRequest {
        group_id: None,
        topics: Some(vec![]),
        groups: None,
        require_stable: None,
    };
    let err = request_v1
        .encode(1)
        .expect_err("missing group id should fail");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("OffsetFetchRequest.group_id")
    );

    let request_v8 = OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: None,
        require_stable: Some(false),
    };
    let err = request_v8
        .encode(8)
        .expect_err("missing groups should fail");
    assert_eq!(
        err,
        ProtocolError::MissingRequiredField("OffsetFetchRequest.groups")
    );
}

#[test]
fn decode_truncated_inputs_fail_for_representative_versions() {
    let req_v3 = sample_produce_request(3, true, false)
        .encode(3)
        .expect("encode");
    for cut in 0..req_v3.len() {
        let err = ProduceRequest::decode(3, &req_v3[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }

    let req_v9 = sample_produce_request(9, true, false)
        .encode(9)
        .expect("encode");
    for cut in 0..req_v9.len() {
        let err = ProduceRequest::decode(9, &req_v9[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }

    let resp_v1 = sample_api_versions_response(1, false)
        .encode(1)
        .expect("encode");
    for cut in 0..resp_v1.len() {
        let err = ApiVersionsResponse::decode(1, &resp_v1[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
}
