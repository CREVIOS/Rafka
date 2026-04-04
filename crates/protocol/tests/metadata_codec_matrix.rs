//! Wire matrix tests for the generated metadata-path codecs: Metadata,
//! ListOffsets, OffsetForLeaderEpoch, and DescribeCluster.

use rafka_protocol::messages::MetadataResponseMetadataResponseBroker;
use rafka_protocol::messages::MetadataResponseMetadataResponsePartition;
use rafka_protocol::messages::MetadataResponseMetadataResponseTopic;
use rafka_protocol::messages::{
    DescribeClusterRequest, DescribeClusterResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
    VersionedCodec,
};

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

fn assert_wires<M, F>(build: F)
where
    M: VersionedCodec + PartialEq + std::fmt::Debug + Clone,
    F: Fn(i16) -> M,
{
    for version in M::MIN_VERSION..=M::MAX_VERSION {
        roundtrip(&build(version), version);
    }
}

// ------------------------------------------------------------------ Metadata

#[test]
fn metadata_request_roundtrip_all_versions() {
    assert_wires(|version| MetadataRequest {
        topics: Some(vec![
            rafka_protocol::messages::MetadataRequestMetadataRequestTopic {
                topic_id: (version >= 10).then_some([1_u8; 16]),
                name: (version <= 9).then(|| "orders".to_string()),
            },
        ]),
        allow_auto_topic_creation: (version >= 4).then_some(true),
        include_cluster_authorized_operations: (8..=10).contains(&version).then_some(true),
        include_topic_authorized_operations: (version >= 8).then_some(true),
    });
}

#[test]
fn metadata_request_null_topics_means_all() {
    let encoded = MetadataRequest::default().encode(1).expect("encode");
    // legacy nullable array of structs is -1
    assert_eq!(&encoded[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    let (decoded, read) = MetadataRequest::decode(1, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded.topics, None);
}

#[test]
fn metadata_response_roundtrip_all_versions() {
    assert_wires(|version| {
        let mut topic = MetadataResponseMetadataResponseTopic {
            error_code: 0,
            name: (version <= 11).then(|| "orders".to_string()),
            topic_id: (version >= 10).then_some([2_u8; 16]),
            is_internal: (version >= 1).then_some(false),
            partitions: vec![MetadataResponseMetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: (version >= 7).then_some(5),
                replica_nodes: vec![1, 2],
                isr_nodes: vec![1, 2],
                offline_replicas: (version >= 5).then(|| vec![2]),
            }],
            topic_authorized_operations: (version >= 8).then_some(0x1F),
        };
        if version < 12 {
            topic.name = Some("orders".to_string());
        }
        MetadataResponse {
            throttle_time_ms: (version >= 3).then_some(0),
            brokers: vec![MetadataResponseMetadataResponseBroker {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: (version >= 1).then(|| "rack-1".to_string()),
            }],
            cluster_id: (version >= 2).then(|| "c-1".to_string()),
            controller_id: (version >= 1).then_some(1),
            topics: vec![topic],
            cluster_authorized_operations: (8..=10).contains(&version).then_some(0x1F),
            error_code: (version >= 13).then_some(0),
        }
    });
}

#[test]
fn metadata_response_v12_flexible_known_bytes() {
    // three nesting levels, flexible framing, uuid topic id
    let encoded = MetadataResponse {
        throttle_time_ms: Some(0),
        brokers: vec![MetadataResponseMetadataResponseBroker {
            node_id: 1,
            host: "h".to_string(),
            port: 9,
            rack: None,
        }],
        cluster_id: Some("c".to_string()),
        controller_id: Some(1),
        topics: vec![MetadataResponseMetadataResponseTopic {
            error_code: 0,
            name: None,
            topic_id: Some([2_u8; 16]),
            is_internal: Some(false),
            partitions: vec![MetadataResponseMetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: Some(5),
                replica_nodes: vec![1],
                isr_nodes: vec![1],
                offline_replicas: Some(vec![]),
            }],
            topic_authorized_operations: Some(30),
        }],
        cluster_authorized_operations: None,
        error_code: Some(0),
    }
    .encode(12)
    .expect("encode");
    assert_eq!(encoded[0], 0);
    let mut expected = vec![
        0, 0, 0, 0,    // throttle_time_ms
        0x02, // brokers compact len 1+1
        0, 0, 0, 1, // node_id
        0x02, b'h', // host
        0, 0, 0, 9,    // port
        0x00, // rack null
        0x00, // broker element tagged section
    ];
    expected.extend_from_slice(&[
        0x02, b'c', // cluster_id
        0, 0, 0, 1,    // controller_id
        0x02, // topics compact len 1+1
        0, 0,    // error_code
        0x00, // name null at v12
    ]);
    expected.extend_from_slice(&[2_u8; 16]); // topic_id
    expected.extend_from_slice(&[
        0x00, // is_internal false
        0x02, // partitions compact len 1+1
        0, 0, // error_code
        0, 0, 0, 0, // partition_index
        0, 0, 0, 1, // leader_id
        0, 0, 0, 5,    // leader_epoch
        0x02, // replica_nodes compact len 1+1
        0, 0, 0, 1,    // replica node 1
        0x02, // isr_nodes compact len 1+1
        0, 0, 0, 1,    // isr node 1
        0x01, // offline_replicas compact len 0+1 (empty)
        0x00, // partition element tagged section
        0, 0, 0, 30,   // topic_authorized_operations (int32)
        0x00, // topic element tagged section
        0x00, // top-level tagged section
    ]);
    assert_eq!(encoded, expected);
}

// -------------------------------------------------------------- ListOffsets

#[test]
fn list_offsets_request_roundtrip_all_versions() {
    assert_wires(|version| ListOffsetsRequest {
        replica_id: -1,
        isolation_level: (version >= 2).then_some(1),
        topics: vec![
            rafka_protocol::messages::ListOffsetsRequestListOffsetsTopic {
                name: "orders".to_string(),
                partitions: vec![
                    rafka_protocol::messages::ListOffsetsRequestListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: (version >= 4).then_some(3),
                        timestamp: -1,
                    },
                ],
            },
        ],
        timeout_ms: (version >= 10).then_some(500),
    });
}

#[test]
fn list_offsets_request_v1_known_bytes() {
    let encoded = ListOffsetsRequest {
        replica_id: -1,
        isolation_level: None,
        topics: vec![
            rafka_protocol::messages::ListOffsetsRequestListOffsetsTopic {
                name: "t".to_string(),
                partitions: vec![
                    rafka_protocol::messages::ListOffsetsRequestListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: None,
                        timestamp: -1,
                    },
                ],
            },
        ],
        timeout_ms: None,
    }
    .encode(1)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![
            0xFF, 0xFF, 0xFF, 0xFF, // replica_id -1
            0, 0, 0, 1, // topics len 1
            0, 1, b't', // name
            0, 0, 0, 1, // partitions len 1
            0, 0, 0, 0, // partition_index
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // timestamp -1 (latest)
        ]
    );
}

#[test]
fn list_offsets_response_roundtrip_all_versions() {
    assert_wires(|version| ListOffsetsResponse {
        throttle_time_ms: (version >= 2).then_some(7),
        topics: vec![
            rafka_protocol::messages::ListOffsetsResponseListOffsetsTopicResponse {
                name: "orders".to_string(),
                partitions: vec![
                    rafka_protocol::messages::ListOffsetsResponseListOffsetsPartitionResponse {
                        partition_index: 0,
                        error_code: 0,
                        timestamp: 1_700_000_000_000,
                        offset: 42,
                        leader_epoch: (version >= 4).then_some(3),
                    },
                ],
            },
        ],
    });
}

// ----------------------------------------------------- OffsetForLeaderEpoch

#[test]
fn offset_for_leader_epoch_request_roundtrip_all_versions() {
    assert_wires(|version| OffsetForLeaderEpochRequest {
        replica_id: (version >= 3).then_some(-1),
        topics: vec![
            rafka_protocol::messages::OffsetForLeaderEpochRequestOffsetForLeaderTopic {
                topic: "orders".to_string(),
                partitions: vec![
                    rafka_protocol::messages::OffsetForLeaderEpochRequestOffsetForLeaderPartition {
                        partition: 0,
                        current_leader_epoch: 3,
                        leader_epoch: 2,
                    },
                ],
            },
        ],
    });
}

#[test]
fn offset_for_leader_epoch_response_roundtrip_all_versions() {
    assert_wires(|_| OffsetForLeaderEpochResponse {
        throttle_time_ms: 3,
        topics: vec![
            rafka_protocol::messages::OffsetForLeaderEpochResponseOffsetForLeaderTopicResult {
                topic: "orders".to_string(),
                partitions: vec![
                    rafka_protocol::messages::OffsetForLeaderEpochResponseEpochEndOffset {
                        error_code: 0,
                        partition: 0,
                        leader_epoch: 2,
                        end_offset: 100,
                    },
                ],
            },
        ],
    });
}

// --------------------------------------------------------- DescribeCluster

#[test]
fn describe_cluster_request_v0_known_bytes() {
    // flexible from v0: default request is one bool + the tagged section
    let encoded = DescribeClusterRequest::default().encode(0).expect("encode");
    assert_eq!(encoded, vec![0x00, 0x00]);
}

#[test]
fn describe_cluster_response_roundtrip_all_versions() {
    assert_wires(|version| DescribeClusterResponse {
        throttle_time_ms: 2,
        error_code: 0,
        error_message: None,
        endpoint_type: (version >= 1).then_some(2),
        cluster_id: "c-1".to_string(),
        controller_id: 1,
        brokers: vec![
            rafka_protocol::messages::DescribeClusterResponseDescribeClusterBroker {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: Some("rack-1".to_string()),
                is_fenced: (version >= 2).then_some(false),
            },
        ],
        cluster_authorized_operations: 30,
    });
}
