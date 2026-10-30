//! Wire matrix tests for the generated admin-API codecs: config CRUD,
//! partition creation, coordinator discovery, and group describe.

use rafka_protocol::messages::AlterConfigsRequestAlterConfigsResource;
use rafka_protocol::messages::AlterConfigsRequestAlterableConfig;
use rafka_protocol::messages::CreatePartitionsRequestCreatePartitionsAssignment;
use rafka_protocol::messages::CreatePartitionsRequestCreatePartitionsTopic;
use rafka_protocol::messages::DescribeConfigsRequestDescribeConfigsResource;
use rafka_protocol::messages::DescribeConfigsResponseDescribeConfigsResourceResult;
use rafka_protocol::messages::DescribeConfigsResponseDescribeConfigsResult;
use rafka_protocol::messages::DescribeConfigsResponseDescribeConfigsSynonym;
use rafka_protocol::messages::DescribeGroupsResponseDescribedGroup;
use rafka_protocol::messages::DescribeGroupsResponseDescribedGroupMember;
use rafka_protocol::messages::FindCoordinatorResponseCoordinator;
use rafka_protocol::messages::IncrementalAlterConfigsRequestAlterConfigsResource;
use rafka_protocol::messages::IncrementalAlterConfigsRequestAlterableConfig;
use rafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    DescribeConfigsRequest, DescribeConfigsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse, VersionedCodec,
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

fn assert_wires<M, F>(build: F)
where
    M: VersionedCodec + PartialEq + std::fmt::Debug + Clone,
    F: Fn(i16) -> M,
{
    for version in M::MIN_VERSION..=M::MAX_VERSION {
        roundtrip(&build(version), version);
    }
}

// ------------------------------------------------------------ DescribeConfigs

#[test]
fn describe_configs_request_roundtrip_all_versions() {
    assert_wires(|version| DescribeConfigsRequest {
        resources: vec![DescribeConfigsRequestDescribeConfigsResource {
            resource_type: 2,
            resource_name: "orders".to_string(),
            configuration_keys: Some(vec!["min.insync.replicas".to_string()]),
        }],
        include_synonyms: version >= 1,
        include_documentation: (version >= 3).then_some(true),
    });
}

#[test]
fn describe_configs_request_v1_known_bytes() {
    let encoded = DescribeConfigsRequest {
        resources: vec![DescribeConfigsRequestDescribeConfigsResource {
            resource_type: 2,
            resource_name: "o".to_string(),
            configuration_keys: None,
        }],
        include_synonyms: false,
        include_documentation: None,
    }
    .encode(1)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![0, 0, 0, 1, 2, 0, 1, b'o', 0xFF, 0xFF, 0xFF, 0xFF, 0]
    );
}

#[test]
fn describe_configs_response_roundtrip_all_versions() {
    assert_wires(|version| {
        let result = DescribeConfigsResponseDescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: 2,
            resource_name: "orders".to_string(),
            configs: vec![DescribeConfigsResponseDescribeConfigsResourceResult {
                name: "retention.ms".to_string(),
                value: Some("604800000".to_string()),
                read_only: false,
                config_source: 1,
                is_sensitive: false,
                synonyms: vec![DescribeConfigsResponseDescribeConfigsSynonym {
                    name: "log.retention.hours".to_string(),
                    value: Some("168".to_string()),
                    source: 5,
                }],
                config_type: (version >= 3).then_some(1),
                documentation: (version >= 3).then_some("docs".to_string()),
            }],
        };
        DescribeConfigsResponse {
            throttle_time_ms: 0,
            results: vec![result],
        }
    });
}

#[test]
fn describe_configs_response_config_type_absent_before_v1() {
    let response = DescribeConfigsResponse {
        throttle_time_ms: 0,
        results: vec![DescribeConfigsResponseDescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: 2,
            resource_name: "orders".to_string(),
            configs: vec![DescribeConfigsResponseDescribeConfigsResourceResult {
                name: "n".to_string(),
                value: None,
                read_only: false,
                config_source: 0,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: Some(2),
                documentation: Some("d".to_string()),
            }],
        }],
    };
    let encoded = response.encode(2).expect("encode");
    let (decoded, read) = DescribeConfigsResponse::decode(2, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded.results[0].configs[0].config_type, None);
    assert_eq!(decoded.results[0].configs[0].documentation, None);
}

// -------------------------------------------------------------- AlterConfigs

#[test]
fn alter_configs_request_roundtrip_all_versions() {
    assert_wires(|_| AlterConfigsRequest {
        resources: vec![AlterConfigsRequestAlterConfigsResource {
            resource_type: 2,
            resource_name: "orders".to_string(),
            configs: vec![AlterConfigsRequestAlterableConfig {
                name: "retention.ms".to_string(),
                value: Some("86400000".to_string()),
            }],
        }],
        validate_only: true,
    });
}

#[test]
fn alter_configs_response_roundtrip_all_versions() {
    assert_wires(|_| AlterConfigsResponse {
        throttle_time_ms: 4,
        responses: vec![
            rafka_protocol::messages::AlterConfigsResponseAlterConfigsResourceResponse {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                resource_name: "orders".to_string(),
            },
        ],
    });
}

// ---------------------------------------------------- IncrementalAlterConfigs

#[test]
fn incremental_alter_configs_request_roundtrip_all_versions() {
    assert_wires(|_| IncrementalAlterConfigsRequest {
        resources: vec![IncrementalAlterConfigsRequestAlterConfigsResource {
            resource_type: 2,
            resource_name: "orders".to_string(),
            configs: vec![
                IncrementalAlterConfigsRequestAlterableConfig {
                    name: "retention.ms".to_string(),
                    config_operation: 0,
                    value: Some("86400000".to_string()),
                },
                IncrementalAlterConfigsRequestAlterableConfig {
                    name: "compression.type".to_string(),
                    config_operation: 2,
                    value: None,
                },
            ],
        }],
        validate_only: false,
    });
}

#[test]
fn incremental_alter_configs_request_v1_known_bytes() {
    let encoded = IncrementalAlterConfigsRequest {
        resources: vec![IncrementalAlterConfigsRequestAlterConfigsResource {
            resource_type: 2,
            resource_name: "o".to_string(),
            configs: vec![IncrementalAlterConfigsRequestAlterableConfig {
                name: "k".to_string(),
                config_operation: 1,
                value: None,
            }],
        }],
        validate_only: true,
    }
    .encode(1)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![
            0x02, // resources compact len 1+1
            2,    // resource_type (int8)
            0x02, b'o', // resource_name
            0x02, // configs compact len 1+1
            0x02, b'k', // name
            1,    // config_operation (int8 enum: SET)
            0x00, // value null (compact nullable string)
            0x00, // config element tagged section
            0x00, // resource element tagged section
            1,    // validate_only
            0x00, // top-level tagged section
        ]
    );
}

#[test]
fn incremental_alter_configs_response_roundtrip_all_versions() {
    assert_wires(|_| IncrementalAlterConfigsResponse {
        throttle_time_ms: 1,
        responses: vec![
            rafka_protocol::messages::IncrementalAlterConfigsResponseAlterConfigsResourceResponse {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                resource_name: "orders".to_string(),
            },
        ],
    });
}

// --------------------------------------------------------- CreatePartitions

#[test]
fn create_partitions_request_roundtrip_all_versions() {
    assert_wires(|_| CreatePartitionsRequest {
        topics: vec![CreatePartitionsRequestCreatePartitionsTopic {
            name: "orders".to_string(),
            count: 6,
            assignments: Some(vec![CreatePartitionsRequestCreatePartitionsAssignment {
                broker_ids: vec![1, 2, 3],
            }]),
        }],
        timeout_ms: 500,
        validate_only: false,
    });
}

#[test]
fn create_partitions_request_v0_null_assignments() {
    let request = CreatePartitionsRequest {
        topics: vec![CreatePartitionsRequestCreatePartitionsTopic {
            name: "orders".to_string(),
            count: 6,
            assignments: None,
        }],
        timeout_ms: 0,
        validate_only: false,
    };
    let encoded = request.encode(0).expect("encode");
    // legacy nullable array of structs is -1 (after array len + name + count)
    assert_eq!(&encoded[4 + 8 + 4..][..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    let (decoded, read) = CreatePartitionsRequest::decode(0, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded.topics[0].assignments, None);
}

#[test]
fn create_partitions_response_roundtrip_all_versions() {
    assert_wires(|_| CreatePartitionsResponse {
        throttle_time_ms: 9,
        results: vec![
            rafka_protocol::messages::CreatePartitionsResponseCreatePartitionsTopicResult {
                name: "orders".to_string(),
                error_code: 0,
                error_message: None,
            },
        ],
    });
}

// --------------------------------------------------------- FindCoordinator

#[test]
fn find_coordinator_request_roundtrip_all_versions() {
    assert_wires(|version| FindCoordinatorRequest {
        key: (version <= 3).then(|| "g".to_string()),
        key_type: (version >= 1).then_some(0),
        coordinator_keys: (version >= 4).then(|| vec!["g1".to_string(), "g2".to_string()]),
    });
}

#[test]
fn find_coordinator_request_v0_known_bytes() {
    let encoded = FindCoordinatorRequest {
        key: Some("g".to_string()),
        key_type: None,
        coordinator_keys: None,
    }
    .encode(0)
    .expect("encode");
    assert_eq!(encoded, vec![0, 1, b'g']);
}

#[test]
fn find_coordinator_response_roundtrip_all_versions() {
    assert_wires(|version| {
        let mut response = FindCoordinatorResponse {
            throttle_time_ms: Some(2),
            error_code: None,
            error_message: None,
            node_id: None,
            host: None,
            port: None,
            coordinators: Some(vec![FindCoordinatorResponseCoordinator {
                key: "g1".to_string(),
                node_id: 3,
                host: "localhost".to_string(),
                port: 9092,
                error_code: 0,
                error_message: None,
            }]),
        };
        if version <= 3 {
            // v0-3 carry a single inline coordinator instead of the array
            response.error_code = Some(0);
            response.node_id = Some(3);
            response.host = Some("localhost".to_string());
            response.port = Some(9092);
            response.coordinators = None;
        }
        if version == 1 || version == 2 {
            response.error_message = Some("none".to_string());
        }
        if version < 1 {
            response.throttle_time_ms = None;
        }
        response
    });
}

// --------------------------------------------------------- DescribeGroups

#[test]
fn describe_groups_request_roundtrip_all_versions() {
    assert_wires(|version| DescribeGroupsRequest {
        groups: vec!["cg-1".to_string()],
        include_authorized_operations: (version >= 3).then_some(true),
    });
}

#[test]
fn describe_groups_response_roundtrip_all_versions() {
    assert_wires(|version| DescribeGroupsResponse {
        throttle_time_ms: (version >= 1).then_some(8),
        groups: vec![DescribeGroupsResponseDescribedGroup {
            error_code: 0,
            error_message: (version >= 6).then(|| "boom".to_string()),
            group_id: "cg-1".to_string(),
            group_state: "Stable".to_string(),
            protocol_type: "consumer".to_string(),
            protocol_data: "range".to_string(),
            members: vec![DescribeGroupsResponseDescribedGroupMember {
                member_id: "m-1".to_string(),
                group_instance_id: (version >= 4).then(|| "inst-1".to_string()),
                client_id: "c".to_string(),
                client_host: "/127.0.0.1".to_string(),
                member_metadata: vec![1, 2],
                member_assignment: vec![3, 4],
            }],
            authorized_operations: (version >= 3).then_some(0x1F),
        }],
    });
}

#[test]
fn describe_groups_response_v5_flexible_known_bytes() {
    let encoded = DescribeGroupsResponse {
        throttle_time_ms: Some(1),
        groups: vec![DescribeGroupsResponseDescribedGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".to_string(),
            group_state: "Empty".to_string(),
            protocol_type: "consumer".to_string(),
            protocol_data: "".to_string(),
            members: Vec::new(),
            authorized_operations: Some(30),
        }],
    }
    .encode(5)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![
            0, 0, 0, 1,    // throttle_time_ms
            0x02, // groups compact len 1+1
            0, 0, // error_code
            0x02, b'g', // group_id
            0x06, b'E', b'm', b'p', b't', b'y', // group_state "Empty"
            0x09, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // protocol_type
            0x01, // protocol_data "" (compact len 0+1)
            0x01, // members compact len 0+1 (empty)
            0, 0, 0, 30,   // authorized_operations (int32)
            0x00, // group element tagged section
            0x00, // top-level tagged section
        ]
    );
}

// ---------------------------------------------------------------- negative

#[test]
fn admin_codec_rejects_out_of_range_version() {
    let err = DescribeGroupsRequest::default()
        .encode(DescribeGroupsRequest::MAX_VERSION + 1)
        .expect_err("out of range");
    assert_eq!(
        err,
        ProtocolError::InvalidVersion {
            api: "DescribeGroupsRequest",
            version: DescribeGroupsRequest::MAX_VERSION + 1,
        }
    );
}

#[test]
fn admin_codec_rejects_truncated_input() {
    let encoded = FindCoordinatorRequest {
        key: Some("abcdef".to_string()),
        key_type: None,
        coordinator_keys: None,
    }
    .encode(0)
    .expect("encode");
    let err = FindCoordinatorRequest::decode(0, &encoded[..2]).expect_err("truncated");
    assert_eq!(err, ProtocolError::Truncated);
}
