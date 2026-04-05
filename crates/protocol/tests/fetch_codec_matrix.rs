//! Wire matrix tests for the generated Fetch codecs (v4-18): legacy framing
//! below v12, flexible framing with per-partition tagged sections (v12+) and
//! the top-level NodeEndpoints tag (v16+).

use rafka_protocol::messages::FetchRequestFetchPartition;
use rafka_protocol::messages::FetchRequestFetchTopic;
use rafka_protocol::messages::FetchRequestForgottenTopic;
use rafka_protocol::messages::FetchRequestReplicaState;
use rafka_protocol::messages::FetchResponseAbortedTransaction;
use rafka_protocol::messages::FetchResponseEpochEndOffset;
use rafka_protocol::messages::FetchResponseFetchableTopicResponse;
use rafka_protocol::messages::FetchResponseLeaderIdAndEpoch;
use rafka_protocol::messages::FetchResponseNodeEndpoint;
use rafka_protocol::messages::FetchResponsePartitionData;
use rafka_protocol::messages::FetchResponseSnapshotId;
use rafka_protocol::messages::{FetchRequest, FetchResponse, VersionedCodec};

fn roundtrip<M: VersionedCodec + PartialEq + std::fmt::Debug>(message: &M, version: i16) {
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = M::decode(version, &encoded).expect("decode");
    assert_eq!(
        read,
        encoded.len(),
        "must consume exactly the encoded bytes"
    );
    assert_eq!(&decoded, message, "roundtrip must be lossless");
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

fn request_for(version: i16) -> FetchRequest {
    FetchRequest {
        cluster_id: (version >= 12).then(|| "c-1".to_string()),
        replica_id: (version <= 14).then_some(-1),
        replica_state: (version >= 15).then_some(FetchRequestReplicaState {
            replica_id: -1,
            replica_epoch: 42,
        }),
        max_wait_ms: 1000,
        min_bytes: 1,
        max_bytes: 1_048_576,
        isolation_level: 1,
        session_id: (version >= 7).then_some(7),
        session_epoch: (version >= 7).then_some(-1),
        topics: vec![FetchRequestFetchTopic {
            topic: (version <= 12).then(|| "orders".to_string()),
            topic_id: (version >= 13).then_some([7_u8; 16]),
            partitions: vec![FetchRequestFetchPartition {
                partition: 5,
                current_leader_epoch: (version >= 9).then_some(3),
                fetch_offset: 42,
                last_fetched_epoch: (version >= 12).then_some(-1),
                log_start_offset: (version >= 5).then_some(-1),
                partition_max_bytes: 1_048_576,
                replica_directory_id: (version >= 17).then_some([8_u8; 16]),
                high_watermark: (version >= 18).then_some(99),
            }],
        }],
        forgotten_topics_data: (version >= 7).then(|| {
            vec![FetchRequestForgottenTopic {
                topic: (version <= 12).then(|| "orders".to_string()),
                topic_id: (version >= 13).then_some([9_u8; 16]),
                partitions: vec![0, 1],
            }]
        }),
        rack_id: (version >= 11).then(|| "rack-1".to_string()),
    }
}

fn response_for(version: i16) -> FetchResponse {
    FetchResponse {
        throttle_time_ms: 0,
        error_code: (version >= 7).then_some(0),
        session_id: (version >= 7).then_some(7),
        responses: vec![FetchResponseFetchableTopicResponse {
            topic: (version <= 12).then(|| "orders".to_string()),
            topic_id: (version >= 13).then_some([2_u8; 16]),
            partitions: vec![FetchResponsePartitionData {
                partition_index: 5,
                error_code: 0,
                high_watermark: 100,
                last_stable_offset: 99,
                log_start_offset: (version >= 5).then_some(0),
                diverging_epoch: (version >= 12).then_some(FetchResponseEpochEndOffset {
                    epoch: 1,
                    end_offset: 50,
                }),
                current_leader: (version >= 12).then_some(FetchResponseLeaderIdAndEpoch {
                    leader_id: 2,
                    leader_epoch: 3,
                }),
                snapshot_id: (version >= 12).then_some(FetchResponseSnapshotId {
                    end_offset: 60,
                    epoch: 4,
                }),
                aborted_transactions: Some(vec![FetchResponseAbortedTransaction {
                    producer_id: 70,
                    first_offset: 71,
                }]),
                preferred_read_replica: (version >= 11).then_some(1),
                records: Some(vec![0xAB]),
            }],
        }],
        node_endpoints: (version >= 16).then(|| {
            vec![FetchResponseNodeEndpoint {
                node_id: 1,
                host: "h".to_string(),
                port: 8080,
                rack: None,
            }]
        }),
    }
}

#[test]
fn fetch_request_roundtrip_all_versions() {
    assert_wires(request_for);
}

#[test]
fn fetch_response_roundtrip_all_versions() {
    assert_wires(response_for);
}

#[test]
fn fetch_request_v4_legacy_known_bytes() {
    // v4: legacy framing, no session/forgotten/rack/tagged sections yet
    let mut request = request_for(4);
    request.isolation_level = 0;
    request.topics[0].partitions[0].partition = 0;
    request.topics[0].partitions[0].fetch_offset = 42;
    request.topics[0].topic = Some("t".to_string());
    let encoded = request.encode(4).expect("encode");
    assert_eq!(
        encoded,
        vec![
            0xFF, 0xFF, 0xFF, 0xFF, // replica_id -1
            0, 0, 0x03, 0xE8, // max_wait_ms 1000
            0, 0, 0, 1, // min_bytes 1
            0, 0x10, 0, 0,    // max_bytes 1048576
            0x00, // isolation_level
            0, 0, 0, 1, // topics len 1
            0, 1, b't', // topic "t"
            0, 0, 0, 1, // partitions len 1
            0, 0, 0, 0, // partition 0
            0, 0, 0, 0, 0, 0, 0, 42, // fetch_offset
            0, 0x10, 0, 0, // partition_max_bytes
        ]
    );
}

#[test]
fn fetch_request_v16_known_bytes() {
    // v16: flexible framing; ClusterId (tag 0) and ReplicaState (tag 1) ride
    // in the top-level tagged section instead of the inline body.
    let mut request = request_for(16);
    request.rack_id = Some("r".to_string());
    request.cluster_id = Some("c".to_string());
    let encoded = request.encode(16).expect("encode");
    let mut expected = vec![
        0, 0, 0x03, 0xE8, // max_wait_ms 1000
        0, 0, 0, 1, // min_bytes 1
        0, 0x10, 0, 0,    // max_bytes 1048576
        0x01, // isolation_level
        0, 0, 0, 7, // session_id
        0xFF, 0xFF, 0xFF, 0xFF, // session_epoch -1
        0x02, // topics compact len 1
    ];
    expected.extend_from_slice(&[7_u8; 16]); // topic_id
    expected.extend_from_slice(&[
        0x02, // partitions compact len 1
        0, 0, 0, 5, // partition
        0, 0, 0, 3, // current_leader_epoch
        0, 0, 0, 0, 0, 0, 0, 42, // fetch_offset
        0xFF, 0xFF, 0xFF, 0xFF, // last_fetched_epoch -1
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // log_start_offset -1
        0, 0x10, 0, 0,    // partition_max_bytes
        0x00, // partition element tagged section
        0x00, // topic element tagged section
        0x02, // forgotten_topics_data compact len 1
    ]);
    expected.extend_from_slice(&[9_u8; 16]); // forgotten topic_id
    expected.extend_from_slice(&[
        0x03, // forgotten partitions compact len 2
        0, 0, 0, 0, // partition 0
        0, 0, 0, 1,    // partition 1
        0x00, // forgotten element tagged section
        0x02, b'r', // rack_id "r"
        0x02, // top-level tagged section count 2
        0x00, // tag 0: ClusterId
        0x02, // payload size 2
        0x02, b'c', // compact string "c"
        0x01, // tag 1: ReplicaState
        0x0D, // payload size 13
        0xFF, 0xFF, 0xFF, 0xFF, // replica_id -1
        0, 0, 0, 0, 0, 0, 0, 42,   // replica_epoch 42
        0x00, // payload struct trailing section
    ]);
    assert_eq!(encoded, expected);
}

#[test]
fn fetch_response_v16_known_bytes() {
    // v16: partition-level tagged section carries DivergingEpoch (tag 0),
    // CurrentLeader (tag 1) and SnapshotId (tag 2) payloads; the top-level
    // section carries NodeEndpoints (tag 0).
    let response = response_for(16);
    let encoded = response.encode(16).expect("encode");
    let mut expected = vec![
        0, 0, 0, 0, // throttle_time_ms
        0, 0, // error_code
        0, 0, 0, 7,    // session_id
        0x02, // responses compact len 1
    ];
    expected.extend_from_slice(&[2_u8; 16]); // topic_id
    expected.extend_from_slice(&[
        0x02, // partitions compact len 1
        0, 0, 0, 5, // partition_index
        0, 0, // error_code
        0, 0, 0, 0, 0, 0, 0, 100, // high_watermark
        0, 0, 0, 0, 0, 0, 0, 99, // last_stable_offset
        0, 0, 0, 0, 0, 0, 0, 0,    // log_start_offset
        0x02, // aborted_transactions compact len 1
        0, 0, 0, 0, 0, 0, 0, 70, // producer_id
        0, 0, 0, 0, 0, 0, 0, 71,   // first_offset
        0x00, // aborted element tagged section
        0, 0, 0, 1, // preferred_read_replica
        0x02, 0xAB, // records compact bytes [0xAB]
        0x03, // partition tagged section count 3
        0x00, // tag 0: DivergingEpoch
        0x0D, // payload size 13
        0, 0, 0, 1, // epoch
        0, 0, 0, 0, 0, 0, 0, 50,   // end_offset
        0x00, // payload struct trailing section
        0x01, // tag 1: CurrentLeader
        0x09, // payload size 9
        0, 0, 0, 2, // leader_id
        0, 0, 0, 3,    // leader_epoch
        0x00, // payload struct trailing section
        0x02, // tag 2: SnapshotId
        0x0D, // payload size 13
        0, 0, 0, 0, 0, 0, 0, 60, // end_offset
        0, 0, 0, 4,    // epoch
        0x00, // payload struct trailing section
        0x00, // topic element tagged section
        0x01, // top-level tagged section count 1
        0x00, // tag 0: NodeEndpoints
        0x0D, // payload size 13
        0x02, // endpoints compact len 1
        0, 0, 0, 1, // node_id
        0x02, b'h', // host
        0, 0, 0x1F, 0x90, // port 8080
        0x00, // rack null
        0x00, // endpoint element tagged section
    ]);
    assert_eq!(encoded, expected);
}

#[test]
fn fetch_response_tagged_fields_absent_below_their_versions() {
    // DivergingEpoch is tagged from v12; at v11 (still flexible-adjacent but
    // legacy) it must not appear, and decode leaves it None.
    let mut response = response_for(11);
    response.responses[0].partitions[0].diverging_epoch = None;
    response.responses[0].partitions[0].current_leader = None;
    response.responses[0].partitions[0].snapshot_id = None;
    response.node_endpoints = None;
    roundtrip(&response, 11);
}

#[test]
fn fetch_request_tagged_fields_ignored_before_their_versions() {
    // At v12 ClusterId exists but ReplicaState does not (v15+): the decoded
    // replica_state stays None even though set on the encode side, so strip
    // it before comparing.
    let mut request = request_for(12);
    request.replica_state = None;
    roundtrip(&request, 12);
}
