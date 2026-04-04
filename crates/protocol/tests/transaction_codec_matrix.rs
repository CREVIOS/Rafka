//! Wire matrix tests for the generated transaction-state codecs:
//! DescribeProducers, DescribeTransactions, and ListTransactions.
//! All three APIs are flexible from v0.

use rafka_protocol::messages::DescribeProducersResponsePartitionResponse;
use rafka_protocol::messages::DescribeProducersResponseProducerState;
use rafka_protocol::messages::DescribeProducersResponseTopicResponse;
use rafka_protocol::messages::DescribeTransactionsResponseTopicData;
use rafka_protocol::messages::DescribeTransactionsResponseTransactionState;
use rafka_protocol::messages::ListTransactionsResponseTransactionState;
use rafka_protocol::messages::{
    DescribeProducersRequest, DescribeProducersResponse, DescribeTransactionsRequest,
    DescribeTransactionsResponse, ListTransactionsRequest, ListTransactionsResponse,
    VersionedCodec,
};

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

// --------------------------------------------------------- DescribeProducers

#[test]
fn describe_producers_request_roundtrip_v0() {
    roundtrip(
        &DescribeProducersRequest {
            topics: vec![
                rafka_protocol::messages::DescribeProducersRequestTopicRequest {
                    name: "orders".to_string(),
                    partition_indexes: vec![0, 1, 2],
                },
            ],
        },
        0,
    );
}

#[test]
fn describe_producers_response_roundtrip_v0() {
    roundtrip(
        &DescribeProducersResponse {
            throttle_time_ms: 1,
            topics: vec![DescribeProducersResponseTopicResponse {
                name: "orders".to_string(),
                partitions: vec![DescribeProducersResponsePartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                    active_producers: vec![DescribeProducersResponseProducerState {
                        producer_id: 5000,
                        producer_epoch: 12,
                        last_sequence: 300,
                        last_timestamp: 1_700_000_000_000,
                        coordinator_epoch: 4,
                        current_txn_start_offset: -1,
                    }],
                }],
            }],
        },
        0,
    );
}

#[test]
fn describe_producers_request_v0_known_bytes() {
    let encoded = DescribeProducersRequest {
        topics: vec![
            rafka_protocol::messages::DescribeProducersRequestTopicRequest {
                name: "t".to_string(),
                partition_indexes: vec![0],
            },
        ],
    }
    .encode(0)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![
            0x02, // topics compact len 1+1
            0x02, b't', // name
            0x02, // partition_indexes compact len 1+1
            0, 0, 0, 0,    // partition index 0
            0x00, // topic element tagged section
            0x00, // top-level tagged section
        ]
    );
}

// ------------------------------------------------------ DescribeTransactions

#[test]
fn describe_transactions_request_roundtrip_v0() {
    roundtrip(
        &DescribeTransactionsRequest {
            transactional_ids: vec!["tx-1".to_string(), "tx-2".to_string()],
        },
        0,
    );
}

#[test]
fn describe_transactions_response_roundtrip_v0() {
    roundtrip(
        &DescribeTransactionsResponse {
            throttle_time_ms: 2,
            transaction_states: vec![DescribeTransactionsResponseTransactionState {
                error_code: 0,
                transactional_id: "tx-1".to_string(),
                transaction_state: "Ongoing".to_string(),
                transaction_timeout_ms: 60_000,
                transaction_start_time_ms: 1_700_000_000_000,
                producer_id: 5000,
                producer_epoch: 7,
                topics: vec![DescribeTransactionsResponseTopicData {
                    topic: "orders".to_string(),
                    partitions: vec![0, 1],
                }],
            }],
        },
        0,
    );
}

#[test]
fn describe_transactions_response_v0_known_bytes() {
    let encoded = DescribeTransactionsResponse {
        throttle_time_ms: 0,
        transaction_states: vec![DescribeTransactionsResponseTransactionState {
            error_code: 0,
            transactional_id: "x".to_string(),
            transaction_state: "Empty".to_string(),
            transaction_timeout_ms: 0,
            transaction_start_time_ms: -1,
            producer_id: -1,
            producer_epoch: -1,
            topics: Vec::new(),
        }],
    }
    .encode(0)
    .expect("encode");
    assert_eq!(
        encoded,
        vec![
            0, 0, 0, 0,    // throttle_time_ms
            0x02, // transaction_states compact len 1+1
            0, 0, // error_code
            0x02, b'x', // transactional_id
            0x06, b'E', b'm', b'p', b't', b'y', // transaction_state "Empty"
            0, 0, 0, 0, // transaction_timeout_ms
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // start time -1
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // producer_id -1
            0xFF, 0xFF, // producer_epoch -1
            0x01, // topics compact len 0+1 (empty)
            0x00, // state element tagged section
            0x00, // top-level tagged section
        ]
    );
}

// ---------------------------------------------------------- ListTransactions

#[test]
fn list_transactions_request_roundtrip_all_versions() {
    assert_wires(|version| ListTransactionsRequest {
        state_filters: vec!["Ongoing".to_string()],
        producer_id_filters: vec![5000, 6000],
        duration_filter: (version >= 1).then_some(1000),
        transactional_id_pattern: (version >= 2).then(|| "tx-*".to_string()),
    });
}

#[test]
fn list_transactions_request_v2_null_pattern() {
    let request = ListTransactionsRequest {
        state_filters: Vec::new(),
        producer_id_filters: Vec::new(),
        duration_filter: Some(5),
        transactional_id_pattern: None,
    };
    let encoded = request.encode(2).expect("encode");
    let (decoded, read) = ListTransactionsRequest::decode(2, &encoded).expect("decode");
    assert_eq!(read, encoded.len());
    assert_eq!(decoded, request);
    // last byte before the tagged section is the compact-null pattern
    assert_eq!(encoded[encoded.len() - 2], 0x00);
}

#[test]
fn list_transactions_response_roundtrip_all_versions() {
    assert_wires(|_| ListTransactionsResponse {
        throttle_time_ms: 3,
        error_code: 0,
        unknown_state_filters: vec!["Bogus".to_string()],
        transaction_states: vec![ListTransactionsResponseTransactionState {
            transactional_id: "tx-1".to_string(),
            producer_id: 5000,
            transaction_state: "PrepareCommit".to_string(),
        }],
    });
}
