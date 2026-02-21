use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_broker::{
    AclAuthorizer, AclOperation, AclResourceType, PartitionedBroker, PersistentLogConfig,
    TransportError, TransportSecurityConfig, TransportServer,
};
use rafka_protocol::messages::{
    ApiVersionsResponse, OffsetCommitRequest, OffsetCommitRequestOffsetCommitRequestPartition,
    OffsetCommitRequestOffsetCommitRequestTopic, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchRequestOffsetFetchRequestGroup, OffsetFetchRequestOffsetFetchRequestTopic,
    OffsetFetchRequestOffsetFetchRequestTopics, OffsetFetchResponse, ProduceRequest,
    ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData, VersionedCodec,
    API_VERSIONS_MAX_VERSION,
};

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_FETCH: i16 = 1;
const API_KEY_OFFSET_COMMIT: i16 = 8;
const API_KEY_OFFSET_FETCH: i16 = 9;
const API_KEY_JOIN_GROUP: i16 = 11;
const API_KEY_HEARTBEAT: i16 = 12;
const API_KEY_LEAVE_GROUP: i16 = 13;
const API_KEY_SYNC_GROUP: i16 = 14;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_INIT_PRODUCER_ID: i16 = 22;
const API_KEY_END_TXN: i16 = 26;
const API_KEY_WRITE_TXN_MARKERS: i16 = 27;
const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERROR_GROUP_AUTHORIZATION_FAILED: i16 = 30;
const ERROR_CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

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
            .expect("system time should be after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-transport-{label}-{millis}-{}-{counter}",
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

fn log_config() -> PersistentLogConfig {
    PersistentLogConfig {
        base_offset: 0,
        segment_max_records: 8,
        sync_on_append: false,
    }
}

fn bind_server_with_acl(temp: &TempDir, acl: AclAuthorizer) -> TransportServer {
    TransportServer::bind_with_security(
        "127.0.0.1:0",
        temp.path(),
        log_config(),
        TransportSecurityConfig {
            tls_server_config: None,
            sasl: None,
            acl: Some(acl),
        },
    )
    .expect("bind server with acl")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedProduceResponse {
    topics: Vec<DecodedTopicResponse>,
    throttle_time_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedTopicResponse {
    name: Option<String>,
    topic_id: Option<[u8; 16]>,
    partitions: Vec<DecodedPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedPartitionResponse {
    index: i32,
    error_code: i16,
    base_offset: i64,
    log_start_offset: Option<i64>,
    record_error_count: Option<usize>,
    error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFetchResponse {
    throttle_time_ms: i32,
    top_level_error_code: Option<i16>,
    session_id: Option<i32>,
    topics: Vec<DecodedFetchTopicResponse>,
    node_endpoints: Vec<DecodedFetchNodeEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFetchTopicResponse {
    name: Option<String>,
    topic_id: Option<[u8; 16]>,
    partitions: Vec<DecodedFetchPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFetchPartitionResponse {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: Option<i64>,
    aborted_transactions: Option<Vec<DecodedFetchAbortedTransaction>>,
    preferred_read_replica: Option<i32>,
    records: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFetchNodeEndpoint {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedFetchAbortedTransaction {
    producer_id: i64,
    first_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedInitProducerIdResponse {
    throttle_time_ms: i32,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
    ongoing_txn_producer_id: Option<i64>,
    ongoing_txn_producer_epoch: Option<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedEndTxnResponse {
    throttle_time_ms: i32,
    error_code: i16,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedWriteTxnMarkersResponse {
    markers: Vec<DecodedWriteTxnMarkerResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedWriteTxnMarkerResponse {
    producer_id: i64,
    topics: Vec<DecodedWriteTxnMarkerTopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedWriteTxnMarkerTopicResponse {
    name: String,
    partitions: Vec<DecodedWriteTxnMarkerPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedWriteTxnMarkerPartitionResponse {
    partition_index: i32,
    error_code: i16,
}

fn encode_fetch_request_body(
    version: i16,
    topic_name: Option<&str>,
    topic_id: Option<[u8; 16]>,
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
) -> Vec<u8> {
    encode_fetch_request_body_with_isolation(
        version,
        topic_name,
        topic_id,
        partition,
        fetch_offset,
        partition_max_bytes,
        0,
    )
}

fn encode_fetch_request_body_with_isolation(
    version: i16,
    topic_name: Option<&str>,
    topic_id: Option<[u8; 16]>,
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
    isolation_level: i8,
) -> Vec<u8> {
    let flexible = version >= 12;
    let mut out = Vec::new();

    if version <= 14 {
        out.extend_from_slice(&(-1_i32).to_be_bytes());
    }
    out.extend_from_slice(&(500_i32).to_be_bytes());
    out.extend_from_slice(&(1_i32).to_be_bytes());
    out.extend_from_slice(&(1_048_576_i32).to_be_bytes());
    out.push(isolation_level.to_be_bytes()[0]);
    if version >= 7 {
        out.extend_from_slice(&(0_i32).to_be_bytes());
        out.extend_from_slice(&(-1_i32).to_be_bytes());
    }

    write_array_len(&mut out, 1, flexible);
    if version <= 12 {
        write_string(
            &mut out,
            topic_name.expect("topic_name required for fetch versions <= 12"),
            flexible,
        );
    } else {
        out.extend_from_slice(&topic_id.expect("topic_id required for fetch versions >= 13"));
    }

    write_array_len(&mut out, 1, flexible);
    out.extend_from_slice(&partition.to_be_bytes());
    if version >= 9 {
        out.extend_from_slice(&(-1_i32).to_be_bytes());
    }
    out.extend_from_slice(&fetch_offset.to_be_bytes());
    if version >= 12 {
        out.extend_from_slice(&(-1_i32).to_be_bytes());
    }
    if version >= 5 {
        out.extend_from_slice(&(-1_i64).to_be_bytes());
    }
    out.extend_from_slice(&partition_max_bytes.to_be_bytes());
    if flexible {
        let mut partition_tags = Vec::new();
        if version >= 17 {
            partition_tags.push((
                0_u32,
                vec![
                    0xde, 0xad, 0xbe, 0xef, 0x10, 0x20, 0x30, 0x40, 0x55, 0x66, 0x77, 0x88, 0x90,
                    0xab, 0xcd, 0xef,
                ],
            ));
        }
        if version >= 18 {
            partition_tags.push((1_u32, 7_i64.to_be_bytes().to_vec()));
        }
        write_tagged_fields(&mut out, partition_tags);
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }

    if version >= 7 {
        write_array_len(&mut out, 0, flexible);
    }
    if version >= 11 {
        write_string(&mut out, "", flexible);
    }
    if flexible {
        let mut top_level_tags = Vec::new();
        let mut cluster_payload = Vec::new();
        write_nullable_string(&mut cluster_payload, None, true);
        top_level_tags.push((0_u32, cluster_payload));
        if version >= 15 {
            let mut replica_state_payload = Vec::new();
            replica_state_payload.extend_from_slice(&2_i32.to_be_bytes());
            replica_state_payload.extend_from_slice(&1_i64.to_be_bytes());
            top_level_tags.push((1_u32, replica_state_payload));
        }
        write_tagged_fields(&mut out, top_level_tags);
    }

    out
}

fn decode_fetch_response(version: i16, payload: &[u8]) -> DecodedFetchResponse {
    let flexible = version >= 12;
    let mut cursor = 0;

    let throttle_time_ms = if version >= 1 {
        read_i32(payload, &mut cursor).expect("read throttle time")
    } else {
        0
    };
    let top_level_error_code = if version >= 7 {
        Some(read_i16(payload, &mut cursor).expect("read top-level error"))
    } else {
        None
    };
    let session_id = if version >= 7 {
        Some(read_i32(payload, &mut cursor).expect("read session id"))
    } else {
        None
    };

    let topic_count = read_array_len(payload, &mut cursor, flexible).expect("topic count");
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = if version <= 12 {
            Some(read_string(payload, &mut cursor, flexible).expect("topic name"))
        } else {
            None
        };
        let topic_id = if version >= 13 {
            Some(read_uuid(payload, &mut cursor).expect("topic id"))
        } else {
            None
        };

        let partition_count =
            read_array_len(payload, &mut cursor, flexible).expect("partition count");
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let partition_index = read_i32(payload, &mut cursor).expect("partition index");
            let error_code = read_i16(payload, &mut cursor).expect("partition error code");
            let high_watermark = read_i64(payload, &mut cursor).expect("high watermark");
            let last_stable_offset = if version >= 4 {
                read_i64(payload, &mut cursor).expect("last stable offset")
            } else {
                -1
            };
            let log_start_offset = if version >= 5 {
                Some(read_i64(payload, &mut cursor).expect("log start offset"))
            } else {
                None
            };
            let aborted_transactions = if version >= 4 {
                read_nullable_aborted_transactions(payload, &mut cursor, flexible)
                    .expect("decode aborted transactions")
            } else {
                None
            };
            let preferred_read_replica = if version >= 11 {
                Some(read_i32(payload, &mut cursor).expect("preferred read replica"))
            } else {
                None
            };
            let records =
                read_nullable_bytes(payload, &mut cursor, flexible).expect("records bytes");

            if flexible {
                skip_tagged_fields(payload, &mut cursor).expect("partition tagged fields");
            }

            partitions.push(DecodedFetchPartitionResponse {
                partition_index,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                preferred_read_replica,
                records,
            });
        }
        if flexible {
            skip_tagged_fields(payload, &mut cursor).expect("topic tagged fields");
        }

        topics.push(DecodedFetchTopicResponse {
            name,
            topic_id,
            partitions,
        });
    }

    let mut node_endpoints = Vec::new();
    if flexible {
        read_tagged_fields(payload, &mut cursor, |tag, tagged_payload| {
            if version >= 16 && tag == 0 {
                let mut tagged_cursor = 0;
                let endpoint_count = read_array_len(tagged_payload, &mut tagged_cursor, true)?;
                let mut endpoints = Vec::with_capacity(endpoint_count);
                for _ in 0..endpoint_count {
                    let node_id = read_i32(tagged_payload, &mut tagged_cursor)?;
                    let host = read_string(tagged_payload, &mut tagged_cursor, true)?;
                    let port = read_i32(tagged_payload, &mut tagged_cursor)?;
                    let rack = read_nullable_string(tagged_payload, &mut tagged_cursor, true)?;
                    skip_tagged_fields(tagged_payload, &mut tagged_cursor)?;
                    endpoints.push(DecodedFetchNodeEndpoint {
                        node_id,
                        host,
                        port,
                        rack,
                    });
                }
                assert_eq!(
                    tagged_cursor,
                    tagged_payload.len(),
                    "node endpoint tag payload should be fully consumed"
                );
                node_endpoints = endpoints;
            }
            Ok(())
        })
        .expect("top-level tagged fields");
    }

    assert_eq!(
        cursor,
        payload.len(),
        "fetch response should be fully consumed"
    );

    DecodedFetchResponse {
        throttle_time_ms,
        top_level_error_code,
        session_id,
        topics,
        node_endpoints,
    }
}

fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        API_KEY_API_VERSIONS => {
            if api_version >= 3 {
                2
            } else {
                1
            }
        }
        API_KEY_PRODUCE => {
            if api_version >= 9 {
                2
            } else {
                1
            }
        }
        API_KEY_FETCH => {
            if api_version >= 12 {
                2
            } else {
                1
            }
        }
        API_KEY_OFFSET_COMMIT => {
            if api_version >= 8 {
                2
            } else {
                1
            }
        }
        API_KEY_OFFSET_FETCH => {
            if api_version >= 6 {
                2
            } else {
                1
            }
        }
        API_KEY_JOIN_GROUP => {
            if api_version >= 6 {
                2
            } else {
                1
            }
        }
        API_KEY_HEARTBEAT => {
            if api_version >= 4 {
                2
            } else {
                1
            }
        }
        API_KEY_LEAVE_GROUP => {
            if api_version >= 4 {
                2
            } else {
                1
            }
        }
        API_KEY_SYNC_GROUP => {
            if api_version >= 4 {
                2
            } else {
                1
            }
        }
        API_KEY_INIT_PRODUCER_ID => {
            if api_version >= 2 {
                2
            } else {
                1
            }
        }
        API_KEY_END_TXN => {
            if api_version >= 3 {
                2
            } else {
                1
            }
        }
        API_KEY_WRITE_TXN_MARKERS => 2,
        _ => 1,
    }
}

fn encode_request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    body: &[u8],
) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&api_key.to_be_bytes());
    header.extend_from_slice(&api_version.to_be_bytes());
    header.extend_from_slice(&correlation_id.to_be_bytes());

    match client_id {
        None => header.extend_from_slice(&(-1_i16).to_be_bytes()),
        Some(client_id) => {
            let bytes = client_id.as_bytes();
            let len = i16::try_from(bytes.len()).expect("client id length fits i16");
            header.extend_from_slice(&len.to_be_bytes());
            header.extend_from_slice(bytes);
        }
    }

    if request_header_version(api_key, api_version) == 2 {
        write_unsigned_varint(&mut header, 0);
    }

    let frame_len = i32::try_from(header.len() + body.len()).expect("frame len fits i32");
    let mut frame = Vec::with_capacity(4 + header.len() + body.len());
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = i32::from_be_bytes(len_buf);
    assert!(frame_len >= 0, "frame_len must be non-negative");
    let mut frame = vec![0_u8; usize::try_from(frame_len).expect("frame len fits usize")];
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn decode_response_header(frame: &[u8], header_version: i16) -> (i32, usize) {
    let mut cursor = 0;
    let correlation_id = read_i32(frame, &mut cursor).expect("read correlation id");
    if header_version == 1 {
        let tagged_fields =
            read_unsigned_varint(frame, &mut cursor).expect("read tagged field count");
        assert_eq!(
            tagged_fields, 0,
            "expected empty tagged fields in test responses"
        );
    }
    (correlation_id, cursor)
}

fn decode_produce_response(version: i16, payload: &[u8]) -> DecodedProduceResponse {
    let flexible = version >= 9;
    let mut cursor = 0;

    let topic_count = read_array_len(payload, &mut cursor, flexible).expect("topic count");
    let mut topics = Vec::with_capacity(topic_count);

    for _ in 0..topic_count {
        let name = if version <= 12 {
            Some(read_string(payload, &mut cursor, flexible).expect("topic name"))
        } else {
            None
        };

        let topic_id = if version >= 13 {
            Some(read_uuid(payload, &mut cursor).expect("topic id"))
        } else {
            None
        };

        let partition_count =
            read_array_len(payload, &mut cursor, flexible).expect("partition count");
        let mut partitions = Vec::with_capacity(partition_count);

        for _ in 0..partition_count {
            let index = read_i32(payload, &mut cursor).expect("partition index");
            let error_code = read_i16(payload, &mut cursor).expect("error code");
            let base_offset = read_i64(payload, &mut cursor).expect("base offset");

            if version >= 2 {
                let _log_append_time = read_i64(payload, &mut cursor).expect("log append time");
            }

            let log_start_offset = if version >= 5 {
                Some(read_i64(payload, &mut cursor).expect("log start offset"))
            } else {
                None
            };

            let (record_error_count, error_message) = if version >= 8 {
                let record_error_count =
                    read_array_len(payload, &mut cursor, flexible).expect("record error count");
                for _ in 0..record_error_count {
                    let _batch_index = read_i32(payload, &mut cursor).expect("batch index");
                    let _message = read_nullable_string(payload, &mut cursor, flexible)
                        .expect("batch error message");
                }
                let error_message =
                    read_nullable_string(payload, &mut cursor, flexible).expect("error message");
                (Some(record_error_count), error_message)
            } else {
                (None, None)
            };

            if flexible {
                skip_tagged_fields(payload, &mut cursor).expect("skip partition tagged fields");
            }

            partitions.push(DecodedPartitionResponse {
                index,
                error_code,
                base_offset,
                log_start_offset,
                record_error_count,
                error_message,
            });
        }

        if flexible {
            skip_tagged_fields(payload, &mut cursor).expect("skip topic tagged fields");
        }

        topics.push(DecodedTopicResponse {
            name,
            topic_id,
            partitions,
        });
    }

    let throttle_time_ms = if version >= 1 {
        read_i32(payload, &mut cursor).expect("throttle time")
    } else {
        0
    };

    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("skip top-level tagged fields");
    }

    assert_eq!(
        cursor,
        payload.len(),
        "produce response should be fully consumed"
    );

    DecodedProduceResponse {
        topics,
        throttle_time_ms,
    }
}

struct OffsetCommitRequestShape<'a> {
    version: i16,
    group_id: &'a str,
    member_id: &'a str,
    member_epoch: i32,
    topic_name: Option<&'a str>,
    topic_id: Option<[u8; 16]>,
    partition: i32,
    committed_offset: i64,
}

fn encode_offset_commit_request_body(request: OffsetCommitRequestShape<'_>) -> Vec<u8> {
    let OffsetCommitRequestShape {
        version,
        group_id,
        member_id,
        member_epoch,
        topic_name,
        topic_id,
        partition,
        committed_offset,
    } = request;

    OffsetCommitRequest {
        group_id: group_id.to_string(),
        generation_id_or_member_epoch: member_epoch,
        member_id: member_id.to_string(),
        group_instance_id: None,
        retention_time_ms: if (2..=4).contains(&version) {
            Some(-1)
        } else {
            None
        },
        topics: vec![OffsetCommitRequestOffsetCommitRequestTopic {
            name: topic_name.map(ToOwned::to_owned),
            topic_id,
            partitions: vec![OffsetCommitRequestOffsetCommitRequestPartition {
                partition_index: partition,
                committed_offset,
                committed_leader_epoch: (version >= 6).then_some(-1),
                committed_metadata: Some(String::new()),
            }],
        }],
    }
    .encode(version)
    .expect("encode offset commit request")
}

fn decode_offset_commit_response(version: i16, payload: &[u8]) -> OffsetCommitResponse {
    let (decoded, read) = OffsetCommitResponse::decode(version, payload).expect("decode response");
    assert_eq!(read, payload.len(), "response should be fully consumed");
    decoded
}

#[allow(clippy::too_many_arguments)]
fn encode_offset_fetch_request_body(
    version: i16,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: Option<i32>,
    topic_name: Option<&str>,
    topic_id: Option<[u8; 16]>,
    partition: i32,
    all_topics: bool,
) -> Vec<u8> {
    if version <= 7 {
        return OffsetFetchRequest {
            group_id: Some(group_id.to_string()),
            topics: if all_topics {
                None
            } else {
                Some(vec![OffsetFetchRequestOffsetFetchRequestTopic {
                    name: topic_name.expect("topic name required for <=7").to_string(),
                    partition_indexes: vec![partition],
                }])
            },
            groups: None,
            require_stable: (version >= 7).then_some(false),
        }
        .encode(version)
        .expect("encode offset fetch request <=7");
    }

    OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: Some(vec![OffsetFetchRequestOffsetFetchRequestGroup {
            group_id: group_id.to_string(),
            member_id: if version >= 9 {
                member_id.map(ToOwned::to_owned)
            } else {
                None
            },
            member_epoch: if version >= 9 {
                Some(member_epoch.unwrap_or(-1))
            } else {
                None
            },
            topics: if all_topics {
                None
            } else {
                Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                    name: if version <= 9 {
                        topic_name.map(ToOwned::to_owned)
                    } else {
                        None
                    },
                    topic_id: if version >= 10 { topic_id } else { None },
                    partition_indexes: vec![partition],
                }])
            },
        }]),
        require_stable: Some(false),
    }
    .encode(version)
    .expect("encode offset fetch request >=8")
}

fn decode_offset_fetch_response(version: i16, payload: &[u8]) -> OffsetFetchResponse {
    let (decoded, read) = OffsetFetchResponse::decode(version, payload).expect("decode response");
    assert_eq!(read, payload.len(), "response should be fully consumed");
    decoded
}

fn read_array_len(input: &[u8], cursor: &mut usize, flexible: bool) -> Result<usize, String> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Err("compact array length cannot be 0 for non-null array".to_string());
        }
        usize::try_from(raw - 1).map_err(|_| "array length overflow".to_string())
    } else {
        let value = read_i32(input, cursor)?;
        if value < 0 {
            return Err("negative array length".to_string());
        }
        usize::try_from(value).map_err(|_| "array length overflow".to_string())
    }
}

fn write_array_len(out: &mut Vec<u8>, len: usize, flexible: bool) {
    if flexible {
        let len_u32 = u32::try_from(len).expect("array length fits u32");
        write_unsigned_varint(out, len_u32 + 1);
    } else {
        let len_i32 = i32::try_from(len).expect("array length fits i32");
        out.extend_from_slice(&len_i32.to_be_bytes());
    }
}

fn write_string(out: &mut Vec<u8>, value: &str, flexible: bool) {
    let bytes = value.as_bytes();
    if flexible {
        let len_u32 = u32::try_from(bytes.len()).expect("string length fits u32");
        write_unsigned_varint(out, len_u32 + 1);
    } else {
        let len_i16 = i16::try_from(bytes.len()).expect("string length fits i16");
        out.extend_from_slice(&len_i16.to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_nullable_string(out: &mut Vec<u8>, value: Option<&str>, flexible: bool) {
    match value {
        Some(value) => write_string(out, value, flexible),
        None => {
            if flexible {
                write_unsigned_varint(out, 0);
            } else {
                out.extend_from_slice(&(-1_i16).to_be_bytes());
            }
        }
    }
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8], flexible: bool) {
    if flexible {
        let len_u32 = u32::try_from(value.len()).expect("bytes length fits u32");
        write_unsigned_varint(out, len_u32 + 1);
    } else {
        let len_i32 = i32::try_from(value.len()).expect("bytes length fits i32");
        out.extend_from_slice(&len_i32.to_be_bytes());
    }
    out.extend_from_slice(value);
}

fn write_tagged_fields(out: &mut Vec<u8>, mut fields: Vec<(u32, Vec<u8>)>) {
    fields.sort_by_key(|(tag, _)| *tag);
    let count = u32::try_from(fields.len()).expect("tag count fits u32");
    write_unsigned_varint(out, count);
    for (tag, payload) in fields {
        write_unsigned_varint(out, tag);
        let payload_len = u32::try_from(payload.len()).expect("payload len fits u32");
        write_unsigned_varint(out, payload_len);
        out.extend_from_slice(&payload);
    }
}

fn read_string(input: &[u8], cursor: &mut usize, flexible: bool) -> Result<String, String> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Err("compact string length cannot be 0".to_string());
        }
        let len = usize::try_from(raw - 1).map_err(|_| "string length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated string".to_string());
        }
        let bytes = &input[*cursor..*cursor + len];
        *cursor += len;
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8".to_string())
    } else {
        let len = read_i16(input, cursor)?;
        if len < 0 {
            return Err("negative string length".to_string());
        }
        let len = usize::try_from(len).map_err(|_| "string length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated string".to_string());
        }
        let bytes = &input[*cursor..*cursor + len];
        *cursor += len;
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8".to_string())
    }
}

fn read_bytes(input: &[u8], cursor: &mut usize, flexible: bool) -> Result<Vec<u8>, String> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Err("compact bytes length cannot be 0".to_string());
        }
        let len = usize::try_from(raw - 1).map_err(|_| "bytes length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated bytes".to_string());
        }
        let bytes = input[*cursor..*cursor + len].to_vec();
        *cursor += len;
        Ok(bytes)
    } else {
        let len = read_i32(input, cursor)?;
        if len < 0 {
            return Err("invalid bytes length".to_string());
        }
        let len = usize::try_from(len).map_err(|_| "bytes length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated bytes".to_string());
        }
        let bytes = input[*cursor..*cursor + len].to_vec();
        *cursor += len;
        Ok(bytes)
    }
}

fn read_nullable_bytes(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<Option<Vec<u8>>, String> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = usize::try_from(raw - 1).map_err(|_| "bytes length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated bytes".to_string());
        }
        let bytes = input[*cursor..*cursor + len].to_vec();
        *cursor += len;
        Ok(Some(bytes))
    } else {
        let len = read_i32(input, cursor)?;
        if len == -1 {
            return Ok(None);
        }
        if len < -1 {
            return Err("invalid nullable bytes length".to_string());
        }
        let len = usize::try_from(len).map_err(|_| "bytes length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated bytes".to_string());
        }
        let bytes = input[*cursor..*cursor + len].to_vec();
        *cursor += len;
        Ok(Some(bytes))
    }
}

fn read_nullable_aborted_transactions(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<Option<Vec<DecodedFetchAbortedTransaction>>, String> {
    let count = if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Ok(None);
        }
        usize::try_from(raw - 1).map_err(|_| "aborted tx length overflow".to_string())?
    } else {
        let raw = read_i32(input, cursor)?;
        if raw == -1 {
            return Ok(None);
        }
        if raw < -1 {
            return Err("invalid aborted tx array length".to_string());
        }
        usize::try_from(raw).map_err(|_| "aborted tx length overflow".to_string())?
    };

    let mut aborted_transactions = Vec::with_capacity(count);
    for _ in 0..count {
        let producer_id = read_i64(input, cursor)?;
        let first_offset = read_i64(input, cursor)?;
        if flexible {
            skip_tagged_fields(input, cursor)?;
        }
        aborted_transactions.push(DecodedFetchAbortedTransaction {
            producer_id,
            first_offset,
        });
    }
    Ok(Some(aborted_transactions))
}

fn read_nullable_string(
    input: &[u8],
    cursor: &mut usize,
    flexible: bool,
) -> Result<Option<String>, String> {
    if flexible {
        let raw = read_unsigned_varint(input, cursor)?;
        if raw == 0 {
            return Ok(None);
        }
        let len = usize::try_from(raw - 1).map_err(|_| "string length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated string".to_string());
        }
        let bytes = &input[*cursor..*cursor + len];
        *cursor += len;
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| "invalid utf8".to_string())
    } else {
        let len = read_i16(input, cursor)?;
        if len == -1 {
            return Ok(None);
        }
        if len < -1 {
            return Err("invalid nullable string length".to_string());
        }
        let len = usize::try_from(len).map_err(|_| "string length overflow".to_string())?;
        if input.len() < cursor.saturating_add(len) {
            return Err("truncated string".to_string());
        }
        let bytes = &input[*cursor..*cursor + len];
        *cursor += len;
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| "invalid utf8".to_string())
    }
}

fn read_uuid(input: &[u8], cursor: &mut usize) -> Result<[u8; 16], String> {
    if input.len() < cursor.saturating_add(16) {
        return Err("truncated uuid".to_string());
    }
    let mut value = [0_u8; 16];
    value.copy_from_slice(&input[*cursor..*cursor + 16]);
    *cursor += 16;
    Ok(value)
}

fn read_tagged_fields<F>(input: &[u8], cursor: &mut usize, mut on_field: F) -> Result<(), String>
where
    F: FnMut(u32, &[u8]) -> Result<(), String>,
{
    let num_fields = read_unsigned_varint(input, cursor)?;
    for _ in 0..num_fields {
        let tag = read_unsigned_varint(input, cursor)?;
        let size = read_unsigned_varint(input, cursor)?;
        let size = usize::try_from(size).map_err(|_| "tag size overflow".to_string())?;
        if input.len() < cursor.saturating_add(size) {
            return Err("truncated tagged field".to_string());
        }
        let payload_start = *cursor;
        let payload_end = payload_start + size;
        *cursor = payload_end;
        on_field(tag, &input[payload_start..payload_end])?;
    }
    Ok(())
}

fn skip_tagged_fields(input: &[u8], cursor: &mut usize) -> Result<(), String> {
    read_tagged_fields(input, cursor, |_tag, _payload| Ok(()))
}

fn read_i16(input: &[u8], cursor: &mut usize) -> Result<i16, String> {
    if input.len() < cursor.saturating_add(2) {
        return Err("truncated i16".to_string());
    }
    let value = i16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_u8(input: &[u8], cursor: &mut usize) -> Result<u8, String> {
    if input.len() < cursor.saturating_add(1) {
        return Err("truncated u8".to_string());
    }
    let value = input[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_i32(input: &[u8], cursor: &mut usize) -> Result<i32, String> {
    if input.len() < cursor.saturating_add(4) {
        return Err("truncated i32".to_string());
    }
    let value = i32::from_be_bytes([
        input[*cursor],
        input[*cursor + 1],
        input[*cursor + 2],
        input[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

fn read_i64(input: &[u8], cursor: &mut usize) -> Result<i64, String> {
    if input.len() < cursor.saturating_add(8) {
        return Err("truncated i64".to_string());
    }
    let value = i64::from_be_bytes([
        input[*cursor],
        input[*cursor + 1],
        input[*cursor + 2],
        input[*cursor + 3],
        input[*cursor + 4],
        input[*cursor + 5],
        input[*cursor + 6],
        input[*cursor + 7],
    ]);
    *cursor += 8;
    Ok(value)
}

fn read_unsigned_varint(input: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let mut value = 0_u32;
    let mut shift = 0_u32;
    for _ in 0..5 {
        let Some(&byte) = input.get(*cursor) else {
            return Err("truncated varint".to_string());
        };
        *cursor += 1;
        value |= u32::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err("varint too long".to_string())
}

fn write_unsigned_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn topic_id_hex(topic_id: [u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in topic_id {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JoinGroupRequestShape<'a> {
    version: i16,
    group_id: &'a str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
    protocol_type: &'a str,
    protocol_name: &'a str,
    metadata: &'a [u8],
    reason: Option<&'a str>,
}

fn encode_join_group_request_body(request: JoinGroupRequestShape<'_>) -> Vec<u8> {
    let JoinGroupRequestShape {
        version,
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        group_instance_id,
        protocol_type,
        protocol_name,
        metadata,
        reason,
    } = request;

    let flexible = version >= 6;
    let mut out = Vec::new();
    write_string(&mut out, group_id, flexible);
    out.extend_from_slice(&session_timeout_ms.to_be_bytes());
    if version >= 1 {
        out.extend_from_slice(&rebalance_timeout_ms.to_be_bytes());
    }
    write_string(&mut out, member_id, flexible);
    if version >= 5 {
        write_nullable_string(&mut out, group_instance_id, flexible);
    }
    write_string(&mut out, protocol_type, flexible);

    write_array_len(&mut out, 1, flexible);
    write_string(&mut out, protocol_name, flexible);
    write_bytes(&mut out, metadata, flexible);
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }

    if version >= 8 {
        write_nullable_string(&mut out, reason, flexible);
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedJoinGroupMember {
    member_id: String,
    group_instance_id: Option<String>,
    metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedJoinGroupResponse {
    throttle_time_ms: Option<i32>,
    error_code: i16,
    generation_id: i32,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: String,
    skip_assignment: Option<bool>,
    member_id: String,
    members: Vec<DecodedJoinGroupMember>,
}

fn decode_join_group_response(version: i16, payload: &[u8]) -> DecodedJoinGroupResponse {
    let flexible = version >= 6;
    let mut cursor = 0;

    let throttle_time_ms = if version >= 2 {
        Some(read_i32(payload, &mut cursor).expect("join throttle time"))
    } else {
        None
    };
    let error_code = read_i16(payload, &mut cursor).expect("join error code");
    let generation_id = read_i32(payload, &mut cursor).expect("join generation");
    let protocol_type = if version >= 7 {
        read_nullable_string(payload, &mut cursor, flexible).expect("join protocol type")
    } else {
        None
    };
    let protocol_name = if version >= 7 {
        read_nullable_string(payload, &mut cursor, flexible).expect("join protocol name")
    } else {
        Some(read_string(payload, &mut cursor, flexible).expect("join protocol name"))
    };
    let leader = read_string(payload, &mut cursor, flexible).expect("join leader");
    let skip_assignment = if version >= 9 {
        Some(read_u8(payload, &mut cursor).expect("join skip assignment") != 0)
    } else {
        None
    };
    let member_id = read_string(payload, &mut cursor, flexible).expect("join member id");

    let member_count = read_array_len(payload, &mut cursor, flexible).expect("join member count");
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        let member_id = read_string(payload, &mut cursor, flexible).expect("join member id");
        let group_instance_id = if version >= 5 {
            read_nullable_string(payload, &mut cursor, flexible).expect("join group instance id")
        } else {
            None
        };
        let metadata = read_bytes(payload, &mut cursor, flexible).expect("join member metadata");
        if flexible {
            skip_tagged_fields(payload, &mut cursor).expect("join member tags");
        }
        members.push(DecodedJoinGroupMember {
            member_id,
            group_instance_id,
            metadata,
        });
    }
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("join top-level tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "join response should be fully consumed"
    );

    DecodedJoinGroupResponse {
        throttle_time_ms,
        error_code,
        generation_id,
        protocol_type,
        protocol_name,
        leader,
        skip_assignment,
        member_id,
        members,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncGroupAssignmentShape<'a> {
    member_id: &'a str,
    assignment: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncGroupRequestShape<'a> {
    version: i16,
    group_id: &'a str,
    generation_id: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
    protocol_type: Option<&'a str>,
    protocol_name: Option<&'a str>,
    assignments: Vec<SyncGroupAssignmentShape<'a>>,
}

fn encode_sync_group_request_body(request: SyncGroupRequestShape<'_>) -> Vec<u8> {
    let SyncGroupRequestShape {
        version,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type,
        protocol_name,
        assignments,
    } = request;

    let flexible = version >= 4;
    let mut out = Vec::new();
    write_string(&mut out, group_id, flexible);
    out.extend_from_slice(&generation_id.to_be_bytes());
    write_string(&mut out, member_id, flexible);
    if version >= 3 {
        write_nullable_string(&mut out, group_instance_id, flexible);
    }
    if version >= 5 {
        write_nullable_string(&mut out, protocol_type, flexible);
        write_nullable_string(&mut out, protocol_name, flexible);
    }
    write_array_len(&mut out, assignments.len(), flexible);
    for assignment in assignments {
        write_string(&mut out, assignment.member_id, flexible);
        write_bytes(&mut out, assignment.assignment, flexible);
        if flexible {
            write_unsigned_varint(&mut out, 0);
        }
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedSyncGroupResponse {
    throttle_time_ms: Option<i32>,
    error_code: i16,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    assignment: Vec<u8>,
}

fn decode_sync_group_response(version: i16, payload: &[u8]) -> DecodedSyncGroupResponse {
    let flexible = version >= 4;
    let mut cursor = 0;
    let throttle_time_ms = if version >= 1 {
        Some(read_i32(payload, &mut cursor).expect("sync throttle time"))
    } else {
        None
    };
    let error_code = read_i16(payload, &mut cursor).expect("sync error");
    let protocol_type = if version >= 5 {
        read_nullable_string(payload, &mut cursor, flexible).expect("sync protocol type")
    } else {
        None
    };
    let protocol_name = if version >= 5 {
        read_nullable_string(payload, &mut cursor, flexible).expect("sync protocol name")
    } else {
        None
    };
    let assignment = read_bytes(payload, &mut cursor, flexible).expect("sync assignment");
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("sync tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "sync response should be fully consumed"
    );
    DecodedSyncGroupResponse {
        throttle_time_ms,
        error_code,
        protocol_type,
        protocol_name,
        assignment,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeartbeatRequestShape<'a> {
    version: i16,
    group_id: &'a str,
    generation_id: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
}

fn encode_heartbeat_request_body(request: HeartbeatRequestShape<'_>) -> Vec<u8> {
    let HeartbeatRequestShape {
        version,
        group_id,
        generation_id,
        member_id,
        group_instance_id,
    } = request;

    let flexible = version >= 4;
    let mut out = Vec::new();
    write_string(&mut out, group_id, flexible);
    out.extend_from_slice(&generation_id.to_be_bytes());
    write_string(&mut out, member_id, flexible);
    if version >= 3 {
        write_nullable_string(&mut out, group_instance_id, flexible);
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedHeartbeatResponse {
    throttle_time_ms: Option<i32>,
    error_code: i16,
}

fn decode_heartbeat_response(version: i16, payload: &[u8]) -> DecodedHeartbeatResponse {
    let flexible = version >= 4;
    let mut cursor = 0;
    let throttle_time_ms = if version >= 1 {
        Some(read_i32(payload, &mut cursor).expect("heartbeat throttle time"))
    } else {
        None
    };
    let error_code = read_i16(payload, &mut cursor).expect("heartbeat error code");
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("heartbeat tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "heartbeat response should be fully consumed"
    );
    DecodedHeartbeatResponse {
        throttle_time_ms,
        error_code,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaveGroupMemberShape<'a> {
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
    reason: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaveGroupRequestShape<'a> {
    version: i16,
    group_id: &'a str,
    member_id: Option<&'a str>,
    members: Vec<LeaveGroupMemberShape<'a>>,
}

fn encode_leave_group_request_body(request: LeaveGroupRequestShape<'_>) -> Vec<u8> {
    let LeaveGroupRequestShape {
        version,
        group_id,
        member_id,
        members,
    } = request;
    let flexible = version >= 4;
    let mut out = Vec::new();
    write_string(&mut out, group_id, flexible);
    if version <= 2 {
        write_string(
            &mut out,
            member_id.expect("member id required for leave group <=2"),
            flexible,
        );
    } else {
        write_array_len(&mut out, members.len(), flexible);
        for member in members {
            write_string(&mut out, member.member_id, flexible);
            write_nullable_string(&mut out, member.group_instance_id, flexible);
            if version >= 5 {
                write_nullable_string(&mut out, member.reason, flexible);
            }
            if flexible {
                write_unsigned_varint(&mut out, 0);
            }
        }
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedLeaveGroupMember {
    member_id: String,
    group_instance_id: Option<String>,
    error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedLeaveGroupResponse {
    throttle_time_ms: Option<i32>,
    error_code: i16,
    members: Vec<DecodedLeaveGroupMember>,
}

fn decode_leave_group_response(version: i16, payload: &[u8]) -> DecodedLeaveGroupResponse {
    let flexible = version >= 4;
    let mut cursor = 0;
    let throttle_time_ms = if version >= 1 {
        Some(read_i32(payload, &mut cursor).expect("leave throttle time"))
    } else {
        None
    };
    let error_code = read_i16(payload, &mut cursor).expect("leave error code");
    let members = if version >= 3 {
        let member_count = read_array_len(payload, &mut cursor, flexible).expect("leave members");
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let member_id = read_string(payload, &mut cursor, flexible).expect("leave member id");
            let group_instance_id =
                read_nullable_string(payload, &mut cursor, flexible).expect("leave instance id");
            let error_code = read_i16(payload, &mut cursor).expect("leave member error");
            if flexible {
                skip_tagged_fields(payload, &mut cursor).expect("leave member tags");
            }
            members.push(DecodedLeaveGroupMember {
                member_id,
                group_instance_id,
                error_code,
            });
        }
        members
    } else {
        Vec::new()
    };
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("leave tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "leave response should be fully consumed"
    );
    DecodedLeaveGroupResponse {
        throttle_time_ms,
        error_code,
        members,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitProducerIdRequestShape<'a> {
    version: i16,
    transactional_id: Option<&'a str>,
    transaction_timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
    enable_2pc: bool,
    keep_prepared_txn: bool,
}

fn encode_init_producer_id_request_body(request: InitProducerIdRequestShape<'_>) -> Vec<u8> {
    let InitProducerIdRequestShape {
        version,
        transactional_id,
        transaction_timeout_ms,
        producer_id,
        producer_epoch,
        enable_2pc,
        keep_prepared_txn,
    } = request;
    let flexible = version >= 2;
    let mut out = Vec::new();
    write_nullable_string(&mut out, transactional_id, flexible);
    out.extend_from_slice(&transaction_timeout_ms.to_be_bytes());
    if version >= 3 {
        out.extend_from_slice(&producer_id.to_be_bytes());
        out.extend_from_slice(&producer_epoch.to_be_bytes());
    }
    if version >= 6 {
        out.push(u8::from(enable_2pc));
        out.push(u8::from(keep_prepared_txn));
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

fn decode_init_producer_id_response(version: i16, payload: &[u8]) -> DecodedInitProducerIdResponse {
    let flexible = version >= 2;
    let mut cursor = 0;
    let throttle_time_ms = read_i32(payload, &mut cursor).expect("init producer throttle");
    let error_code = read_i16(payload, &mut cursor).expect("init producer error code");
    let producer_id = read_i64(payload, &mut cursor).expect("init producer id");
    let producer_epoch = read_i16(payload, &mut cursor).expect("init producer epoch");
    let ongoing_txn_producer_id = if version >= 6 {
        Some(read_i64(payload, &mut cursor).expect("ongoing producer id"))
    } else {
        None
    };
    let ongoing_txn_producer_epoch = if version >= 6 {
        Some(read_i16(payload, &mut cursor).expect("ongoing producer epoch"))
    } else {
        None
    };
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("init producer tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "init producer response should be fully consumed"
    );
    DecodedInitProducerIdResponse {
        throttle_time_ms,
        error_code,
        producer_id,
        producer_epoch,
        ongoing_txn_producer_id,
        ongoing_txn_producer_epoch,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EndTxnRequestShape<'a> {
    version: i16,
    transactional_id: &'a str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
}

fn encode_end_txn_request_body(request: EndTxnRequestShape<'_>) -> Vec<u8> {
    let EndTxnRequestShape {
        version,
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
    } = request;
    let flexible = version >= 3;
    let mut out = Vec::new();
    write_string(&mut out, transactional_id, flexible);
    out.extend_from_slice(&producer_id.to_be_bytes());
    out.extend_from_slice(&producer_epoch.to_be_bytes());
    out.push(u8::from(committed));
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

fn decode_end_txn_response(version: i16, payload: &[u8]) -> DecodedEndTxnResponse {
    let flexible = version >= 3;
    let mut cursor = 0;
    let throttle_time_ms = read_i32(payload, &mut cursor).expect("end txn throttle");
    let error_code = read_i16(payload, &mut cursor).expect("end txn error");
    let producer_id = if version >= 5 {
        Some(read_i64(payload, &mut cursor).expect("end txn producer id"))
    } else {
        None
    };
    let producer_epoch = if version >= 5 {
        Some(read_i16(payload, &mut cursor).expect("end txn producer epoch"))
    } else {
        None
    };
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("end txn tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "end txn response should be fully consumed"
    );
    DecodedEndTxnResponse {
        throttle_time_ms,
        error_code,
        producer_id,
        producer_epoch,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteTxnMarkersTopicShape<'a> {
    name: &'a str,
    partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteTxnMarkersShape<'a> {
    producer_id: i64,
    producer_epoch: i16,
    transaction_result: bool,
    topics: Vec<WriteTxnMarkersTopicShape<'a>>,
    coordinator_epoch: i32,
    transaction_version: i8,
}

fn encode_write_txn_markers_request_body(
    version: i16,
    markers: &[WriteTxnMarkersShape<'_>],
) -> Vec<u8> {
    let flexible = version >= 1;
    let mut out = Vec::new();
    write_array_len(&mut out, markers.len(), flexible);
    for marker in markers {
        out.extend_from_slice(&marker.producer_id.to_be_bytes());
        out.extend_from_slice(&marker.producer_epoch.to_be_bytes());
        out.push(u8::from(marker.transaction_result));
        write_array_len(&mut out, marker.topics.len(), flexible);
        for topic in &marker.topics {
            write_string(&mut out, topic.name, flexible);
            write_array_len(&mut out, topic.partition_indexes.len(), flexible);
            for partition_index in &topic.partition_indexes {
                out.extend_from_slice(&partition_index.to_be_bytes());
            }
            if flexible {
                write_unsigned_varint(&mut out, 0);
            }
        }
        out.extend_from_slice(&marker.coordinator_epoch.to_be_bytes());
        if version >= 2 {
            out.extend_from_slice(&marker.transaction_version.to_be_bytes());
        }
        if flexible {
            write_unsigned_varint(&mut out, 0);
        }
    }
    if flexible {
        write_unsigned_varint(&mut out, 0);
    }
    out
}

fn decode_write_txn_markers_response(
    version: i16,
    payload: &[u8],
) -> DecodedWriteTxnMarkersResponse {
    let flexible = version >= 1;
    let mut cursor = 0;
    let marker_count = read_array_len(payload, &mut cursor, flexible).expect("marker count");
    let mut markers = Vec::with_capacity(marker_count);
    for _ in 0..marker_count {
        let producer_id = read_i64(payload, &mut cursor).expect("producer id");
        let topic_count = read_array_len(payload, &mut cursor, flexible).expect("topic count");
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(payload, &mut cursor, flexible).expect("topic name");
            let partition_count =
                read_array_len(payload, &mut cursor, flexible).expect("partition count");
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = read_i32(payload, &mut cursor).expect("partition index");
                let error_code = read_i16(payload, &mut cursor).expect("partition error");
                if flexible {
                    skip_tagged_fields(payload, &mut cursor).expect("partition tags");
                }
                partitions.push(DecodedWriteTxnMarkerPartitionResponse {
                    partition_index,
                    error_code,
                });
            }
            if flexible {
                skip_tagged_fields(payload, &mut cursor).expect("topic tags");
            }
            topics.push(DecodedWriteTxnMarkerTopicResponse { name, partitions });
        }
        if flexible {
            skip_tagged_fields(payload, &mut cursor).expect("marker tags");
        }
        markers.push(DecodedWriteTxnMarkerResponse {
            producer_id,
            topics,
        });
    }
    if flexible {
        skip_tagged_fields(payload, &mut cursor).expect("top level tags");
    }
    assert_eq!(
        cursor,
        payload.len(),
        "write txn markers response should be fully consumed"
    );
    DecodedWriteTxnMarkersResponse { markers }
}

#[test]
fn transport_api_versions_v0_roundtrip() {
    let temp = TempDir::new("transport-apiversions-v0");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let request = encode_request_frame(API_KEY_API_VERSIONS, 0, 101, Some("it-client"), &[]);
    stream.write_all(&request).expect("write request");

    let response_frame = read_frame(&mut stream).expect("read response frame");
    let (correlation_id, header_len) = decode_response_header(&response_frame, 0);
    assert_eq!(correlation_id, 101);

    let (decoded, read) =
        ApiVersionsResponse::decode(0, &response_frame[header_len..]).expect("decode response");
    assert_eq!(read, response_frame.len() - header_len);
    assert_eq!(decoded.error_code, 0);

    assert!(decoded.api_keys.iter().any(|api| api.api_key == 0));
    assert!(decoded.api_keys.iter().any(|api| api.api_key == 1));
    assert!(decoded.api_keys.iter().any(|api| api.api_key == 18));

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_api_versions_v4_uses_request_header_v2_and_response_header_v0() {
    let temp = TempDir::new("transport-apiversions-v4");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let request_body = rafka_protocol::messages::ApiVersionsRequest {
        client_software_name: Some("rafka-client".to_string()),
        client_software_version: Some("1.0.0".to_string()),
    }
    .encode(4)
    .expect("encode ApiVersionsRequest v4");

    let request = encode_request_frame(
        API_KEY_API_VERSIONS,
        4,
        102,
        Some("it-client"),
        &request_body,
    );
    stream.write_all(&request).expect("write request");

    let response_frame = read_frame(&mut stream).expect("read response frame");
    let (correlation_id, header_len) = decode_response_header(&response_frame, 0);
    assert_eq!(correlation_id, 102);

    let (decoded, read) =
        ApiVersionsResponse::decode(4, &response_frame[header_len..]).expect("decode response");
    assert_eq!(read, response_frame.len() - header_len);
    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.throttle_time_ms, Some(0));

    let produce = decoded
        .api_keys
        .iter()
        .find(|api| api.api_key == 0)
        .expect("produce API key present");
    assert_eq!(produce.min_version, 0);
    let fetch = decoded
        .api_keys
        .iter()
        .find(|api| api.api_key == 1)
        .expect("fetch API key present");
    assert_eq!(fetch.min_version, 4);
    assert_eq!(fetch.max_version, 18);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_produce_v3_roundtrip_and_persistence() {
    let temp = TempDir::new("transport-produce-v3");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let request_one = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![1, 2, 3]),
            }],
        }],
    }
    .encode(3)
    .expect("encode first produce request");

    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            301,
            Some("it-client"),
            &request_one,
        ))
        .expect("write first produce request");

    let response_one = read_frame(&mut stream).expect("read first produce response");
    let (correlation_one, header_len_one) = decode_response_header(&response_one, 0);
    assert_eq!(correlation_one, 301);
    let decoded_one = decode_produce_response(3, &response_one[header_len_one..]);
    assert_eq!(decoded_one.topics.len(), 1);
    assert_eq!(decoded_one.topics[0].partitions.len(), 1);
    assert_eq!(decoded_one.topics[0].partitions[0].error_code, 0);
    assert_eq!(decoded_one.topics[0].partitions[0].base_offset, 0);

    let request_two = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![4]),
            }],
        }],
    }
    .encode(3)
    .expect("encode second produce request");

    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            302,
            Some("it-client"),
            &request_two,
        ))
        .expect("write second produce request");

    let response_two = read_frame(&mut stream).expect("read second produce response");
    let (correlation_two, header_len_two) = decode_response_header(&response_two, 0);
    assert_eq!(correlation_two, 302);
    let decoded_two = decode_produce_response(3, &response_two[header_len_two..]);
    assert_eq!(decoded_two.topics[0].partitions[0].base_offset, 1);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");

    let mut reopened = PartitionedBroker::open(temp.path(), log_config()).expect("reopen broker state");
    let persisted = reopened
        .fetch_from_partition("orders", 0, 0, 10)
        .expect("fetch persisted records");

    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].offset, 0);
    assert_eq!(persisted[1].offset, 1);
    assert_eq!(persisted[0].value, vec![1, 2, 3]);
    assert_eq!(persisted[1].value, vec![4]);
}

#[test]
fn transport_produce_v13_topic_id_roundtrip() {
    let temp = TempDir::new("transport-produce-v13");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let topic_id = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x11, 0x12, 0x13, 0x14, 0xaa, 0xbb, 0xcc,
        0xdd,
    ];

    for (correlation_id, expected_offset, payload_byte) in [(401, 0_i64, 9_u8), (402, 1_i64, 8_u8)]
    {
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: None,
                topic_id: Some(topic_id),
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 5,
                    records: Some(vec![payload_byte]),
                }],
            }],
        }
        .encode(13)
        .expect("encode produce request v13");

        stream
            .write_all(&encode_request_frame(
                API_KEY_PRODUCE,
                13,
                correlation_id,
                Some("it-client"),
                &request,
            ))
            .expect("write produce request v13");

        let response = read_frame(&mut stream).expect("read produce response v13");
        let (decoded_corr, header_len) = decode_response_header(&response, 1);
        assert_eq!(decoded_corr, correlation_id);

        let decoded = decode_produce_response(13, &response[header_len..]);
        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].topic_id, Some(topic_id));
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
        assert_eq!(decoded.topics[0].partitions[0].base_offset, expected_offset);
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");

    let mut reopened = PartitionedBroker::open(temp.path(), log_config()).expect("reopen broker state");
    let route_topic = topic_id_hex(topic_id);
    let persisted = reopened
        .fetch_from_partition(&route_topic, 5, 0, 10)
        .expect("fetch persisted topic-id records");

    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].offset, 0);
    assert_eq!(persisted[1].offset, 1);
    assert_eq!(persisted[0].value, vec![9]);
    assert_eq!(persisted[1].value, vec![8]);
}

#[test]
fn transport_api_versions_unsupported_version_falls_back_to_v0_error_response() {
    let temp = TempDir::new("transport-apiversions-unsupported");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let request = encode_request_frame(API_KEY_API_VERSIONS, 99, 501, Some("it-client"), &[]);
    stream
        .write_all(&request)
        .expect("write unsupported request");

    let response = read_frame(&mut stream).expect("read fallback response");
    let (correlation_id, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation_id, 501);

    let (decoded, read) =
        ApiVersionsResponse::decode(0, &response[header_len..]).expect("decode v0 response");
    assert_eq!(read, response.len() - header_len);
    assert_eq!(decoded.error_code, 35);
    assert!(decoded
        .api_keys
        .iter()
        .any(|api| api.api_key == API_KEY_API_VERSIONS
            && api.max_version == API_VERSIONS_MAX_VERSION));

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_unknown_api_key_returns_server_error_and_no_response() {
    let temp = TempDir::new("transport-unknown-api");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let unknown_api_request = encode_request_frame(999, 0, 601, Some("it-client"), &[]);
    stream
        .write_all(&unknown_api_request)
        .expect("write unknown api request");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close connection without response"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(server_result, Err(TransportError::UnsupportedApiKey(999))),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_rejects_oversized_frame() {
    let temp = TempDir::new("transport-oversized-frame");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    server.set_max_frame_size(64);
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let large_body = vec![0_u8; 256];
    let request =
        encode_request_frame(API_KEY_API_VERSIONS, 0, 701, Some("it-client"), &large_body);
    stream.write_all(&request).expect("write oversized request");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close oversized frame connection"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(
            server_result,
            Err(TransportError::FrameTooLarge {
                size: _,
                max_size: 64
            })
        ),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_produce_negative_partition_returns_partition_error() {
    let temp = TempDir::new("transport-produce-negative-partition");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: -1,
                records: Some(vec![1, 2, 3]),
            }],
        }],
    }
    .encode(3)
    .expect("encode invalid produce request");

    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            801,
            Some("it-client"),
            &request,
        ))
        .expect("write invalid produce request");

    let response = read_frame(&mut stream).expect("read invalid produce response");
    let (correlation, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation, 801);
    let decoded = decode_produce_response(3, &response[header_len..]);

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 3);
    assert_eq!(decoded.topics[0].partitions[0].base_offset, -1);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_api_versions_invalid_payload_returns_invalid_request_error() {
    let temp = TempDir::new("transport-apiversions-invalid-payload");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let malformed_body = vec![0_u8];
    stream
        .write_all(&encode_request_frame(
            API_KEY_API_VERSIONS,
            4,
            901,
            Some("it-client"),
            &malformed_body,
        ))
        .expect("write malformed ApiVersions request");

    let response = read_frame(&mut stream).expect("read response");
    let (correlation, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation, 901);

    let (decoded, read) =
        ApiVersionsResponse::decode(4, &response[header_len..]).expect("decode response");
    assert_eq!(read, response.len() - header_len);
    assert_eq!(decoded.error_code, 42);
    assert!(decoded.api_keys.is_empty());
    assert_eq!(decoded.throttle_time_ms, Some(0));

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_produce_v9_roundtrip_uses_flexible_response_header() {
    let temp = TempDir::new("transport-produce-v9");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders-v9".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 2,
                records: Some(vec![1, 9, 9]),
            }],
        }],
    }
    .encode(9)
    .expect("encode produce v9 request");

    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            9,
            902,
            Some("it-client"),
            &request,
        ))
        .expect("write produce v9 request");

    let response = read_frame(&mut stream).expect("read produce v9 response");
    let (correlation, header_len) = decode_response_header(&response, 1);
    assert_eq!(correlation, 902);
    let decoded = decode_produce_response(9, &response[header_len..]);

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].name.as_deref(), Some("orders-v9"));
    assert_eq!(decoded.topics[0].topic_id, None);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    assert_eq!(decoded.topics[0].partitions[0].base_offset, 0);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_produce_empty_topic_name_returns_invalid_topic_error() {
    let temp = TempDir::new("transport-produce-empty-topic");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some(String::new()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![7]),
            }],
        }],
    }
    .encode(3)
    .expect("encode produce request");

    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            903,
            Some("it-client"),
            &request,
        ))
        .expect("write produce request");

    let response = read_frame(&mut stream).expect("read produce response");
    let (correlation, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation, 903);
    let decoded = decode_produce_response(3, &response[header_len..]);

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 17);
    assert_eq!(decoded.topics[0].partitions[0].base_offset, -1);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_produce_unsupported_version_returns_server_error_and_no_response() {
    let temp = TempDir::new("transport-produce-unsupported-version");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            99,
            904,
            Some("it-client"),
            &[],
        ))
        .expect("write unsupported produce request");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close connection without response"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(
            server_result,
            Err(TransportError::UnsupportedApiVersion {
                api_key: API_KEY_PRODUCE,
                api_version: 99
            })
        ),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_rejects_negative_frame_size() {
    let temp = TempDir::new("transport-negative-frame-size");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .write_all(&(-1_i32).to_be_bytes())
        .expect("write negative frame size");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close connection when frame size is invalid"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(server_result, Err(TransportError::InvalidFrameSize(-1))),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_truncated_frame_returns_read_error() {
    let temp = TempDir::new("transport-truncated-frame");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let declared_len = 16_i32.to_be_bytes();
    stream
        .write_all(&declared_len)
        .expect("write frame length prefix");
    stream
        .write_all(&[0_u8; 3])
        .expect("write partial frame bytes");
    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(
            server_result,
            Err(TransportError::Io {
                operation: "read_exact",
                message: _
            })
        ),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_handles_mixed_api_sequence_on_single_connection() {
    let temp = TempDir::new("transport-mixed-sequence");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    stream
        .write_all(&encode_request_frame(
            API_KEY_API_VERSIONS,
            0,
            905,
            Some("it-client"),
            &[],
        ))
        .expect("write ApiVersions v0");
    let response_a = read_frame(&mut stream).expect("read ApiVersions v0 response");
    let (correlation_a, header_len_a) = decode_response_header(&response_a, 0);
    assert_eq!(correlation_a, 905);
    let (decoded_a, read_a) =
        ApiVersionsResponse::decode(0, &response_a[header_len_a..]).expect("decode v0 response");
    assert_eq!(read_a, response_a.len() - header_len_a);
    assert_eq!(decoded_a.error_code, 0);

    let produce = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders-mixed".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 1,
                records: Some(vec![4, 5, 6]),
            }],
        }],
    }
    .encode(3)
    .expect("encode produce v3");
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            906,
            Some("it-client"),
            &produce,
        ))
        .expect("write produce v3");
    let response_b = read_frame(&mut stream).expect("read produce response");
    let (correlation_b, header_len_b) = decode_response_header(&response_b, 0);
    assert_eq!(correlation_b, 906);
    let decoded_b = decode_produce_response(3, &response_b[header_len_b..]);
    assert_eq!(decoded_b.topics.len(), 1);
    assert_eq!(decoded_b.topics[0].partitions[0].error_code, 0);

    let request_v4 = rafka_protocol::messages::ApiVersionsRequest {
        client_software_name: Some("rafka-client".to_string()),
        client_software_version: Some("1.0.0".to_string()),
    }
    .encode(4)
    .expect("encode ApiVersions v4");
    stream
        .write_all(&encode_request_frame(
            API_KEY_API_VERSIONS,
            4,
            907,
            Some("it-client"),
            &request_v4,
        ))
        .expect("write ApiVersions v4");
    let response_c = read_frame(&mut stream).expect("read ApiVersions v4 response");
    let (correlation_c, header_len_c) = decode_response_header(&response_c, 0);
    assert_eq!(correlation_c, 907);
    let (decoded_c, read_c) =
        ApiVersionsResponse::decode(4, &response_c[header_len_c..]).expect("decode v4 response");
    assert_eq!(read_c, response_c.len() - header_len_c);
    assert_eq!(decoded_c.error_code, 0);
    assert_eq!(decoded_c.throttle_time_ms, Some(0));

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_api_versions_matrix_matches_kafka_default_response_contract() {
    let temp = TempDir::new("transport-apiversions-matrix");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    for version in 0..=4_i16 {
        let correlation = 1_100 + i32::from(version);
        let body = if version >= 3 {
            rafka_protocol::messages::ApiVersionsRequest {
                client_software_name: Some("rafka-client".to_string()),
                client_software_version: Some("1.0.0".to_string()),
            }
            .encode(version)
            .expect("encode ApiVersions request")
        } else {
            Vec::new()
        };

        stream
            .write_all(&encode_request_frame(
                API_KEY_API_VERSIONS,
                version,
                correlation,
                Some("it-client"),
                &body,
            ))
            .expect("write ApiVersions request");

        let response = read_frame(&mut stream).expect("read ApiVersions response");
        let (decoded_correlation, header_len) = decode_response_header(&response, 0);
        assert_eq!(decoded_correlation, correlation);

        let (decoded, read) =
            ApiVersionsResponse::decode(version, &response[header_len..]).expect("decode response");
        assert_eq!(read, response.len() - header_len);
        assert_eq!(decoded.error_code, 0);
        if version >= 1 {
            assert_eq!(decoded.throttle_time_ms, Some(0));
        } else {
            assert_eq!(decoded.throttle_time_ms, None);
        }

        let produce = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_PRODUCE)
            .expect("produce api in ApiVersions response");
        assert_eq!(produce.min_version, 0);
        assert_eq!(produce.max_version, 13);

        let fetch = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_FETCH)
            .expect("fetch api in ApiVersions response");
        assert_eq!(fetch.min_version, 4);
        assert_eq!(fetch.max_version, 18);

        let api_versions = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_API_VERSIONS)
            .expect("ApiVersions api in ApiVersions response");
        assert_eq!(api_versions.min_version, 0);
        assert_eq!(api_versions.max_version, 4);

        let offset_commit = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_OFFSET_COMMIT)
            .expect("OffsetCommit api in ApiVersions response");
        assert_eq!(offset_commit.min_version, 2);
        assert_eq!(offset_commit.max_version, 10);

        let offset_fetch = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_OFFSET_FETCH)
            .expect("OffsetFetch api in ApiVersions response");
        assert_eq!(offset_fetch.min_version, 1);
        assert_eq!(offset_fetch.max_version, 10);

        let join_group = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_JOIN_GROUP)
            .expect("JoinGroup api in ApiVersions response");
        assert_eq!(join_group.min_version, 0);
        assert_eq!(join_group.max_version, 9);

        let heartbeat = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_HEARTBEAT)
            .expect("Heartbeat api in ApiVersions response");
        assert_eq!(heartbeat.min_version, 0);
        assert_eq!(heartbeat.max_version, 4);

        let leave_group = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_LEAVE_GROUP)
            .expect("LeaveGroup api in ApiVersions response");
        assert_eq!(leave_group.min_version, 0);
        assert_eq!(leave_group.max_version, 5);

        let sync_group = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_SYNC_GROUP)
            .expect("SyncGroup api in ApiVersions response");
        assert_eq!(sync_group.min_version, 0);
        assert_eq!(sync_group.max_version, 5);

        let init_producer_id = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_INIT_PRODUCER_ID)
            .expect("InitProducerId api in ApiVersions response");
        assert_eq!(init_producer_id.min_version, 0);
        assert_eq!(init_producer_id.max_version, 6);

        let end_txn = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_END_TXN)
            .expect("EndTxn api in ApiVersions response");
        assert_eq!(end_txn.min_version, 0);
        assert_eq!(end_txn.max_version, 5);

        let write_txn_markers = decoded
            .api_keys
            .iter()
            .find(|api| api.api_key == API_KEY_WRITE_TXN_MARKERS)
            .expect("WriteTxnMarkers api in ApiVersions response");
        assert_eq!(write_txn_markers.min_version, 1);
        assert_eq!(write_txn_markers.max_version, 2);
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_produce_response_version_matrix_matches_record_error_gates() {
    let temp = TempDir::new("transport-produce-response-matrix");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let topic_id = [
        0x10, 0x11, 0x12, 0x13, 0x20, 0x21, 0x22, 0x23, 0x30, 0x31, 0x32, 0x33, 0x40, 0x41, 0x42,
        0x43,
    ];

    for version in 3..=13_i16 {
        let correlation = 1_200 + i32::from(version);
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: (version <= 12).then(|| format!("matrix-v{version}")),
                topic_id: (version >= 13).then_some(topic_id),
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 0,
                    records: Some(vec![u8::try_from(version).expect("version in u8 range")]),
                }],
            }],
        }
        .encode(version)
        .expect("encode produce request");

        stream
            .write_all(&encode_request_frame(
                API_KEY_PRODUCE,
                version,
                correlation,
                Some("it-client"),
                &request,
            ))
            .expect("write produce request");

        let response = read_frame(&mut stream).expect("read produce response");
        let expected_header_version = if version >= 9 { 1 } else { 0 };
        let (decoded_correlation, header_len) =
            decode_response_header(&response, expected_header_version);
        assert_eq!(decoded_correlation, correlation);

        let decoded = decode_produce_response(version, &response[header_len..]);
        assert_eq!(decoded.throttle_time_ms, 0);
        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].partitions.len(), 1);
        assert_eq!(decoded.topics[0].partitions[0].index, 0);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
        assert_eq!(decoded.topics[0].partitions[0].base_offset, 0);

        if version <= 12 {
            assert_eq!(
                decoded.topics[0].name.as_deref(),
                Some(format!("matrix-v{version}").as_str())
            );
            assert_eq!(decoded.topics[0].topic_id, None);
        } else {
            assert_eq!(decoded.topics[0].name, None);
            assert_eq!(decoded.topics[0].topic_id, Some(topic_id));
        }

        if version >= 8 {
            assert_eq!(decoded.topics[0].partitions[0].record_error_count, Some(0));
            assert_eq!(decoded.topics[0].partitions[0].error_message, None);
        } else {
            assert_eq!(decoded.topics[0].partitions[0].record_error_count, None);
            assert_eq!(decoded.topics[0].partitions[0].error_message, None);
        }

        if version >= 5 {
            assert_eq!(decoded.topics[0].partitions[0].log_start_offset, Some(0));
        } else {
            assert_eq!(decoded.topics[0].partitions[0].log_start_offset, None);
        }
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_accepts_null_client_id_in_request_header() {
    let temp = TempDir::new("transport-null-client-id");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    stream
        .write_all(&encode_request_frame(
            API_KEY_API_VERSIONS,
            0,
            1_300,
            None,
            &[],
        ))
        .expect("write ApiVersions request with null client id");

    let response = read_frame(&mut stream).expect("read response");
    let (correlation, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation, 1_300);
    let (decoded, read) = ApiVersionsResponse::decode(0, &response[header_len..]).expect("decode");
    assert_eq!(read, response.len() - header_len);
    assert_eq!(decoded.error_code, 0);
    assert!(!decoded.api_keys.is_empty());

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_fetch_v4_roundtrip_and_offset_error() {
    let temp = TempDir::new("transport-fetch-v4");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let produce = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders-fetch".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![1, 2, 3]),
            }],
        }],
    }
    .encode(3)
    .expect("encode produce");
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            1_401,
            Some("it-client"),
            &produce,
        ))
        .expect("write produce");
    let produce_response = read_frame(&mut stream).expect("read produce response");
    let (produce_corr, produce_header) = decode_response_header(&produce_response, 0);
    assert_eq!(produce_corr, 1_401);
    let decoded_produce = decode_produce_response(3, &produce_response[produce_header..]);
    assert_eq!(decoded_produce.topics[0].partitions[0].error_code, 0);

    let fetch = encode_fetch_request_body(4, Some("orders-fetch"), None, 0, 0, 1_024);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            1_402,
            Some("it-client"),
            &fetch,
        ))
        .expect("write fetch v4");

    let response = read_frame(&mut stream).expect("read fetch response");
    let (corr, header_len) = decode_response_header(&response, 0);
    assert_eq!(corr, 1_402);
    let decoded = decode_fetch_response(4, &response[header_len..]);
    assert_eq!(decoded.throttle_time_ms, 0);
    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].name.as_deref(), Some("orders-fetch"));
    assert_eq!(decoded.topics[0].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    assert_eq!(decoded.topics[0].partitions[0].records, Some(vec![1, 2, 3]));

    let fetch_bad_offset = encode_fetch_request_body(4, Some("orders-fetch"), None, 0, 99, 1_024);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            1_403,
            Some("it-client"),
            &fetch_bad_offset,
        ))
        .expect("write fetch v4 out-of-range");

    let bad_response = read_frame(&mut stream).expect("read bad fetch response");
    let (bad_corr, bad_header_len) = decode_response_header(&bad_response, 0);
    assert_eq!(bad_corr, 1_403);
    let bad_decoded = decode_fetch_response(4, &bad_response[bad_header_len..]);
    assert_eq!(bad_decoded.topics.len(), 1);
    assert_eq!(bad_decoded.topics[0].partitions.len(), 1);
    assert_eq!(bad_decoded.topics[0].partitions[0].error_code, 1);
    assert_eq!(bad_decoded.topics[0].partitions[0].records, None);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_fetch_v13_topic_id_roundtrip() {
    let temp = TempDir::new("transport-fetch-v13");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let topic_id = [
        0xaa, 0xbb, 0xcc, 0xdd, 0x10, 0x20, 0x30, 0x40, 0x11, 0x22, 0x33, 0x44, 0xfe, 0xdc, 0xba,
        0x98,
    ];

    for (correlation, payload) in [(1_411, vec![9, 8]), (1_412, vec![7])] {
        let produce = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: None,
                topic_id: Some(topic_id),
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 2,
                    records: Some(payload),
                }],
            }],
        }
        .encode(13)
        .expect("encode produce v13");

        stream
            .write_all(&encode_request_frame(
                API_KEY_PRODUCE,
                13,
                correlation,
                Some("it-client"),
                &produce,
            ))
            .expect("write produce v13");

        let response = read_frame(&mut stream).expect("read produce v13 response");
        let (decoded_corr, header_len) = decode_response_header(&response, 1);
        assert_eq!(decoded_corr, correlation);
        let decoded = decode_produce_response(13, &response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    }

    let fetch = encode_fetch_request_body(13, None, Some(topic_id), 2, 0, 8_192);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            13,
            1_413,
            Some("it-client"),
            &fetch,
        ))
        .expect("write fetch v13");

    let response = read_frame(&mut stream).expect("read fetch v13 response");
    let (corr, header_len) = decode_response_header(&response, 1);
    assert_eq!(corr, 1_413);

    let decoded = decode_fetch_response(13, &response[header_len..]);
    assert_eq!(decoded.throttle_time_ms, 0);
    assert_eq!(decoded.top_level_error_code, Some(0));
    assert_eq!(decoded.session_id, Some(0));
    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].name, None);
    assert_eq!(decoded.topics[0].topic_id, Some(topic_id));
    assert_eq!(decoded.topics[0].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    assert_eq!(decoded.topics[0].partitions[0].records, Some(vec![9, 8, 7]));

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_fetch_unknown_partition_returns_partition_error() {
    let temp = TempDir::new("transport-fetch-unknown-partition");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let fetch = encode_fetch_request_body(4, Some("missing-topic"), None, 9, 0, 1_024);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            1_421,
            Some("it-client"),
            &fetch,
        ))
        .expect("write fetch request");

    let response = read_frame(&mut stream).expect("read fetch response");
    let (corr, header_len) = decode_response_header(&response, 0);
    assert_eq!(corr, 1_421);
    let decoded = decode_fetch_response(4, &response[header_len..]);
    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 3);
    assert_eq!(decoded.topics[0].partitions[0].records, None);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_fetch_unsupported_version_returns_server_error_and_no_response() {
    let temp = TempDir::new("transport-fetch-unsupported-version");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            99,
            1_422,
            Some("it-client"),
            &[],
        ))
        .expect("write unsupported fetch request");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close connection without response"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(
            server_result,
            Err(TransportError::UnsupportedApiVersion {
                api_key: API_KEY_FETCH,
                api_version: 99
            })
        ),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_fetch_v18_truncated_tagged_payload_returns_server_error_and_no_response() {
    let temp = TempDir::new("transport-fetch-v18-truncated-tagged-payload");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let topic_id = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x10,
        0x20,
    ];
    let mut fetch = encode_fetch_request_body(18, None, Some(topic_id), 0, 0, 4_096);
    let _ = fetch.pop();
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            18,
            1_423,
            Some("it-client"),
            &fetch,
        ))
        .expect("write truncated fetch v18 request");

    let mut len_buf = [0_u8; 4];
    let read_result = stream.read_exact(&mut len_buf);
    assert!(
        read_result.is_err(),
        "server should close connection without response"
    );

    let server_result = handle.join().expect("join server thread");
    assert!(
        matches!(
            server_result,
            Err(TransportError::Truncated) | Err(TransportError::InvalidHeader(_))
        ),
        "unexpected server result: {server_result:?}"
    );
}

#[test]
fn transport_fetch_version_matrix_matches_header_and_field_gates() {
    let temp = TempDir::new("transport-fetch-version-matrix");

    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let topic_id = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc,
        0xfe,
    ];

    for version in [4_i16, 7, 11, 12, 13, 14, 15, 16, 17, 18] {
        let produce = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: (version <= 12).then_some("fetch-matrix".to_string()),
                topic_id: (version >= 13).then_some(topic_id),
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 0,
                    records: Some(vec![u8::try_from(version).expect("version fits u8")]),
                }],
            }],
        }
        .encode(if version >= 13 { 13 } else { 3 })
        .expect("encode produce for fetch matrix");

        stream
            .write_all(&encode_request_frame(
                API_KEY_PRODUCE,
                if version >= 13 { 13 } else { 3 },
                1_430 + i32::from(version),
                Some("it-client"),
                &produce,
            ))
            .expect("write produce for fetch matrix");
        let _ = read_frame(&mut stream).expect("read produce response");

        let fetch = encode_fetch_request_body(
            version,
            (version <= 12).then_some("fetch-matrix"),
            (version >= 13).then_some(topic_id),
            0,
            0,
            4_096,
        );
        stream
            .write_all(&encode_request_frame(
                API_KEY_FETCH,
                version,
                1_500 + i32::from(version),
                Some("it-client"),
                &fetch,
            ))
            .expect("write fetch matrix request");

        let response = read_frame(&mut stream).expect("read fetch matrix response");
        let expected_header_version = if version >= 12 { 1 } else { 0 };
        let (corr, header_len) = decode_response_header(&response, expected_header_version);
        assert_eq!(corr, 1_500 + i32::from(version));

        let decoded = decode_fetch_response(version, &response[header_len..]);
        assert_eq!(decoded.throttle_time_ms, 0);
        if version >= 7 {
            assert_eq!(decoded.top_level_error_code, Some(0));
            assert_eq!(decoded.session_id, Some(0));
        } else {
            assert_eq!(decoded.top_level_error_code, None);
            assert_eq!(decoded.session_id, None);
        }
        assert_eq!(decoded.topics.len(), 1);
        if version <= 12 {
            assert_eq!(decoded.topics[0].name.as_deref(), Some("fetch-matrix"));
            assert_eq!(decoded.topics[0].topic_id, None);
        } else {
            assert_eq!(decoded.topics[0].name, None);
            assert_eq!(decoded.topics[0].topic_id, Some(topic_id));
        }

        assert_eq!(decoded.topics[0].partitions.len(), 1);
        let partition = &decoded.topics[0].partitions[0];
        assert_eq!(partition.error_code, 0);
        if version >= 5 {
            assert_eq!(partition.log_start_offset, Some(0));
        } else {
            assert_eq!(partition.log_start_offset, None);
        }
        if version >= 11 {
            assert_eq!(partition.preferred_read_replica, Some(-1));
        } else {
            assert_eq!(partition.preferred_read_replica, None);
        }
        assert!(partition.records.is_some());
        assert!(decoded.node_endpoints.is_empty());
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_commit_fetch_matrix_and_restart_recovery() {
    let temp = TempDir::new("transport-offset-matrix-restart");
    let topic_id = [
        0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e,
        0x9f,
    ];

    let mut last_group = String::new();
    let mut last_partition = 0_i32;
    let mut last_offset = 0_i64;
    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to server");

        for version in 2..=10_i16 {
            let group_id = format!("group-offset-v{version}");
            let partition = i32::from(version % 3);
            let committed_offset = 100 + i64::from(version);

            let commit_body = encode_offset_commit_request_body(OffsetCommitRequestShape {
                version,
                group_id: &group_id,
                member_id: "",
                member_epoch: -1,
                topic_name: (version <= 9)
                    .then(|| format!("topic-v{version}"))
                    .as_deref(),
                topic_id: (version >= 10).then_some(topic_id),
                partition,
                committed_offset,
            });
            stream
                .write_all(&encode_request_frame(
                    API_KEY_OFFSET_COMMIT,
                    version,
                    2_000 + i32::from(version),
                    Some("it-client"),
                    &commit_body,
                ))
                .expect("write offset commit");

            let commit_response = read_frame(&mut stream).expect("read offset commit response");
            let expected_commit_header = if version >= 8 { 1 } else { 0 };
            let (commit_corr, commit_header_len) =
                decode_response_header(&commit_response, expected_commit_header);
            assert_eq!(commit_corr, 2_000 + i32::from(version));
            let commit_decoded =
                decode_offset_commit_response(version, &commit_response[commit_header_len..]);
            assert_eq!(commit_decoded.topics.len(), 1);
            assert_eq!(commit_decoded.topics[0].partitions.len(), 1);
            assert_eq!(commit_decoded.topics[0].partitions[0].error_code, 0);

            let fetch_body = encode_offset_fetch_request_body(
                version,
                &group_id,
                None,
                None,
                (version <= 9)
                    .then(|| format!("topic-v{version}"))
                    .as_deref(),
                (version >= 10).then_some(topic_id),
                partition,
                false,
            );
            stream
                .write_all(&encode_request_frame(
                    API_KEY_OFFSET_FETCH,
                    version,
                    2_100 + i32::from(version),
                    Some("it-client"),
                    &fetch_body,
                ))
                .expect("write offset fetch");

            let fetch_response = read_frame(&mut stream).expect("read offset fetch response");
            let expected_fetch_header = if version >= 6 { 1 } else { 0 };
            let (fetch_corr, fetch_header_len) =
                decode_response_header(&fetch_response, expected_fetch_header);
            assert_eq!(fetch_corr, 2_100 + i32::from(version));
            let fetch_decoded =
                decode_offset_fetch_response(version, &fetch_response[fetch_header_len..]);

            if version <= 7 {
                assert_eq!(fetch_decoded.error_code, (version >= 2).then_some(0));
                let topics = fetch_decoded.topics.expect("legacy topics");
                assert_eq!(topics.len(), 1);
                assert_eq!(topics[0].partitions.len(), 1);
                assert_eq!(topics[0].partitions[0].error_code, 0);
                assert_eq!(topics[0].partitions[0].committed_offset, committed_offset);
            } else {
                let groups = fetch_decoded.groups.expect("grouped response");
                assert_eq!(groups.len(), 1);
                assert_eq!(groups[0].error_code, 0);
                assert_eq!(groups[0].topics.len(), 1);
                assert_eq!(groups[0].topics[0].partitions.len(), 1);
                assert_eq!(groups[0].topics[0].partitions[0].error_code, 0);
                assert_eq!(
                    groups[0].topics[0].partitions[0].committed_offset,
                    committed_offset
                );
            }

            last_group = group_id;
            last_partition = partition;
            last_offset = committed_offset;
        }

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }

    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("rebind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to rebound server");

        let fetch_body = encode_offset_fetch_request_body(
            10,
            &last_group,
            None,
            None,
            None,
            Some(topic_id),
            last_partition,
            false,
        );
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_FETCH,
                10,
                2_999,
                Some("it-client"),
                &fetch_body,
            ))
            .expect("write restart fetch");

        let response = read_frame(&mut stream).expect("read restart fetch response");
        let (corr, header_len) = decode_response_header(&response, 1);
        assert_eq!(corr, 2_999);
        let decoded = decode_offset_fetch_response(10, &response[header_len..]);
        let groups = decoded.groups.expect("group response");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].error_code, 0);
        assert_eq!(groups[0].topics.len(), 1);
        assert_eq!(groups[0].topics[0].partitions.len(), 1);
        assert_eq!(
            groups[0].topics[0].partitions[0].committed_offset,
            last_offset
        );

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }
}

#[test]
fn transport_offset_commit_empty_group_returns_kafka_compatible_error_codes() {
    let temp = TempDir::new("transport-offset-commit-empty-group");
    let topic_id = [0x11_u8; 16];
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for (version, expected_error) in [(8_i16, 22_i16), (9_i16, 69_i16), (10_i16, 69_i16)] {
        let body = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version,
            group_id: "",
            member_id: "member-a",
            member_epoch: 1,
            topic_name: (version <= 9).then_some("topic-a"),
            topic_id: (version >= 10).then_some(topic_id),
            partition: 0,
            committed_offset: 5,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                version,
                3_000 + i32::from(version),
                Some("it-client"),
                &body,
            ))
            .expect("write commit request");
        let response = read_frame(&mut stream).expect("read commit response");
        let (corr, header_len) = decode_response_header(&response, 1);
        assert_eq!(corr, 3_000 + i32::from(version));
        let decoded = decode_offset_commit_response(version, &response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, expected_error);
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_fetch_unknown_group_with_member_returns_group_error() {
    let temp = TempDir::new("transport-offset-fetch-group-error");
    let topic_id = [0x33_u8; 16];
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for version in [9_i16, 10_i16] {
        let body = encode_offset_fetch_request_body(
            version,
            "unknown-group",
            Some("member-a"),
            Some(1),
            (version <= 9).then_some("topic-a"),
            (version >= 10).then_some(topic_id),
            0,
            false,
        );
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_FETCH,
                version,
                3_200 + i32::from(version),
                Some("it-client"),
                &body,
            ))
            .expect("write fetch request");

        let response = read_frame(&mut stream).expect("read fetch response");
        let (corr, header_len) = decode_response_header(&response, 1);
        assert_eq!(corr, 3_200 + i32::from(version));
        let decoded = decode_offset_fetch_response(version, &response[header_len..]);
        let groups = decoded.groups.expect("grouped offset fetch response");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].error_code, 69);
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_fetch_legacy_all_topics_v2_roundtrip() {
    let temp = TempDir::new("transport-offset-fetch-legacy-all-topics");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for (partition, offset) in [(0_i32, 100_i64), (1_i32, 101_i64)] {
        let commit = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version: 2,
            group_id: "legacy-all-cg",
            member_id: "",
            member_epoch: -1,
            topic_name: Some("legacy-topic"),
            topic_id: None,
            partition,
            committed_offset: offset,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                2,
                3_300 + partition,
                Some("it-client"),
                &commit,
            ))
            .expect("write v2 commit");

        let commit_response = read_frame(&mut stream).expect("read v2 commit response");
        let (corr, header_len) = decode_response_header(&commit_response, 0);
        assert_eq!(corr, 3_300 + partition);
        let decoded = decode_offset_commit_response(2, &commit_response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    }

    let fetch_all =
        encode_offset_fetch_request_body(2, "legacy-all-cg", None, None, None, None, 0, true);
    stream
        .write_all(&encode_request_frame(
            API_KEY_OFFSET_FETCH,
            2,
            3_399,
            Some("it-client"),
            &fetch_all,
        ))
        .expect("write v2 fetch-all");

    let fetch_response = read_frame(&mut stream).expect("read v2 fetch-all response");
    let (corr, header_len) = decode_response_header(&fetch_response, 0);
    assert_eq!(corr, 3_399);
    let decoded = decode_offset_fetch_response(2, &fetch_response[header_len..]);
    assert_eq!(decoded.error_code, Some(0));
    let topics = decoded.topics.expect("legacy topics");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "legacy-topic");
    assert_eq!(topics[0].partitions.len(), 2);
    assert_eq!(topics[0].partitions[0].partition_index, 0);
    assert_eq!(topics[0].partitions[0].committed_offset, 100);
    assert_eq!(topics[0].partitions[1].partition_index, 1);
    assert_eq!(topics[0].partitions[1].committed_offset, 101);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_fetch_multigroup_matrix_v8() {
    let temp = TempDir::new("transport-offset-fetch-multigroup-v8");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for (group_id, topic_name, partition, offset) in [
        ("group-a", "topic-a", 0_i32, 100_i64),
        ("group-a", "topic-a", 1_i32, 101_i64),
        ("group-b", "topic-b", 0_i32, 200_i64),
    ] {
        let commit = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version: 8,
            group_id,
            member_id: "",
            member_epoch: -1,
            topic_name: Some(topic_name),
            topic_id: None,
            partition,
            committed_offset: offset,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                8,
                3_400 + partition,
                Some("it-client"),
                &commit,
            ))
            .expect("write v8 commit");
        let commit_response = read_frame(&mut stream).expect("read v8 commit response");
        let (_, header_len) = decode_response_header(&commit_response, 1);
        let decoded = decode_offset_commit_response(8, &commit_response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    }

    let request = OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: Some(vec![
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-a".to_string(),
                member_id: None,
                member_epoch: None,
                topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                    name: Some("topic-a".to_string()),
                    topic_id: None,
                    partition_indexes: vec![0, 1, 5],
                }]),
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-b".to_string(),
                member_id: None,
                member_epoch: None,
                topics: None,
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-empty".to_string(),
                member_id: None,
                member_epoch: None,
                topics: Some(Vec::new()),
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-unknown".to_string(),
                member_id: None,
                member_epoch: None,
                topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                    name: Some("topic-a".to_string()),
                    topic_id: None,
                    partition_indexes: vec![0],
                }]),
            },
        ]),
        require_stable: Some(false),
    }
    .encode(8)
    .expect("encode grouped fetch v8");

    stream
        .write_all(&encode_request_frame(
            API_KEY_OFFSET_FETCH,
            8,
            3_499,
            Some("it-client"),
            &request,
        ))
        .expect("write grouped fetch v8");

    let response = read_frame(&mut stream).expect("read grouped fetch v8 response");
    let (corr, header_len) = decode_response_header(&response, 1);
    assert_eq!(corr, 3_499);
    let decoded = decode_offset_fetch_response(8, &response[header_len..]);
    let groups = decoded.groups.expect("grouped response v8");
    assert_eq!(groups.len(), 4);

    let group_a = groups
        .iter()
        .find(|group| group.group_id == "group-a")
        .expect("group-a response");
    assert_eq!(group_a.error_code, 0);
    assert_eq!(group_a.topics.len(), 1);
    assert_eq!(group_a.topics[0].name.as_deref(), Some("topic-a"));
    assert_eq!(group_a.topics[0].partitions.len(), 3);
    assert_eq!(group_a.topics[0].partitions[0].partition_index, 0);
    assert_eq!(group_a.topics[0].partitions[0].committed_offset, 100);
    assert_eq!(group_a.topics[0].partitions[1].partition_index, 1);
    assert_eq!(group_a.topics[0].partitions[1].committed_offset, 101);
    assert_eq!(group_a.topics[0].partitions[2].partition_index, 5);
    assert_eq!(group_a.topics[0].partitions[2].committed_offset, -1);
    assert_eq!(group_a.topics[0].partitions[2].error_code, 0);

    let group_b = groups
        .iter()
        .find(|group| group.group_id == "group-b")
        .expect("group-b response");
    assert_eq!(group_b.error_code, 0);
    assert_eq!(group_b.topics.len(), 1);
    assert_eq!(group_b.topics[0].name.as_deref(), Some("topic-b"));
    assert_eq!(group_b.topics[0].partitions.len(), 1);
    assert_eq!(group_b.topics[0].partitions[0].partition_index, 0);
    assert_eq!(group_b.topics[0].partitions[0].committed_offset, 200);

    let group_empty = groups
        .iter()
        .find(|group| group.group_id == "group-empty")
        .expect("group-empty response");
    assert_eq!(group_empty.error_code, 0);
    assert!(group_empty.topics.is_empty());

    let group_unknown = groups
        .iter()
        .find(|group| group.group_id == "group-unknown")
        .expect("group-unknown response");
    assert_eq!(group_unknown.error_code, 0);
    assert_eq!(group_unknown.topics.len(), 1);
    assert_eq!(group_unknown.topics[0].partitions.len(), 1);
    assert_eq!(group_unknown.topics[0].partitions[0].committed_offset, -1);
    assert_eq!(group_unknown.topics[0].partitions[0].error_code, 0);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_fetch_multigroup_matrix_v10_with_mixed_group_errors() {
    let temp = TempDir::new("transport-offset-fetch-multigroup-v10");
    let topic_id_a = [0x71_u8; 16];
    let topic_id_b = [0x72_u8; 16];
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for (group_id, topic_id, partition, offset, correlation_id) in [
        ("id-group-a", topic_id_a, 0_i32, 1_000_i64, 3_510_i32),
        ("id-group-b", topic_id_b, 2_i32, 2_000_i64, 3_511_i32),
    ] {
        let commit = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version: 10,
            group_id,
            member_id: "",
            member_epoch: -1,
            topic_name: None,
            topic_id: Some(topic_id),
            partition,
            committed_offset: offset,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                10,
                correlation_id,
                Some("it-client"),
                &commit,
            ))
            .expect("write v10 commit");
        let commit_response = read_frame(&mut stream).expect("read v10 commit response");
        let (_, header_len) = decode_response_header(&commit_response, 1);
        let decoded = decode_offset_commit_response(10, &commit_response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    }

    let request = OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: Some(vec![
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "id-group-a".to_string(),
                member_id: None,
                member_epoch: Some(-1),
                topics: Some(vec![
                    OffsetFetchRequestOffsetFetchRequestTopics {
                        name: None,
                        topic_id: Some(topic_id_a),
                        partition_indexes: vec![0, 3],
                    },
                    OffsetFetchRequestOffsetFetchRequestTopics {
                        name: None,
                        topic_id: Some([0_u8; 16]),
                        partition_indexes: vec![1],
                    },
                ]),
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "id-group-b".to_string(),
                member_id: None,
                member_epoch: Some(-1),
                topics: None,
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "id-group-unknown".to_string(),
                member_id: None,
                member_epoch: Some(-1),
                topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                    name: None,
                    topic_id: Some(topic_id_a),
                    partition_indexes: vec![0],
                }]),
            },
            OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "id-group-auth".to_string(),
                member_id: Some("member-x".to_string()),
                member_epoch: Some(1),
                topics: Some(Vec::new()),
            },
        ]),
        require_stable: Some(false),
    }
    .encode(10)
    .expect("encode grouped fetch v10");

    stream
        .write_all(&encode_request_frame(
            API_KEY_OFFSET_FETCH,
            10,
            3_599,
            Some("it-client"),
            &request,
        ))
        .expect("write grouped fetch v10");

    let response = read_frame(&mut stream).expect("read grouped fetch v10 response");
    let (corr, header_len) = decode_response_header(&response, 1);
    assert_eq!(corr, 3_599);
    let decoded = decode_offset_fetch_response(10, &response[header_len..]);
    let groups = decoded.groups.expect("grouped response v10");
    assert_eq!(groups.len(), 4);

    let group_a = groups
        .iter()
        .find(|group| group.group_id == "id-group-a")
        .expect("id-group-a response");
    assert_eq!(group_a.error_code, 0);
    assert_eq!(group_a.topics.len(), 2);
    assert_eq!(group_a.topics[0].topic_id, Some(topic_id_a));
    assert_eq!(group_a.topics[0].partitions.len(), 2);
    assert_eq!(group_a.topics[0].partitions[0].partition_index, 0);
    assert_eq!(group_a.topics[0].partitions[0].committed_offset, 1_000);
    assert_eq!(group_a.topics[0].partitions[1].partition_index, 3);
    assert_eq!(group_a.topics[0].partitions[1].committed_offset, -1);
    assert_eq!(group_a.topics[0].partitions[1].error_code, 0);
    assert_eq!(group_a.topics[1].topic_id, Some([0_u8; 16]));
    assert_eq!(group_a.topics[1].partitions.len(), 1);
    assert_eq!(group_a.topics[1].partitions[0].error_code, 100);
    assert_eq!(group_a.topics[1].partitions[0].committed_offset, -1);

    let group_b = groups
        .iter()
        .find(|group| group.group_id == "id-group-b")
        .expect("id-group-b response");
    assert_eq!(group_b.error_code, 0);
    assert_eq!(group_b.topics.len(), 1);
    assert_eq!(group_b.topics[0].topic_id, Some(topic_id_b));
    assert_eq!(group_b.topics[0].partitions.len(), 1);
    assert_eq!(group_b.topics[0].partitions[0].partition_index, 2);
    assert_eq!(group_b.topics[0].partitions[0].committed_offset, 2_000);
    assert_eq!(group_b.topics[0].partitions[0].error_code, 0);

    let group_unknown = groups
        .iter()
        .find(|group| group.group_id == "id-group-unknown")
        .expect("id-group-unknown response");
    assert_eq!(group_unknown.error_code, 0);
    assert_eq!(group_unknown.topics.len(), 1);
    assert_eq!(group_unknown.topics[0].topic_id, Some(topic_id_a));
    assert_eq!(group_unknown.topics[0].partitions.len(), 1);
    assert_eq!(group_unknown.topics[0].partitions[0].committed_offset, -1);
    assert_eq!(group_unknown.topics[0].partitions[0].error_code, 0);

    let group_auth = groups
        .iter()
        .find(|group| group.group_id == "id-group-auth")
        .expect("id-group-auth response");
    assert_eq!(group_auth.error_code, 69);
    assert!(group_auth.topics.is_empty());

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_commit_error_matrix_all_versions() {
    let temp = TempDir::new("transport-offset-commit-error-matrix");
    let topic_id = [0x51_u8; 16];
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");

    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for version in 2_i16..=10_i16 {
        let response_header_version = if version >= 8 { 1 } else { 0 };

        let invalid_group = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version,
            group_id: "",
            member_id: "member-a",
            member_epoch: 1,
            topic_name: (version <= 9).then_some("topic-a"),
            topic_id: (version >= 10).then_some(topic_id),
            partition: 0,
            committed_offset: 1,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                version,
                4_000 + i32::from(version),
                Some("it-client"),
                &invalid_group,
            ))
            .expect("write invalid-group commit");

        let invalid_group_response = read_frame(&mut stream).expect("read invalid-group response");
        let (invalid_group_corr, invalid_group_header_len) =
            decode_response_header(&invalid_group_response, response_header_version);
        assert_eq!(invalid_group_corr, 4_000 + i32::from(version));
        let invalid_group_decoded = decode_offset_commit_response(
            version,
            &invalid_group_response[invalid_group_header_len..],
        );
        let expected_group_error = if version >= 9 { 69 } else { 22 };
        assert_eq!(
            invalid_group_decoded.topics[0].partitions[0].error_code,
            expected_group_error
        );

        if version <= 9 {
            let invalid_topic = encode_offset_commit_request_body(OffsetCommitRequestShape {
                version,
                group_id: "group-a",
                member_id: "",
                member_epoch: -1,
                topic_name: Some(""),
                topic_id: None,
                partition: 0,
                committed_offset: 2,
            });
            stream
                .write_all(&encode_request_frame(
                    API_KEY_OFFSET_COMMIT,
                    version,
                    4_100 + i32::from(version),
                    Some("it-client"),
                    &invalid_topic,
                ))
                .expect("write invalid-topic commit");
            let invalid_topic_response =
                read_frame(&mut stream).expect("read invalid-topic response");
            let (invalid_topic_corr, invalid_topic_header_len) =
                decode_response_header(&invalid_topic_response, response_header_version);
            assert_eq!(invalid_topic_corr, 4_100 + i32::from(version));
            let invalid_topic_decoded = decode_offset_commit_response(
                version,
                &invalid_topic_response[invalid_topic_header_len..],
            );
            assert_eq!(invalid_topic_decoded.topics[0].partitions[0].error_code, 3);
        }

        let invalid_partition = encode_offset_commit_request_body(OffsetCommitRequestShape {
            version,
            group_id: "group-a",
            member_id: "",
            member_epoch: -1,
            topic_name: (version <= 9).then_some("topic-a"),
            topic_id: (version >= 10).then_some(topic_id),
            partition: -1,
            committed_offset: 3,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_COMMIT,
                version,
                4_200 + i32::from(version),
                Some("it-client"),
                &invalid_partition,
            ))
            .expect("write invalid-partition commit");
        let invalid_partition_response =
            read_frame(&mut stream).expect("read invalid-partition response");
        let (invalid_partition_corr, invalid_partition_header_len) =
            decode_response_header(&invalid_partition_response, response_header_version);
        assert_eq!(invalid_partition_corr, 4_200 + i32::from(version));
        let invalid_partition_decoded = decode_offset_commit_response(
            version,
            &invalid_partition_response[invalid_partition_header_len..],
        );
        assert_eq!(
            invalid_partition_decoded.topics[0].partitions[0].error_code,
            3
        );
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_offset_commit_storm_recovers_after_restart() {
    let temp = TempDir::new("transport-offset-commit-storm");
    let topic_id = [0x66_u8; 16];
    const PARTITIONS: usize = 32;
    const COMMITS: usize = 2_000;
    let mut expected = vec![-1_i64; PARTITIONS];

    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to server");

        for i in 0..COMMITS {
            let partition = i32::try_from(i % PARTITIONS).expect("partition fits i32");
            let committed_offset = i64::try_from(i).expect("offset fits i64") * 10;
            let body = encode_offset_commit_request_body(OffsetCommitRequestShape {
                version: 10,
                group_id: "storm-wire-cg",
                member_id: "",
                member_epoch: -1,
                topic_name: None,
                topic_id: Some(topic_id),
                partition,
                committed_offset,
            });
            stream
                .write_all(&encode_request_frame(
                    API_KEY_OFFSET_COMMIT,
                    10,
                    5_000 + i32::try_from(i).expect("i fits i32"),
                    Some("it-client"),
                    &body,
                ))
                .expect("write storm commit request");

            let response = read_frame(&mut stream).expect("read storm commit response");
            let (corr, header_len) = decode_response_header(&response, 1);
            assert_eq!(corr, 5_000 + i32::try_from(i).expect("i fits i32"));
            let decoded = decode_offset_commit_response(10, &response[header_len..]);
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].partitions.len(), 1);
            assert_eq!(decoded.topics[0].partitions[0].error_code, 0);

            expected[usize::try_from(partition).expect("partition fits usize")] = committed_offset;
        }

        let fetch_all =
            encode_offset_fetch_request_body(10, "storm-wire-cg", None, None, None, None, 0, true);
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_FETCH,
                10,
                5_999,
                Some("it-client"),
                &fetch_all,
            ))
            .expect("write fetch-all request");

        let fetch_response = read_frame(&mut stream).expect("read fetch-all response");
        let (fetch_corr, fetch_header_len) = decode_response_header(&fetch_response, 1);
        assert_eq!(fetch_corr, 5_999);
        let fetch_decoded = decode_offset_fetch_response(10, &fetch_response[fetch_header_len..]);
        let groups = fetch_decoded.groups.expect("grouped response");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].error_code, 0);
        assert_eq!(groups[0].topics.len(), 1);
        assert_eq!(groups[0].topics[0].topic_id, Some(topic_id));
        assert_eq!(groups[0].topics[0].partitions.len(), PARTITIONS);
        for partition in &groups[0].topics[0].partitions {
            let idx = usize::try_from(partition.partition_index).expect("partition fits usize");
            assert_eq!(partition.error_code, 0);
            assert_eq!(partition.committed_offset, expected[idx]);
        }

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }

    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("rebind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to rebound server");

        let fetch_all =
            encode_offset_fetch_request_body(10, "storm-wire-cg", None, None, None, None, 0, true);
        stream
            .write_all(&encode_request_frame(
                API_KEY_OFFSET_FETCH,
                10,
                6_001,
                Some("it-client"),
                &fetch_all,
            ))
            .expect("write restart fetch-all request");

        let fetch_response = read_frame(&mut stream).expect("read restart fetch-all response");
        let (fetch_corr, fetch_header_len) = decode_response_header(&fetch_response, 1);
        assert_eq!(fetch_corr, 6_001);
        let fetch_decoded = decode_offset_fetch_response(10, &fetch_response[fetch_header_len..]);
        let groups = fetch_decoded.groups.expect("grouped response");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].error_code, 0);
        assert_eq!(groups[0].topics.len(), 1);
        assert_eq!(groups[0].topics[0].topic_id, Some(topic_id));
        assert_eq!(groups[0].topics[0].partitions.len(), PARTITIONS);
        for partition in &groups[0].topics[0].partitions {
            let idx = usize::try_from(partition.partition_index).expect("partition fits usize");
            assert_eq!(partition.error_code, 0);
            assert_eq!(partition.committed_offset, expected[idx]);
        }

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }
}

#[test]
fn transport_join_group_member_id_required_and_legacy_paths() {
    let temp = TempDir::new("transport-join-group");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let first_join = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "grp-v4",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 2, 3],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_001,
            Some("it-client"),
            &first_join,
        ))
        .expect("write first join");
    let first_join_response = read_frame(&mut stream).expect("read first join response");
    let (corr, header_len) = decode_response_header(&first_join_response, 0);
    assert_eq!(corr, 7_001);
    let decoded_first = decode_join_group_response(4, &first_join_response[header_len..]);
    assert_eq!(decoded_first.error_code, 79);
    assert_eq!(decoded_first.generation_id, -1);
    assert_eq!(decoded_first.protocol_name.as_deref(), Some(""));
    assert!(!decoded_first.member_id.is_empty());
    let assigned_member_id = decoded_first.member_id.clone();

    let rejoin = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "grp-v4",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &assigned_member_id,
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 2, 3],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_002,
            Some("it-client"),
            &rejoin,
        ))
        .expect("write rejoin");
    let rejoin_response = read_frame(&mut stream).expect("read rejoin response");
    let (corr, header_len) = decode_response_header(&rejoin_response, 0);
    assert_eq!(corr, 7_002);
    let decoded_rejoin = decode_join_group_response(4, &rejoin_response[header_len..]);
    assert_eq!(decoded_rejoin.error_code, 0);
    assert_eq!(decoded_rejoin.generation_id, 1);
    assert_eq!(decoded_rejoin.member_id, assigned_member_id);
    assert_eq!(decoded_rejoin.leader, decoded_rejoin.member_id);
    assert_eq!(decoded_rejoin.members.len(), 1);

    let unknown_member = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "grp-v4",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "member-id-unknown",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 2, 3],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_003,
            Some("it-client"),
            &unknown_member,
        ))
        .expect("write unknown-member join");
    let unknown_response = read_frame(&mut stream).expect("read unknown-member response");
    let (corr, header_len) = decode_response_header(&unknown_response, 0);
    assert_eq!(corr, 7_003);
    let decoded_unknown = decode_join_group_response(4, &unknown_response[header_len..]);
    assert_eq!(decoded_unknown.error_code, 25);

    let empty_group = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 2, 3],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_004,
            Some("it-client"),
            &empty_group,
        ))
        .expect("write empty-group join");
    let empty_group_response = read_frame(&mut stream).expect("read empty-group response");
    let (corr, header_len) = decode_response_header(&empty_group_response, 0);
    assert_eq!(corr, 7_004);
    let decoded_empty_group = decode_join_group_response(4, &empty_group_response[header_len..]);
    assert_eq!(decoded_empty_group.error_code, 24);

    let legacy_join = encode_join_group_request_body(JoinGroupRequestShape {
        version: 0,
        group_id: "grp-v0",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[5, 6, 7],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            0,
            7_005,
            Some("it-client"),
            &legacy_join,
        ))
        .expect("write legacy join");
    let legacy_response = read_frame(&mut stream).expect("read legacy join response");
    let (corr, header_len) = decode_response_header(&legacy_response, 0);
    assert_eq!(corr, 7_005);
    let decoded_legacy = decode_join_group_response(0, &legacy_response[header_len..]);
    assert_eq!(decoded_legacy.error_code, 0);
    assert_eq!(decoded_legacy.generation_id, 1);
    assert_eq!(decoded_legacy.members.len(), 1);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_sync_group_assignment_and_error_matrix() {
    let temp = TempDir::new("transport-sync-group");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let join_1 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 6,
        group_id: "sync-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            6,
            7_101,
            Some("it-client"),
            &join_1,
        ))
        .expect("write join #1");
    let join_1_response = read_frame(&mut stream).expect("read join #1 response");
    let (corr, header_len) = decode_response_header(&join_1_response, 1);
    assert_eq!(corr, 7_101);
    let decoded_join_1 = decode_join_group_response(6, &join_1_response[header_len..]);
    assert_eq!(decoded_join_1.error_code, 79);
    let member_id = decoded_join_1.member_id;

    let join_2 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 6,
        group_id: "sync-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &member_id,
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            6,
            7_102,
            Some("it-client"),
            &join_2,
        ))
        .expect("write join #2");
    let join_2_response = read_frame(&mut stream).expect("read join #2 response");
    let (corr, header_len) = decode_response_header(&join_2_response, 1);
    assert_eq!(corr, 7_102);
    let decoded_join_2 = decode_join_group_response(6, &join_2_response[header_len..]);
    assert_eq!(decoded_join_2.error_code, 0);
    assert_eq!(decoded_join_2.generation_id, 1);

    let sync_bad_protocol = encode_sync_group_request_body(SyncGroupRequestShape {
        version: 5,
        group_id: "sync-grp",
        generation_id: 1,
        member_id: &member_id,
        group_instance_id: None,
        protocol_type: Some("consumer"),
        protocol_name: Some("unmatched"),
        assignments: vec![SyncGroupAssignmentShape {
            member_id: &member_id,
            assignment: &[10, 11],
        }],
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_SYNC_GROUP,
            5,
            7_103,
            Some("it-client"),
            &sync_bad_protocol,
        ))
        .expect("write bad-protocol sync");
    let sync_bad_protocol_response = read_frame(&mut stream).expect("read bad-protocol sync");
    let (corr, header_len) = decode_response_header(&sync_bad_protocol_response, 1);
    assert_eq!(corr, 7_103);
    let decoded_bad_protocol =
        decode_sync_group_response(5, &sync_bad_protocol_response[header_len..]);
    assert_eq!(decoded_bad_protocol.error_code, 23);

    let sync_ok = encode_sync_group_request_body(SyncGroupRequestShape {
        version: 5,
        group_id: "sync-grp",
        generation_id: 1,
        member_id: &member_id,
        group_instance_id: None,
        protocol_type: Some("consumer"),
        protocol_name: Some("consumer-range"),
        assignments: vec![SyncGroupAssignmentShape {
            member_id: &member_id,
            assignment: &[10, 11],
        }],
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_SYNC_GROUP,
            5,
            7_104,
            Some("it-client"),
            &sync_ok,
        ))
        .expect("write sync ok");
    let sync_ok_response = read_frame(&mut stream).expect("read sync ok");
    let (corr, header_len) = decode_response_header(&sync_ok_response, 1);
    assert_eq!(corr, 7_104);
    let decoded_sync_ok = decode_sync_group_response(5, &sync_ok_response[header_len..]);
    assert_eq!(decoded_sync_ok.error_code, 0);
    assert_eq!(decoded_sync_ok.protocol_type.as_deref(), Some("consumer"));
    assert_eq!(
        decoded_sync_ok.protocol_name.as_deref(),
        Some("consumer-range")
    );
    assert_eq!(decoded_sync_ok.assignment, vec![10, 11]);

    let sync_replay = encode_sync_group_request_body(SyncGroupRequestShape {
        version: 5,
        group_id: "sync-grp",
        generation_id: 1,
        member_id: &member_id,
        group_instance_id: None,
        protocol_type: Some("consumer"),
        protocol_name: Some("consumer-range"),
        assignments: Vec::new(),
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_SYNC_GROUP,
            5,
            7_105,
            Some("it-client"),
            &sync_replay,
        ))
        .expect("write sync replay");
    let sync_replay_response = read_frame(&mut stream).expect("read sync replay");
    let (corr, header_len) = decode_response_header(&sync_replay_response, 1);
    assert_eq!(corr, 7_105);
    let decoded_sync_replay = decode_sync_group_response(5, &sync_replay_response[header_len..]);
    assert_eq!(decoded_sync_replay.error_code, 0);
    assert_eq!(decoded_sync_replay.assignment, vec![10, 11]);

    let sync_unknown_member = encode_sync_group_request_body(SyncGroupRequestShape {
        version: 5,
        group_id: "sync-grp",
        generation_id: 1,
        member_id: "unknown-member",
        group_instance_id: None,
        protocol_type: Some("consumer"),
        protocol_name: Some("consumer-range"),
        assignments: Vec::new(),
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_SYNC_GROUP,
            5,
            7_106,
            Some("it-client"),
            &sync_unknown_member,
        ))
        .expect("write sync unknown member");
    let sync_unknown_member_response = read_frame(&mut stream).expect("read sync unknown member");
    let (corr, header_len) = decode_response_header(&sync_unknown_member_response, 1);
    assert_eq!(corr, 7_106);
    let decoded_sync_unknown =
        decode_sync_group_response(5, &sync_unknown_member_response[header_len..]);
    assert_eq!(decoded_sync_unknown.error_code, 25);

    let sync_illegal_generation = encode_sync_group_request_body(SyncGroupRequestShape {
        version: 5,
        group_id: "sync-grp",
        generation_id: 2,
        member_id: &member_id,
        group_instance_id: None,
        protocol_type: Some("consumer"),
        protocol_name: Some("consumer-range"),
        assignments: Vec::new(),
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_SYNC_GROUP,
            5,
            7_107,
            Some("it-client"),
            &sync_illegal_generation,
        ))
        .expect("write sync illegal generation");
    let sync_illegal_generation_response =
        read_frame(&mut stream).expect("read sync illegal generation");
    let (corr, header_len) = decode_response_header(&sync_illegal_generation_response, 1);
    assert_eq!(corr, 7_107);
    let decoded_sync_illegal_generation =
        decode_sync_group_response(5, &sync_illegal_generation_response[header_len..]);
    assert_eq!(decoded_sync_illegal_generation.error_code, 22);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_heartbeat_rebalance_and_group_errors() {
    let temp = TempDir::new("transport-heartbeat");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let join_leader_1 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "hb-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_201,
            Some("it-client"),
            &join_leader_1,
        ))
        .expect("write leader join #1");
    let join_leader_1_response = read_frame(&mut stream).expect("read leader join #1");
    let (_, header_len) = decode_response_header(&join_leader_1_response, 0);
    let decoded_join_leader_1 =
        decode_join_group_response(4, &join_leader_1_response[header_len..]);
    assert_eq!(decoded_join_leader_1.error_code, 79);
    let leader_member_id = decoded_join_leader_1.member_id;

    let join_leader_2 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "hb-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &leader_member_id,
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_202,
            Some("it-client"),
            &join_leader_2,
        ))
        .expect("write leader join #2");
    let join_leader_2_response = read_frame(&mut stream).expect("read leader join #2");
    let (_, header_len) = decode_response_header(&join_leader_2_response, 0);
    let decoded_join_leader_2 =
        decode_join_group_response(4, &join_leader_2_response[header_len..]);
    assert_eq!(decoded_join_leader_2.error_code, 0);
    assert_eq!(decoded_join_leader_2.generation_id, 1);

    let heartbeat_ok = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "hb-grp",
        generation_id: 1,
        member_id: &leader_member_id,
        group_instance_id: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_203,
            Some("it-client"),
            &heartbeat_ok,
        ))
        .expect("write heartbeat ok");
    let heartbeat_ok_response = read_frame(&mut stream).expect("read heartbeat ok");
    let (corr, header_len) = decode_response_header(&heartbeat_ok_response, 1);
    assert_eq!(corr, 7_203);
    let decoded_heartbeat_ok = decode_heartbeat_response(4, &heartbeat_ok_response[header_len..]);
    assert_eq!(decoded_heartbeat_ok.error_code, 0);

    let join_follower_1 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "hb-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[2],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_204,
            Some("it-client"),
            &join_follower_1,
        ))
        .expect("write follower join #1");
    let join_follower_1_response = read_frame(&mut stream).expect("read follower join #1");
    let (_, header_len) = decode_response_header(&join_follower_1_response, 0);
    let decoded_join_follower_1 =
        decode_join_group_response(4, &join_follower_1_response[header_len..]);
    assert_eq!(decoded_join_follower_1.error_code, 79);
    let follower_member_id = decoded_join_follower_1.member_id;

    let join_follower_2 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "hb-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &follower_member_id,
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[2],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_205,
            Some("it-client"),
            &join_follower_2,
        ))
        .expect("write follower join #2");
    let join_follower_2_response = read_frame(&mut stream).expect("read follower join #2");
    let (_, header_len) = decode_response_header(&join_follower_2_response, 0);
    let decoded_join_follower_2 =
        decode_join_group_response(4, &join_follower_2_response[header_len..]);
    assert_eq!(decoded_join_follower_2.error_code, 0);
    assert_eq!(decoded_join_follower_2.generation_id, 2);

    let heartbeat_rebalance = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "hb-grp",
        generation_id: 2,
        member_id: &leader_member_id,
        group_instance_id: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_206,
            Some("it-client"),
            &heartbeat_rebalance,
        ))
        .expect("write rebalance heartbeat");
    let heartbeat_rebalance_response = read_frame(&mut stream).expect("read rebalance heartbeat");
    let (corr, header_len) = decode_response_header(&heartbeat_rebalance_response, 1);
    assert_eq!(corr, 7_206);
    let decoded_heartbeat_rebalance =
        decode_heartbeat_response(4, &heartbeat_rebalance_response[header_len..]);
    assert_eq!(decoded_heartbeat_rebalance.error_code, 27);

    let leader_rejoin = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "hb-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &leader_member_id,
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            7_207,
            Some("it-client"),
            &leader_rejoin,
        ))
        .expect("write leader rejoin");
    let leader_rejoin_response = read_frame(&mut stream).expect("read leader rejoin");
    let (_, header_len) = decode_response_header(&leader_rejoin_response, 0);
    let decoded_leader_rejoin =
        decode_join_group_response(4, &leader_rejoin_response[header_len..]);
    assert_eq!(decoded_leader_rejoin.error_code, 0);
    assert_eq!(decoded_leader_rejoin.generation_id, 2);

    let heartbeat_after_rejoin = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "hb-grp",
        generation_id: 2,
        member_id: &leader_member_id,
        group_instance_id: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_208,
            Some("it-client"),
            &heartbeat_after_rejoin,
        ))
        .expect("write heartbeat after rejoin");
    let heartbeat_after_rejoin_response =
        read_frame(&mut stream).expect("read heartbeat after rejoin");
    let (corr, header_len) = decode_response_header(&heartbeat_after_rejoin_response, 1);
    assert_eq!(corr, 7_208);
    let decoded_heartbeat_after_rejoin =
        decode_heartbeat_response(4, &heartbeat_after_rejoin_response[header_len..]);
    assert_eq!(decoded_heartbeat_after_rejoin.error_code, 0);

    let heartbeat_unknown_group = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "missing",
        generation_id: 2,
        member_id: &leader_member_id,
        group_instance_id: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_209,
            Some("it-client"),
            &heartbeat_unknown_group,
        ))
        .expect("write unknown-group heartbeat");
    let heartbeat_unknown_group_response =
        read_frame(&mut stream).expect("read unknown-group heartbeat");
    let (corr, header_len) = decode_response_header(&heartbeat_unknown_group_response, 1);
    assert_eq!(corr, 7_209);
    let decoded_heartbeat_unknown_group =
        decode_heartbeat_response(4, &heartbeat_unknown_group_response[header_len..]);
    assert_eq!(decoded_heartbeat_unknown_group.error_code, 25);

    let heartbeat_empty_group = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "",
        generation_id: 2,
        member_id: &leader_member_id,
        group_instance_id: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_210,
            Some("it-client"),
            &heartbeat_empty_group,
        ))
        .expect("write empty-group heartbeat");
    let heartbeat_empty_group_response =
        read_frame(&mut stream).expect("read empty-group heartbeat");
    let (corr, header_len) = decode_response_header(&heartbeat_empty_group_response, 1);
    assert_eq!(corr, 7_210);
    let decoded_heartbeat_empty_group =
        decode_heartbeat_response(4, &heartbeat_empty_group_response[header_len..]);
    assert_eq!(decoded_heartbeat_empty_group.error_code, 24);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_leave_group_legacy_and_member_error_matrix() {
    let temp = TempDir::new("transport-leave-group");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let leave_unknown_legacy = encode_leave_group_request_body(LeaveGroupRequestShape {
        version: 2,
        group_id: "missing",
        member_id: Some("member-1"),
        members: Vec::new(),
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_LEAVE_GROUP,
            2,
            7_301,
            Some("it-client"),
            &leave_unknown_legacy,
        ))
        .expect("write legacy leave");
    let leave_unknown_legacy_response = read_frame(&mut stream).expect("read legacy leave");
    let (corr, header_len) = decode_response_header(&leave_unknown_legacy_response, 0);
    assert_eq!(corr, 7_301);
    let decoded_leave_unknown_legacy =
        decode_leave_group_response(2, &leave_unknown_legacy_response[header_len..]);
    assert_eq!(decoded_leave_unknown_legacy.error_code, 25);

    let join_static_1 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 7,
        group_id: "leave-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: "",
        group_instance_id: Some("instance-a"),
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 9],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            7,
            7_302,
            Some("it-client"),
            &join_static_1,
        ))
        .expect("write static join #1");
    let join_static_1_response = read_frame(&mut stream).expect("read static join #1");
    let (_, header_len) = decode_response_header(&join_static_1_response, 1);
    let decoded_join_static_1 =
        decode_join_group_response(7, &join_static_1_response[header_len..]);
    assert_eq!(decoded_join_static_1.error_code, 79);
    let static_member_id = decoded_join_static_1.member_id;

    let join_static_2 = encode_join_group_request_body(JoinGroupRequestShape {
        version: 7,
        group_id: "leave-grp",
        session_timeout_ms: 5_000,
        rebalance_timeout_ms: 5_000,
        member_id: &static_member_id,
        group_instance_id: Some("instance-a"),
        protocol_type: "consumer",
        protocol_name: "consumer-range",
        metadata: &[1, 9],
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            7,
            7_303,
            Some("it-client"),
            &join_static_2,
        ))
        .expect("write static join #2");
    let join_static_2_response = read_frame(&mut stream).expect("read static join #2");
    let (_, header_len) = decode_response_header(&join_static_2_response, 1);
    let decoded_join_static_2 =
        decode_join_group_response(7, &join_static_2_response[header_len..]);
    assert_eq!(decoded_join_static_2.error_code, 0);

    let leave_member_errors = encode_leave_group_request_body(LeaveGroupRequestShape {
        version: 5,
        group_id: "leave-grp",
        member_id: None,
        members: vec![
            LeaveGroupMemberShape {
                member_id: "wrong-member-id",
                group_instance_id: Some("instance-a"),
                reason: Some("rolling restart"),
            },
            LeaveGroupMemberShape {
                member_id: "missing-member",
                group_instance_id: None,
                reason: Some("gone"),
            },
        ],
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_LEAVE_GROUP,
            5,
            7_304,
            Some("it-client"),
            &leave_member_errors,
        ))
        .expect("write leave member error matrix");
    let leave_member_errors_response = read_frame(&mut stream).expect("read leave member matrix");
    let (corr, header_len) = decode_response_header(&leave_member_errors_response, 1);
    assert_eq!(corr, 7_304);
    let decoded_leave_member_errors =
        decode_leave_group_response(5, &leave_member_errors_response[header_len..]);
    assert_eq!(decoded_leave_member_errors.error_code, 0);
    assert_eq!(decoded_leave_member_errors.members.len(), 2);
    assert_eq!(decoded_leave_member_errors.members[0].error_code, 82);
    assert_eq!(decoded_leave_member_errors.members[1].error_code, 25);

    let leave_success = encode_leave_group_request_body(LeaveGroupRequestShape {
        version: 5,
        group_id: "leave-grp",
        member_id: None,
        members: vec![LeaveGroupMemberShape {
            member_id: "",
            group_instance_id: Some("instance-a"),
            reason: Some("shutdown"),
        }],
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_LEAVE_GROUP,
            5,
            7_305,
            Some("it-client"),
            &leave_success,
        ))
        .expect("write leave success");
    let leave_success_response = read_frame(&mut stream).expect("read leave success");
    let (corr, header_len) = decode_response_header(&leave_success_response, 1);
    assert_eq!(corr, 7_305);
    let decoded_leave_success =
        decode_leave_group_response(5, &leave_success_response[header_len..]);
    assert_eq!(decoded_leave_success.error_code, 0);
    assert_eq!(decoded_leave_success.members.len(), 1);
    assert_eq!(decoded_leave_success.members[0].error_code, 0);

    let heartbeat_after_leave = encode_heartbeat_request_body(HeartbeatRequestShape {
        version: 4,
        group_id: "leave-grp",
        generation_id: 1,
        member_id: &static_member_id,
        group_instance_id: Some("instance-a"),
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_HEARTBEAT,
            4,
            7_306,
            Some("it-client"),
            &heartbeat_after_leave,
        ))
        .expect("write heartbeat after leave");
    let heartbeat_after_leave_response =
        read_frame(&mut stream).expect("read heartbeat after leave");
    let (corr, header_len) = decode_response_header(&heartbeat_after_leave_response, 1);
    assert_eq!(corr, 7_306);
    let decoded_heartbeat_after_leave =
        decode_heartbeat_response(4, &heartbeat_after_leave_response[header_len..]);
    assert_eq!(decoded_heartbeat_after_leave.error_code, 25);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_sync_group_assignment_survives_restart() {
    let temp = TempDir::new("transport-sync-restart");
    let member_id;

    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to server");

        let join_1 = encode_join_group_request_body(JoinGroupRequestShape {
            version: 6,
            group_id: "sync-restart-grp",
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            member_id: "",
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "consumer-range",
            metadata: &[3, 3, 3],
            reason: None,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_JOIN_GROUP,
                6,
                7_401,
                Some("it-client"),
                &join_1,
            ))
            .expect("write restart join #1");
        let join_1_response = read_frame(&mut stream).expect("read restart join #1");
        let (_, header_len) = decode_response_header(&join_1_response, 1);
        let decoded_join_1 = decode_join_group_response(6, &join_1_response[header_len..]);
        assert_eq!(decoded_join_1.error_code, 79);
        member_id = decoded_join_1.member_id;

        let join_2 = encode_join_group_request_body(JoinGroupRequestShape {
            version: 6,
            group_id: "sync-restart-grp",
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            member_id: &member_id,
            group_instance_id: None,
            protocol_type: "consumer",
            protocol_name: "consumer-range",
            metadata: &[3, 3, 3],
            reason: None,
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_JOIN_GROUP,
                6,
                7_402,
                Some("it-client"),
                &join_2,
            ))
            .expect("write restart join #2");
        let join_2_response = read_frame(&mut stream).expect("read restart join #2");
        let (_, header_len) = decode_response_header(&join_2_response, 1);
        let decoded_join_2 = decode_join_group_response(6, &join_2_response[header_len..]);
        assert_eq!(decoded_join_2.error_code, 0);
        assert_eq!(decoded_join_2.generation_id, 1);

        let sync = encode_sync_group_request_body(SyncGroupRequestShape {
            version: 5,
            group_id: "sync-restart-grp",
            generation_id: 1,
            member_id: &member_id,
            group_instance_id: None,
            protocol_type: Some("consumer"),
            protocol_name: Some("consumer-range"),
            assignments: vec![SyncGroupAssignmentShape {
                member_id: &member_id,
                assignment: &[9, 8, 7],
            }],
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_SYNC_GROUP,
                5,
                7_403,
                Some("it-client"),
                &sync,
            ))
            .expect("write sync");
        let sync_response = read_frame(&mut stream).expect("read sync");
        let (_, header_len) = decode_response_header(&sync_response, 1);
        let decoded_sync = decode_sync_group_response(5, &sync_response[header_len..]);
        assert_eq!(decoded_sync.error_code, 0);
        assert_eq!(decoded_sync.assignment, vec![9, 8, 7]);

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }

    {
        let mut server =
            TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("rebind server");
        let addr = server.local_addr().expect("server local addr");
        let handle = thread::spawn(move || server.serve_one_connection());
        let mut stream = TcpStream::connect(addr).expect("connect to rebound server");

        let sync_after_restart = encode_sync_group_request_body(SyncGroupRequestShape {
            version: 5,
            group_id: "sync-restart-grp",
            generation_id: 1,
            member_id: &member_id,
            group_instance_id: None,
            protocol_type: Some("consumer"),
            protocol_name: Some("consumer-range"),
            assignments: Vec::new(),
        });
        stream
            .write_all(&encode_request_frame(
                API_KEY_SYNC_GROUP,
                5,
                7_404,
                Some("it-client"),
                &sync_after_restart,
            ))
            .expect("write sync after restart");
        let sync_after_restart_response = read_frame(&mut stream).expect("read sync after restart");
        let (corr, header_len) = decode_response_header(&sync_after_restart_response, 1);
        assert_eq!(corr, 7_404);
        let decoded_sync_after_restart =
            decode_sync_group_response(5, &sync_after_restart_response[header_len..]);
        assert_eq!(decoded_sync_after_restart.error_code, 0);
        assert_eq!(decoded_sync_after_restart.assignment, vec![9, 8, 7]);

        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown client socket");
        handle
            .join()
            .expect("join server thread")
            .expect("server should finish normally");
    }
}

#[test]
fn transport_init_producer_id_matrix_and_fencing() {
    let temp = TempDir::new("transport-init-producer-id-matrix");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    for version in [0_i16, 2, 3, 6] {
        let tx_id = format!("tx-init-v{version}");
        let init_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
            version,
            transactional_id: Some(&tx_id),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable_2pc: version >= 6,
            keep_prepared_txn: false,
        });
        let init_corr = 8_000 + i32::from(version);
        stream
            .write_all(&encode_request_frame(
                API_KEY_INIT_PRODUCER_ID,
                version,
                init_corr,
                Some("it-client"),
                &init_body,
            ))
            .expect("write init producer id");
        let init_response = read_frame(&mut stream).expect("read init producer id response");
        let expected_header = if version >= 2 { 1 } else { 0 };
        let (corr, header_len) = decode_response_header(&init_response, expected_header);
        assert_eq!(corr, init_corr);
        let decoded_init = decode_init_producer_id_response(version, &init_response[header_len..]);
        assert_eq!(decoded_init.error_code, 0);
        assert!(decoded_init.producer_id >= 1);
        assert_eq!(decoded_init.producer_epoch, 0);
        if version >= 6 {
            assert_eq!(decoded_init.ongoing_txn_producer_id, Some(-1));
            assert_eq!(decoded_init.ongoing_txn_producer_epoch, Some(-1));
        }

        let reinit_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
            version,
            transactional_id: Some(&tx_id),
            transaction_timeout_ms: 30_000,
            producer_id: if version >= 3 {
                decoded_init.producer_id
            } else {
                -1
            },
            producer_epoch: if version >= 3 {
                decoded_init.producer_epoch
            } else {
                -1
            },
            enable_2pc: version >= 6,
            keep_prepared_txn: false,
        });
        let reinit_corr = init_corr + 100;
        stream
            .write_all(&encode_request_frame(
                API_KEY_INIT_PRODUCER_ID,
                version,
                reinit_corr,
                Some("it-client"),
                &reinit_body,
            ))
            .expect("write re-init producer id");
        let reinit_response = read_frame(&mut stream).expect("read re-init producer id response");
        let (corr, header_len) = decode_response_header(&reinit_response, expected_header);
        assert_eq!(corr, reinit_corr);
        let decoded_reinit =
            decode_init_producer_id_response(version, &reinit_response[header_len..]);
        assert_eq!(decoded_reinit.error_code, 0);
        assert_eq!(decoded_reinit.producer_id, decoded_init.producer_id);
        assert_eq!(
            decoded_reinit.producer_epoch,
            decoded_init.producer_epoch.saturating_add(1)
        );

        if version >= 3 {
            let fenced_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
                version,
                transactional_id: Some(&tx_id),
                transaction_timeout_ms: 30_000,
                producer_id: decoded_init.producer_id.saturating_add(99),
                producer_epoch: decoded_init.producer_epoch,
                enable_2pc: version >= 6,
                keep_prepared_txn: false,
            });
            let fenced_corr = init_corr + 200;
            stream
                .write_all(&encode_request_frame(
                    API_KEY_INIT_PRODUCER_ID,
                    version,
                    fenced_corr,
                    Some("it-client"),
                    &fenced_body,
                ))
                .expect("write fenced init producer id");
            let fenced_response =
                read_frame(&mut stream).expect("read fenced init producer id response");
            let (corr, header_len) = decode_response_header(&fenced_response, expected_header);
            assert_eq!(corr, fenced_corr);
            let decoded_fenced =
                decode_init_producer_id_response(version, &fenced_response[header_len..]);
            assert_eq!(decoded_fenced.error_code, 90);
        }
    }

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_end_txn_v5_success_and_invalid_epoch() {
    let temp = TempDir::new("transport-end-txn-v5");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let transactional_id = "txn-end-v5";
    let init_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
        version: 3,
        transactional_id: Some(transactional_id),
        transaction_timeout_ms: 30_000,
        producer_id: -1,
        producer_epoch: -1,
        enable_2pc: false,
        keep_prepared_txn: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_INIT_PRODUCER_ID,
            3,
            8_301,
            Some("it-client"),
            &init_body,
        ))
        .expect("write init producer id");
    let init_response = read_frame(&mut stream).expect("read init producer id response");
    let (_, init_header_len) = decode_response_header(&init_response, 1);
    let init_decoded = decode_init_producer_id_response(3, &init_response[init_header_len..]);
    assert_eq!(init_decoded.error_code, 0);

    let produce = ProduceRequest {
        transactional_id: Some(transactional_id.to_string()),
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("txn-end-topic".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![7]),
            }],
        }],
    };
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            8_302,
            Some("it-client"),
            &produce.encode(3).expect("encode produce"),
        ))
        .expect("write produce");
    let produce_response = read_frame(&mut stream).expect("read produce response");
    let (_, produce_header_len) = decode_response_header(&produce_response, 0);
    let decoded_produce = decode_produce_response(3, &produce_response[produce_header_len..]);
    assert_eq!(decoded_produce.topics[0].partitions[0].error_code, 0);

    let end_ok = encode_end_txn_request_body(EndTxnRequestShape {
        version: 5,
        transactional_id,
        producer_id: init_decoded.producer_id,
        producer_epoch: init_decoded.producer_epoch,
        committed: true,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_END_TXN,
            5,
            8_303,
            Some("it-client"),
            &end_ok,
        ))
        .expect("write end txn");
    let end_ok_response = read_frame(&mut stream).expect("read end txn response");
    let (corr, end_ok_header_len) = decode_response_header(&end_ok_response, 1);
    assert_eq!(corr, 8_303);
    let decoded_end_ok = decode_end_txn_response(5, &end_ok_response[end_ok_header_len..]);
    assert_eq!(decoded_end_ok.error_code, 0);
    assert_eq!(decoded_end_ok.producer_id, Some(init_decoded.producer_id));
    assert_eq!(
        decoded_end_ok.producer_epoch,
        Some(init_decoded.producer_epoch.saturating_add(1))
    );

    let end_stale = encode_end_txn_request_body(EndTxnRequestShape {
        version: 5,
        transactional_id,
        producer_id: init_decoded.producer_id,
        producer_epoch: init_decoded.producer_epoch,
        committed: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_END_TXN,
            5,
            8_304,
            Some("it-client"),
            &end_stale,
        ))
        .expect("write stale end txn");
    let stale_response = read_frame(&mut stream).expect("read stale end txn response");
    let (corr, stale_header_len) = decode_response_header(&stale_response, 1);
    assert_eq!(corr, 8_304);
    let decoded_stale = decode_end_txn_response(5, &stale_response[stale_header_len..]);
    assert_eq!(decoded_stale.error_code, 47);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_write_txn_markers_v2_error_matrix() {
    let temp = TempDir::new("transport-write-txn-markers-v2");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let transactional_id = "txn-markers-v2";
    let init_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
        version: 3,
        transactional_id: Some(transactional_id),
        transaction_timeout_ms: 30_000,
        producer_id: -1,
        producer_epoch: -1,
        enable_2pc: false,
        keep_prepared_txn: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_INIT_PRODUCER_ID,
            3,
            8_401,
            Some("it-client"),
            &init_body,
        ))
        .expect("write init producer id");
    let init_response = read_frame(&mut stream).expect("read init response");
    let (_, init_header_len) = decode_response_header(&init_response, 1);
    let init_decoded = decode_init_producer_id_response(3, &init_response[init_header_len..]);
    assert_eq!(init_decoded.error_code, 0);

    let produce = ProduceRequest {
        transactional_id: Some(transactional_id.to_string()),
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("txn-marker-topic".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![9]),
            }],
        }],
    };
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            8_402,
            Some("it-client"),
            &produce.encode(3).expect("encode produce"),
        ))
        .expect("write produce");
    let _ = read_frame(&mut stream).expect("read produce response");

    let markers_ok = vec![WriteTxnMarkersShape {
        producer_id: init_decoded.producer_id,
        producer_epoch: init_decoded.producer_epoch,
        transaction_result: true,
        topics: vec![WriteTxnMarkersTopicShape {
            name: "txn-marker-topic",
            partition_indexes: vec![0],
        }],
        coordinator_epoch: 1,
        transaction_version: 2,
    }];
    stream
        .write_all(&encode_request_frame(
            API_KEY_WRITE_TXN_MARKERS,
            2,
            8_403,
            Some("it-client"),
            &encode_write_txn_markers_request_body(2, &markers_ok),
        ))
        .expect("write write-txn-markers ok");
    let ok_response = read_frame(&mut stream).expect("read write-txn-markers ok response");
    let (corr, ok_header_len) = decode_response_header(&ok_response, 1);
    assert_eq!(corr, 8_403);
    let decoded_ok = decode_write_txn_markers_response(2, &ok_response[ok_header_len..]);
    assert_eq!(decoded_ok.markers.len(), 1);
    assert_eq!(decoded_ok.markers[0].topics[0].partitions[0].error_code, 0);

    let markers_unknown_producer = vec![WriteTxnMarkersShape {
        producer_id: init_decoded.producer_id.saturating_add(10_000),
        producer_epoch: init_decoded.producer_epoch,
        transaction_result: true,
        topics: vec![WriteTxnMarkersTopicShape {
            name: "txn-marker-topic",
            partition_indexes: vec![0],
        }],
        coordinator_epoch: 1,
        transaction_version: 2,
    }];
    stream
        .write_all(&encode_request_frame(
            API_KEY_WRITE_TXN_MARKERS,
            2,
            8_404,
            Some("it-client"),
            &encode_write_txn_markers_request_body(2, &markers_unknown_producer),
        ))
        .expect("write write-txn-markers unknown producer");
    let unknown_response = read_frame(&mut stream).expect("read unknown producer response");
    let (corr, unknown_header_len) = decode_response_header(&unknown_response, 1);
    assert_eq!(corr, 8_404);
    let decoded_unknown =
        decode_write_txn_markers_response(2, &unknown_response[unknown_header_len..]);
    assert_eq!(
        decoded_unknown.markers[0].topics[0].partitions[0].error_code,
        59
    );

    let markers_invalid_partition = vec![WriteTxnMarkersShape {
        producer_id: init_decoded.producer_id,
        producer_epoch: init_decoded.producer_epoch,
        transaction_result: true,
        topics: vec![WriteTxnMarkersTopicShape {
            name: "txn-marker-topic",
            partition_indexes: vec![-1],
        }],
        coordinator_epoch: 1,
        transaction_version: 2,
    }];
    stream
        .write_all(&encode_request_frame(
            API_KEY_WRITE_TXN_MARKERS,
            2,
            8_405,
            Some("it-client"),
            &encode_write_txn_markers_request_body(2, &markers_invalid_partition),
        ))
        .expect("write write-txn-markers invalid partition");
    let invalid_partition_response =
        read_frame(&mut stream).expect("read invalid partition response");
    let (corr, invalid_partition_header_len) =
        decode_response_header(&invalid_partition_response, 1);
    assert_eq!(corr, 8_405);
    let decoded_invalid_partition = decode_write_txn_markers_response(
        2,
        &invalid_partition_response[invalid_partition_header_len..],
    );
    assert_eq!(
        decoded_invalid_partition.markers[0].topics[0].partitions[0].error_code,
        3
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_fetch_read_committed_filters_aborted_records() {
    let temp = TempDir::new("transport-fetch-read-committed");
    let mut server =
        TransportServer::bind("127.0.0.1:0", temp.path(), log_config()).expect("bind server");
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let transactional_id = "txn-read-committed";
    let init_body = encode_init_producer_id_request_body(InitProducerIdRequestShape {
        version: 3,
        transactional_id: Some(transactional_id),
        transaction_timeout_ms: 30_000,
        producer_id: -1,
        producer_epoch: -1,
        enable_2pc: false,
        keep_prepared_txn: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_INIT_PRODUCER_ID,
            3,
            8_501,
            Some("it-client"),
            &init_body,
        ))
        .expect("write init producer id");
    let init_response = read_frame(&mut stream).expect("read init response");
    let (_, init_header_len) = decode_response_header(&init_response, 1);
    let init_decoded = decode_init_producer_id_response(3, &init_response[init_header_len..]);
    assert_eq!(init_decoded.error_code, 0);

    for (correlation_id, value) in [(8_502, 1_u8), (8_503, 2_u8)] {
        let produce = ProduceRequest {
            transactional_id: Some(transactional_id.to_string()),
            acks: 1,
            timeout_ms: 1_000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: Some("orders-read".to_string()),
                topic_id: None,
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 0,
                    records: Some(vec![value]),
                }],
            }],
        };
        stream
            .write_all(&encode_request_frame(
                API_KEY_PRODUCE,
                3,
                correlation_id,
                Some("it-client"),
                &produce.encode(3).expect("encode transactional produce"),
            ))
            .expect("write transactional produce");
        let response = read_frame(&mut stream).expect("read transactional produce response");
        let (_, header_len) = decode_response_header(&response, 0);
        let decoded = decode_produce_response(3, &response[header_len..]);
        assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
    }

    let read_committed_before_abort =
        encode_fetch_request_body_with_isolation(4, Some("orders-read"), None, 0, 0, 8_192, 1);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            8_504,
            Some("it-client"),
            &read_committed_before_abort,
        ))
        .expect("write read committed fetch before abort");
    let before_abort_response = read_frame(&mut stream).expect("read before-abort fetch response");
    let (_, before_abort_header_len) = decode_response_header(&before_abort_response, 0);
    let decoded_before_abort =
        decode_fetch_response(4, &before_abort_response[before_abort_header_len..]);
    let partition_before_abort = &decoded_before_abort.topics[0].partitions[0];
    assert_eq!(partition_before_abort.error_code, 0);
    assert_eq!(partition_before_abort.last_stable_offset, 0);
    assert_eq!(partition_before_abort.records, Some(Vec::new()));
    assert_eq!(
        partition_before_abort.aborted_transactions,
        Some(Vec::new())
    );

    let abort = encode_end_txn_request_body(EndTxnRequestShape {
        version: 4,
        transactional_id,
        producer_id: init_decoded.producer_id,
        producer_epoch: init_decoded.producer_epoch,
        committed: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_END_TXN,
            4,
            8_505,
            Some("it-client"),
            &abort,
        ))
        .expect("write abort end txn");
    let abort_response = read_frame(&mut stream).expect("read abort response");
    let (_, abort_header_len) = decode_response_header(&abort_response, 1);
    let decoded_abort = decode_end_txn_response(4, &abort_response[abort_header_len..]);
    assert_eq!(decoded_abort.error_code, 0);

    let non_tx_produce = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("orders-read".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![3]),
            }],
        }],
    };
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            3,
            8_506,
            Some("it-client"),
            &non_tx_produce
                .encode(3)
                .expect("encode non transactional produce"),
        ))
        .expect("write non transactional produce");
    let non_tx_response = read_frame(&mut stream).expect("read non transactional produce response");
    let (_, non_tx_header_len) = decode_response_header(&non_tx_response, 0);
    let decoded_non_tx = decode_produce_response(3, &non_tx_response[non_tx_header_len..]);
    assert_eq!(decoded_non_tx.topics[0].partitions[0].error_code, 0);

    let read_committed_after_abort =
        encode_fetch_request_body_with_isolation(4, Some("orders-read"), None, 0, 0, 8_192, 1);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            8_507,
            Some("it-client"),
            &read_committed_after_abort,
        ))
        .expect("write read committed fetch after abort");
    let after_abort_response = read_frame(&mut stream).expect("read after-abort fetch response");
    let (_, after_abort_header_len) = decode_response_header(&after_abort_response, 0);
    let decoded_after_abort =
        decode_fetch_response(4, &after_abort_response[after_abort_header_len..]);
    let partition_after_abort = &decoded_after_abort.topics[0].partitions[0];
    assert_eq!(partition_after_abort.error_code, 0);
    assert_eq!(partition_after_abort.high_watermark, 3);
    assert_eq!(partition_after_abort.last_stable_offset, 3);
    assert_eq!(partition_after_abort.records, Some(vec![3]));
    assert_eq!(
        partition_after_abort.aborted_transactions,
        Some(vec![DecodedFetchAbortedTransaction {
            producer_id: init_decoded.producer_id,
            first_offset: 0,
        }])
    );

    let read_uncommitted =
        encode_fetch_request_body_with_isolation(4, Some("orders-read"), None, 0, 0, 8_192, 0);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            8_508,
            Some("it-client"),
            &read_uncommitted,
        ))
        .expect("write read uncommitted fetch");
    let uncommitted_response = read_frame(&mut stream).expect("read uncommitted fetch response");
    let (_, uncommitted_header_len) = decode_response_header(&uncommitted_response, 0);
    let decoded_uncommitted =
        decode_fetch_response(4, &uncommitted_response[uncommitted_header_len..]);
    let partition_uncommitted = &decoded_uncommitted.topics[0].partitions[0];
    assert_eq!(partition_uncommitted.error_code, 0);
    assert_eq!(partition_uncommitted.records, Some(vec![1, 2, 3]));
    assert_eq!(partition_uncommitted.aborted_transactions, None);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_produce_returns_topic_authorization_failed() {
    let temp = TempDir::new("transport-acl-produce-denied");
    let mut server = bind_server_with_acl(&temp, AclAuthorizer::new());
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());

    let mut stream = TcpStream::connect(addr).expect("connect to server");
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topic_data: vec![ProduceRequestTopicProduceData {
            name: Some("acl-orders".to_string()),
            topic_id: None,
            partition_data: vec![ProduceRequestPartitionProduceData {
                index: 0,
                records: Some(vec![1, 2, 3]),
            }],
        }],
    }
    .encode(9)
    .expect("encode produce request");
    stream
        .write_all(&encode_request_frame(
            API_KEY_PRODUCE,
            9,
            9_001,
            Some("it-client"),
            &request,
        ))
        .expect("write produce request");

    let response = read_frame(&mut stream).expect("read produce response");
    let (correlation_id, header_len) = decode_response_header(&response, 1);
    assert_eq!(correlation_id, 9_001);
    let decoded = decode_produce_response(9, &response[header_len..]);
    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        ERROR_TOPIC_AUTHORIZATION_FAILED
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_fetch_returns_topic_authorization_failed() {
    let temp = TempDir::new("transport-acl-fetch-denied");
    {
        let mut bootstrap =
            PartitionedBroker::open(temp.path(), log_config()).expect("open bootstrap broker");
        bootstrap
            .produce_to("acl-fetch", 0, Vec::new(), vec![5, 6, 7], 0)
            .expect("seed partition data");
    }

    let mut server = bind_server_with_acl(&temp, AclAuthorizer::new());
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let fetch = encode_fetch_request_body(4, Some("acl-fetch"), None, 0, 0, 8_192);
    stream
        .write_all(&encode_request_frame(
            API_KEY_FETCH,
            4,
            9_002,
            Some("it-client"),
            &fetch,
        ))
        .expect("write fetch request");
    let response = read_frame(&mut stream).expect("read fetch response");
    let (correlation_id, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation_id, 9_002);
    let decoded = decode_fetch_response(4, &response[header_len..]);
    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        ERROR_TOPIC_AUTHORIZATION_FAILED
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_offset_commit_returns_group_authorization_failed() {
    let temp = TempDir::new("transport-acl-offset-commit-group-denied");
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "ANONYMOUS",
        AclOperation::Read,
        AclResourceType::Topic,
        "acl-offset",
    )
    .expect("allow topic read");
    let mut server = bind_server_with_acl(&temp, acl);
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let commit = encode_offset_commit_request_body(OffsetCommitRequestShape {
        version: 8,
        group_id: "acl-group",
        member_id: "",
        member_epoch: -1,
        topic_name: Some("acl-offset"),
        topic_id: None,
        partition: 0,
        committed_offset: 42,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_OFFSET_COMMIT,
            8,
            9_003,
            Some("it-client"),
            &commit,
        ))
        .expect("write offset commit");
    let response = read_frame(&mut stream).expect("read offset commit response");
    let (correlation_id, header_len) = decode_response_header(&response, 1);
    assert_eq!(correlation_id, 9_003);
    let decoded = decode_offset_commit_response(8, &response[header_len..]);
    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        ERROR_GROUP_AUTHORIZATION_FAILED
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_offset_fetch_returns_topic_authorization_failed() {
    let temp = TempDir::new("transport-acl-offset-fetch-topic-denied");
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "ANONYMOUS",
        AclOperation::Describe,
        AclResourceType::Group,
        "acl-group",
    )
    .expect("allow group describe");
    let mut server = bind_server_with_acl(&temp, acl);
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let fetch = encode_offset_fetch_request_body(
        8,
        "acl-group",
        None,
        None,
        Some("acl-offset"),
        None,
        0,
        false,
    );
    stream
        .write_all(&encode_request_frame(
            API_KEY_OFFSET_FETCH,
            8,
            9_004,
            Some("it-client"),
            &fetch,
        ))
        .expect("write offset fetch");
    let response = read_frame(&mut stream).expect("read offset fetch response");
    let (correlation_id, header_len) = decode_response_header(&response, 1);
    assert_eq!(correlation_id, 9_004);
    let decoded = decode_offset_fetch_response(8, &response[header_len..]);
    let groups = decoded.groups.expect("grouped offset fetch response");
    assert_eq!(groups[0].error_code, 0);
    assert_eq!(
        groups[0].topics[0].partitions[0].error_code,
        ERROR_TOPIC_AUTHORIZATION_FAILED
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_join_group_returns_group_authorization_failed() {
    let temp = TempDir::new("transport-acl-join-group-denied");
    let mut server = bind_server_with_acl(&temp, AclAuthorizer::new());
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let join = encode_join_group_request_body(JoinGroupRequestShape {
        version: 4,
        group_id: "acl-group",
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: "",
        group_instance_id: None,
        protocol_type: "consumer",
        protocol_name: "range",
        metadata: b"metadata",
        reason: None,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_JOIN_GROUP,
            4,
            9_005,
            Some("it-client"),
            &join,
        ))
        .expect("write join group");
    let response = read_frame(&mut stream).expect("read join group response");
    let (correlation_id, header_len) = decode_response_header(&response, 0);
    assert_eq!(correlation_id, 9_005);
    let decoded = decode_join_group_response(4, &response[header_len..]);
    assert_eq!(decoded.error_code, ERROR_GROUP_AUTHORIZATION_FAILED);

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}

#[test]
fn transport_acl_transaction_apis_return_authorization_errors() {
    let temp = TempDir::new("transport-acl-transaction-denied");
    let mut server = bind_server_with_acl(&temp, AclAuthorizer::new());
    let addr = server.local_addr().expect("server local addr");
    let handle = thread::spawn(move || server.serve_one_connection());
    let mut stream = TcpStream::connect(addr).expect("connect to server");

    let init = encode_init_producer_id_request_body(InitProducerIdRequestShape {
        version: 3,
        transactional_id: Some("acl-txn"),
        transaction_timeout_ms: 30_000,
        producer_id: -1,
        producer_epoch: -1,
        enable_2pc: false,
        keep_prepared_txn: false,
    });
    stream
        .write_all(&encode_request_frame(
            API_KEY_INIT_PRODUCER_ID,
            3,
            9_006,
            Some("it-client"),
            &init,
        ))
        .expect("write init producer id");
    let init_response = read_frame(&mut stream).expect("read init producer id response");
    let (init_corr, init_header_len) = decode_response_header(&init_response, 1);
    assert_eq!(init_corr, 9_006);
    let decoded_init = decode_init_producer_id_response(3, &init_response[init_header_len..]);
    assert_eq!(
        decoded_init.error_code,
        ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    );

    let markers = vec![WriteTxnMarkersShape {
        producer_id: 42,
        producer_epoch: 1,
        transaction_result: true,
        topics: vec![WriteTxnMarkersTopicShape {
            name: "txn-topic",
            partition_indexes: vec![0],
        }],
        coordinator_epoch: 1,
        transaction_version: 2,
    }];
    stream
        .write_all(&encode_request_frame(
            API_KEY_WRITE_TXN_MARKERS,
            2,
            9_007,
            Some("it-client"),
            &encode_write_txn_markers_request_body(2, &markers),
        ))
        .expect("write write txn markers");
    let markers_response = read_frame(&mut stream).expect("read write txn markers response");
    let (markers_corr, markers_header_len) = decode_response_header(&markers_response, 1);
    assert_eq!(markers_corr, 9_007);
    let decoded_markers =
        decode_write_txn_markers_response(2, &markers_response[markers_header_len..]);
    assert_eq!(
        decoded_markers.markers[0].topics[0].partitions[0].error_code,
        ERROR_CLUSTER_AUTHORIZATION_FAILED
    );

    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown client socket");
    handle
        .join()
        .expect("join server thread")
        .expect("server should finish normally");
}
