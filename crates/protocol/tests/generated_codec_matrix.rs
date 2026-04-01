//! Wire matrix tests for generated `VersionedCodec` implementations.
//!
//! Every test here exercises the codecs emitted by `rafka-codegen` from the
//! vendored Kafka specs, across the full valid version range for each API,
//! plus golden-byte vectors and negative paths.

use rafka_protocol::messages::DeleteGroupsResponseDeletableGroupResult;
use rafka_protocol::messages::ListGroupsResponseListedGroup;
use rafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, ListGroupsRequest, ListGroupsResponse,
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
