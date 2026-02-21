#![forbid(unsafe_code)]

use rafka_client::{FetchRequest, ProduceRequest};

// ──────────────────────────────────────────────────────────────────────────────
// ProduceRequest construction
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn produce_request_fields_set_correctly() {
    let req = ProduceRequest::new(
        "orders",
        3,
        Some(b"key-1".to_vec()),
        Some(b"value-1".to_vec()),
        1_700_000_000_000,
    );
    assert_eq!(req.topic, "orders");
    assert_eq!(req.partition, 3);
    assert_eq!(req.key.as_deref(), Some(b"key-1".as_ref()));
    assert_eq!(req.value.as_deref(), Some(b"value-1".as_ref()));
    assert_eq!(req.timestamp_ms, 1_700_000_000_000);
}

#[test]
fn produce_request_null_key_and_value() {
    let req = ProduceRequest::new("t", 0, None, None, 0);
    assert!(req.key.is_none());
    assert!(req.value.is_none());
}

#[test]
fn produce_request_partition_zero_is_valid() {
    let req = ProduceRequest::new("t", 0, None, Some(b"v".to_vec()), 1);
    assert_eq!(req.partition, 0);
}

#[test]
fn produce_request_large_partition_index() {
    let req = ProduceRequest::new("t", 1023, None, None, 0);
    assert_eq!(req.partition, 1023);
}

#[test]
fn produce_request_clone_is_equal() {
    let req = ProduceRequest::new(
        "clone-test",
        1,
        Some(vec![0xDE, 0xAD]),
        Some(vec![0xBE, 0xEF]),
        999,
    );
    assert_eq!(req.clone(), req);
}

// ──────────────────────────────────────────────────────────────────────────────
// ProduceRequest::payload_hint encoding
//
// Protocol encodes nullable bytes using varint (zigzag):
//   None  → [0x01]  (zigzag(-1) = 1)
//   Some([b]) → [0x02, b]  (zigzag(1) = 2, then the byte)
//   Some([]) → [0x00]  (zigzag(0) = 0)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn payload_hint_both_key_and_value_encoded() {
    let req = ProduceRequest::new("t", 0, Some(b"k".to_vec()), Some(b"v".to_vec()), 0);
    let hint = req.payload_hint();
    // key=Some("k") → [0x02, b'k'], value=Some("v") → [0x02, b'v']
    assert_eq!(hint, vec![0x02, b'k', 0x02, b'v']);
}

#[test]
fn payload_hint_null_key_non_null_value() {
    let req = ProduceRequest::new("t", 0, None, Some(b"v".to_vec()), 0);
    let hint = req.payload_hint();
    // null key → [0x01], value=Some("v") → [0x02, b'v']
    assert_eq!(hint, vec![0x01, 0x02, b'v']);
}

#[test]
fn payload_hint_non_null_key_null_value() {
    let req = ProduceRequest::new("t", 0, Some(b"k".to_vec()), None, 0);
    let hint = req.payload_hint();
    // key=Some("k") → [0x02, b'k'], null value → [0x01]
    assert_eq!(hint, vec![0x02, b'k', 0x01]);
}

#[test]
fn payload_hint_both_null() {
    let req = ProduceRequest::new("t", 0, None, None, 0);
    let hint = req.payload_hint();
    // null key → [0x01], null value → [0x01]
    assert_eq!(hint, vec![0x01, 0x01]);
}

#[test]
fn payload_hint_empty_key_non_null_value() {
    let req = ProduceRequest::new("t", 0, Some(vec![]), Some(b"v".to_vec()), 0);
    let hint = req.payload_hint();
    // empty key → [0x00], value=Some("v") → [0x02, b'v']
    assert_eq!(hint, vec![0x00, 0x02, b'v']);
}

#[test]
fn payload_hint_multi_byte_key_and_value() {
    let req = ProduceRequest::new(
        "t",
        0,
        Some(vec![0xAA, 0xBB]),
        Some(vec![0xCC, 0xDD, 0xEE]),
        0,
    );
    let hint = req.payload_hint();
    // key len=2 → zigzag(2)=4=0x04, then [0xAA, 0xBB]
    // value len=3 → zigzag(3)=6=0x06, then [0xCC, 0xDD, 0xEE]
    assert_eq!(hint, vec![0x04, 0xAA, 0xBB, 0x06, 0xCC, 0xDD, 0xEE]);
}

#[test]
fn payload_hint_length_reflects_both_fields() {
    // key=4 bytes → varint(4) = 0x08, value=4 bytes → varint(4) = 0x08
    let req = ProduceRequest::new("t", 0, Some(vec![1, 2, 3, 4]), Some(vec![5, 6, 7, 8]), 0);
    let hint = req.payload_hint();
    // 1 byte for key length + 4 bytes key + 1 byte for value length + 4 bytes value
    assert_eq!(hint.len(), 10);
}

// ──────────────────────────────────────────────────────────────────────────────
// FetchRequest construction
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn fetch_request_fields_set_correctly() {
    let req = FetchRequest::new("events", 7, 42, 500);
    assert_eq!(req.topic, "events");
    assert_eq!(req.partition, 7);
    assert_eq!(req.offset, 42);
    assert_eq!(req.max_records, 500);
}

#[test]
fn fetch_request_offset_zero_is_valid() {
    let req = FetchRequest::new("t", 0, 0, 1);
    assert_eq!(req.offset, 0);
}

#[test]
fn fetch_request_large_offset() {
    let req = FetchRequest::new("t", 0, i64::MAX, 1);
    assert_eq!(req.offset, i64::MAX);
}

#[test]
fn fetch_request_max_records_one() {
    let req = FetchRequest::new("t", 0, 0, 1);
    assert_eq!(req.max_records, 1);
}

#[test]
fn fetch_request_max_records_large() {
    let req = FetchRequest::new("t", 0, 0, 1_000_000);
    assert_eq!(req.max_records, 1_000_000);
}

#[test]
fn fetch_request_clone_is_equal() {
    let req = FetchRequest::new("clone-test", 2, 100, 200);
    assert_eq!(req.clone(), req);
}

#[test]
fn fetch_request_different_partitions_not_equal() {
    let a = FetchRequest::new("t", 0, 0, 100);
    let b = FetchRequest::new("t", 1, 0, 100);
    assert_ne!(a, b);
}

#[test]
fn fetch_request_different_offsets_not_equal() {
    let a = FetchRequest::new("t", 0, 0, 100);
    let b = FetchRequest::new("t", 0, 1, 100);
    assert_ne!(a, b);
}

// ──────────────────────────────────────────────────────────────────────────────
// ProduceRequest / FetchRequest topic names
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn produce_request_accepts_topic_with_dashes_and_underscores() {
    let req = ProduceRequest::new("my-topic_v2", 0, None, None, 0);
    assert_eq!(req.topic, "my-topic_v2");
}

#[test]
fn fetch_request_accepts_topic_with_dashes_and_underscores() {
    let req = FetchRequest::new("my-topic_v2", 0, 0, 1);
    assert_eq!(req.topic, "my-topic_v2");
}

#[test]
fn produce_request_into_string_coercion() {
    // topic accepts impl Into<String>: verify with a &'static str
    let req = ProduceRequest::new("static-topic", 0, None, None, 0);
    assert_eq!(req.topic, "static-topic");

    // And with a String
    let topic = String::from("owned-topic");
    let req2 = ProduceRequest::new(topic, 0, None, None, 0);
    assert_eq!(req2.topic, "owned-topic");
}
