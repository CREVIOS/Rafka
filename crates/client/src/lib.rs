#![forbid(unsafe_code)]

use rafka_protocol::encode_nullable_bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub max_records: usize,
}

impl ProduceRequest {
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        timestamp_ms: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            key,
            value,
            timestamp_ms,
        }
    }

    pub fn payload_hint(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(encode_nullable_bytes(self.key.as_deref()));
        out.extend(encode_nullable_bytes(self.value.as_deref()));
        out
    }
}

impl FetchRequest {
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64, max_records: usize) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            max_records,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_payload_hint_contains_nullable_fields() {
        let req = ProduceRequest::new("t", 0, Some(b"k".to_vec()), None, 1);
        let hint = req.payload_hint();
        assert_eq!(hint, vec![0x02, b'k', 0x01]);
    }

    #[test]
    fn fetch_request_constructs() {
        let req = FetchRequest::new("topic-a", 1, 42, 100);
        assert_eq!(req.topic, "topic-a");
        assert_eq!(req.partition, 1);
        assert_eq!(req.offset, 42);
        assert_eq!(req.max_records, 100);
    }
}
