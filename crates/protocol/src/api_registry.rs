#![forbid(unsafe_code)]

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionRangeSpec {
    pub low: i16,
    pub high: i16,
}

impl VersionRangeSpec {
    pub const fn none() -> Self {
        Self { low: 0, high: -1 }
    }

    pub const fn contains(self, version: i16) -> bool {
        self.low <= version && version <= self.high
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolMessageType {
    Request,
    Response,
    Header,
    Data,
    Metadata,
    CoordinatorKey,
    CoordinatorValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolMessageMeta {
    pub name: &'static str,
    pub api_key: Option<i16>,
    pub message_type: ProtocolMessageType,
    pub valid_versions: VersionRangeSpec,
    pub flexible_versions: VersionRangeSpec,
}

include!("generated_api_registry.rs");

pub fn find_by_name(name: &str) -> Option<&'static ProtocolMessageMeta> {
    PROTOCOL_MESSAGE_REGISTRY
        .iter()
        .find(|entry| entry.name == name)
}

pub fn find_by_api_key(
    api_key: i16,
    message_type: ProtocolMessageType,
) -> Option<&'static ProtocolMessageMeta> {
    PROTOCOL_MESSAGE_REGISTRY
        .iter()
        .find(|entry| entry.api_key == Some(api_key) && entry.message_type == message_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_produce_request() {
        let produce = find_by_name("ProduceRequest").expect("produce request");
        assert_eq!(produce.api_key, Some(0));
        assert_eq!(produce.message_type, ProtocolMessageType::Request);
        assert!(produce.valid_versions.contains(3));
        assert!(!produce.valid_versions.contains(2));
    }

    #[test]
    fn find_by_api_key_and_type() {
        let maybe = find_by_api_key(18, ProtocolMessageType::Request);
        let req = maybe.expect("api versions request");
        assert_eq!(req.name, "ApiVersionsRequest");
    }
}
