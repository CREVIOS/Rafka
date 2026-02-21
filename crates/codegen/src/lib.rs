#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{Display, Formatter, Write as _};
use std::fs;
use std::path::{Path, PathBuf};

use jsonc_parser::{parse_to_serde_value, ParseOptions};
use serde::Deserialize;

#[derive(Debug)]
pub enum CodegenError {
    Io(std::io::Error),
    JsonParse(String),
    JsonDecode(serde_json::Error),
    MissingJsonValue,
    InvalidVersionRange(String),
    InvalidTag(String),
    UnsupportedFieldType(String),
    MissingTargetMessage(String),
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::JsonParse(err) => write!(f, "jsonc parse error: {err}"),
            Self::JsonDecode(err) => write!(f, "json decode error: {err}"),
            Self::MissingJsonValue => write!(f, "jsonc parser returned no value"),
            Self::InvalidVersionRange(value) => write!(f, "invalid version range: {value}"),
            Self::InvalidTag(value) => write!(f, "invalid tag value: {value}"),
            Self::UnsupportedFieldType(value) => write!(f, "unsupported field type: {value}"),
            Self::MissingTargetMessage(value) => write!(f, "target message not found: {value}"),
        }
    }
}

impl Error for CodegenError {}

impl From<std::io::Error> for CodegenError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for CodegenError {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonDecode(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    Request,
    Response,
    Header,
    Data,
    Metadata,
    CoordinatorKey,
    CoordinatorValue,
}

impl MessageType {
    fn as_generated_enum_variant(self) -> &'static str {
        match self {
            Self::Request => "Request",
            Self::Response => "Response",
            Self::Header => "Header",
            Self::Data => "Data",
            Self::Metadata => "Metadata",
            Self::CoordinatorKey => "CoordinatorKey",
            Self::CoordinatorValue => "CoordinatorValue",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionRange {
    pub low: i16,
    pub high: i16,
}

impl VersionRange {
    pub const fn none() -> Self {
        Self { low: 0, high: -1 }
    }

    pub const fn is_none(self) -> bool {
        self.low > self.high
    }

    pub const fn intersect(self, other: Self) -> Self {
        let low = if self.low > other.low {
            self.low
        } else {
            other.low
        };
        let high = if self.high < other.high {
            self.high
        } else {
            other.high
        };
        Self { low, high }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldTypeSpec {
    Bool,
    Int8,
    Int16,
    Uint16,
    Uint32,
    Int32,
    Int64,
    Float64,
    String,
    Bytes,
    Records,
    Uuid,
    Struct(String),
    Array(Box<FieldTypeSpec>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageFieldSpec {
    pub name: String,
    pub field_type: FieldTypeSpec,
    pub versions: VersionRange,
    pub nullable_versions: VersionRange,
    pub tagged_versions: VersionRange,
    pub tag: Option<u32>,
    pub fields: Vec<MessageFieldSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolMessageSpec {
    pub name: String,
    pub api_key: Option<i16>,
    pub message_type: MessageType,
    pub valid_versions: VersionRange,
    pub flexible_versions: VersionRange,
    pub fields: Vec<MessageFieldSpec>,
    pub source_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMessageSpec {
    name: String,
    #[serde(rename = "type")]
    message_type: RawMessageType,
    valid_versions: String,
    flexible_versions: Option<String>,
    #[serde(default)]
    api_key: Option<i16>,
    #[serde(default)]
    fields: Vec<RawFieldSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFieldSpec {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    versions: Option<String>,
    #[serde(default)]
    nullable_versions: Option<String>,
    #[serde(default)]
    tagged_versions: Option<String>,
    #[serde(default)]
    tag: Option<RawTag>,
    #[serde(default)]
    fields: Vec<RawFieldSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawTag {
    Number(u32),
    String(String),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawMessageType {
    Request,
    Response,
    Header,
    Data,
    Metadata,
    CoordinatorKey,
    CoordinatorValue,
}

impl From<RawMessageType> for MessageType {
    fn from(value: RawMessageType) -> Self {
        match value {
            RawMessageType::Request => Self::Request,
            RawMessageType::Response => Self::Response,
            RawMessageType::Header => Self::Header,
            RawMessageType::Data => Self::Data,
            RawMessageType::Metadata => Self::Metadata,
            RawMessageType::CoordinatorKey => Self::CoordinatorKey,
            RawMessageType::CoordinatorValue => Self::CoordinatorValue,
        }
    }
}

pub fn parse_version_range(input: &str) -> Result<VersionRange, CodegenError> {
    let trimmed = input.trim();
    if trimmed.eq_ignore_ascii_case("none") {
        return Ok(VersionRange::none());
    }
    if let Some(low) = trimmed.strip_suffix('+') {
        let low = parse_non_negative_i16(low, trimmed)?;
        return Ok(VersionRange {
            low,
            high: i16::MAX,
        });
    }
    if let Some((low, high)) = trimmed.split_once('-') {
        let low = parse_non_negative_i16(low, trimmed)?;
        let high = parse_non_negative_i16(high, trimmed)?;
        if low > high {
            return Err(CodegenError::InvalidVersionRange(trimmed.to_string()));
        }
        return Ok(VersionRange { low, high });
    }
    let single = parse_non_negative_i16(trimmed, trimmed)?;
    Ok(VersionRange {
        low: single,
        high: single,
    })
}

fn parse_non_negative_i16(value: &str, full: &str) -> Result<i16, CodegenError> {
    let parsed = value
        .trim()
        .parse::<i16>()
        .map_err(|_| CodegenError::InvalidVersionRange(full.to_string()))?;
    if parsed < 0 {
        return Err(CodegenError::InvalidVersionRange(full.to_string()));
    }
    Ok(parsed)
}

fn parse_field_type(input: &str) -> Result<FieldTypeSpec, CodegenError> {
    let trimmed = input.trim();
    if let Some(inner) = trimmed.strip_prefix("[]") {
        return Ok(FieldTypeSpec::Array(Box::new(parse_field_type(inner)?)));
    }
    let ty = match trimmed {
        "bool" => FieldTypeSpec::Bool,
        "int8" => FieldTypeSpec::Int8,
        "int16" => FieldTypeSpec::Int16,
        "uint16" => FieldTypeSpec::Uint16,
        "uint32" => FieldTypeSpec::Uint32,
        "int32" => FieldTypeSpec::Int32,
        "int64" => FieldTypeSpec::Int64,
        "float64" => FieldTypeSpec::Float64,
        "string" => FieldTypeSpec::String,
        "bytes" => FieldTypeSpec::Bytes,
        "records" => FieldTypeSpec::Records,
        "uuid" => FieldTypeSpec::Uuid,
        other => {
            if other
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_uppercase())
            {
                FieldTypeSpec::Struct(other.to_string())
            } else {
                return Err(CodegenError::UnsupportedFieldType(other.to_string()));
            }
        }
    };
    Ok(ty)
}

fn parse_fields(
    raw_fields: &[RawFieldSpec],
    parent_versions: VersionRange,
) -> Result<Vec<MessageFieldSpec>, CodegenError> {
    let mut parsed = Vec::with_capacity(raw_fields.len());
    for raw in raw_fields {
        let versions = match raw.versions.as_deref() {
            Some(v) => parse_version_range(v)?,
            None => parent_versions,
        }
        .intersect(parent_versions);
        let nullable_versions = match raw.nullable_versions.as_deref() {
            Some(v) => parse_version_range(v)?,
            None => VersionRange::none(),
        };
        let tagged_versions = match raw.tagged_versions.as_deref() {
            Some(v) => parse_version_range(v)?,
            None => VersionRange::none(),
        };
        let field_type = parse_field_type(&raw.type_name)?;
        let fields = parse_fields(&raw.fields, versions)?;
        let tag = match &raw.tag {
            Some(RawTag::Number(value)) => Some(*value),
            Some(RawTag::String(value)) => Some(
                value
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| CodegenError::InvalidTag(value.clone()))?,
            ),
            None => None,
        };
        parsed.push(MessageFieldSpec {
            name: raw.name.clone(),
            field_type,
            versions,
            nullable_versions,
            tagged_versions,
            tag,
            fields,
        });
    }
    Ok(parsed)
}

pub fn load_message_specs(input_dir: &Path) -> Result<Vec<ProtocolMessageSpec>, CodegenError> {
    let mut entries = fs::read_dir(input_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    entries.sort();

    let mut specs = Vec::with_capacity(entries.len());
    for path in entries {
        let raw_text = fs::read_to_string(&path)?;
        let parsed = parse_to_serde_value(&raw_text, &ParseOptions::default())
            .map_err(|err| CodegenError::JsonParse(err.to_string()))?
            .ok_or(CodegenError::MissingJsonValue)?;
        let raw: RawMessageSpec = serde_json::from_value(parsed)?;
        let valid_versions = parse_version_range(&raw.valid_versions)?;
        let flexible_versions = match raw.flexible_versions.as_deref() {
            Some(v) => parse_version_range(v)?,
            None => VersionRange::none(),
        };
        let fields = parse_fields(&raw.fields, valid_versions)?;
        specs.push(ProtocolMessageSpec {
            name: raw.name,
            api_key: raw.api_key,
            message_type: raw.message_type.into(),
            valid_versions,
            flexible_versions,
            fields,
            source_file: path,
        });
    }
    Ok(specs)
}

pub fn generate_registry_source(specs: &[ProtocolMessageSpec]) -> String {
    let mut sorted = specs.to_vec();
    sorted.sort_by(|a, b| {
        a.api_key
            .cmp(&b.api_key)
            .then(a.name.cmp(&b.name))
            .then(a.source_file.cmp(&b.source_file))
    });

    let mut out = String::new();
    out.push_str("// @generated by rafka-codegen. DO NOT EDIT.\n");
    out.push_str("pub const PROTOCOL_MESSAGE_REGISTRY: &[ProtocolMessageMeta] = &[\n");
    for spec in &sorted {
        let _ = writeln!(
            out,
            "    ProtocolMessageMeta {{ name: {:?}, api_key: {:?}, message_type: ProtocolMessageType::{}, valid_versions: VersionRangeSpec {{ low: {}, high: {} }}, flexible_versions: VersionRangeSpec {{ low: {}, high: {} }} }},",
            spec.name,
            spec.api_key,
            spec.message_type.as_generated_enum_variant(),
            spec.valid_versions.low,
            spec.valid_versions.high,
            spec.flexible_versions.low,
            spec.flexible_versions.high
        );
    }
    out.push_str("];\n");
    out
}

fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    let mut prev_is_lower_or_digit = false;
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            if prev_is_lower_or_digit {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            prev_is_lower_or_digit = false;
        } else if matches!(ch, '-' | ' ' | '.') {
            if !out.ends_with('_') {
                out.push('_');
            }
            prev_is_lower_or_digit = false;
        } else {
            out.push(ch.to_ascii_lowercase());
            prev_is_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    if out.is_empty() {
        return "_".to_string();
    }
    match out.as_str() {
        "type" | "match" | "move" | "loop" | "where" | "self" => format!("{out}_"),
        _ => out,
    }
}

fn sanitize_type_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        }
    }
    if out.is_empty() {
        "Unknown".to_string()
    } else {
        out
    }
}

fn nested_struct_name(top_level: &str, struct_name: &str) -> String {
    format!(
        "{}{}",
        sanitize_type_name(top_level),
        sanitize_type_name(struct_name)
    )
}

fn rust_base_type(top_level: &str, field_type: &FieldTypeSpec) -> String {
    match field_type {
        FieldTypeSpec::Bool => "bool".to_string(),
        FieldTypeSpec::Int8 => "i8".to_string(),
        FieldTypeSpec::Int16 => "i16".to_string(),
        FieldTypeSpec::Uint16 => "u16".to_string(),
        FieldTypeSpec::Uint32 => "u32".to_string(),
        FieldTypeSpec::Int32 => "i32".to_string(),
        FieldTypeSpec::Int64 => "i64".to_string(),
        FieldTypeSpec::Float64 => "f64".to_string(),
        FieldTypeSpec::String => "String".to_string(),
        FieldTypeSpec::Bytes | FieldTypeSpec::Records => "Vec<u8>".to_string(),
        FieldTypeSpec::Uuid => "[u8; 16]".to_string(),
        FieldTypeSpec::Struct(name) => nested_struct_name(top_level, name),
        FieldTypeSpec::Array(inner) => format!("Vec<{}>", rust_base_type(top_level, inner)),
    }
}

fn field_is_optional(field: &MessageFieldSpec, parent_versions: VersionRange) -> bool {
    let effective = field.versions.intersect(parent_versions);
    effective != parent_versions
        || !field.nullable_versions.is_none()
        || !field.tagged_versions.is_none()
}

fn emit_struct_definition(
    out: &mut String,
    top_level: &str,
    struct_name: &str,
    fields: &[MessageFieldSpec],
    parent_versions: VersionRange,
) {
    let _ = writeln!(out, "#[derive(Debug, Clone, PartialEq, Default)]");
    let _ = writeln!(out, "pub struct {struct_name} {{");
    for field in fields {
        let field_name = snake_case(&field.name);
        let base = rust_base_type(top_level, &field.field_type);
        let rust_type = if field_is_optional(field, parent_versions) {
            format!("Option<{base}>")
        } else {
            base
        };
        let _ = writeln!(out, "    pub {field_name}: {rust_type},");
    }
    let _ = writeln!(out, "}}\n");
}

fn emit_nested_structs(
    out: &mut String,
    emitted: &mut std::collections::BTreeSet<String>,
    top_level: &str,
    fields: &[MessageFieldSpec],
    parent_versions: VersionRange,
) {
    for field in fields {
        let nested_type_name = match &field.field_type {
            FieldTypeSpec::Struct(name) => Some(name.as_str()),
            FieldTypeSpec::Array(inner) => match inner.as_ref() {
                FieldTypeSpec::Struct(name) => Some(name.as_str()),
                _ => None,
            },
            _ => None,
        };
        let Some(nested) = nested_type_name else {
            continue;
        };
        if field.fields.is_empty() {
            continue;
        }
        let nested_name = nested_struct_name(top_level, nested);
        if emitted.insert(nested_name.clone()) {
            let nested_parent = field.versions.intersect(parent_versions);
            emit_struct_definition(out, top_level, &nested_name, &field.fields, nested_parent);
            emit_nested_structs(
                out,
                emitted,
                top_level,
                &field.fields,
                field.versions.intersect(parent_versions),
            );
        }
    }
}

pub fn generate_message_structs_source(
    specs: &[ProtocolMessageSpec],
    target_message_names: &[&str],
) -> Result<String, CodegenError> {
    let mut out = String::new();
    out.push_str("// @generated by rafka-codegen. DO NOT EDIT.\n");
    let mut emitted = std::collections::BTreeSet::new();

    for target_name in target_message_names {
        let spec = specs
            .iter()
            .find(|spec| spec.name == *target_name)
            .ok_or_else(|| CodegenError::MissingTargetMessage((*target_name).to_string()))?;
        if emitted.insert(spec.name.clone()) {
            emit_struct_definition(
                &mut out,
                &spec.name,
                &spec.name,
                &spec.fields,
                spec.valid_versions,
            );
            emit_nested_structs(
                &mut out,
                &mut emitted,
                &spec.name,
                &spec.fields,
                spec.valid_versions,
            );
        }
    }
    Ok(out)
}

pub fn write_registry_file(output_path: &Path, source: &str) -> Result<(), CodegenError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, source)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../clients/src/main/resources/common/message")
    }

    #[test]
    fn parses_version_ranges() {
        assert_eq!(
            parse_version_range("3-13").expect("range"),
            VersionRange { low: 3, high: 13 }
        );
        assert_eq!(
            parse_version_range("9+").expect("open"),
            VersionRange {
                low: 9,
                high: i16::MAX
            }
        );
        assert_eq!(
            parse_version_range("none").expect("none"),
            VersionRange::none()
        );
    }

    #[test]
    fn loads_real_kafka_specs() {
        let specs = load_message_specs(&specs_dir()).expect("load specs");
        assert!(specs.len() >= 190);
        let produce = specs
            .iter()
            .find(|spec| spec.name == "ProduceRequest")
            .expect("produce request");
        assert_eq!(produce.api_key, Some(0));
        assert_eq!(produce.valid_versions, VersionRange { low: 3, high: 13 });
        assert_eq!(
            produce.flexible_versions,
            VersionRange {
                low: 9,
                high: i16::MAX
            }
        );
        assert!(!produce.fields.is_empty());
    }

    #[test]
    fn generated_registry_contains_known_entries() {
        let specs = load_message_specs(&specs_dir()).expect("load specs");
        let output = generate_registry_source(&specs);
        assert!(output.contains("ProduceRequest"));
        assert!(output.contains("ApiVersionsRequest"));
        assert!(output.contains("PROTOCOL_MESSAGE_REGISTRY"));
    }

    #[test]
    fn generated_message_structs_contain_targets() {
        let specs = load_message_specs(&specs_dir()).expect("load specs");
        let output = generate_message_structs_source(
            &specs,
            &[
                "ApiVersionsRequest",
                "ApiVersionsResponse",
                "ProduceRequest",
                "OffsetCommitRequest",
                "OffsetCommitResponse",
                "OffsetFetchRequest",
                "OffsetFetchResponse",
                "JoinGroupRequest",
                "JoinGroupResponse",
                "SyncGroupRequest",
                "SyncGroupResponse",
                "HeartbeatRequest",
                "HeartbeatResponse",
                "LeaveGroupRequest",
                "LeaveGroupResponse",
                "SaslHandshakeRequest",
                "SaslHandshakeResponse",
                "SaslAuthenticateRequest",
                "SaslAuthenticateResponse",
            ],
        )
        .expect("message structs");
        assert!(output.contains("pub struct ApiVersionsRequest"));
        assert!(output.contains("pub struct ApiVersionsResponse"));
        assert!(output.contains("pub struct ProduceRequest"));
        assert!(output.contains("pub struct OffsetCommitRequest"));
        assert!(output.contains("pub struct OffsetCommitResponse"));
        assert!(output.contains("pub struct OffsetFetchRequest"));
        assert!(output.contains("pub struct OffsetFetchResponse"));
        assert!(output.contains("pub struct JoinGroupRequest"));
        assert!(output.contains("pub struct JoinGroupResponse"));
        assert!(output.contains("pub struct SyncGroupRequest"));
        assert!(output.contains("pub struct SyncGroupResponse"));
        assert!(output.contains("pub struct HeartbeatRequest"));
        assert!(output.contains("pub struct HeartbeatResponse"));
        assert!(output.contains("pub struct LeaveGroupRequest"));
        assert!(output.contains("pub struct LeaveGroupResponse"));
        assert!(output.contains("pub struct SaslHandshakeRequest"));
        assert!(output.contains("pub struct SaslHandshakeResponse"));
        assert!(output.contains("pub struct SaslAuthenticateRequest"));
        assert!(output.contains("pub struct SaslAuthenticateResponse"));
        assert!(output.contains("pub struct ApiVersionsResponseApiVersion"));
        assert!(output.contains("pub struct ProduceRequestTopicProduceData"));
    }
}
