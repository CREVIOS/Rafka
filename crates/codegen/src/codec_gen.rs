//! Generation of `VersionedCodec` implementations from parsed Kafka message
//! specs. The emitted source is included into `rafka_protocol::messages`,
//! where the write_*/read_* wire helpers live, so generated code can use them
//! directly.
//!
//! Supported wire features:
//! - scalars (bool, int8, int16, uint16, uint32, int32, int64, float64, uuid)
//! - strings/bytes, nullable and non-nullable
//! - arrays of scalars/strings/bytes/structs, nullable arrays
//! - nested inline structs (recursive)
//! - per-field version gating
//! - flexible versions: compact strings/arrays/bytes plus the trailing
//!   tagged-field section at every struct level (payload set is empty in this
//!   revision; tagged fields are encoded/decoded in a follow-up)

use std::collections::BTreeSet;
use std::fmt::Write as _;

use crate::{
    CodegenError, FieldTypeSpec, MessageFieldSpec, MessageType, ProtocolMessageSpec, VersionRange,
};

fn upper_screaming(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev_lower = false;
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(ch);
            prev_lower = false;
        } else if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            prev_lower = true;
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    out
}

fn gate_expr(field: &MessageFieldSpec) -> String {
    if field.versions.high == i16::MAX {
        format!("version >= {}", field.versions.low)
    } else if field.versions.low == field.versions.high {
        format!("version == {}", field.versions.low)
    } else {
        format!(
            "version >= {} && version <= {}",
            field.versions.low, field.versions.high
        )
    }
}

fn field_is_optional(field: &MessageFieldSpec, parent_versions: VersionRange) -> bool {
    let effective = field.versions.intersect(parent_versions);
    effective != parent_versions
        || !field.nullable_versions.is_none()
        || !field.tagged_versions.is_none()
}

fn field_is_nullable(field: &MessageFieldSpec) -> bool {
    !field.nullable_versions.is_none()
}

enum WireScalar {
    I8,
    I16,
    U16,
    U32,
    I32,
    I64,
    F64,
    Bool,
    Uuid,
}

fn scalar_kind(field_type: &FieldTypeSpec) -> Option<WireScalar> {
    match field_type {
        FieldTypeSpec::Bool => Some(WireScalar::Bool),
        FieldTypeSpec::Int8 => Some(WireScalar::I8),
        FieldTypeSpec::Int16 => Some(WireScalar::I16),
        FieldTypeSpec::Uint16 => Some(WireScalar::U16),
        FieldTypeSpec::Uint32 => Some(WireScalar::U32),
        FieldTypeSpec::Int32 => Some(WireScalar::I32),
        FieldTypeSpec::Int64 => Some(WireScalar::I64),
        FieldTypeSpec::Float64 => Some(WireScalar::F64),
        FieldTypeSpec::Uuid => Some(WireScalar::Uuid),
        _ => None,
    }
}

fn read_call(kind: &WireScalar) -> &'static str {
    match kind {
        WireScalar::I8 => "reader.read_i8()?",
        WireScalar::I16 => "reader.read_i16()?",
        WireScalar::U16 => "reader.read_u16()?",
        WireScalar::U32 => "reader.read_u32()?",
        WireScalar::I32 => "reader.read_i32()?",
        WireScalar::I64 => "reader.read_i64()?",
        WireScalar::F64 => "reader.read_f64()?",
        WireScalar::Bool => "reader.read_bool()?",
        WireScalar::Uuid => "reader.read_uuid()?",
    }
}

fn write_call(kind: &WireScalar, value: &str) -> String {
    match kind {
        WireScalar::I8 => format!("write_i8(&mut out, {value})?;"),
        WireScalar::I16 => format!("write_i16(&mut out, {value});"),
        WireScalar::U16 => format!("write_u16(&mut out, {value});"),
        WireScalar::U32 => format!("write_u32(&mut out, {value});"),
        WireScalar::I32 => format!("write_i32(&mut out, {value});"),
        WireScalar::I64 => format!("write_i64(&mut out, {value});"),
        WireScalar::F64 => format!("write_f64(&mut out, {value});"),
        WireScalar::Bool => format!("write_bool(&mut out, {value});"),
        WireScalar::Uuid => format!("write_uuid(&mut out, {value});"),
    }
}

/// Emit statements encoding the fields of one struct level from `subject`
/// into `out`, followed by the trailing tagged-field section when flexible.
#[allow(clippy::too_many_arguments)] // recursive emitter: each arg is used at every level
fn emit_struct_encode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    fields: &[MessageFieldSpec],
    parent_versions: VersionRange,
    subject: &str,
    flexible: &str,
    msg_name: &str,
) -> Result<(), CodegenError> {
    for field in fields {
        if field.versions == parent_versions {
            emit_field_encode(
                out,
                indent,
                top_level,
                field,
                parent_versions,
                subject,
                flexible,
                msg_name,
            )?;
        } else {
            let _ = writeln!(out, "{indent}if {} {{", gate_expr(field));
            emit_field_encode(
                out,
                indent,
                top_level,
                field,
                parent_versions,
                subject,
                flexible,
                msg_name,
            )?;
            let _ = writeln!(out, "{indent}}}");
        }
    }
    let _ = writeln!(
        out,
        "{indent}if {flexible} {{ write_tagged_fields(&mut out, Vec::new())?; }}"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)] // recursive emitter: each arg is used at every level
fn emit_field_encode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    field: &MessageFieldSpec,
    parent_versions: VersionRange,
    subject: &str,
    flexible: &str,
    msg_name: &str,
) -> Result<(), CodegenError> {
    let fname = crate::snake_case_pub(&field.name);
    let accessor = format!("{subject}.{fname}");
    // `optional` mirrors struct generation: the field is Option-wrapped when
    // absent in some parent version, nullable, or tagged.
    let optional = field_is_optional(field, parent_versions);
    let label = format!("{msg_name}.{}", field.name);

    match &field.field_type {
        FieldTypeSpec::String => {
            if field_is_nullable(field) {
                let _ = writeln!(
                    out,
                    "{indent}write_nullable_string(&mut out, {accessor}.as_deref(), {flexible})?;"
                );
            } else if optional {
                let _ = writeln!(out, "{indent}write_string(&mut out, {accessor}.as_deref().unwrap_or_default(), {flexible})?;");
            } else {
                let _ = writeln!(
                    out,
                    "{indent}write_string(&mut out, &{accessor}, {flexible})?;"
                );
            }
        }
        FieldTypeSpec::Bytes | FieldTypeSpec::Records => {
            if field_is_nullable(field) {
                let _ = writeln!(
                    out,
                    "{indent}write_nullable_bytes(&mut out, {accessor}.as_deref(), {flexible})?;"
                );
            } else if optional {
                let _ = writeln!(out, "{indent}write_bytes(&mut out, {accessor}.as_deref().unwrap_or_default(), {flexible})?;");
            } else {
                let _ = writeln!(
                    out,
                    "{indent}write_bytes(&mut out, &{accessor}, {flexible})?;"
                );
            }
        }
        FieldTypeSpec::Array(inner) => {
            // nullable arrays (and arrays absent in some versions) encode
            // through the nullable length helper so `None` round-trips.
            if field_is_nullable(field) || optional {
                let _ = writeln!(out, "{indent}write_nullable_array_len(&mut out, {accessor}.as_ref().map(Vec::len), {flexible})?;");
                let _ = writeln!(out, "{indent}if let Some(items) = {accessor}.as_ref() {{");
                emit_array_encode(
                    out,
                    &format!("{indent}    "),
                    top_level,
                    inner,
                    field,
                    "items",
                    flexible,
                    &label,
                )?;
                let _ = writeln!(out, "{indent}}}");
            } else {
                let _ = writeln!(
                    out,
                    "{indent}write_array_len(&mut out, {accessor}.len(), {flexible})?;"
                );
                emit_array_encode(
                    out,
                    indent,
                    top_level,
                    inner,
                    field,
                    &format!("&{accessor}"),
                    flexible,
                    &label,
                )?;
            }
        }
        FieldTypeSpec::Struct(n) => {
            if field.fields.is_empty() {
                return Err(CodegenError::UnsupportedFieldType(format!(
                    "codec struct without inline fields: {n}"
                )));
            }
            if optional {
                let _ = writeln!(out, "{indent}let subject = {accessor}.as_ref().ok_or(ProtocolError::MissingRequiredField(\"{label}\"))?;");
            } else {
                let _ = writeln!(out, "{indent}let subject = &{accessor};");
            }
            emit_struct_encode(
                out,
                indent,
                top_level,
                &field.fields,
                field.versions.intersect(parent_versions),
                "subject",
                flexible,
                msg_name,
            )?;
        }
        other => {
            let Some(kind) = scalar_kind(other) else {
                return Err(CodegenError::UnsupportedFieldType(format!(
                    "codec encode: {other:?}"
                )));
            };
            let value = if optional {
                match kind {
                    WireScalar::Bool => format!("{accessor}.unwrap_or(false)"),
                    WireScalar::Uuid => format!("*{accessor}.as_ref().unwrap_or(&[0_u8; 16])"),
                    _ => format!(
                        "{accessor}.ok_or(ProtocolError::MissingRequiredField(\"{label}\"))?"
                    ),
                }
            } else {
                match kind {
                    WireScalar::Uuid => format!("&{accessor}"),
                    _ => accessor.clone(),
                }
            };
            let _ = writeln!(out, "{indent}{}", write_call(&kind, &value));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // recursive emitter: each arg is used at every level
fn emit_array_encode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    inner: &FieldTypeSpec,
    field: &MessageFieldSpec,
    accessor: &str,
    flexible: &str,
    label: &str,
) -> Result<(), CodegenError> {
    let _ = writeln!(out, "{indent}for item in {accessor} {{");
    match inner {
        FieldTypeSpec::Struct(_) => {
            emit_struct_encode(
                out,
                &format!("{indent}    "),
                top_level,
                &field.fields,
                field.versions,
                "item",
                flexible,
                label,
            )?;
        }
        FieldTypeSpec::String => {
            let _ = writeln!(
                out,
                "{indent}    write_string(&mut out, item, {flexible})?;"
            );
        }
        FieldTypeSpec::Bytes | FieldTypeSpec::Records => {
            let _ = writeln!(out, "{indent}    write_bytes(&mut out, item, {flexible})?;");
        }
        other => {
            let Some(kind) = scalar_kind(other) else {
                return Err(CodegenError::UnsupportedFieldType(format!(
                    "codec array: {other:?}"
                )));
            };
            let call = match kind {
                WireScalar::I8 => "write_i8(&mut out, *item)?;".to_string(),
                WireScalar::I16 => "write_i16(&mut out, *item);".to_string(),
                WireScalar::U16 => "write_u16(&mut out, *item);".to_string(),
                WireScalar::U32 => "write_u32(&mut out, *item);".to_string(),
                WireScalar::I32 => "write_i32(&mut out, *item);".to_string(),
                WireScalar::I64 => "write_i64(&mut out, *item);".to_string(),
                WireScalar::F64 => "write_f64(&mut out, *item);".to_string(),
                WireScalar::Bool => "write_bool(&mut out, *item);".to_string(),
                WireScalar::Uuid => "write_uuid(&mut out, item);".to_string(),
            };
            let _ = writeln!(out, "{indent}    {call}");
        }
    }
    let _ = writeln!(out, "{indent}}}");
    Ok(())
}

/// Emit statements decoding the fields of one struct level from `reader`
/// into `subject`, followed by the trailing tagged-field section when
/// flexible.
fn emit_struct_decode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    fields: &[MessageFieldSpec],
    parent_versions: VersionRange,
    subject: &str,
    flexible: &str,
) -> Result<(), CodegenError> {
    for field in fields {
        if field.versions == parent_versions {
            emit_field_decode(
                out,
                indent,
                top_level,
                field,
                parent_versions,
                flexible,
                subject,
            )?;
        } else {
            let _ = writeln!(out, "{indent}if {} {{", gate_expr(field));
            emit_field_decode(
                out,
                indent,
                top_level,
                field,
                parent_versions,
                flexible,
                subject,
            )?;
            let _ = writeln!(out, "{indent}}}");
        }
    }
    let _ = writeln!(
        out,
        "{indent}if {flexible} {{ read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?; }}"
    );
    Ok(())
}

fn emit_field_decode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    field: &MessageFieldSpec,
    parent_versions: VersionRange,
    flexible: &str,
    subject: &str,
) -> Result<(), CodegenError> {
    let fname = crate::snake_case_pub(&field.name);
    let optional = field_is_optional(field, parent_versions);
    let assign = |expr: &str| {
        if optional {
            format!("{subject}.{fname} = Some({expr});")
        } else {
            format!("{subject}.{fname} = {expr};")
        }
    };

    match &field.field_type {
        FieldTypeSpec::String => {
            if field_is_nullable(field) {
                let _ = writeln!(
                    out,
                    "{indent}{}",
                    assign(&format!("read_nullable_string(&mut reader, {flexible})?"))
                );
            } else {
                let _ = writeln!(
                    out,
                    "{indent}{}",
                    assign(&format!("read_string(&mut reader, {flexible})?"))
                );
            }
        }
        FieldTypeSpec::Bytes | FieldTypeSpec::Records => {
            if field_is_nullable(field) {
                let _ = writeln!(
                    out,
                    "{indent}{}",
                    assign(&format!("read_nullable_bytes(&mut reader, {flexible})?"))
                );
            } else {
                let _ = writeln!(
                    out,
                    "{indent}{}",
                    assign(&format!("read_bytes(&mut reader, {flexible})?"))
                );
            }
        }
        FieldTypeSpec::Array(inner) => {
            if field_is_nullable(field) || optional {
                let _ = writeln!(
                    out,
                    "{indent}match read_nullable_array_len(&mut reader, {flexible})? {{"
                );
                let _ = writeln!(out, "{indent}    Some(len) => {{");
                let _ = writeln!(
                    out,
                    "{indent}        let mut items = Vec::with_capacity(len);"
                );
                emit_array_decode(
                    out,
                    &format!("{indent}        "),
                    top_level,
                    inner,
                    field,
                    flexible,
                )?;
                let _ = writeln!(out, "{indent}        {subject}.{fname} = Some(items);");
                let _ = writeln!(out, "{indent}    }}");
                let _ = writeln!(out, "{indent}    None => {{}}");
                let _ = writeln!(out, "{indent}}}");
            } else {
                let _ = writeln!(
                    out,
                    "{indent}let len = read_array_len(&mut reader, {flexible})?;"
                );
                let _ = writeln!(out, "{indent}let mut items = Vec::with_capacity(len);");
                emit_array_decode(out, indent, top_level, inner, field, flexible)?;
                let _ = writeln!(out, "{indent}{}", assign("items"));
            }
        }
        FieldTypeSpec::Struct(n) => {
            let nested_name = crate::nested_struct_name_pub(top_level, n);
            let _ = writeln!(out, "{indent}let mut nested = {nested_name}::default();");
            emit_struct_decode(
                out,
                indent,
                top_level,
                &field.fields,
                field.versions,
                "nested",
                flexible,
            )?;
            let _ = writeln!(out, "{indent}{}", assign("nested"));
        }
        other => {
            let Some(kind) = scalar_kind(other) else {
                return Err(CodegenError::UnsupportedFieldType(format!(
                    "codec decode: {other:?}"
                )));
            };
            let _ = writeln!(out, "{indent}{}", assign(read_call(&kind)));
        }
    }
    Ok(())
}

fn emit_array_decode(
    out: &mut String,
    indent: &str,
    top_level: &str,
    inner: &FieldTypeSpec,
    field: &MessageFieldSpec,
    flexible: &str,
) -> Result<(), CodegenError> {
    let _ = writeln!(out, "{indent}for _ in 0..len {{");
    match inner {
        FieldTypeSpec::Struct(n) => {
            let nested_name = crate::nested_struct_name_pub(top_level, n);
            let _ = writeln!(out, "{indent}    let mut item = {nested_name}::default();");
            emit_struct_decode(
                out,
                &format!("{indent}    "),
                top_level,
                &field.fields,
                field.versions,
                "item",
                flexible,
            )?;
            let _ = writeln!(out, "{indent}    items.push(item);");
        }
        FieldTypeSpec::String => {
            let _ = writeln!(
                out,
                "{indent}    items.push(read_string(&mut reader, {flexible})?);"
            );
        }
        FieldTypeSpec::Bytes | FieldTypeSpec::Records => {
            let _ = writeln!(
                out,
                "{indent}    items.push(read_bytes(&mut reader, {flexible})?);"
            );
        }
        other => {
            let Some(kind) = scalar_kind(other) else {
                return Err(CodegenError::UnsupportedFieldType(format!(
                    "codec array decode: {other:?}"
                )));
            };
            let deref = match kind {
                WireScalar::Uuid | WireScalar::Bool => "",
                _ => "*",
            };
            let _ = writeln!(out, "{indent}    items.push({deref}{});", read_call(&kind));
        }
    }
    let _ = writeln!(out, "{indent}}}");
    Ok(())
}

pub fn generate_codec_source(
    specs: &[ProtocolMessageSpec],
    target_message_names: &[&str],
) -> Result<String, CodegenError> {
    let mut out = String::new();
    out.push_str("// @generated by rafka-codegen. DO NOT EDIT.\n");
    let mut emitted = BTreeSet::new();

    for target_name in target_message_names {
        let spec = specs
            .iter()
            .find(|spec| spec.name == *target_name)
            .ok_or_else(|| CodegenError::MissingTargetMessage((*target_name).to_string()))?;
        if !emitted.insert(spec.name.clone()) {
            continue;
        }
        if !matches!(
            spec.message_type,
            MessageType::Request | MessageType::Response
        ) {
            return Err(CodegenError::UnsupportedFieldType(format!(
                "codec target must be a request or response: {}",
                spec.name
            )));
        }
        emit_codec_impl(&mut out, spec)?;
    }
    Ok(out)
}

fn emit_codec_impl(out: &mut String, spec: &ProtocolMessageSpec) -> Result<(), CodegenError> {
    let name = &spec.name;
    let upper = upper_screaming(name);
    let first_flexible = if spec.flexible_versions.is_none() {
        "i16::MAX".to_string()
    } else {
        spec.flexible_versions.low.to_string()
    };

    // One `#[allow]` per item: an attribute before the first item of an
    // included file covers only that item, not the whole file.
    let _ = writeln!(out, "#[allow(clippy::all)]");
    let _ = writeln!(
        out,
        "pub const {upper}_MIN_VERSION: i16 = {};",
        spec.valid_versions.low
    );
    let _ = writeln!(out, "#[allow(clippy::all)]");
    let _ = writeln!(
        out,
        "pub const {upper}_MAX_VERSION: i16 = {};",
        spec.valid_versions.high
    );
    let _ = writeln!(out, "#[allow(clippy::all)]");
    let _ = writeln!(
        out,
        "pub const {upper}_FIRST_FLEXIBLE_VERSION: i16 = {first_flexible};"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "#[allow(clippy::all)]");
    let _ = writeln!(out, "impl VersionedCodec for {name} {{");
    let _ = writeln!(out, "    const API_NAME: &'static str = \"{name}\";");
    let _ = writeln!(out, "    const MIN_VERSION: i16 = {upper}_MIN_VERSION;");
    let _ = writeln!(out, "    const MAX_VERSION: i16 = {upper}_MAX_VERSION;");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {{"
    );
    let _ = writeln!(
        out,
        "        ensure_version(Self::API_NAME, version, Self::MIN_VERSION, Self::MAX_VERSION)?;"
    );
    let _ = writeln!(
        out,
        "        let flexible = is_flexible(version, {upper}_FIRST_FLEXIBLE_VERSION);"
    );
    let _ = writeln!(out, "        let mut out = Vec::new();");
    emit_struct_encode(
        out,
        "        ",
        name,
        &spec.fields,
        spec.valid_versions,
        "self",
        "flexible",
        name,
    )?;
    let _ = writeln!(out, "        Ok(out)");
    let _ = writeln!(out, "    }}");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {{"
    );
    let _ = writeln!(
        out,
        "        ensure_version(Self::API_NAME, version, Self::MIN_VERSION, Self::MAX_VERSION)?;"
    );
    let _ = writeln!(
        out,
        "        let flexible = is_flexible(version, {upper}_FIRST_FLEXIBLE_VERSION);"
    );
    let _ = writeln!(out, "        let mut reader = Reader::new(input);");
    let _ = writeln!(out, "        let mut decoded = Self::default();");
    emit_struct_decode(
        out,
        "        ",
        name,
        &spec.fields,
        spec.valid_versions,
        "decoded",
        "flexible",
    )?;
    let _ = writeln!(out, "        Ok((decoded, reader.cursor))");
    let _ = writeln!(out, "    }}");
    let _ = writeln!(out, "}}");
    let _ = writeln!(out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs_dir() -> std::path::PathBuf {
        let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let vendored = manifest.join("../../specs/kafka");
        if vendored.is_dir() {
            return vendored;
        }
        manifest.join("../../../clients/src/main/resources/common/message")
    }

    #[test]
    fn generates_codec_for_delete_groups_request() {
        let specs = crate::load_message_specs(&specs_dir()).expect("specs");
        let src = generate_codec_source(&specs, &["DeleteGroupsRequest"]).expect("codec");
        assert!(src.contains("pub const DELETE_GROUPS_REQUEST_MIN_VERSION: i16 = 0;"));
        assert!(src.contains("pub const DELETE_GROUPS_REQUEST_MAX_VERSION: i16 = 2;"));
        assert!(src.contains("impl VersionedCodec for DeleteGroupsRequest"));
        assert!(src.contains("write_array_len(&mut out, self.groups_names.len(), flexible)?;"));
        assert!(src.contains("items.push(read_string(&mut reader, flexible)?);"));
    }

    #[test]
    fn generates_flexible_array_of_structs_for_delete_groups() {
        let specs = crate::load_message_specs(&specs_dir()).expect("specs");
        let src = generate_codec_source(&specs, &["DeleteGroupsResponse"]).expect("codec");
        assert!(src.contains("DELETE_GROUPS_RESPONSE_FIRST_FLEXIBLE_VERSION: i16 = 2"));
        assert!(src.contains("write_array_len(&mut out, self.results.len(), flexible)?;"));
        assert!(src.contains("write_i16(&mut out, item.error_code);"));
        assert!(src.contains("let mut item = DeleteGroupsResponseDeletableGroupResult::default();"));
        assert!(src.contains("read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;"));
    }

    #[test]
    fn codec_target_must_exist() {
        let specs = crate::load_message_specs(&specs_dir()).expect("specs");
        let err = generate_codec_source(&specs, &["NoSuchMessage"]).expect_err("missing");
        assert!(matches!(err, CodegenError::MissingTargetMessage(_)));
    }
}
