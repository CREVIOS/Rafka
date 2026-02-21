#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{Display, Formatter};

pub mod api_registry;
pub mod messages;

pub const CURRENT_RECORD_BATCH_MAGIC: i8 = 2;
pub const RECORD_BATCH_HEADER_LEN: usize = 61;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Truncated,
    VarintTooLong,
    VarlongTooLong,
    InvalidVersion { api: &'static str, version: i16 },
    MissingRequiredField(&'static str),
    InvalidMagic(i8),
    InvalidBoolean(u8),
    InvalidCompactLength(u32),
    InvalidString,
    InvalidLength(i32),
    InvalidBatchLength(i32),
    InvalidRecordsCount(i32),
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "input is truncated"),
            Self::VarintTooLong => write!(f, "varint overflow (more than 5 bytes)"),
            Self::VarlongTooLong => write!(f, "varlong overflow (more than 10 bytes)"),
            Self::InvalidVersion { api, version } => {
                write!(f, "unsupported version {version} for {api}")
            }
            Self::MissingRequiredField(field) => write!(f, "missing required field: {field}"),
            Self::InvalidMagic(magic) => write!(f, "invalid record-batch magic: {magic}"),
            Self::InvalidBoolean(value) => write!(f, "invalid boolean value: {value}"),
            Self::InvalidCompactLength(length) => {
                write!(f, "invalid compact length value: {length}")
            }
            Self::InvalidString => write!(f, "invalid UTF-8 string"),
            Self::InvalidLength(length) => write!(f, "invalid length value: {length}"),
            Self::InvalidBatchLength(length) => write!(f, "invalid batch_length: {length}"),
            Self::InvalidRecordsCount(count) => write!(f, "invalid records_count: {count}"),
        }
    }
}

impl Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
}

pub fn size_of_varint(value: i32) -> usize {
    encode_varint(value).len()
}

pub fn size_of_varlong(value: i64) -> usize {
    encode_varlong(value).len()
}

pub fn encode_varint(value: i32) -> Vec<u8> {
    encode_unsigned_varint(zigzag_encode_i32(value))
}

pub fn decode_varint(input: &[u8]) -> Result<(i32, usize), ProtocolError> {
    let (raw, read) = decode_unsigned_varint(input)?;
    Ok((zigzag_decode_i32(raw), read))
}

pub fn encode_varlong(value: i64) -> Vec<u8> {
    encode_unsigned_varlong(zigzag_encode_i64(value))
}

pub fn decode_varlong(input: &[u8]) -> Result<(i64, usize), ProtocolError> {
    let (raw, read) = decode_unsigned_varlong(input)?;
    Ok((zigzag_decode_i64(raw), read))
}

pub fn encode_nullable_bytes(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        None => encode_varint(-1),
        Some(bytes) => {
            let mut out = Vec::new();
            out.extend(encode_varint(bytes.len() as i32));
            out.extend(bytes);
            out
        }
    }
}

pub fn decode_nullable_bytes(input: &[u8]) -> Result<(Option<Vec<u8>>, usize), ProtocolError> {
    let (length, read_len) = decode_varint(input)?;
    if length == -1 {
        return Ok((None, read_len));
    }
    if length < -1 {
        return Err(ProtocolError::InvalidLength(length));
    }
    let length = length as usize;
    if input.len() < read_len + length {
        return Err(ProtocolError::Truncated);
    }
    let payload = input[read_len..read_len + length].to_vec();
    Ok((Some(payload), read_len + length))
}

impl RecordBatchHeader {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.magic != CURRENT_RECORD_BATCH_MAGIC {
            return Err(ProtocolError::InvalidMagic(self.magic));
        }
        if self.batch_length < 0 {
            return Err(ProtocolError::InvalidBatchLength(self.batch_length));
        }
        if self.records_count < 0 {
            return Err(ProtocolError::InvalidRecordsCount(self.records_count));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; RECORD_BATCH_HEADER_LEN] {
        let mut out = [0_u8; RECORD_BATCH_HEADER_LEN];
        let mut cursor = 0;
        write_i64(&mut out, &mut cursor, self.base_offset);
        write_i32(&mut out, &mut cursor, self.batch_length);
        write_i32(&mut out, &mut cursor, self.partition_leader_epoch);
        out[cursor] = self.magic as u8;
        cursor += 1;
        write_u32(&mut out, &mut cursor, self.crc);
        write_i16(&mut out, &mut cursor, self.attributes);
        write_i32(&mut out, &mut cursor, self.last_offset_delta);
        write_i64(&mut out, &mut cursor, self.base_timestamp);
        write_i64(&mut out, &mut cursor, self.max_timestamp);
        write_i64(&mut out, &mut cursor, self.producer_id);
        write_i16(&mut out, &mut cursor, self.producer_epoch);
        write_i32(&mut out, &mut cursor, self.base_sequence);
        write_i32(&mut out, &mut cursor, self.records_count);
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if input.len() < RECORD_BATCH_HEADER_LEN {
            return Err(ProtocolError::Truncated);
        }
        let mut cursor = 0;
        let base_offset = read_i64(input, &mut cursor);
        let batch_length = read_i32(input, &mut cursor);
        let partition_leader_epoch = read_i32(input, &mut cursor);
        let magic = input[cursor] as i8;
        cursor += 1;
        let crc = read_u32(input, &mut cursor);
        let attributes = read_i16(input, &mut cursor);
        let last_offset_delta = read_i32(input, &mut cursor);
        let base_timestamp = read_i64(input, &mut cursor);
        let max_timestamp = read_i64(input, &mut cursor);
        let producer_id = read_i64(input, &mut cursor);
        let producer_epoch = read_i16(input, &mut cursor);
        let base_sequence = read_i32(input, &mut cursor);
        let records_count = read_i32(input, &mut cursor);
        let header = Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
        };
        header.validate()?;
        Ok((header, cursor))
    }
}

fn zigzag_encode_i32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

fn zigzag_decode_i32(value: u32) -> i32 {
    ((value >> 1) as i32) ^ (-((value & 1) as i32))
}

fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn encode_unsigned_varint(mut value: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

fn decode_unsigned_varint(input: &[u8]) -> Result<(u32, usize), ProtocolError> {
    let mut value = 0_u32;
    for i in 0..5 {
        let Some(&byte) = input.get(i) else {
            return Err(ProtocolError::Truncated);
        };
        if i == 4 && (byte & 0xf0) != 0 {
            return Err(ProtocolError::VarintTooLong);
        }
        value |= ((byte & 0x7f) as u32) << (7 * i);
        if (byte & 0x80) == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(ProtocolError::VarintTooLong)
}

fn encode_unsigned_varlong(mut value: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

fn decode_unsigned_varlong(input: &[u8]) -> Result<(u64, usize), ProtocolError> {
    let mut value = 0_u64;
    for i in 0..10 {
        let Some(&byte) = input.get(i) else {
            return Err(ProtocolError::Truncated);
        };
        if i == 9 && (byte & 0xfe) != 0 {
            return Err(ProtocolError::VarlongTooLong);
        }
        value |= ((byte & 0x7f) as u64) << (7 * i);
        if (byte & 0x80) == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(ProtocolError::VarlongTooLong)
}

fn write_i16(out: &mut [u8], cursor: &mut usize, value: i16) {
    let bytes = value.to_be_bytes();
    out[*cursor..*cursor + 2].copy_from_slice(&bytes);
    *cursor += 2;
}

fn write_i32(out: &mut [u8], cursor: &mut usize, value: i32) {
    let bytes = value.to_be_bytes();
    out[*cursor..*cursor + 4].copy_from_slice(&bytes);
    *cursor += 4;
}

fn write_u32(out: &mut [u8], cursor: &mut usize, value: u32) {
    let bytes = value.to_be_bytes();
    out[*cursor..*cursor + 4].copy_from_slice(&bytes);
    *cursor += 4;
}

fn write_i64(out: &mut [u8], cursor: &mut usize, value: i64) {
    let bytes = value.to_be_bytes();
    out[*cursor..*cursor + 8].copy_from_slice(&bytes);
    *cursor += 8;
}

fn read_i16(input: &[u8], cursor: &mut usize) -> i16 {
    let mut bytes = [0_u8; 2];
    bytes.copy_from_slice(&input[*cursor..*cursor + 2]);
    *cursor += 2;
    i16::from_be_bytes(bytes)
}

fn read_i32(input: &[u8], cursor: &mut usize) -> i32 {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&input[*cursor..*cursor + 4]);
    *cursor += 4;
    i32::from_be_bytes(bytes)
}

fn read_u32(input: &[u8], cursor: &mut usize) -> u32 {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&input[*cursor..*cursor + 4]);
    *cursor += 4;
    u32::from_be_bytes(bytes)
}

fn read_i64(input: &[u8], cursor: &mut usize) -> i64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&input[*cursor..*cursor + 8]);
    *cursor += 8;
    i64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let values = [0, 1, -1, 63, -64, 8192, -8192, i32::MIN, i32::MAX];
        for value in values {
            let encoded = encode_varint(value);
            let (decoded, read) = decode_varint(&encoded).expect("varint decode");
            assert_eq!(decoded, value);
            assert_eq!(read, encoded.len());
        }
    }

    #[test]
    fn varint_known_compat_vectors_from_kafka() {
        let vectors: &[(i32, &[u8])] = &[
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (63, &[0x7e]),
            (-64, &[0x7f]),
            (64, &[0x80, 0x01]),
            (-65, &[0x81, 0x01]),
            (i32::MAX, &[0xfe, 0xff, 0xff, 0xff, 0x0f]),
            (i32::MIN, &[0xff, 0xff, 0xff, 0xff, 0x0f]),
        ];
        for (value, expected) in vectors {
            assert_eq!(&encode_varint(*value), expected);
        }
    }

    #[test]
    fn varlong_roundtrip() {
        let values = [0, 1, -1, 63, -64, 8192, -8192, i64::MIN, i64::MAX];
        for value in values {
            let encoded = encode_varlong(value);
            let (decoded, read) = decode_varlong(&encoded).expect("varlong decode");
            assert_eq!(decoded, value);
            assert_eq!(read, encoded.len());
        }
    }

    #[test]
    fn decode_truncated_varint_fails() {
        let bytes = [0x80_u8, 0x80_u8];
        let err = decode_varint(&bytes).expect_err("should fail");
        assert_eq!(err, ProtocolError::Truncated);
    }

    #[test]
    fn decode_overflow_varint_fails() {
        let bytes = [0xff_u8, 0xff, 0xff, 0xff, 0xff, 0x01];
        let err = decode_varint(&bytes).expect_err("overflow varint");
        assert_eq!(err, ProtocolError::VarintTooLong);
    }

    #[test]
    fn decode_overflow_varlong_fails() {
        let bytes = [
            0xff_u8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];
        let err = decode_varlong(&bytes).expect_err("overflow varlong");
        assert_eq!(err, ProtocolError::VarlongTooLong);
    }

    #[test]
    fn nullable_bytes_roundtrip() {
        let encoded_some = encode_nullable_bytes(Some(b"abc"));
        let (decoded_some, read_some) = decode_nullable_bytes(&encoded_some).expect("decode some");
        assert_eq!(read_some, encoded_some.len());
        assert_eq!(decoded_some, Some(b"abc".to_vec()));

        let encoded_none = encode_nullable_bytes(None);
        let (decoded_none, read_none) = decode_nullable_bytes(&encoded_none).expect("decode none");
        assert_eq!(read_none, encoded_none.len());
        assert_eq!(decoded_none, None);
    }

    #[test]
    fn nullable_bytes_invalid_negative_length() {
        let bytes = encode_varint(-2);
        let err = decode_nullable_bytes(&bytes).expect_err("invalid length");
        assert_eq!(err, ProtocolError::InvalidLength(-2));
    }

    #[test]
    fn record_batch_header_roundtrip() {
        let header = RecordBatchHeader {
            base_offset: 42,
            batch_length: 512,
            partition_leader_epoch: 3,
            magic: CURRENT_RECORD_BATCH_MAGIC,
            crc: 0xfeed_beef,
            attributes: 0b1_0010,
            last_offset_delta: 9,
            base_timestamp: 1_700_000_000_000,
            max_timestamp: 1_700_000_000_999,
            producer_id: 7,
            producer_epoch: 2,
            base_sequence: 11,
            records_count: 2,
        };
        let encoded = header.encode();
        let (decoded, read) = RecordBatchHeader::decode(&encoded).expect("header decode");
        assert_eq!(read, RECORD_BATCH_HEADER_LEN);
        assert_eq!(decoded, header);
    }

    #[test]
    fn record_batch_invalid_magic_fails() {
        let header = RecordBatchHeader {
            base_offset: 0,
            batch_length: 1,
            partition_leader_epoch: 0,
            magic: CURRENT_RECORD_BATCH_MAGIC,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            records_count: 0,
        };
        let mut encoded = header.encode();
        encoded[16] = 1;
        let err = RecordBatchHeader::decode(&encoded).expect_err("invalid magic");
        assert_eq!(err, ProtocolError::InvalidMagic(1));
    }

    #[test]
    fn record_batch_negative_batch_length_fails() {
        let header = RecordBatchHeader {
            base_offset: 0,
            batch_length: -1,
            partition_leader_epoch: 0,
            magic: CURRENT_RECORD_BATCH_MAGIC,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            records_count: 1,
        };
        let err = header.validate().expect_err("invalid batch length");
        assert_eq!(err, ProtocolError::InvalidBatchLength(-1));
    }

    #[test]
    fn record_batch_negative_records_count_fails() {
        let header = RecordBatchHeader {
            base_offset: 0,
            batch_length: 1,
            partition_leader_epoch: 0,
            magic: CURRENT_RECORD_BATCH_MAGIC,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            records_count: -1,
        };
        let err = header.validate().expect_err("invalid records count");
        assert_eq!(err, ProtocolError::InvalidRecordsCount(-1));
    }
}
