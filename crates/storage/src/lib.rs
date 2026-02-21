#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

mod persistent;
mod sendfile;

pub use persistent::{FileRange, PersistentLogConfig, PersistentSegmentLog, SegmentSnapshot};
pub use sendfile::sendfile_range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    OffsetOutOfRange {
        requested: i64,
        earliest: i64,
        latest: i64,
    },
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    InvalidConfiguration {
        message: String,
    },
    InvalidRecord {
        message: String,
    },
    CorruptData {
        path: PathBuf,
        position: u64,
        message: String,
    },
}

impl StorageError {
    fn io(operation: &'static str, path: &Path, err: std::io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            message: err.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentLog {
    base_offset: i64,
    records: Vec<Record>,
    next_offset: i64,
}

impl SegmentLog {
    pub fn new(base_offset: i64) -> Self {
        Self {
            base_offset,
            records: Vec::new(),
            next_offset: base_offset,
        }
    }

    pub fn append(&mut self, key: Vec<u8>, value: Vec<u8>, timestamp_ms: i64) -> i64 {
        let offset = self.next_offset;
        self.records.push(Record {
            offset,
            timestamp_ms,
            key,
            value,
        });
        self.next_offset += 1;
        offset
    }

    pub fn fetch_from(&self, offset: i64, max_records: usize) -> Result<Vec<Record>, StorageError> {
        let earliest = self.earliest_offset();
        let latest = self.latest_offset();
        if offset < earliest || offset > self.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest,
                latest,
            });
        }
        if offset == self.next_offset || max_records == 0 {
            return Ok(Vec::new());
        }
        let Ok(start) = usize::try_from(offset - self.base_offset) else {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest,
                latest,
            });
        };
        let end = (start + max_records).min(self.records.len());
        Ok(self.records[start..end].to_vec())
    }

    pub fn earliest_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn latest_offset(&self) -> i64 {
        self.next_offset - 1
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_offsets_are_monotonic() {
        let mut log = SegmentLog::new(10);
        let o1 = log.append(b"k1".to_vec(), b"v1".to_vec(), 1000);
        let o2 = log.append(b"k2".to_vec(), b"v2".to_vec(), 1001);
        assert_eq!(o1, 10);
        assert_eq!(o2, 11);
        assert_eq!(log.next_offset(), 12);
    }

    #[test]
    fn fetch_is_bounded() {
        let mut log = SegmentLog::new(0);
        log.append(vec![], b"a".to_vec(), 1);
        log.append(vec![], b"b".to_vec(), 2);
        log.append(vec![], b"c".to_vec(), 3);

        let records = log.fetch_from(1, 1).expect("fetch");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, b"b".to_vec());
    }

    #[test]
    fn fetch_at_next_offset_is_empty() {
        let mut log = SegmentLog::new(5);
        log.append(vec![], vec![], 0);
        let records = log.fetch_from(6, 10).expect("fetch");
        assert!(records.is_empty());
    }

    #[test]
    fn fetch_below_earliest_fails() {
        let log = SegmentLog::new(3);
        let err = log.fetch_from(2, 1).expect_err("out of range");
        assert_eq!(
            err,
            StorageError::OffsetOutOfRange {
                requested: 2,
                earliest: 3,
                latest: 2,
            }
        );
    }
}
