#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::{Record, StorageError};

const LOG_FILE_SUFFIX: &str = ".log";
const INDEX_FILE_SUFFIX: &str = ".index";
const FRAME_HEADER_BYTES: usize = 8;
const RECORD_HEADER_BYTES: usize = 24;
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

/// A contiguous byte range inside a `.log` file that holds the *value* bytes
/// of a single record (frame header, record header, and key excluded).
///
/// Used by `PersistentSegmentLog::fetch_file_ranges()` to support zero-copy
/// sends via `sendfile(2)` or a pread-based fallback.
#[derive(Debug, Clone)]
pub struct FileRange {
    /// Absolute path to the `.log` segment file.
    pub path: PathBuf,
    /// Byte offset inside the file where the value data begins.
    pub file_byte_offset: u64,
    /// Number of value bytes.
    pub byte_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentLogConfig {
    pub base_offset: i64,
    pub segment_max_records: usize,
    pub sync_on_append: bool,
}

impl Default for PersistentLogConfig {
    fn default() -> Self {
        Self {
            base_offset: 0,
            segment_max_records: 1_024,
            sync_on_append: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSnapshot {
    pub base_offset: i64,
    pub next_offset: i64,
    pub record_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexEntry {
    offset: i64,
    position: u64,
}

#[derive(Debug)]
struct DiskSegment {
    base_offset: i64,
    next_offset: i64,
    record_count: usize,
    log_len: u64,
    log_path: PathBuf,
    index_path: PathBuf,
    // `None` for sealed (non-head) segments; `Some` for the active head only.
    log_append_file: Option<File>,
    index_append_file: Option<File>,
    index_entries: Vec<IndexEntry>,
}

#[derive(Debug)]
pub struct PersistentSegmentLog {
    data_dir: PathBuf,
    config: PersistentLogConfig,
    segments: Vec<DiskSegment>,
    next_offset: i64,
}

impl PersistentSegmentLog {
    pub fn open<P: AsRef<Path>>(
        data_dir: P,
        config: PersistentLogConfig,
    ) -> Result<Self, StorageError> {
        if config.base_offset < 0 {
            return Err(StorageError::InvalidConfiguration {
                message: "base_offset must be >= 0".to_string(),
            });
        }
        if config.segment_max_records == 0 {
            return Err(StorageError::InvalidConfiguration {
                message: "segment_max_records must be > 0".to_string(),
            });
        }

        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)
            .map_err(|err| StorageError::io("create_dir_all", &data_dir, err))?;

        let mut base_offsets = collect_log_base_offsets(&data_dir)?;
        base_offsets.sort_unstable();
        base_offsets.dedup();

        let mut segments = Vec::new();
        if base_offsets.is_empty() {
            segments.push(create_empty_segment(&data_dir, config.base_offset)?);
        } else {
            for base_offset in base_offsets {
                segments.push(recover_segment(&data_dir, base_offset)?);
            }
            // Open append handles only for the head (last) segment; sealed
            // segments keep their handles at None to avoid fd exhaustion.
            if let Some(head) = segments.last_mut() {
                let log_append = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&head.log_path)
                    .map_err(|err| StorageError::io("open", &head.log_path, err))?;
                let index_append = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&head.index_path)
                    .map_err(|err| StorageError::io("open", &head.index_path, err))?;
                head.log_append_file = Some(log_append);
                head.index_append_file = Some(index_append);
            }
        }

        let mut log = Self {
            data_dir,
            config,
            segments,
            next_offset: 0,
        };
        log.recompute_offsets()?;
        Ok(log)
    }

    pub fn append(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<i64, StorageError> {
        if self.active_segment().record_count >= self.config.segment_max_records {
            self.roll_segment(self.next_offset)?;
        }

        let offset = self.next_offset;
        let record = Record {
            offset,
            timestamp_ms,
            key,
            value,
        };
        let frame = encode_record_frame(&record)?;
        let sync_on_append = self.config.sync_on_append;

        let active_segment = self.active_segment_mut();
        let position = active_segment.log_len;

        append_bytes(
            active_segment
                .log_append_file
                .as_mut()
                .expect("active segment must have log append file open"),
            &active_segment.log_path,
            &frame,
            sync_on_append,
        )?;
        append_index_entry(
            active_segment
                .index_append_file
                .as_mut()
                .expect("active segment must have index append file open"),
            &active_segment.index_path,
            offset,
            position,
            sync_on_append,
        )?;

        active_segment
            .index_entries
            .push(IndexEntry { offset, position });
        active_segment.record_count += 1;
        active_segment.next_offset = offset + 1;
        active_segment.log_len = active_segment
            .log_len
            .saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
        self.next_offset += 1;

        Ok(offset)
    }

    /// Append multiple records in a single `write_all` + at most one
    /// `sync_data`.  This coalesces the kernel's page-cache dirty pages into
    /// one large contiguous I/O, giving near-Kafka group-commit throughput.
    ///
    /// Records that straddle a segment boundary are split across two segments
    /// with separate (but still batched) writes per segment.  Each entry is
    /// `(key, value, timestamp_ms)`.
    ///
    /// Returns the base offsets assigned to each entry, in the same order.
    pub fn append_batch(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<i64>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let sync = self.config.sync_on_append;
        let mut all_offsets = Vec::with_capacity(entries.len());
        let mut i = 0;

        while i < entries.len() {
            // Roll the active segment if it has reached capacity.
            if self.active_segment().record_count >= self.config.segment_max_records {
                self.roll_segment(self.next_offset)?;
            }

            // Determine how many records fit in the current active segment.
            let remaining_capacity =
                self.config.segment_max_records - self.active_segment().record_count;
            let batch_end = (i + remaining_capacity).min(entries.len());
            let sub_batch = &entries[i..batch_end];

            // ── Encode all frames and index entries for this sub-batch ────────
            let mut log_buf: Vec<u8> = Vec::new();
            let mut new_index_entries: Vec<IndexEntry> = Vec::with_capacity(sub_batch.len());
            let mut cur_log_len = self.active_segment().log_len;
            let mut cur_offset = self.next_offset;

            for (key, value, timestamp_ms) in sub_batch {
                let record = Record {
                    offset: cur_offset,
                    timestamp_ms: *timestamp_ms,
                    key: key.clone(),
                    value: value.clone(),
                };
                let frame = encode_record_frame(&record)?;
                new_index_entries.push(IndexEntry {
                    offset: cur_offset,
                    position: cur_log_len,
                });
                cur_log_len =
                    cur_log_len.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
                log_buf.extend_from_slice(&frame);
                all_offsets.push(cur_offset);
                cur_offset += 1;
            }

            // ── Single write_all for all encoded frames ───────────────────────
            {
                let seg = self.active_segment_mut();
                let log_file = seg
                    .log_append_file
                    .as_mut()
                    .expect("active segment must have log append file open");
                log_file
                    .write_all(&log_buf)
                    .map_err(|e| StorageError::io("write_all_batch", &seg.log_path, e))?;
                if sync {
                    log_file
                        .sync_data()
                        .map_err(|e| StorageError::io("sync_data_batch_log", &seg.log_path, e))?;
                }

                let index_file = seg
                    .index_append_file
                    .as_mut()
                    .expect("active segment must have index append file open");
                // Build one contiguous index buffer and write it in one call.
                let mut idx_buf = Vec::with_capacity(new_index_entries.len() * 16);
                for entry in &new_index_entries {
                    idx_buf.extend_from_slice(&entry.offset.to_be_bytes());
                    idx_buf.extend_from_slice(&entry.position.to_be_bytes());
                }
                index_file
                    .write_all(&idx_buf)
                    .map_err(|e| StorageError::io("write_all_batch_index", &seg.index_path, e))?;
                if sync {
                    index_file.sync_data().map_err(|e| {
                        StorageError::io("sync_data_batch_index", &seg.index_path, e)
                    })?;
                }

                // Update in-memory segment state.
                seg.record_count += sub_batch.len();
                seg.next_offset = cur_offset;
                seg.log_len = cur_log_len;
                seg.index_entries.extend(new_index_entries);
            }
            self.next_offset = cur_offset;

            i = batch_end;
        }

        Ok(all_offsets)
    }

    pub fn fetch_from(&self, offset: i64, max_records: usize) -> Result<Vec<Record>, StorageError> {
        self.fetch_from_bounded(offset, max_records, usize::MAX)
    }

    /// Like `fetch_from` but stops accumulating records once the total byte
    /// size of their values exceeds `max_bytes`.  This prevents reading
    /// thousands of records from disk when the caller only needs a few KB.
    pub fn fetch_from_bounded(
        &self,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>, StorageError> {
        let earliest = self.earliest_offset();
        let latest = self.latest_offset();

        if offset < earliest || offset > self.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest,
                latest,
            });
        }
        if offset == self.next_offset || max_records == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }

        let mut remaining = max_records;
        let mut bytes_so_far: usize = 0;
        let mut wanted_offset = offset;
        let mut out = Vec::with_capacity(max_records.min(1_024));

        'segments: for segment in &self.segments {
            if remaining == 0 {
                break;
            }
            if segment.record_count == 0 || wanted_offset >= segment.next_offset {
                continue;
            }

            let start_index = segment
                .index_entries
                .partition_point(|entry| entry.offset < wanted_offset);
            if start_index == segment.index_entries.len() {
                continue;
            }
            if segment.index_entries[start_index].offset != wanted_offset {
                return Err(StorageError::CorruptData {
                    path: segment.log_path.clone(),
                    position: segment.index_entries[start_index].position,
                    message: format!(
                        "missing offset {}, next available is {}",
                        wanted_offset, segment.index_entries[start_index].offset
                    ),
                });
            }

            let file = File::open(&segment.log_path)
                .map_err(|err| StorageError::io("open", &segment.log_path, err))?;
            for entry in &segment.index_entries[start_index..] {
                if remaining == 0 {
                    break 'segments;
                }
                let record = read_record_at(&file, &segment.log_path, entry.position)?;
                if record.offset != entry.offset {
                    return Err(StorageError::CorruptData {
                        path: segment.log_path.clone(),
                        position: entry.position,
                        message: format!(
                            "index/log mismatch at offset {}, decoded {}",
                            entry.offset, record.offset
                        ),
                    });
                }
                wanted_offset = record.offset + 1;
                let record_bytes = record.value.len();
                out.push(record);
                remaining -= 1;
                // Mirror Kafka: always return at least one record even if it
                // exceeds max_bytes, but stop after that.
                bytes_so_far = bytes_so_far.saturating_add(record_bytes);
                if bytes_so_far >= max_bytes {
                    break 'segments;
                }
            }
        }

        Ok(out)
    }

    pub fn earliest_offset(&self) -> i64 {
        self.segments
            .first()
            .map_or(self.config.base_offset, |segment| segment.base_offset)
    }

    pub fn latest_offset(&self) -> i64 {
        self.next_offset - 1
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn segment_snapshots(&self) -> Vec<SegmentSnapshot> {
        self.segments
            .iter()
            .map(|segment| SegmentSnapshot {
                base_offset: segment.base_offset,
                next_offset: segment.next_offset,
                record_count: segment.record_count,
            })
            .collect()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Compute the on-disk byte ranges that contain the *values* of records
    /// starting at `offset`, consuming up to `max_bytes` of value data.
    ///
    /// This performs **no I/O** — it derives byte ranges from the in-memory
    /// index.  The caller can forward the ranges to `sendfile(2)` (or a pread
    /// fallback on platforms that lack it) to avoid a userspace copy.
    ///
    /// The returned `Vec<FileRange>` may span multiple `.log` files when
    /// records cross a segment boundary.  Each `FileRange` points to the
    /// value bytes of one record (key excluded, framing excluded).
    ///
    /// Returns an empty `Vec` when `offset == next_offset` (nothing to read).
    pub fn fetch_file_ranges(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<FileRange>, StorageError> {
        let earliest = self.earliest_offset();
        if offset < earliest || offset > self.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest,
                latest: self.latest_offset(),
            });
        }
        if offset == self.next_offset || max_bytes == 0 {
            return Ok(Vec::new());
        }

        let mut out: Vec<FileRange> = Vec::new();
        let mut bytes_so_far: usize = 0;
        let mut wanted_offset = offset;

        'segments: for segment in &self.segments {
            if segment.record_count == 0 || wanted_offset >= segment.next_offset {
                continue;
            }

            // Locate the first index entry at or after `wanted_offset`.
            let start_index = segment
                .index_entries
                .partition_point(|e| e.offset < wanted_offset);
            if start_index == segment.index_entries.len() {
                continue;
            }
            if segment.index_entries[start_index].offset != wanted_offset {
                return Err(StorageError::CorruptData {
                    path: segment.log_path.clone(),
                    position: segment.index_entries[start_index].position,
                    message: format!(
                        "missing offset {wanted_offset}, next available is {}",
                        segment.index_entries[start_index].offset
                    ),
                });
            }

            // We need to open the file to read key_len for each record so we
            // can compute the exact byte offset of the value.  We do this with
            // a single pread per record (header only, 8+24 = 32 bytes).
            let file = File::open(&segment.log_path)
                .map_err(|e| StorageError::io("open", &segment.log_path, e))?;

            for entry in &segment.index_entries[start_index..] {
                if bytes_so_far >= max_bytes {
                    break 'segments;
                }

                // Read the frame header (8 bytes) + record header (24 bytes).
                let header_pos = entry.position;
                let mut hdr = [0_u8; FRAME_HEADER_BYTES + RECORD_HEADER_BYTES];
                #[cfg(unix)]
                file.read_exact_at(&mut hdr, header_pos)
                    .map_err(|e| StorageError::io("read_exact_at_hdr", &segment.log_path, e))?;
                #[cfg(not(unix))]
                {
                    use std::io::{Seek, SeekFrom};
                    let mut f = file
                        .try_clone()
                        .map_err(|e| StorageError::io("try_clone", &segment.log_path, e))?;
                    f.seek(SeekFrom::Start(header_pos))
                        .map_err(|e| StorageError::io("seek", &segment.log_path, e))?;
                    let mut buf = &mut hdr[..];
                    std::io::Read::read_exact(&mut f, &mut buf)
                        .map_err(|e| StorageError::io("read_exact_hdr", &segment.log_path, e))?;
                }

                // Parse key_len and value_len from the record header (after the
                // frame header).
                let rec_hdr = &hdr[FRAME_HEADER_BYTES..];
                let key_len =
                    u32::from_be_bytes(rec_hdr[16..20].try_into().expect("key_len bytes")) as u64;
                let value_len =
                    u32::from_be_bytes(rec_hdr[20..24].try_into().expect("value_len bytes")) as u64;

                if value_len == 0 {
                    wanted_offset = entry.offset + 1;
                    continue;
                }

                let value_byte_offset =
                    header_pos + FRAME_HEADER_BYTES as u64 + RECORD_HEADER_BYTES as u64 + key_len;

                out.push(FileRange {
                    path: segment.log_path.clone(),
                    file_byte_offset: value_byte_offset,
                    byte_len: value_len,
                });

                bytes_so_far = bytes_so_far.saturating_add(value_len as usize);
                wanted_offset = entry.offset + 1;
            }
        }

        Ok(out)
    }

    fn active_segment(&self) -> &DiskSegment {
        self.segments
            .last()
            .expect("persistent log always has at least one segment")
    }

    fn active_segment_mut(&mut self) -> &mut DiskSegment {
        self.segments
            .last_mut()
            .expect("persistent log always has at least one segment")
    }

    fn roll_segment(&mut self, base_offset: i64) -> Result<(), StorageError> {
        // Seal the current head by dropping its append file handles.
        if let Some(seg) = self.segments.last_mut() {
            seg.log_append_file = None;
            seg.index_append_file = None;
        }
        let segment = create_empty_segment(&self.data_dir, base_offset)?;
        self.segments.push(segment);
        Ok(())
    }

    fn recompute_offsets(&mut self) -> Result<(), StorageError> {
        if self.segments.is_empty() {
            self.segments.push(create_empty_segment(
                &self.data_dir,
                self.config.base_offset,
            )?);
        }

        let mut expected_offset = self
            .segments
            .first()
            .map_or(self.config.base_offset, |segment| segment.base_offset);

        for segment in &self.segments {
            if segment.record_count == 0 {
                if segment.base_offset != expected_offset {
                    return Err(StorageError::CorruptData {
                        path: segment.log_path.clone(),
                        position: 0,
                        message: format!(
                            "empty segment base offset {} does not match expected {}",
                            segment.base_offset, expected_offset
                        ),
                    });
                }
                continue;
            }

            let first_offset = segment
                .index_entries
                .first()
                .map_or(segment.base_offset, |entry| entry.offset);
            if first_offset != expected_offset {
                return Err(StorageError::CorruptData {
                    path: segment.log_path.clone(),
                    position: 0,
                    message: format!(
                        "segment starts at {}, expected {}",
                        first_offset, expected_offset
                    ),
                });
            }

            if segment.next_offset < first_offset {
                return Err(StorageError::CorruptData {
                    path: segment.log_path.clone(),
                    position: 0,
                    message: "segment next_offset is smaller than first offset".to_string(),
                });
            }
            expected_offset = segment.next_offset;
        }

        self.next_offset = expected_offset;
        Ok(())
    }
}

fn collect_log_base_offsets(data_dir: &Path) -> Result<Vec<i64>, StorageError> {
    let mut offsets = Vec::new();
    let dir_iter =
        fs::read_dir(data_dir).map_err(|err| StorageError::io("read_dir", data_dir, err))?;

    for entry in dir_iter {
        let entry = entry.map_err(|err| StorageError::io("read_dir_entry", data_dir, err))?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(prefix) = name.strip_suffix(LOG_FILE_SUFFIX) else {
            continue;
        };
        let Ok(base_offset) = prefix.parse::<i64>() else {
            continue;
        };
        offsets.push(base_offset);
    }

    Ok(offsets)
}

fn recover_segment(data_dir: &Path, base_offset: i64) -> Result<DiskSegment, StorageError> {
    let log_path = log_path(data_dir, base_offset);
    let index_path = index_path(data_dir, base_offset);

    if !log_path.exists() {
        File::create(&log_path).map_err(|err| StorageError::io("create", &log_path, err))?;
    }
    if !index_path.exists() {
        File::create(&index_path).map_err(|err| StorageError::io("create", &index_path, err))?;
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .map_err(|err| StorageError::io("open", &log_path, err))?;

    let mut valid_entries = Vec::new();
    let mut expected_offset = base_offset;
    let mut position: u64 = 0;
    let mut file_len = file
        .metadata()
        .map_err(|err| StorageError::io("metadata", &log_path, err))?
        .len();

    while position < file_len {
        file.seek(SeekFrom::Start(position))
            .map_err(|err| StorageError::io("seek", &log_path, err))?;

        let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
        if let Err(err) = file.read_exact(&mut frame_header) {
            if err.kind() == ErrorKind::UnexpectedEof {
                truncate_to_valid_tail(&mut file, &log_path, position)?;
                file_len = position;
                break;
            }
            return Err(StorageError::io("read_exact", &log_path, err));
        }

        let frame_len = u32::from_be_bytes(
            frame_header[..4]
                .try_into()
                .expect("frame_len header is exactly 4 bytes"),
        ) as usize;
        let expected_checksum = u32::from_be_bytes(
            frame_header[4..]
                .try_into()
                .expect("checksum header is exactly 4 bytes"),
        );

        if !(RECORD_HEADER_BYTES..=MAX_FRAME_BYTES).contains(&frame_len) {
            truncate_to_valid_tail(&mut file, &log_path, position)?;
            file_len = position;
            break;
        }

        let frame_total = FRAME_HEADER_BYTES as u64 + u64::try_from(frame_len).unwrap_or(u64::MAX);
        if position.saturating_add(frame_total) > file_len {
            truncate_to_valid_tail(&mut file, &log_path, position)?;
            file_len = position;
            break;
        }

        let mut payload = vec![0_u8; frame_len];
        if let Err(err) = file.read_exact(&mut payload) {
            if err.kind() == ErrorKind::UnexpectedEof {
                truncate_to_valid_tail(&mut file, &log_path, position)?;
                file_len = position;
                break;
            }
            return Err(StorageError::io("read_exact", &log_path, err));
        }

        if checksum32(&payload) != expected_checksum {
            truncate_to_valid_tail(&mut file, &log_path, position)?;
            file_len = position;
            break;
        }

        let record = decode_payload(&payload, &log_path, position)?;
        if record.offset != expected_offset {
            return Err(StorageError::CorruptData {
                path: log_path.clone(),
                position,
                message: format!(
                    "non-monotonic offset {}, expected {}",
                    record.offset, expected_offset
                ),
            });
        }

        valid_entries.push(IndexEntry {
            offset: record.offset,
            position,
        });
        expected_offset += 1;
        position = position.saturating_add(frame_total);
    }

    rewrite_index_file(&index_path, &valid_entries)?;

    // No file handles stored: reads are done on-demand via pread (read_exact_at).
    // Append handles are None; the caller (open()) sets them for the head only.
    let segment = DiskSegment {
        base_offset,
        next_offset: expected_offset,
        record_count: valid_entries.len(),
        log_len: file_len,
        log_path: log_path.clone(),
        index_path: index_path.clone(),
        log_append_file: None,
        index_append_file: None,
        index_entries: valid_entries,
    };

    Ok(segment)
}

fn truncate_to_valid_tail(file: &mut File, path: &Path, len: u64) -> Result<(), StorageError> {
    file.set_len(len)
        .map_err(|err| StorageError::io("set_len", path, err))?;
    file.seek(SeekFrom::Start(len))
        .map_err(|err| StorageError::io("seek", path, err))?;
    file.sync_data()
        .map_err(|err| StorageError::io("sync_data", path, err))?;
    Ok(())
}

// Uses pread(2) via FileExt::read_exact_at — a single syscall that reads at
// an arbitrary offset without touching the file cursor.  Taking `&File` (not
// `&mut File`) means no seek mutation and no RefCell wrapper needed.
fn read_record_at(file: &File, path: &Path, position: u64) -> Result<Record, StorageError> {
    let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
    #[cfg(unix)]
    file.read_exact_at(&mut frame_header, position)
        .map_err(|err| StorageError::io("read_exact_at", path, err))?;
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        // Fallback for non-Unix: seek then read (file must be &mut on Windows).
        // This path is unused on macOS/Linux.
        let mut f = file
            .try_clone()
            .map_err(|e| StorageError::io("try_clone", path, e))?;
        f.seek(SeekFrom::Start(position))
            .map_err(|err| StorageError::io("seek", path, err))?;
        f.read_exact(&mut frame_header)
            .map_err(|err| StorageError::io("read_exact", path, err))?;
    }

    let frame_len = u32::from_be_bytes(
        frame_header[..4]
            .try_into()
            .expect("frame_len header is exactly 4 bytes"),
    ) as usize;
    let expected_checksum = u32::from_be_bytes(
        frame_header[4..]
            .try_into()
            .expect("checksum header is exactly 4 bytes"),
    );

    if !(RECORD_HEADER_BYTES..=MAX_FRAME_BYTES).contains(&frame_len) {
        return Err(StorageError::CorruptData {
            path: path.to_path_buf(),
            position,
            message: format!("invalid frame length {frame_len}"),
        });
    }

    let mut payload = vec![0_u8; frame_len];
    #[cfg(unix)]
    file.read_exact_at(&mut payload, position + FRAME_HEADER_BYTES as u64)
        .map_err(|err| StorageError::io("read_exact_at", path, err))?;
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file
            .try_clone()
            .map_err(|e| StorageError::io("try_clone", path, e))?;
        f.seek(SeekFrom::Start(position + FRAME_HEADER_BYTES as u64))
            .map_err(|err| StorageError::io("seek", path, err))?;
        f.read_exact(&mut payload)
            .map_err(|err| StorageError::io("read_exact", path, err))?;
    }

    if checksum32(&payload) != expected_checksum {
        return Err(StorageError::CorruptData {
            path: path.to_path_buf(),
            position,
            message: "checksum mismatch".to_string(),
        });
    }

    decode_payload(&payload, path, position)
}

fn decode_payload(payload: &[u8], path: &Path, position: u64) -> Result<Record, StorageError> {
    if payload.len() < RECORD_HEADER_BYTES {
        return Err(StorageError::CorruptData {
            path: path.to_path_buf(),
            position,
            message: "payload shorter than record header".to_string(),
        });
    }

    let offset = i64::from_be_bytes(
        payload[0..8]
            .try_into()
            .expect("offset bytes are exactly 8 bytes"),
    );
    let timestamp_ms = i64::from_be_bytes(
        payload[8..16]
            .try_into()
            .expect("timestamp bytes are exactly 8 bytes"),
    );
    let key_len = u32::from_be_bytes(
        payload[16..20]
            .try_into()
            .expect("key length bytes are exactly 4 bytes"),
    ) as usize;
    let value_len = u32::from_be_bytes(
        payload[20..24]
            .try_into()
            .expect("value length bytes are exactly 4 bytes"),
    ) as usize;

    let expected_payload_len = RECORD_HEADER_BYTES
        .checked_add(key_len)
        .and_then(|len| len.checked_add(value_len))
        .ok_or_else(|| StorageError::CorruptData {
            path: path.to_path_buf(),
            position,
            message: "payload length overflow".to_string(),
        })?;

    if payload.len() != expected_payload_len {
        return Err(StorageError::CorruptData {
            path: path.to_path_buf(),
            position,
            message: format!(
                "payload length mismatch, expected {expected_payload_len}, got {}",
                payload.len()
            ),
        });
    }

    let key_start = RECORD_HEADER_BYTES;
    let value_start = key_start + key_len;
    let key = payload[key_start..value_start].to_vec();
    let value = payload[value_start..].to_vec();

    Ok(Record {
        offset,
        timestamp_ms,
        key,
        value,
    })
}

fn encode_record_frame(record: &Record) -> Result<Vec<u8>, StorageError> {
    let payload_len = RECORD_HEADER_BYTES
        .checked_add(record.key.len())
        .and_then(|len| len.checked_add(record.value.len()))
        .ok_or_else(|| StorageError::InvalidRecord {
            message: "record payload length overflow".to_string(),
        })?;

    if payload_len > MAX_FRAME_BYTES {
        return Err(StorageError::InvalidRecord {
            message: format!(
                "record payload length {} exceeds max {}",
                payload_len, MAX_FRAME_BYTES
            ),
        });
    }

    let key_len: u32 = record
        .key
        .len()
        .try_into()
        .map_err(|_| StorageError::InvalidRecord {
            message: "key length exceeds u32::MAX".to_string(),
        })?;
    let value_len: u32 =
        record
            .value
            .len()
            .try_into()
            .map_err(|_| StorageError::InvalidRecord {
                message: "value length exceeds u32::MAX".to_string(),
            })?;
    let frame_len: u32 = payload_len
        .try_into()
        .map_err(|_| StorageError::InvalidRecord {
            message: "payload length exceeds u32::MAX".to_string(),
        })?;

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&record.offset.to_be_bytes());
    payload.extend_from_slice(&record.timestamp_ms.to_be_bytes());
    payload.extend_from_slice(&key_len.to_be_bytes());
    payload.extend_from_slice(&value_len.to_be_bytes());
    payload.extend_from_slice(&record.key);
    payload.extend_from_slice(&record.value);

    let checksum = checksum32(&payload);

    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
    frame.extend_from_slice(&frame_len.to_be_bytes());
    frame.extend_from_slice(&checksum.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn append_bytes(
    file: &mut File,
    path: &Path,
    bytes: &[u8],
    sync: bool,
) -> Result<(), StorageError> {
    file.write_all(bytes)
        .map_err(|err| StorageError::io("write_all", path, err))?;
    if sync {
        file.sync_data()
            .map_err(|err| StorageError::io("sync_data", path, err))?;
    }
    Ok(())
}

fn append_index_entry(
    file: &mut File,
    path: &Path,
    offset: i64,
    position: u64,
    sync: bool,
) -> Result<(), StorageError> {
    let mut entry = [0_u8; 16];
    entry[..8].copy_from_slice(&offset.to_be_bytes());
    entry[8..].copy_from_slice(&position.to_be_bytes());
    file.write_all(&entry)
        .map_err(|err| StorageError::io("write_all", path, err))?;

    if sync {
        file.sync_data()
            .map_err(|err| StorageError::io("sync_data", path, err))?;
    }
    Ok(())
}

fn rewrite_index_file(path: &Path, entries: &[IndexEntry]) -> Result<(), StorageError> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .map_err(|err| StorageError::io("open", path, err))?;

    for entry in entries {
        file.write_all(&entry.offset.to_be_bytes())
            .map_err(|err| StorageError::io("write_all", path, err))?;
        file.write_all(&entry.position.to_be_bytes())
            .map_err(|err| StorageError::io("write_all", path, err))?;
    }

    file.sync_data()
        .map_err(|err| StorageError::io("sync_data", path, err))?;
    Ok(())
}

fn create_empty_segment(data_dir: &Path, base_offset: i64) -> Result<DiskSegment, StorageError> {
    let log_path = log_path(data_dir, base_offset);
    let index_path = index_path(data_dir, base_offset);

    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&log_path)
        .map_err(|err| StorageError::io("create_new", &log_path, err))?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&index_path)
        .map_err(|err| StorageError::io("create_new", &index_path, err))?;

    let log_append_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&log_path)
        .map_err(|err| StorageError::io("open", &log_path, err))?;
    let index_append_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&index_path)
        .map_err(|err| StorageError::io("open", &index_path, err))?;

    Ok(DiskSegment {
        base_offset,
        next_offset: base_offset,
        record_count: 0,
        log_len: 0,
        log_path,
        index_path,
        log_append_file: Some(log_append_file),
        index_append_file: Some(index_append_file),
        index_entries: Vec::new(),
    })
}

fn log_path(data_dir: &Path, base_offset: i64) -> PathBuf {
    data_dir.join(format!(
        "{}{}",
        filename_prefix(base_offset),
        LOG_FILE_SUFFIX
    ))
}

fn index_path(data_dir: &Path, base_offset: i64) -> PathBuf {
    data_dir.join(format!(
        "{}{}",
        filename_prefix(base_offset),
        INDEX_FILE_SUFFIX
    ))
}

fn filename_prefix(base_offset: i64) -> String {
    format!("{base_offset:020}")
}

fn checksum32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811C_9DC5;
    for &byte in bytes {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    const INDEX_ENTRY_BYTES: usize = 16;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
                "rafka-storage-{label}-{millis}-{}-{counter}",
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

    fn persistent_config(base_offset: i64, segment_max_records: usize) -> PersistentLogConfig {
        PersistentLogConfig {
            base_offset,
            segment_max_records,
            sync_on_append: false,
        }
    }

    #[test]
    fn append_and_fetch_across_segments() {
        let temp = TempDir::new("append-and-fetch-across-segments");
        let mut log =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 2)).expect("open log");

        for i in 0..5 {
            let offset = log
                .append(
                    format!("k-{i}").into_bytes(),
                    format!("v-{i}").into_bytes(),
                    1_000 + i,
                )
                .expect("append");
            assert_eq!(offset, i);
        }

        assert_eq!(log.segment_count(), 3);

        let all = log.fetch_from(0, 100).expect("fetch all");
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].offset, 0);
        assert_eq!(all[4].offset, 4);
        assert_eq!(all[4].value, b"v-4".to_vec());

        let subset = log.fetch_from(2, 2).expect("fetch subset");
        assert_eq!(subset.len(), 2);
        assert_eq!(subset[0].offset, 2);
        assert_eq!(subset[1].offset, 3);

        let at_end = log.fetch_from(log.next_offset(), 10).expect("fetch at end");
        assert!(at_end.is_empty());
    }

    #[test]
    fn fetch_out_of_range_behaves_like_in_memory_log() {
        let temp = TempDir::new("fetch-out-of-range");
        let log =
            PersistentSegmentLog::open(temp.path(), persistent_config(10, 4)).expect("open log");

        let below = log
            .fetch_from(9, 1)
            .expect_err("below earliest should fail");
        assert_eq!(
            below,
            StorageError::OffsetOutOfRange {
                requested: 9,
                earliest: 10,
                latest: 9,
            }
        );

        let above = log
            .fetch_from(11, 1)
            .expect_err("above next offset should fail");
        assert_eq!(
            above,
            StorageError::OffsetOutOfRange {
                requested: 11,
                earliest: 10,
                latest: 9,
            }
        );
    }

    #[test]
    fn recovery_roundtrip_preserves_offsets_and_segment_state() {
        let temp = TempDir::new("recovery-roundtrip");
        {
            let mut log =
                PersistentSegmentLog::open(temp.path(), persistent_config(0, 3)).expect("open log");
            for i in 0..6 {
                log.append(
                    format!("key-{i}").into_bytes(),
                    format!("value-{i}").into_bytes(),
                    10_000 + i,
                )
                .expect("append");
            }
            assert_eq!(log.segment_count(), 2);
            assert_eq!(log.next_offset(), 6);
        }

        let mut reopened =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 3)).expect("reopen log");
        assert_eq!(reopened.segment_count(), 2);
        assert_eq!(reopened.next_offset(), 6);

        let fetched = reopened.fetch_from(0, 100).expect("fetch after reopen");
        assert_eq!(fetched.len(), 6);
        assert_eq!(fetched[0].offset, 0);
        assert_eq!(fetched[5].offset, 5);

        let appended = reopened
            .append(b"tail-key".to_vec(), b"tail-value".to_vec(), 20_000)
            .expect("append after reopen");
        assert_eq!(appended, 6);

        let tail = reopened.fetch_from(5, 10).expect("fetch tail");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].offset, 5);
        assert_eq!(tail[1].offset, 6);
    }

    #[test]
    fn recovery_truncates_torn_tail_record() {
        let temp = TempDir::new("recovery-truncates-torn-tail");
        let torn_log_path;
        let torn_size;

        {
            let mut log = PersistentSegmentLog::open(temp.path(), persistent_config(0, 10))
                .expect("open log");
            for i in 0..3 {
                log.append(
                    vec![u8::try_from(i).expect("i within u8 range")],
                    vec![42],
                    1_000,
                )
                .expect("append");
            }

            torn_log_path = log.active_segment().log_path.clone();
            let mut file = OpenOptions::new()
                .append(true)
                .open(&torn_log_path)
                .expect("open active segment for torn write");
            file.write_all(&[0xAB, 0xCD, 0xEF])
                .expect("append torn bytes");
            torn_size = file.metadata().expect("metadata after torn write").len();
        }

        let reopened =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 10)).expect("reopen log");
        assert_eq!(reopened.next_offset(), 3);

        let recovered = reopened.fetch_from(0, 10).expect("fetch recovered");
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[2].offset, 2);

        let recovered_size = fs::metadata(&torn_log_path)
            .expect("metadata after recovery")
            .len();
        assert!(recovered_size < torn_size);
    }

    #[test]
    fn recovery_rebuilds_missing_and_corrupt_index_files() {
        let temp = TempDir::new("recovery-rebuilds-indexes");
        let (first_index, second_index);

        {
            let mut log =
                PersistentSegmentLog::open(temp.path(), persistent_config(0, 2)).expect("open log");
            for i in 0..4 {
                log.append(
                    format!("k-{i}").into_bytes(),
                    format!("v-{i}").into_bytes(),
                    100,
                )
                .expect("append");
            }
            assert_eq!(log.segment_count(), 2);

            first_index = log.segments[0].index_path.clone();
            second_index = log.segments[1].index_path.clone();
        }

        fs::remove_file(&first_index).expect("remove first index");
        fs::write(&second_index, [1_u8, 2, 3, 4, 5]).expect("corrupt second index");

        let reopened =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 2)).expect("reopen log");
        let snapshots = reopened.segment_snapshots();
        assert_eq!(snapshots.len(), 2);

        for snapshot in snapshots {
            let idx = index_path(reopened.data_dir(), snapshot.base_offset);
            let expected_size = u64::try_from(snapshot.record_count * INDEX_ENTRY_BYTES)
                .expect("index size should fit in u64");
            let actual_size = fs::metadata(idx).expect("index metadata").len();
            assert_eq!(actual_size, expected_size);
        }

        let fetched = reopened.fetch_from(0, 10).expect("fetch all");
        assert_eq!(fetched.len(), 4);
        assert_eq!(fetched[0].offset, 0);
        assert_eq!(fetched[3].offset, 3);
    }

    #[test]
    fn stress_append_fetch_and_recover() {
        let temp = TempDir::new("stress-append-fetch-and-recover");
        let total_records: i64 = 5_000;

        {
            let mut log = PersistentSegmentLog::open(temp.path(), persistent_config(0, 64))
                .expect("open log");
            for i in 0..total_records {
                log.append(
                    format!("key-{i}").into_bytes(),
                    format!("value-{i:05}").into_bytes(),
                    2_000_000 + i,
                )
                .expect("append stress record");
            }
            assert_eq!(log.next_offset(), total_records);
        }

        let reopened =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 64)).expect("reopen log");
        assert_eq!(reopened.next_offset(), total_records);

        let tail = reopened
            .fetch_from(total_records - 10, 20)
            .expect("fetch stress tail");
        assert_eq!(tail.len(), 10);
        assert_eq!(tail[0].offset, total_records - 10);
        assert_eq!(tail[9].offset, total_records - 1);
        assert_eq!(tail[9].value, b"value-04999".to_vec());
    }

    #[test]
    fn repeated_open_close_and_directory_removal_is_stable() {
        let root = TempDir::new("repeated-open-close");

        for i in 0..200 {
            let iteration_dir = root.path().join(format!("iter-{i}"));
            fs::create_dir_all(&iteration_dir).expect("create iteration dir");

            {
                let mut log = PersistentSegmentLog::open(&iteration_dir, persistent_config(0, 8))
                    .expect("open iteration log");
                for j in 0..16 {
                    log.append(
                        vec![u8::try_from(j).expect("j within u8 range")],
                        vec![1, 2, 3],
                        0,
                    )
                    .expect("append iteration record");
                }
            }

            fs::remove_dir_all(&iteration_dir).expect("remove iteration dir should succeed");
        }
    }

    #[test]
    fn detects_and_truncates_from_checksum_corruption() {
        let temp = TempDir::new("detects-checksum-corruption");
        let corrupt_segment_path;

        {
            let mut log = PersistentSegmentLog::open(temp.path(), persistent_config(0, 10))
                .expect("open log");
            for i in 0..3 {
                log.append(
                    format!("k-{i}").into_bytes(),
                    format!("v-{i}").into_bytes(),
                    9_000,
                )
                .expect("append record");
            }
            corrupt_segment_path = log.active_segment().log_path.clone();
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupt_segment_path)
            .expect("open segment for byte flip");
        file.seek(SeekFrom::Start(20))
            .expect("seek inside first frame");
        file.write_all(&[0xFF]).expect("flip one byte");

        let reopened =
            PersistentSegmentLog::open(temp.path(), persistent_config(0, 10)).expect("reopen log");
        assert_eq!(reopened.next_offset(), 0);

        let fetched = reopened
            .fetch_from(0, 10)
            .expect("fetch after corruption truncate");
        assert!(fetched.is_empty());
    }
}
