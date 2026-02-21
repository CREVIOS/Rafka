#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use rafka_storage::Record;

const TRANSACTION_LOG_FILE: &str = "transactions.wal";
const TRANSACTION_RECORD_VERSION: u8 = 1;
const TRANSACTION_RECORD_KIND_SNAPSHOT: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransactionStoreConfig {
    pub sync_on_commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionCoordinatorError {
    InvalidTransactionalId,
    UnknownTransactionalId(String),
    UnknownProducerId(i64),
    InvalidProducerEpoch,
    ProducerFenced,
    InvalidTxnState,
    ConcurrentTransactions,
    InvalidTopic,
    InvalidPartition(i32),
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerInput {
    pub api_version: i16,
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerResult {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub ongoing_txn_producer_id: i64,
    pub ongoing_txn_producer_epoch: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnInput {
    pub api_version: i16,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnResult {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkerTopicInput {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkerInput {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
    pub topics: Vec<WriteTxnMarkerTopicInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkerTopicPartitionResult {
    pub partition_index: i32,
    pub error: Option<TransactionCoordinatorError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkerTopicResult {
    pub name: String,
    pub partitions: Vec<WriteTxnMarkerTopicPartitionResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkerResult {
    pub producer_id: i64,
    pub topics: Vec<WriteTxnMarkerTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadCommittedResult {
    pub visible_records: Vec<Record>,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub last_stable_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OffsetRange {
    first_offset: i64,
    last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AbortedRange {
    producer_id: i64,
    first_offset: i64,
    last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionStatus {
    Ready,
    Ongoing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransactionState {
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    transaction_timeout_ms: i32,
    status: TransactionStatus,
    ongoing_partitions: BTreeMap<TopicPartitionKey, OffsetRange>,
}

#[derive(Debug)]
pub struct TransactionCoordinator {
    transactions: BTreeMap<String, TransactionState>,
    producer_index: BTreeMap<i64, String>,
    aborted_ranges: BTreeMap<TopicPartitionKey, Vec<AbortedRange>>,
    next_producer_id: i64,
    log_file: File,
    log_path: PathBuf,
    sync_on_commit: bool,
}

impl TransactionCoordinator {
    pub fn open<P: AsRef<Path>>(
        data_dir: P,
        config: TransactionStoreConfig,
    ) -> Result<Self, TransactionCoordinatorError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir).map_err(|err| TransactionCoordinatorError::Io {
            operation: "create_dir_all",
            path: data_dir.to_path_buf(),
            message: err.to_string(),
        })?;

        let log_path = data_dir.join(TRANSACTION_LOG_FILE);
        let (transactions, aborted_ranges, next_producer_id) = load_state_from_log(&log_path)?;

        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(|err| TransactionCoordinatorError::Io {
                operation: "open",
                path: log_path.clone(),
                message: err.to_string(),
            })?;

        let mut producer_index = BTreeMap::new();
        for (tx_id, state) in &transactions {
            producer_index.insert(state.producer_id, tx_id.clone());
        }

        Ok(Self {
            transactions,
            producer_index,
            aborted_ranges,
            next_producer_id,
            log_file,
            log_path,
            sync_on_commit: config.sync_on_commit,
        })
    }

    pub fn init_producer(
        &mut self,
        input: InitProducerInput,
    ) -> Result<InitProducerResult, TransactionCoordinatorError> {
        if input.transaction_timeout_ms <= 0 {
            return Err(TransactionCoordinatorError::InvalidTxnState);
        }

        if input.transactional_id.is_none() {
            let producer_id = self.allocate_producer_id();
            let result = InitProducerResult {
                producer_id,
                producer_epoch: 0,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            };
            self.persist_snapshot()?;
            return Ok(result);
        }

        let transactional_id = input
            .transactional_id
            .ok_or(TransactionCoordinatorError::InvalidTransactionalId)?;
        if transactional_id.is_empty() {
            return Err(TransactionCoordinatorError::InvalidTransactionalId);
        }

        let result = if let Some(state) = self.transactions.get_mut(&transactional_id) {
            if input.api_version >= 3
                && input.producer_id != -1
                && input.producer_epoch != -1
                && (input.producer_id != state.producer_id
                    || input.producer_epoch != state.producer_epoch)
            {
                return Err(TransactionCoordinatorError::ProducerFenced);
            }

            if matches!(state.status, TransactionStatus::Ongoing) {
                return Err(TransactionCoordinatorError::ConcurrentTransactions);
            }

            state.producer_epoch = state.producer_epoch.saturating_add(1);
            state.transaction_timeout_ms = input.transaction_timeout_ms;

            InitProducerResult {
                producer_id: state.producer_id,
                producer_epoch: state.producer_epoch,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            }
        } else {
            let producer_id = self.allocate_producer_id();
            let state = TransactionState {
                transactional_id: transactional_id.clone(),
                producer_id,
                producer_epoch: 0,
                transaction_timeout_ms: input.transaction_timeout_ms,
                status: TransactionStatus::Ready,
                ongoing_partitions: BTreeMap::new(),
            };
            self.transactions.insert(transactional_id.clone(), state);
            self.producer_index.insert(producer_id, transactional_id);
            InitProducerResult {
                producer_id,
                producer_epoch: 0,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            }
        };

        self.persist_snapshot()?;
        Ok(result)
    }

    pub fn ensure_transactional_id(
        &self,
        transactional_id: &str,
    ) -> Result<(), TransactionCoordinatorError> {
        if transactional_id.is_empty() {
            return Err(TransactionCoordinatorError::InvalidTransactionalId);
        }
        if !self.transactions.contains_key(transactional_id) {
            return Err(TransactionCoordinatorError::UnknownTransactionalId(
                transactional_id.to_string(),
            ));
        }
        Ok(())
    }

    pub fn record_produce(
        &mut self,
        transactional_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), TransactionCoordinatorError> {
        if topic.is_empty() {
            return Err(TransactionCoordinatorError::InvalidTopic);
        }
        if partition < 0 {
            return Err(TransactionCoordinatorError::InvalidPartition(partition));
        }

        let state = self.transactions.get_mut(transactional_id).ok_or_else(|| {
            TransactionCoordinatorError::UnknownTransactionalId(transactional_id.to_string())
        })?;

        state.status = TransactionStatus::Ongoing;
        let key = TopicPartitionKey {
            topic: topic.to_string(),
            partition,
        };
        let range = state.ongoing_partitions.entry(key).or_insert(OffsetRange {
            first_offset: offset,
            last_offset: offset,
        });
        range.last_offset = range.last_offset.max(offset);
        range.first_offset = range.first_offset.min(offset);

        self.persist_snapshot()
    }

    pub fn end_txn(
        &mut self,
        input: EndTxnInput,
    ) -> Result<EndTxnResult, TransactionCoordinatorError> {
        if input.transactional_id.is_empty() {
            return Err(TransactionCoordinatorError::InvalidTransactionalId);
        }

        let state = self
            .transactions
            .get_mut(&input.transactional_id)
            .ok_or_else(|| {
                TransactionCoordinatorError::UnknownTransactionalId(input.transactional_id.clone())
            })?;

        if state.producer_id != input.producer_id {
            return Err(TransactionCoordinatorError::ProducerFenced);
        }
        if state.producer_epoch != input.producer_epoch {
            return Err(TransactionCoordinatorError::InvalidProducerEpoch);
        }
        if !matches!(state.status, TransactionStatus::Ongoing) {
            return Err(TransactionCoordinatorError::InvalidTxnState);
        }

        if !input.committed {
            for (partition, range) in &state.ongoing_partitions {
                self.aborted_ranges
                    .entry(partition.clone())
                    .or_default()
                    .push(AbortedRange {
                        producer_id: state.producer_id,
                        first_offset: range.first_offset,
                        last_offset: range.last_offset,
                    });
            }
        }

        state.ongoing_partitions.clear();
        state.status = TransactionStatus::Ready;
        if input.api_version >= 5 {
            state.producer_epoch = state.producer_epoch.saturating_add(1);
        }

        let result = EndTxnResult {
            producer_id: state.producer_id,
            producer_epoch: state.producer_epoch,
        };
        self.persist_snapshot()?;
        Ok(result)
    }

    pub fn write_txn_markers(
        &mut self,
        markers: Vec<WriteTxnMarkerInput>,
    ) -> Result<Vec<WriteTxnMarkerResult>, TransactionCoordinatorError> {
        let mut changed = false;
        let mut results = Vec::with_capacity(markers.len());

        for marker in markers {
            let mut producer_topics = Vec::with_capacity(marker.topics.len());
            let tx_id = self.producer_index.get(&marker.producer_id).cloned();

            for topic in marker.topics {
                let mut partition_results = Vec::with_capacity(topic.partition_indexes.len());
                for partition_index in topic.partition_indexes {
                    let error = if let Some(tx_id) = tx_id.as_deref() {
                        self.apply_marker_partition(
                            tx_id,
                            marker.producer_id,
                            marker.producer_epoch,
                            marker.committed,
                            &topic.name,
                            partition_index,
                        )?
                    } else {
                        Some(TransactionCoordinatorError::UnknownProducerId(
                            marker.producer_id,
                        ))
                    };
                    if error.is_none() {
                        changed = true;
                    }
                    partition_results.push(WriteTxnMarkerTopicPartitionResult {
                        partition_index,
                        error,
                    });
                }

                producer_topics.push(WriteTxnMarkerTopicResult {
                    name: topic.name,
                    partitions: partition_results,
                });
            }

            results.push(WriteTxnMarkerResult {
                producer_id: marker.producer_id,
                topics: producer_topics,
            });
        }

        if changed {
            self.persist_snapshot()?;
        }

        Ok(results)
    }

    pub fn read_committed(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        high_watermark: i64,
        records: &[Record],
    ) -> ReadCommittedResult {
        let key = TopicPartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let mut first_unstable_offset: Option<i64> = None;
        for state in self.transactions.values() {
            if !matches!(state.status, TransactionStatus::Ongoing) {
                continue;
            }
            if let Some(range) = state.ongoing_partitions.get(&key) {
                first_unstable_offset =
                    Some(first_unstable_offset.map_or(range.first_offset, |current| {
                        current.min(range.first_offset)
                    }));
            }
        }

        let last_stable_offset =
            first_unstable_offset.map_or(high_watermark, |offset| high_watermark.min(offset));

        let aborted = self.aborted_ranges.get(&key).cloned().unwrap_or_default();

        let mut aborted_transactions = Vec::new();
        for range in &aborted {
            if range.first_offset >= fetch_offset && range.first_offset < last_stable_offset {
                aborted_transactions.push(AbortedTransaction {
                    producer_id: range.producer_id,
                    first_offset: range.first_offset,
                });
            }
        }

        let mut visible_records = Vec::new();
        for record in records {
            if record.offset >= last_stable_offset {
                continue;
            }
            let mut is_aborted = false;
            for range in &aborted {
                if record.offset >= range.first_offset && record.offset <= range.last_offset {
                    is_aborted = true;
                    break;
                }
            }
            if !is_aborted {
                visible_records.push(record.clone());
            }
        }

        ReadCommittedResult {
            visible_records,
            aborted_transactions,
            last_stable_offset,
        }
    }

    fn apply_marker_partition(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        topic: &str,
        partition: i32,
    ) -> Result<Option<TransactionCoordinatorError>, TransactionCoordinatorError> {
        if topic.is_empty() {
            return Ok(Some(TransactionCoordinatorError::InvalidTopic));
        }
        if partition < 0 {
            return Ok(Some(TransactionCoordinatorError::InvalidPartition(
                partition,
            )));
        }

        let state = self.transactions.get_mut(transactional_id).ok_or_else(|| {
            TransactionCoordinatorError::UnknownTransactionalId(transactional_id.to_string())
        })?;

        if state.producer_id != producer_id {
            return Ok(Some(TransactionCoordinatorError::UnknownProducerId(
                producer_id,
            )));
        }
        if state.producer_epoch != producer_epoch {
            return Ok(Some(TransactionCoordinatorError::InvalidProducerEpoch));
        }
        if !matches!(state.status, TransactionStatus::Ongoing) {
            return Ok(Some(TransactionCoordinatorError::InvalidTxnState));
        }

        let key = TopicPartitionKey {
            topic: topic.to_string(),
            partition,
        };
        let Some(range) = state.ongoing_partitions.remove(&key) else {
            return Ok(Some(TransactionCoordinatorError::InvalidTxnState));
        };

        if !committed {
            self.aborted_ranges
                .entry(key)
                .or_default()
                .push(AbortedRange {
                    producer_id: state.producer_id,
                    first_offset: range.first_offset,
                    last_offset: range.last_offset,
                });
        }

        if state.ongoing_partitions.is_empty() {
            state.status = TransactionStatus::Ready;
        }

        Ok(None)
    }

    fn allocate_producer_id(&mut self) -> i64 {
        let producer_id = self.next_producer_id;
        self.next_producer_id = self.next_producer_id.saturating_add(1);
        producer_id
    }

    fn persist_snapshot(&mut self) -> Result<(), TransactionCoordinatorError> {
        let record = encode_snapshot_record(
            self.next_producer_id,
            &self.transactions,
            &self.aborted_ranges,
        )?;

        self.log_file
            .write_all(&record)
            .map_err(|err| TransactionCoordinatorError::Io {
                operation: "write_all",
                path: self.log_path.clone(),
                message: err.to_string(),
            })?;

        if self.sync_on_commit {
            self.log_file
                .sync_data()
                .map_err(|err| TransactionCoordinatorError::Io {
                    operation: "sync_data",
                    path: self.log_path.clone(),
                    message: err.to_string(),
                })?;
        }

        Ok(())
    }
}

type LoadedState = (
    BTreeMap<String, TransactionState>,
    BTreeMap<TopicPartitionKey, Vec<AbortedRange>>,
    i64,
);

fn load_state_from_log(path: &Path) -> Result<LoadedState, TransactionCoordinatorError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((BTreeMap::new(), BTreeMap::new(), 1));
        }
        Err(err) => {
            return Err(TransactionCoordinatorError::Io {
                operation: "read",
                path: path.to_path_buf(),
                message: err.to_string(),
            });
        }
    };

    let mut transactions = BTreeMap::new();
    let mut aborted_ranges = BTreeMap::new();
    let mut next_producer_id = 1_i64;
    let mut cursor = 0_usize;
    let mut valid_end = 0_usize;

    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            break;
        }

        let mut header_cursor = cursor;
        let len_u32 = read_u32(
            &bytes,
            &mut header_cursor,
            path,
            "read transaction record length",
        )?;
        let len = usize::try_from(len_u32).map_err(|_| TransactionCoordinatorError::Io {
            operation: "read transaction record length",
            path: path.to_path_buf(),
            message: "record length overflow".to_string(),
        })?;
        cursor = header_cursor;

        if bytes.len().saturating_sub(cursor) < len {
            break;
        }

        let record = &bytes[cursor..cursor + len];
        let (decoded_transactions, decoded_aborted_ranges, decoded_next_pid) =
            decode_snapshot_record(record, path)?;
        transactions = decoded_transactions;
        aborted_ranges = decoded_aborted_ranges;
        next_producer_id = decoded_next_pid;

        cursor += len;
        valid_end = cursor;
    }

    if valid_end < bytes.len() {
        truncate_file(path, valid_end)?;
    }

    Ok((transactions, aborted_ranges, next_producer_id.max(1)))
}

fn truncate_file(path: &Path, len: usize) -> Result<(), TransactionCoordinatorError> {
    let len_u64 = u64::try_from(len).map_err(|_| TransactionCoordinatorError::Io {
        operation: "set_len",
        path: path.to_path_buf(),
        message: "file length overflow".to_string(),
    })?;

    OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_len(len_u64))
        .map_err(|err| TransactionCoordinatorError::Io {
            operation: "set_len",
            path: path.to_path_buf(),
            message: err.to_string(),
        })
}

fn encode_snapshot_record(
    next_producer_id: i64,
    transactions: &BTreeMap<String, TransactionState>,
    aborted_ranges: &BTreeMap<TopicPartitionKey, Vec<AbortedRange>>,
) -> Result<Vec<u8>, TransactionCoordinatorError> {
    let mut payload = Vec::new();
    payload.push(TRANSACTION_RECORD_VERSION);
    payload.push(TRANSACTION_RECORD_KIND_SNAPSHOT);
    payload.extend_from_slice(&next_producer_id.to_be_bytes());

    write_u32_len(&mut payload, transactions.len())?;
    for (transactional_id, state) in transactions {
        write_u16_string(&mut payload, transactional_id)?;
        payload.extend_from_slice(&state.producer_id.to_be_bytes());
        payload.extend_from_slice(&state.producer_epoch.to_be_bytes());
        payload.extend_from_slice(&state.transaction_timeout_ms.to_be_bytes());
        payload.push(match state.status {
            TransactionStatus::Ready => 0,
            TransactionStatus::Ongoing => 1,
        });

        write_u32_len(&mut payload, state.ongoing_partitions.len())?;
        for (partition, range) in &state.ongoing_partitions {
            write_u16_string(&mut payload, &partition.topic)?;
            payload.extend_from_slice(&partition.partition.to_be_bytes());
            payload.extend_from_slice(&range.first_offset.to_be_bytes());
            payload.extend_from_slice(&range.last_offset.to_be_bytes());
        }
    }

    write_u32_len(&mut payload, aborted_ranges.len())?;
    for (partition, ranges) in aborted_ranges {
        write_u16_string(&mut payload, &partition.topic)?;
        payload.extend_from_slice(&partition.partition.to_be_bytes());
        write_u32_len(&mut payload, ranges.len())?;
        for range in ranges {
            payload.extend_from_slice(&range.producer_id.to_be_bytes());
            payload.extend_from_slice(&range.first_offset.to_be_bytes());
            payload.extend_from_slice(&range.last_offset.to_be_bytes());
        }
    }

    let mut out = Vec::new();
    write_u32_len(&mut out, payload.len())?;
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_snapshot_record(
    record: &[u8],
    path: &Path,
) -> Result<LoadedState, TransactionCoordinatorError> {
    let mut cursor = 0_usize;
    let version = read_u8(record, &mut cursor, path, "read transaction record version")?;
    if version != TRANSACTION_RECORD_VERSION {
        return Err(TransactionCoordinatorError::Io {
            operation: "decode transaction record",
            path: path.to_path_buf(),
            message: format!("unsupported transaction record version {version}"),
        });
    }

    let kind = read_u8(record, &mut cursor, path, "read transaction record kind")?;
    if kind != TRANSACTION_RECORD_KIND_SNAPSHOT {
        return Err(TransactionCoordinatorError::Io {
            operation: "decode transaction record",
            path: path.to_path_buf(),
            message: format!("unsupported transaction record kind {kind}"),
        });
    }

    let next_producer_id = read_i64(record, &mut cursor, path, "read next producer id")?;

    let tx_count = read_u32(record, &mut cursor, path, "read transaction count")?;
    let tx_count = usize::try_from(tx_count).map_err(|_| TransactionCoordinatorError::Io {
        operation: "read transaction count",
        path: path.to_path_buf(),
        message: "transaction count overflow".to_string(),
    })?;
    let mut transactions = BTreeMap::new();
    for _ in 0..tx_count {
        let transactional_id = read_u16_string(record, &mut cursor, path, "read transactional id")?;
        let producer_id = read_i64(record, &mut cursor, path, "read producer id")?;
        let producer_epoch = read_i16(record, &mut cursor, path, "read producer epoch")?;
        let transaction_timeout_ms = read_i32(record, &mut cursor, path, "read timeout")?;
        let status = match read_u8(record, &mut cursor, path, "read txn status")? {
            0 => TransactionStatus::Ready,
            1 => TransactionStatus::Ongoing,
            other => {
                return Err(TransactionCoordinatorError::Io {
                    operation: "read txn status",
                    path: path.to_path_buf(),
                    message: format!("invalid txn status {other}"),
                });
            }
        };

        let partition_count = read_u32(record, &mut cursor, path, "read ongoing partition count")?;
        let partition_count =
            usize::try_from(partition_count).map_err(|_| TransactionCoordinatorError::Io {
                operation: "read ongoing partition count",
                path: path.to_path_buf(),
                message: "ongoing partition count overflow".to_string(),
            })?;
        let mut ongoing_partitions = BTreeMap::new();
        for _ in 0..partition_count {
            let topic = read_u16_string(record, &mut cursor, path, "read ongoing topic")?;
            let partition = read_i32(record, &mut cursor, path, "read ongoing partition")?;
            let first_offset = read_i64(record, &mut cursor, path, "read first offset")?;
            let last_offset = read_i64(record, &mut cursor, path, "read last offset")?;
            ongoing_partitions.insert(
                TopicPartitionKey { topic, partition },
                OffsetRange {
                    first_offset,
                    last_offset,
                },
            );
        }

        transactions.insert(
            transactional_id.clone(),
            TransactionState {
                transactional_id,
                producer_id,
                producer_epoch,
                transaction_timeout_ms,
                status,
                ongoing_partitions,
            },
        );
    }

    let partition_count = read_u32(record, &mut cursor, path, "read aborted partition count")?;
    let partition_count =
        usize::try_from(partition_count).map_err(|_| TransactionCoordinatorError::Io {
            operation: "read aborted partition count",
            path: path.to_path_buf(),
            message: "aborted partition count overflow".to_string(),
        })?;
    let mut aborted_ranges = BTreeMap::new();
    for _ in 0..partition_count {
        let topic = read_u16_string(record, &mut cursor, path, "read aborted topic")?;
        let partition = read_i32(record, &mut cursor, path, "read aborted partition")?;
        let range_count = read_u32(record, &mut cursor, path, "read aborted range count")?;
        let range_count =
            usize::try_from(range_count).map_err(|_| TransactionCoordinatorError::Io {
                operation: "read aborted range count",
                path: path.to_path_buf(),
                message: "aborted range count overflow".to_string(),
            })?;
        let mut ranges = Vec::with_capacity(range_count);
        for _ in 0..range_count {
            let producer_id = read_i64(record, &mut cursor, path, "read aborted producer id")?;
            let first_offset = read_i64(record, &mut cursor, path, "read aborted first offset")?;
            let last_offset = read_i64(record, &mut cursor, path, "read aborted last offset")?;
            ranges.push(AbortedRange {
                producer_id,
                first_offset,
                last_offset,
            });
        }
        aborted_ranges.insert(TopicPartitionKey { topic, partition }, ranges);
    }

    if cursor != record.len() {
        return Err(TransactionCoordinatorError::Io {
            operation: "decode transaction record",
            path: path.to_path_buf(),
            message: "transaction record has trailing bytes".to_string(),
        });
    }

    Ok((transactions, aborted_ranges, next_producer_id))
}

fn write_u32_len(out: &mut Vec<u8>, len: usize) -> Result<(), TransactionCoordinatorError> {
    let len = u32::try_from(len).map_err(|_| TransactionCoordinatorError::Io {
        operation: "encode length",
        path: PathBuf::new(),
        message: "length overflow".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn write_u16_string(out: &mut Vec<u8>, value: &str) -> Result<(), TransactionCoordinatorError> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| TransactionCoordinatorError::Io {
        operation: "encode string",
        path: PathBuf::new(),
        message: "string length overflow".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<u8, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(1) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated u8".to_string(),
        });
    }
    let value = input[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_i16(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<i16, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated i16".to_string(),
        });
    }
    let value = i16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_i32(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<i32, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated i32".to_string(),
        });
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

fn read_u32(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<u32, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated u32".to_string(),
        });
    }
    let value = u32::from_be_bytes([
        input[*cursor],
        input[*cursor + 1],
        input[*cursor + 2],
        input[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

fn read_i64(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<i64, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(8) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated i64".to_string(),
        });
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

fn read_u16_string(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<String, TransactionCoordinatorError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated string length".to_string(),
        });
    }
    let len = u16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    let len = usize::from(len);
    if input.len() < cursor.saturating_add(len) {
        return Err(TransactionCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated string".to_string(),
        });
    }
    let bytes = &input[*cursor..*cursor + len];
    *cursor += len;
    String::from_utf8(bytes.to_vec()).map_err(|_| TransactionCoordinatorError::Io {
        operation,
        path: path.to_path_buf(),
        message: "invalid utf8".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

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
                "rafka-transaction-{label}-{millis}-{}-{counter}",
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

    #[test]
    fn init_producer_allocates_and_bumps_epoch() {
        let temp = TempDir::new("init-producer");
        let mut coordinator =
            TransactionCoordinator::open(temp.path(), TransactionStoreConfig::default())
                .expect("open");

        let first = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: -1,
                producer_epoch: -1,
            })
            .expect("first init");
        assert!(first.producer_id >= 1);
        assert_eq!(first.producer_epoch, 0);

        let second = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: first.producer_id,
                producer_epoch: first.producer_epoch,
            })
            .expect("second init");
        assert_eq!(second.producer_id, first.producer_id);
        assert_eq!(second.producer_epoch, 1);
    }

    #[test]
    fn concurrent_init_fails_while_transaction_ongoing() {
        let temp = TempDir::new("concurrent-init");
        let mut coordinator =
            TransactionCoordinator::open(temp.path(), TransactionStoreConfig::default())
                .expect("open");

        let init = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: -1,
                producer_epoch: -1,
            })
            .expect("init");
        coordinator
            .record_produce("tx-a", "orders", 0, 0)
            .expect("record produce");

        let err = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: init.producer_id,
                producer_epoch: init.producer_epoch,
            })
            .expect_err("should reject concurrent init");
        assert_eq!(err, TransactionCoordinatorError::ConcurrentTransactions);
    }

    #[test]
    fn read_committed_filters_aborted_and_ongoing_records() {
        let temp = TempDir::new("read-committed");
        let mut coordinator =
            TransactionCoordinator::open(temp.path(), TransactionStoreConfig::default())
                .expect("open");

        let init = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: -1,
                producer_epoch: -1,
            })
            .expect("init");
        coordinator
            .record_produce("tx-a", "orders", 0, 0)
            .expect("produce 0");
        coordinator
            .record_produce("tx-a", "orders", 0, 1)
            .expect("produce 1");
        coordinator
            .end_txn(EndTxnInput {
                api_version: 4,
                transactional_id: "tx-a".to_string(),
                producer_id: init.producer_id,
                producer_epoch: init.producer_epoch,
                committed: false,
            })
            .expect("abort");

        let init2 = coordinator
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id: init.producer_id,
                producer_epoch: init.producer_epoch,
            })
            .expect("re-init");
        coordinator
            .record_produce("tx-a", "orders", 0, 2)
            .expect("produce ongoing");

        let records = vec![
            Record {
                offset: 0,
                timestamp_ms: 0,
                key: Vec::new(),
                value: vec![0],
            },
            Record {
                offset: 1,
                timestamp_ms: 0,
                key: Vec::new(),
                value: vec![1],
            },
            Record {
                offset: 2,
                timestamp_ms: 0,
                key: Vec::new(),
                value: vec![2],
            },
        ];
        let committed = coordinator.read_committed("orders", 0, 0, 3, &records);
        assert!(committed.visible_records.is_empty());
        assert_eq!(committed.last_stable_offset, 2);
        assert_eq!(
            committed.aborted_transactions,
            vec![AbortedTransaction {
                producer_id: init2.producer_id,
                first_offset: 0,
            }]
        );
    }

    #[test]
    fn restart_recovers_transaction_state() {
        let temp = TempDir::new("restart-recovery");
        let producer_id;
        let producer_epoch;
        {
            let mut coordinator =
                TransactionCoordinator::open(temp.path(), TransactionStoreConfig::default())
                    .expect("open");
            let init = coordinator
                .init_producer(InitProducerInput {
                    api_version: 3,
                    transactional_id: Some("tx-a".to_string()),
                    transaction_timeout_ms: 10_000,
                    producer_id: -1,
                    producer_epoch: -1,
                })
                .expect("init");
            producer_id = init.producer_id;
            producer_epoch = init.producer_epoch;
            coordinator
                .record_produce("tx-a", "orders", 0, 10)
                .expect("produce");
            coordinator
                .end_txn(EndTxnInput {
                    api_version: 4,
                    transactional_id: "tx-a".to_string(),
                    producer_id,
                    producer_epoch,
                    committed: false,
                })
                .expect("abort");
        }

        let mut reopened =
            TransactionCoordinator::open(temp.path(), TransactionStoreConfig::default())
                .expect("reopen");
        let records = vec![Record {
            offset: 10,
            timestamp_ms: 0,
            key: Vec::new(),
            value: vec![10],
        }];
        let committed = reopened.read_committed("orders", 0, 0, 11, &records);
        assert!(committed.visible_records.is_empty());
        assert_eq!(
            committed.aborted_transactions,
            vec![AbortedTransaction {
                producer_id,
                first_offset: 10,
            }]
        );

        let init_again = reopened
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-a".to_string()),
                transaction_timeout_ms: 10_000,
                producer_id,
                producer_epoch,
            })
            .expect("init again");
        assert_eq!(init_again.producer_id, producer_id);
        assert_eq!(init_again.producer_epoch, producer_epoch.saturating_add(1));
    }
}
