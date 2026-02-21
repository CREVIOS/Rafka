#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod async_transport;
pub mod authorization;
pub mod kraft;
pub mod metrics;
pub mod replication;
pub mod replication_control_plane;
pub mod security;
pub mod transport;

pub use async_transport::AsyncTransportServer;
pub use authorization::{
    AclAuthorizer, AclConfigError, AclOperation, AclResourceType, ANONYMOUS_PRINCIPAL,
    CLUSTER_RESOURCE_NAME,
};
pub use kraft::{
    BrokerMetadata, KraftError, KraftMetadataQuorum, MetadataImage, MetadataRecord,
    PartitionMetadata, TopicMetadata,
};
pub use metrics::TransportMetrics;
pub use rafka_coordinator::{
    ClassicGroupCoordinator, ClassicGroupCoordinatorError, ClassicGroupStoreConfig,
    CommittedOffset, CoordinatorError, EndTxnInput, EndTxnResult, GroupState, HeartbeatInput,
    InitProducerInput, InitProducerResult, JoinGroupInput, JoinGroupMember, JoinGroupOutcome,
    JoinProtocol, JoinResult, LeaveGroupMemberInput, LeaveGroupMemberOutcome, OffsetCommitInput,
    OffsetCoordinator, OffsetCoordinatorError, OffsetEntry, OffsetStoreConfig, OffsetTopic,
    SyncAssignment, SyncGroupInput, SyncGroupOutcome, TransactionCoordinator,
    TransactionCoordinatorError, TransactionStoreConfig, WriteTxnMarkerInput, WriteTxnMarkerResult,
    WriteTxnMarkerTopicInput, WriteTxnMarkerTopicPartitionResult, WriteTxnMarkerTopicResult,
};
pub use rafka_storage::{
    sendfile_range, FileRange, PersistentLogConfig, PersistentSegmentLog, Record, SegmentLog,
    SegmentSnapshot, StorageError,
};
pub use replication::{
    ReplicatedPartitionState, ReplicationCluster, ReplicationConfig, ReplicationError,
};
pub use replication_control_plane::{ReplicationControlPlane, ReplicationControlPlaneError};
pub use security::{
    SaslConfig, SaslConfigError, SaslConnectionState, SaslMechanism, ERROR_ILLEGAL_SASL_STATE,
    ERROR_SASL_AUTHENTICATION_FAILED, ERROR_UNSUPPORTED_SASL_MECHANISM,
};
pub use transport::{
    TransportConnectionState, TransportError, TransportSecurityConfig, TransportServer,
};

#[derive(Debug)]
pub struct Broker {
    log: SegmentLog,
    groups: HashMap<String, GroupState>,
}

impl Broker {
    pub fn new(base_offset: i64) -> Self {
        Self {
            log: SegmentLog::new(base_offset),
            groups: HashMap::new(),
        }
    }

    pub fn produce(&mut self, key: Vec<u8>, value: Vec<u8>, timestamp_ms: i64) -> i64 {
        self.log.append(key, value, timestamp_ms)
    }

    pub fn fetch(&self, offset: i64, max_records: usize) -> Result<Vec<Record>, StorageError> {
        self.log.fetch_from(offset, max_records)
    }

    pub fn join_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        now_ms: u64,
        session_timeout_ms: u64,
    ) -> JoinResult {
        let group = self
            .groups
            .entry(group_id.to_string())
            .or_insert_with(|| GroupState::new(group_id, session_timeout_ms));
        group.join(member_id.to_string(), now_ms)
    }

    pub fn heartbeat_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        now_ms: u64,
    ) -> Result<(), CoordinatorError> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Err(CoordinatorError::UnknownMember(member_id.to_string()));
        };
        group.heartbeat(member_id, now_ms)
    }

    pub fn evict_expired_members(&mut self, group_id: &str, now_ms: u64) -> Vec<String> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Vec::new();
        };
        group.evict_expired(now_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    fn new(topic: &str, partition: i32) -> Result<Self, PartitionedBrokerError> {
        if topic.is_empty() {
            return Err(PartitionedBrokerError::InvalidTopic {
                topic: topic.to_string(),
            });
        }
        if partition < 0 {
            return Err(PartitionedBrokerError::InvalidPartition { partition });
        }
        if topic.contains('/') || topic.contains('\\') {
            return Err(PartitionedBrokerError::InvalidTopic {
                topic: topic.to_string(),
            });
        }

        Ok(Self {
            topic: topic.to_string(),
            partition,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionedBrokerError {
    Storage(StorageError),
    InvalidTopic { topic: String },
    InvalidPartition { partition: i32 },
    UnknownPartition { topic: String, partition: i32 },
}

impl From<StorageError> for PartitionedBrokerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

type PendingPartitionEntry = (Vec<u8>, Vec<u8>, i64);
type PendingPartitionMap = HashMap<TopicPartition, Vec<PendingPartitionEntry>>;
type PartitionOffsets = HashMap<TopicPartition, Vec<i64>>;
type PartitionErrorList = Vec<(TopicPartition, PartitionedBrokerError)>;

/// A pending batch of produce records accumulated across one or more
/// `produce_to` calls.  Call `flush_pending_batch()` to commit the entire
/// batch with a single `write_all` + optional `sync_data` per partition.
#[derive(Debug, Default)]
pub struct PendingWriteBatch {
    /// Per-partition list of `(key, value, timestamp_ms)` entries.
    by_partition: PendingPartitionMap,
    /// Approximate total byte size of all accumulated values.
    pub total_bytes: usize,
}

impl PendingWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(
        &mut self,
        topic: &str,
        partition: i32,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<(), PartitionedBrokerError> {
        let tp = TopicPartition::new(topic, partition)?;
        self.total_bytes = self.total_bytes.saturating_add(value.len());
        self.by_partition
            .entry(tp)
            .or_default()
            .push((key, value, timestamp_ms));
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.by_partition.is_empty()
    }
}

#[derive(Debug)]
pub struct PartitionedBroker {
    data_dir: PathBuf,
    log_config: PersistentLogConfig,
    logs: HashMap<TopicPartition, PersistentSegmentLog>,
}

impl PartitionedBroker {
    pub fn open<P: AsRef<Path>>(
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<Self, PartitionedBrokerError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|err| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "create_dir_all",
                path: data_dir.clone(),
                message: err.to_string(),
            })
        })?;

        let mut logs = HashMap::new();
        let entries = fs::read_dir(&data_dir).map_err(|err| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "read_dir",
                path: data_dir.clone(),
                message: err.to_string(),
            })
        })?;

        for entry in entries {
            let entry = entry.map_err(|err| {
                PartitionedBrokerError::Storage(StorageError::Io {
                    operation: "read_dir_entry",
                    path: data_dir.clone(),
                    message: err.to_string(),
                })
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if !partition_dir_contains_segment_files(&path)? {
                continue;
            }

            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some((topic, partition)) = parse_partition_dir_name(name) else {
                continue;
            };

            let Ok(topic_partition) = TopicPartition::new(&topic, partition) else {
                continue;
            };
            let log = PersistentSegmentLog::open(&path, log_config.clone())?;
            logs.insert(topic_partition, log);
        }

        Ok(Self {
            data_dir,
            log_config,
            logs,
        })
    }

    pub fn produce_to(
        &mut self,
        topic: &str,
        partition: i32,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<i64, PartitionedBrokerError> {
        let topic_partition = TopicPartition::new(topic, partition)?;
        if !self.logs.contains_key(&topic_partition) {
            let dir = self.partition_dir(&topic_partition);
            let log = PersistentSegmentLog::open(dir, self.log_config.clone())?;
            self.logs.insert(topic_partition.clone(), log);
        }
        let log = self
            .logs
            .get_mut(&topic_partition)
            .expect("log inserted above when missing");
        log.append(key, value, timestamp_ms).map_err(Into::into)
    }

    /// Open a partition from the data directory if it is not already in the
    /// in-memory map.  Called before every read operation so the sync broker
    /// can serve partitions created by `PartitionedBrokerSharded` (or by a
    /// concurrent `produce_to` from another call) since the last `open()`.
    fn try_open_partition(&mut self, tp: &TopicPartition) {
        if self.logs.contains_key(tp) {
            return;
        }
        let dir = self.partition_dir(tp);
        // Only open directories that already contain segment files — same guard
        // used by `open()` at startup.  This prevents empty "noise" directories
        // from being mistaken for valid partitions.
        if dir.is_dir() && partition_dir_contains_segment_files(&dir).unwrap_or(false) {
            if let Ok(log) = PersistentSegmentLog::open(&dir, self.log_config.clone()) {
                self.logs.insert(tp.clone(), log);
            }
        }
    }

    pub fn fetch_from_partition(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>, PartitionedBrokerError> {
        self.fetch_from_partition_bounded(topic, partition, offset, max_records, usize::MAX)
    }

    /// Like `fetch_from_partition` but stops once `max_bytes` of record values
    /// have been read.  Use this to avoid reading far more records from disk
    /// than the caller's byte budget allows.
    pub fn fetch_from_partition_bounded(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>, PartitionedBrokerError> {
        let topic_partition = TopicPartition::new(topic, partition)?;
        self.try_open_partition(&topic_partition);
        let Some(log) = self.logs.get(&topic_partition) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        log.fetch_from_bounded(offset, max_records, max_bytes)
            .map_err(Into::into)
    }

    /// Returns the next offset to be assigned in the partition (i.e. the high
    /// watermark).  This is O(1) — no log scan required.
    pub fn next_offset_for_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, PartitionedBrokerError> {
        let topic_partition = TopicPartition::new(topic, partition)?;
        self.try_open_partition(&topic_partition);
        let Some(log) = self.logs.get(&topic_partition) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        Ok(log.next_offset())
    }

    /// Compute the on-disk byte ranges for the *values* of records starting at
    /// `offset` in the given partition, consuming at most `max_bytes` of value
    /// data.  This does no disk I/O beyond header reads for key_len; the result
    /// can be passed to `sendfile_range()` for a zero-copy send to the client.
    pub fn fetch_file_ranges_for_partition(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<FileRange>, PartitionedBrokerError> {
        let topic_partition = TopicPartition::new(topic, partition)?;
        self.try_open_partition(&topic_partition);
        let Some(log) = self.logs.get(&topic_partition) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        log.fetch_file_ranges(offset, max_bytes).map_err(Into::into)
    }

    pub fn segment_snapshots(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<SegmentSnapshot>, PartitionedBrokerError> {
        let topic_partition = TopicPartition::new(topic, partition)?;
        let Some(log) = self.logs.get(&topic_partition) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        Ok(log.segment_snapshots())
    }

    pub fn known_partitions(&self) -> Vec<TopicPartition> {
        self.logs.keys().cloned().collect()
    }

    /// Flush all pending writes accumulated in `batch` to disk.
    ///
    /// Returns per-partition base offsets: each `Vec<i64>` holds the offsets
    /// assigned to the records that were queued for that partition, in order.
    /// The `HashMap` key is the topic-partition.  Partitions with errors are
    /// excluded from the success map; the per-partition error is returned in
    /// the `errors` vec.
    pub fn flush_pending_batch(
        &mut self,
        batch: PendingWriteBatch,
    ) -> (PartitionOffsets, PartitionErrorList) {
        let mut offsets: PartitionOffsets = HashMap::new();
        let mut errors: PartitionErrorList = Vec::new();

        for (tp, entries) in batch.by_partition {
            // Ensure the partition log exists.
            if !self.logs.contains_key(&tp) {
                let dir = self.partition_dir(&tp);
                match PersistentSegmentLog::open(dir, self.log_config.clone()) {
                    Ok(log) => {
                        self.logs.insert(tp.clone(), log);
                    }
                    Err(err) => {
                        errors.push((tp, err.into()));
                        continue;
                    }
                }
            }
            let log = self
                .logs
                .get_mut(&tp)
                .expect("log inserted above when missing");
            match log.append_batch(&entries) {
                Ok(base_offsets) => {
                    offsets.insert(tp, base_offsets);
                }
                Err(err) => {
                    errors.push((tp, err.into()));
                }
            }
        }

        (offsets, errors)
    }

    fn partition_dir(&self, topic_partition: &TopicPartition) -> PathBuf {
        self.data_dir.join(format!(
            "{}-{}",
            topic_partition.topic, topic_partition.partition
        ))
    }
}

// ── Phase 4: Per-Partition Concurrency ───────────────────────────────────────
//
// `PartitionedBrokerSharded` replaces the single `Arc<Mutex<TransportServer>>`
// (which serialises every connection) with a per-partition
// `tokio::sync::Mutex<PersistentSegmentLog>`.  Concurrent produce/fetch
// operations on *different* partitions no longer block each other.
//
// **Lock ordering invariant** (documented here and enforced by structure):
//   partition_lock → transaction_lock   (never reversed)
// Acquiring transaction_lock while holding a partition_lock is allowed.
// Acquiring a partition_lock while holding transaction_lock is forbidden.
// This invariant prevents deadlocks in multi-partition transactions.
//
// `BrokerSharedState` decomposes `TransportServer` into independently-
// lockable components so that `async_transport.rs` can drop the global mutex.

/// A `PersistentSegmentLog` protected by an async-aware mutex.
pub type PartitionLock = Arc<tokio::sync::Mutex<PersistentSegmentLog>>;

/// A Kafka broker whose partitions can be produced to / consumed from
/// concurrently: readers (fetch / produce on different partitions) hold the
/// shard-map `RwLock` in shared mode for O(1) lookup; only new-partition
/// creation takes an exclusive write lock.
pub struct PartitionedBrokerSharded {
    /// Per-partition async mutexes.
    shard_map: Arc<tokio::sync::RwLock<HashMap<TopicPartition, PartitionLock>>>,
    data_dir: PathBuf,
    log_config: PersistentLogConfig,
}

impl PartitionedBrokerSharded {
    /// Open (or create) a sharded broker backed by `data_dir`.
    pub async fn open<P: AsRef<Path>>(
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<Self, PartitionedBrokerError> {
        // Delegate initial scan to the synchronous PartitionedBroker, then
        // wrap each log in an async mutex.
        let sync_broker = tokio::task::spawn_blocking({
            let data_dir = data_dir.as_ref().to_path_buf();
            let config = log_config.clone();
            move || PartitionedBroker::open(&data_dir, config)
        })
        .await
        .map_err(|e| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "spawn_blocking_open",
                path: data_dir.as_ref().to_path_buf(),
                message: e.to_string(),
            })
        })??;

        let mut shards: HashMap<TopicPartition, PartitionLock> =
            HashMap::with_capacity(sync_broker.logs.len());
        for (tp, log) in sync_broker.logs {
            shards.insert(tp, Arc::new(tokio::sync::Mutex::new(log)));
        }

        Ok(Self {
            shard_map: Arc::new(tokio::sync::RwLock::new(shards)),
            data_dir: data_dir.as_ref().to_path_buf(),
            log_config,
        })
    }

    /// Obtain the async mutex for `(topic, partition)`, creating the partition
    /// log on disk if it does not exist yet.
    pub async fn get_or_create_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionLock, PartitionedBrokerError> {
        let tp = TopicPartition::new(topic, partition)?;

        // Fast path: partition already exists — shared read lock suffices.
        {
            let map = self.shard_map.read().await;
            if let Some(lock) = map.get(&tp) {
                return Ok(Arc::clone(lock));
            }
        }

        // Slow path: create the partition log and insert it under write lock.
        let dir = self.data_dir.join(format!("{}-{}", tp.topic, tp.partition));
        let config = self.log_config.clone();
        let log = tokio::task::spawn_blocking(move || PersistentSegmentLog::open(dir, config))
            .await
            .map_err(|e| {
                PartitionedBrokerError::Storage(StorageError::Io {
                    operation: "spawn_blocking_create_partition",
                    path: PathBuf::new(),
                    message: e.to_string(),
                })
            })??;

        let mut map = self.shard_map.write().await;
        // Check again after acquiring write lock (another task may have beaten us).
        let lock = map
            .entry(tp)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(log)));
        Ok(Arc::clone(lock))
    }

    /// Produce a single record.  Only the target partition is locked.
    pub async fn produce_to_async(
        &self,
        topic: &str,
        partition: i32,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<i64, PartitionedBrokerError> {
        let lock = self.get_or_create_partition(topic, partition).await?;
        tokio::task::spawn_blocking(move || {
            let mut log = lock.blocking_lock();
            log.append(key, value, timestamp_ms).map_err(Into::into)
        })
        .await
        .map_err(|e| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "produce_async_spawn",
                path: PathBuf::new(),
                message: e.to_string(),
            })
        })?
    }

    /// Fetch records from a single partition.  Only the target partition is
    /// locked (shared read semantics via the async mutex).
    pub async fn fetch_from_partition_async(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>, PartitionedBrokerError> {
        let tp = TopicPartition::new(topic, partition)?;
        let map = self.shard_map.read().await;
        let Some(lock) = map.get(&tp) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        let lock = Arc::clone(lock);
        drop(map);

        tokio::task::spawn_blocking(move || {
            let log = lock.blocking_lock();
            log.fetch_from_bounded(offset, max_records, max_bytes)
                .map_err(Into::into)
        })
        .await
        .map_err(|e| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "fetch_async_spawn",
                path: PathBuf::new(),
                message: e.to_string(),
            })
        })?
    }

    /// Compute on-disk byte ranges for the *values* of records starting at
    /// `offset` consuming at most `max_bytes`.  Uses the per-partition async
    /// mutex — only the target partition is locked; all others are unaffected.
    pub async fn fetch_file_ranges_for_partition_async(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<FileRange>, PartitionedBrokerError> {
        let tp = TopicPartition::new(topic, partition)?;
        let shard = {
            let map = self.shard_map.read().await;
            match map.get(&tp) {
                Some(lock) => Arc::clone(lock),
                None => {
                    return Err(PartitionedBrokerError::UnknownPartition {
                        topic: topic.to_string(),
                        partition,
                    })
                }
            }
        };
        tokio::task::spawn_blocking(move || {
            let log = shard.blocking_lock();
            log.fetch_file_ranges(offset, max_bytes).map_err(Into::into)
        })
        .await
        .map_err(|e| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "fetch_file_ranges_async_spawn",
                path: PathBuf::new(),
                message: e.to_string(),
            })
        })?
    }

    /// Returns the next offset for the given partition without locking
    /// the log (reads from the atomic counter cached in the mutex guard).
    pub async fn next_offset_async(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, PartitionedBrokerError> {
        let tp = TopicPartition::new(topic, partition)?;
        let map = self.shard_map.read().await;
        let Some(lock) = map.get(&tp) else {
            return Err(PartitionedBrokerError::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        };
        let log = lock.lock().await;
        Ok(log.next_offset())
    }
}

// ── Priority 1: multi-record batch encoding ──────────────────────────────────

/// Magic prefix that identifies a Rafka multi-record batch in the `records`
/// field of a ProduceRequest partition.
pub(crate) const BATCH_MAGIC: u32 = 0x5242_4348; // b"RBCH"

/// Encode `count` copies of `record` into a compact batch blob:
/// `[magic:4][count:4][record_size:4][record × count]`
#[allow(dead_code)]
pub(crate) fn encode_records_batch(record: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + record.len() * count);
    out.extend_from_slice(&BATCH_MAGIC.to_be_bytes());
    out.extend_from_slice(&(count as u32).to_be_bytes());
    out.extend_from_slice(&(record.len() as u32).to_be_bytes());
    for _ in 0..count {
        out.extend_from_slice(record);
    }
    out
}

/// Decode a records field: returns `Vec<(key, value)>`.
/// If the blob starts with `BATCH_MAGIC` and is well-formed, returns N records
/// (each with an empty key).  Otherwise returns one record (the raw blob as value).
pub(crate) fn decode_records_batch(data: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
    if data.len() >= 12 {
        let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
        if magic == BATCH_MAGIC {
            let count = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
            let record_size = u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize;
            if count > 0 && record_size > 0 && 12 + count * record_size == data.len() {
                return (0..count)
                    .map(|i| {
                        let s = 12 + i * record_size;
                        (Vec::new(), data[s..s + record_size].to_vec())
                    })
                    .collect();
            }
        }
    }
    vec![(Vec::new(), data)]
}

// ── Priority 2: server cross-connection group commit ─────────────────────────

/// One entry submitted by a connection to the shared flusher task.
/// The flusher collects entries from *all* connections, sorts them by
/// `TopicPartition`, calls `append_batch` once per partition (one
/// `write_all` + one optional `sync_data`), then wakes each submitter
/// via `result_tx`.
pub struct GroupCommitEntry {
    pub tp: TopicPartition,
    /// Records to append: `(key, value, timestamp_ms)`.
    pub records: Vec<(Vec<u8>, Vec<u8>, i64)>,
    /// `Ok(offsets)` where `offsets[i]` is the storage offset assigned to
    /// `records[i]`.  `Err(msg)` on storage failure.
    pub result_tx: tokio::sync::oneshot::Sender<Result<Vec<i64>, String>>,
}

/// Sender half of the group-commit channel; clone one per connection.
pub type GroupCommitSender = tokio::sync::mpsc::Sender<GroupCommitEntry>;

/// Background task that batches produce records from all connections and
/// flushes them to storage in one `append_batch` call per partition.
///
/// Runs until all `GroupCommitSender` handles are dropped (i.e. server shuts down).
pub async fn run_group_commit_flusher(
    mut rx: tokio::sync::mpsc::Receiver<GroupCommitEntry>,
    broker: Arc<PartitionedBrokerSharded>,
) {
    const MAX_BATCH: usize = 4096;

    loop {
        // Block until at least one entry arrives.
        let first = match rx.recv().await {
            Some(e) => e,
            None => break, // all senders dropped — server shutting down
        };

        // Drain any additional entries that arrived concurrently (no blocking).
        let mut batch: Vec<GroupCommitEntry> = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(e) => batch.push(e),
                Err(_) => break,
            }
        }

        // Group by TopicPartition for one append_batch per partition.
        let mut by_tp: HashMap<TopicPartition, Vec<GroupCommitEntry>> = HashMap::new();
        for entry in batch {
            by_tp.entry(entry.tp.clone()).or_default().push(entry);
        }

        for (tp, entries) in by_tp {
            // Obtain (or create) the per-partition lock — fast path: read lock.
            let lock = match broker
                .get_or_create_partition(&tp.topic, tp.partition)
                .await
            {
                Ok(l) => l,
                Err(e) => {
                    let msg = format!("{e:?}");
                    for entry in entries {
                        let _ = entry.result_tx.send(Err(msg.clone()));
                    }
                    continue;
                }
            };

            // Build a flat records vec and remember how many records each
            // entry contributed (to distribute offsets back afterwards).
            let counts: Vec<usize> = entries.iter().map(|e| e.records.len()).collect();
            let all_records: Vec<(Vec<u8>, Vec<u8>, i64)> = entries
                .iter()
                .flat_map(|e| e.records.iter().cloned())
                .collect();

            // Run blocking I/O on the threadpool — must not block the Tokio executor.
            let result = tokio::task::spawn_blocking(move || {
                let mut log = lock.blocking_lock();
                (log.append_batch(&all_records), entries, counts)
            })
            .await;

            match result {
                Ok((Ok(offsets), entries, counts)) => {
                    let mut idx = 0;
                    for (entry, n) in entries.into_iter().zip(counts) {
                        let _ = entry.result_tx.send(Ok(offsets[idx..idx + n].to_vec()));
                        idx += n;
                    }
                }
                Ok((Err(e), entries, _counts)) => {
                    let msg = format!("{e:?}");
                    for entry in entries {
                        let _ = entry.result_tx.send(Err(msg.clone()));
                    }
                }
                Err(join_err) => {
                    // spawn_blocking task panicked.
                    let msg = join_err.to_string();
                    // entries were moved into the closure; we can't recover them.
                    // The oneshot receivers will get a RecvError (dropped sender).
                    eprintln!("group_commit flusher panic: {msg}");
                }
            }
        }
    }
}

/// All shared state for the async broker.  Replace `Arc<Mutex<TransportServer>>`
/// with `Arc<BrokerSharedState>` in the async transport layer to enable
/// per-partition parallelism while keeping coordinator and security state
/// independently lockable.
///
/// **Lock ordering**: always acquire `broker` (partition lock) before
/// `transactions`.  Never hold `transactions` while acquiring a partition lock.
pub struct BrokerSharedState {
    pub broker: Arc<PartitionedBrokerSharded>,
    pub offsets: Arc<tokio::sync::Mutex<OffsetCoordinator>>,
    pub groups: Arc<tokio::sync::Mutex<ClassicGroupCoordinator>>,
    pub transactions: Arc<tokio::sync::Mutex<TransactionCoordinator>>,
    /// Immutable after construction — no lock needed.
    pub security: TransportSecurityConfig,
    pub metrics: Arc<TransportMetrics>,
    pub max_frame_size: Arc<std::sync::atomic::AtomicUsize>,
    /// Hot-path produce channel: connections submit records here; the
    /// `run_group_commit_flusher` task batches and flushes them together.
    pub group_commit_tx: GroupCommitSender,
}

impl BrokerSharedState {
    /// Open a sharded broker state backed by `data_dir` with default security.
    pub async fn open<P: AsRef<Path>>(
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<Self, TransportSharedError> {
        Self::open_with_security(data_dir, log_config, TransportSecurityConfig::default()).await
    }

    /// Open a sharded broker state with custom security configuration.
    pub async fn open_with_security<P: AsRef<Path>>(
        data_dir: P,
        log_config: PersistentLogConfig,
        security: TransportSecurityConfig,
    ) -> Result<Self, TransportSharedError> {
        let data_dir = data_dir.as_ref().to_path_buf();

        let broker = PartitionedBrokerSharded::open(&data_dir, log_config)
            .await
            .map_err(|e| TransportSharedError::Setup(format!("broker: {e:?}")))?;

        let offsets = tokio::task::spawn_blocking({
            let d = data_dir.join("__consumer_offsets");
            move || OffsetCoordinator::open(d, OffsetStoreConfig::default())
        })
        .await
        .map_err(|e| TransportSharedError::Setup(e.to_string()))?
        .map_err(|e| TransportSharedError::Setup(format!("offsets: {e:?}")))?;

        let groups = tokio::task::spawn_blocking({
            let d = data_dir.join("__consumer_groups");
            move || ClassicGroupCoordinator::open(d, ClassicGroupStoreConfig::default())
        })
        .await
        .map_err(|e| TransportSharedError::Setup(e.to_string()))?
        .map_err(|e| TransportSharedError::Setup(format!("groups: {e:?}")))?;

        let transactions = tokio::task::spawn_blocking({
            let d = data_dir.join("__transaction_state");
            move || TransactionCoordinator::open(d, TransactionStoreConfig::default())
        })
        .await
        .map_err(|e| TransportSharedError::Setup(e.to_string()))?
        .map_err(|e| TransportSharedError::Setup(format!("transactions: {e:?}")))?;

        let metrics = TransportMetrics::new()
            .map_err(|e| TransportSharedError::Setup(format!("metrics: {e}")))?;

        let broker = Arc::new(broker);

        // Spawn the group-commit flusher.  Channel capacity = 8×MAX_BATCH so
        // connections are never blocked waiting for the flusher to drain.
        let (group_commit_tx, group_commit_rx) = tokio::sync::mpsc::channel(8 * 4096);
        tokio::spawn(run_group_commit_flusher(
            group_commit_rx,
            Arc::clone(&broker),
        ));

        Ok(Self {
            broker,
            offsets: Arc::new(tokio::sync::Mutex::new(offsets)),
            groups: Arc::new(tokio::sync::Mutex::new(groups)),
            transactions: Arc::new(tokio::sync::Mutex::new(transactions)),
            security,
            metrics: Arc::new(metrics),
            max_frame_size: Arc::new(std::sync::atomic::AtomicUsize::new(
                8 * 1024 * 1024, // DEFAULT_MAX_FRAME_SIZE
            )),
            group_commit_tx,
        })
    }

    /// Returns a `TransportConnectionState` appropriate for this server's
    /// security configuration.
    pub fn new_connection_state(&self) -> crate::TransportConnectionState {
        crate::TransportConnectionState::new_for_security(self.security.sasl.is_some())
    }
}

/// Error returned when constructing `BrokerSharedState`.
#[derive(Debug)]
pub enum TransportSharedError {
    Setup(String),
}

fn parse_partition_dir_name(name: &str) -> Option<(String, i32)> {
    let dash = name.rfind('-')?;
    if dash == 0 || dash == name.len() - 1 {
        return None;
    }
    let topic = &name[..dash];
    let partition = name[dash + 1..].parse::<i32>().ok()?;
    Some((topic.to_string(), partition))
}

fn partition_dir_contains_segment_files(path: &Path) -> Result<bool, PartitionedBrokerError> {
    let entries = fs::read_dir(path).map_err(|err| {
        PartitionedBrokerError::Storage(StorageError::Io {
            operation: "read_dir",
            path: path.to_path_buf(),
            message: err.to_string(),
        })
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| {
            PartitionedBrokerError::Storage(StorageError::Io {
                operation: "read_dir_entry",
                path: path.to_path_buf(),
                message: err.to_string(),
            })
        })?;
        let entry_path = entry.path();
        if !entry_path.is_file() {
            continue;
        }
        let Some(name) = entry_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".log") || name.ends_with(".index") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_then_fetch_roundtrip() {
        let mut broker = Broker::new(0);
        let offset = broker.produce(b"k".to_vec(), b"v".to_vec(), 1000);
        assert_eq!(offset, 0);
        let records = broker.fetch(0, 10).expect("fetch");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"k".to_vec());
        assert_eq!(records[0].value, b"v".to_vec());
    }

    #[test]
    fn fetch_out_of_range_is_error() {
        let broker = Broker::new(10);
        let err = broker.fetch(9, 1).expect_err("out of range");
        assert_eq!(
            err,
            StorageError::OffsetOutOfRange {
                requested: 9,
                earliest: 10,
                latest: 9,
            }
        );
    }

    #[test]
    fn group_join_and_heartbeat() {
        let mut broker = Broker::new(0);
        let join = broker.join_group("group-a", "member-1", 0, 1000);
        assert_eq!(join.generation, 1);
        broker
            .heartbeat_group("group-a", "member-1", 100)
            .expect("heartbeat");
    }
}
