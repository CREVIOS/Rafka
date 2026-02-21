#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{CoordinatorError, GroupState, JoinResult};

const OFFSETS_LOG_FILE: &str = "offsets.wal";
const RECORD_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OffsetStoreConfig {
    pub sync_on_commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TopicStorageKey {
    Name(String),
    Id([u8; 16]),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OffsetKey {
    group_id: String,
    topic: TopicStorageKey,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetTopic {
    pub name: Option<String>,
    pub topic_id: Option<[u8; 16]>,
}

impl OffsetTopic {
    pub fn by_name(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            topic_id: None,
        }
    }

    pub fn by_id(topic_id: [u8; 16]) -> Self {
        Self {
            name: None,
            topic_id: Some(topic_id),
        }
    }

    fn to_storage_key(&self) -> Result<TopicStorageKey, OffsetCoordinatorError> {
        match (&self.name, self.topic_id) {
            (Some(name), None) if !name.is_empty() => Ok(TopicStorageKey::Name(name.clone())),
            (None, Some(topic_id)) => Ok(TopicStorageKey::Id(topic_id)),
            _ => Err(OffsetCoordinatorError::InvalidTopic),
        }
    }

    fn from_storage_key(key: &TopicStorageKey) -> Self {
        match key {
            TopicStorageKey::Name(name) => Self {
                name: Some(name.clone()),
                topic_id: None,
            },
            TopicStorageKey::Id(topic_id) => Self {
                name: None,
                topic_id: Some(*topic_id),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetEntry {
    pub topic: OffsetTopic,
    pub partition: i32,
    pub value: CommittedOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitInput<'a> {
    pub api_version: i16,
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub member_epoch: i32,
    pub topic: OffsetTopic,
    pub partition: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetCoordinatorError {
    Group(CoordinatorError),
    InvalidGroupId,
    InvalidTopic,
    InvalidPartition(i32),
    GroupIdNotFound,
    UnknownMemberId(String),
    IllegalGeneration,
    StaleMemberEpoch,
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

impl From<CoordinatorError> for OffsetCoordinatorError {
    fn from(value: CoordinatorError) -> Self {
        Self::Group(value)
    }
}

#[derive(Debug)]
pub struct OffsetCoordinator {
    offsets: BTreeMap<OffsetKey, CommittedOffset>,
    groups: BTreeMap<String, GroupState>,
    log_file: File,
    log_path: PathBuf,
    sync_on_commit: bool,
}

impl OffsetCoordinator {
    pub fn open<P: AsRef<Path>>(
        data_dir: P,
        config: OffsetStoreConfig,
    ) -> Result<Self, OffsetCoordinatorError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir).map_err(|err| OffsetCoordinatorError::Io {
            operation: "create_dir_all",
            path: data_dir.to_path_buf(),
            message: err.to_string(),
        })?;

        let log_path = data_dir.join(OFFSETS_LOG_FILE);
        let offsets = load_offsets_from_log(&log_path)?;
        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(|err| OffsetCoordinatorError::Io {
                operation: "open",
                path: log_path.clone(),
                message: err.to_string(),
            })?;

        Ok(Self {
            offsets,
            groups: BTreeMap::new(),
            log_file,
            log_path,
            sync_on_commit: config.sync_on_commit,
        })
    }

    pub fn join_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        now_ms: u64,
        session_timeout_ms: u64,
    ) -> Result<JoinResult, OffsetCoordinatorError> {
        if group_id.is_empty() {
            return Err(OffsetCoordinatorError::InvalidGroupId);
        }
        let group = self
            .groups
            .entry(group_id.to_string())
            .or_insert_with(|| GroupState::new(group_id, session_timeout_ms));
        Ok(group.join(member_id.to_string(), now_ms))
    }

    pub fn heartbeat_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        now_ms: u64,
    ) -> Result<(), OffsetCoordinatorError> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Err(OffsetCoordinatorError::UnknownMemberId(
                member_id.to_string(),
            ));
        };
        group.heartbeat(member_id, now_ms)?;
        Ok(())
    }

    pub fn evict_expired_members(
        &mut self,
        group_id: &str,
        now_ms: u64,
    ) -> Result<Vec<String>, OffsetCoordinatorError> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Ok(Vec::new());
        };
        Ok(group.evict_expired(now_ms))
    }

    pub fn commit_offset(
        &mut self,
        commit: OffsetCommitInput<'_>,
    ) -> Result<(), OffsetCoordinatorError> {
        let OffsetCommitInput {
            api_version,
            group_id,
            member_id,
            member_epoch,
            topic,
            partition,
            committed_offset,
            committed_leader_epoch,
            metadata,
            commit_timestamp_ms,
        } = commit;

        if group_id.is_empty() {
            return Err(OffsetCoordinatorError::InvalidGroupId);
        }
        if partition < 0 {
            return Err(OffsetCoordinatorError::InvalidPartition(partition));
        }
        let topic_key = topic.to_storage_key()?;
        self.validate_commit_membership(api_version, group_id, member_id, member_epoch)?;

        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic_key,
            partition,
        };
        let value = CommittedOffset {
            committed_offset,
            committed_leader_epoch,
            metadata,
            commit_timestamp_ms,
        };

        let encoded = encode_record(&key, &value)?;
        self.log_file
            .write_all(&encoded)
            .map_err(|err| OffsetCoordinatorError::Io {
                operation: "write_all",
                path: self.log_path.clone(),
                message: err.to_string(),
            })?;
        if self.sync_on_commit {
            self.log_file
                .sync_data()
                .map_err(|err| OffsetCoordinatorError::Io {
                    operation: "sync_data",
                    path: self.log_path.clone(),
                    message: err.to_string(),
                })?;
        }
        self.offsets.insert(key, value);
        Ok(())
    }

    pub fn fetch_offset(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: Option<i32>,
        topic: &OffsetTopic,
        partition: i32,
    ) -> Result<Option<CommittedOffset>, OffsetCoordinatorError> {
        if group_id.is_empty() {
            return Err(OffsetCoordinatorError::InvalidGroupId);
        }
        if partition < 0 {
            return Err(OffsetCoordinatorError::InvalidPartition(partition));
        }
        let topic_key = topic.to_storage_key()?;
        self.validate_fetch_membership(group_id, member_id, member_epoch)?;

        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic_key,
            partition,
        };
        Ok(self.offsets.get(&key).cloned())
    }

    pub fn fetch_group_offsets(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: Option<i32>,
    ) -> Result<Vec<OffsetEntry>, OffsetCoordinatorError> {
        if group_id.is_empty() {
            return Err(OffsetCoordinatorError::InvalidGroupId);
        }
        self.validate_fetch_membership(group_id, member_id, member_epoch)?;

        let mut entries = Vec::new();
        for (key, value) in &self.offsets {
            if key.group_id != group_id {
                continue;
            }
            entries.push(OffsetEntry {
                topic: OffsetTopic::from_storage_key(&key.topic),
                partition: key.partition,
                value: value.clone(),
            });
        }
        Ok(entries)
    }

    pub fn validate_fetch_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: Option<i32>,
    ) -> Result<(), OffsetCoordinatorError> {
        if group_id.is_empty() {
            return Err(OffsetCoordinatorError::InvalidGroupId);
        }
        self.validate_fetch_membership(group_id, member_id, member_epoch)
    }

    fn validate_commit_membership(
        &self,
        api_version: i16,
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
    ) -> Result<(), OffsetCoordinatorError> {
        let admin_style_commit = member_id.is_empty() && member_epoch == -1;
        if admin_style_commit {
            return Ok(());
        }

        let Some(group) = self.groups.get(group_id) else {
            if api_version >= 9 {
                return Err(OffsetCoordinatorError::GroupIdNotFound);
            }
            return Err(OffsetCoordinatorError::IllegalGeneration);
        };
        if !group.contains_member(member_id) {
            return Err(OffsetCoordinatorError::UnknownMemberId(
                member_id.to_string(),
            ));
        }

        let group_epoch = i32::try_from(group.generation()).unwrap_or(i32::MAX);
        if member_epoch != group_epoch {
            if api_version >= 9 {
                return Err(OffsetCoordinatorError::StaleMemberEpoch);
            }
            return Err(OffsetCoordinatorError::IllegalGeneration);
        }
        Ok(())
    }

    fn validate_fetch_membership(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: Option<i32>,
    ) -> Result<(), OffsetCoordinatorError> {
        let member_id = member_id.unwrap_or("");
        let member_epoch = member_epoch.unwrap_or(-1);
        let admin_style_fetch = member_id.is_empty() && member_epoch == -1;
        if admin_style_fetch {
            return Ok(());
        }

        let Some(group) = self.groups.get(group_id) else {
            return Err(OffsetCoordinatorError::GroupIdNotFound);
        };
        if !group.contains_member(member_id) {
            return Err(OffsetCoordinatorError::UnknownMemberId(
                member_id.to_string(),
            ));
        }

        let group_epoch = i32::try_from(group.generation()).unwrap_or(i32::MAX);
        if member_epoch != group_epoch {
            return Err(OffsetCoordinatorError::StaleMemberEpoch);
        }
        Ok(())
    }
}

fn load_offsets_from_log(
    path: &Path,
) -> Result<BTreeMap<OffsetKey, CommittedOffset>, OffsetCoordinatorError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => {
            return Err(OffsetCoordinatorError::Io {
                operation: "read",
                path: path.to_path_buf(),
                message: err.to_string(),
            });
        }
    };

    let mut offsets = BTreeMap::new();
    let mut cursor = 0_usize;
    let mut valid_end = 0_usize;

    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            break;
        }

        let record_len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        cursor += 4;

        if bytes.len().saturating_sub(cursor) < record_len {
            break;
        }
        let record_end = cursor + record_len;
        let payload = &bytes[cursor..record_end];
        match decode_record(payload) {
            Ok((key, value)) => {
                offsets.insert(key, value);
                cursor = record_end;
                valid_end = record_end;
            }
            Err(_) => {
                break;
            }
        }
    }

    if valid_end < bytes.len() {
        truncate_file(path, valid_end)?;
    }

    Ok(offsets)
}

fn truncate_file(path: &Path, len: usize) -> Result<(), OffsetCoordinatorError> {
    let file =
        OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|err| OffsetCoordinatorError::Io {
                operation: "open",
                path: path.to_path_buf(),
                message: err.to_string(),
            })?;
    file.set_len(u64::try_from(len).unwrap_or(u64::MAX))
        .map_err(|err| OffsetCoordinatorError::Io {
            operation: "set_len",
            path: path.to_path_buf(),
            message: err.to_string(),
        })?;
    Ok(())
}

fn encode_record(
    key: &OffsetKey,
    value: &CommittedOffset,
) -> Result<Vec<u8>, OffsetCoordinatorError> {
    let mut payload = Vec::new();
    payload.push(RECORD_VERSION);
    write_u16_string(&mut payload, &key.group_id)?;
    match &key.topic {
        TopicStorageKey::Name(name) => {
            payload.push(0);
            write_u16_string(&mut payload, name)?;
        }
        TopicStorageKey::Id(topic_id) => {
            payload.push(1);
            payload.extend_from_slice(topic_id);
        }
    }
    payload.extend_from_slice(&key.partition.to_be_bytes());
    payload.extend_from_slice(&value.committed_offset.to_be_bytes());
    payload.extend_from_slice(&value.committed_leader_epoch.to_be_bytes());
    write_nullable_i32_string(&mut payload, value.metadata.as_deref())?;
    payload.extend_from_slice(&value.commit_timestamp_ms.to_be_bytes());

    let payload_len = u32::try_from(payload.len()).map_err(|_| OffsetCoordinatorError::Io {
        operation: "encode",
        path: PathBuf::new(),
        message: "offset record too large".to_string(),
    })?;

    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_record(input: &[u8]) -> Result<(OffsetKey, CommittedOffset), OffsetCoordinatorError> {
    let mut cursor = 0_usize;
    let Some(version) = input.get(cursor).copied() else {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "missing version".to_string(),
        });
    };
    cursor += 1;
    if version != RECORD_VERSION {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "unsupported record version".to_string(),
        });
    }

    let group_id = read_u16_string(input, &mut cursor)?;
    let topic_kind = read_u8(input, &mut cursor)?;
    let topic = match topic_kind {
        0 => TopicStorageKey::Name(read_u16_string(input, &mut cursor)?),
        1 => TopicStorageKey::Id(read_uuid(input, &mut cursor)?),
        _ => {
            return Err(OffsetCoordinatorError::Io {
                operation: "decode",
                path: PathBuf::new(),
                message: "invalid topic kind".to_string(),
            });
        }
    };
    let partition = read_i32(input, &mut cursor)?;
    let committed_offset = read_i64(input, &mut cursor)?;
    let committed_leader_epoch = read_i32(input, &mut cursor)?;
    let metadata = read_nullable_i32_string(input, &mut cursor)?;
    let commit_timestamp_ms = read_i64(input, &mut cursor)?;

    if cursor != input.len() {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "trailing bytes in offset record".to_string(),
        });
    }

    Ok((
        OffsetKey {
            group_id,
            topic,
            partition,
        },
        CommittedOffset {
            committed_offset,
            committed_leader_epoch,
            metadata,
            commit_timestamp_ms,
        },
    ))
}

fn write_u16_string(out: &mut Vec<u8>, value: &str) -> Result<(), OffsetCoordinatorError> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| OffsetCoordinatorError::Io {
        operation: "encode",
        path: PathBuf::new(),
        message: "string too large".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_nullable_i32_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), OffsetCoordinatorError> {
    match value {
        Some(value) => {
            let bytes = value.as_bytes();
            let len = i32::try_from(bytes.len()).map_err(|_| OffsetCoordinatorError::Io {
                operation: "encode",
                path: PathBuf::new(),
                message: "string too large".to_string(),
            })?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
    }
    Ok(())
}

fn read_u8(input: &[u8], cursor: &mut usize) -> Result<u8, OffsetCoordinatorError> {
    let Some(value) = input.get(*cursor).copied() else {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "truncated u8".to_string(),
        });
    };
    *cursor += 1;
    Ok(value)
}

fn read_u16(input: &[u8], cursor: &mut usize) -> Result<u16, OffsetCoordinatorError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "truncated u16".to_string(),
        });
    }
    let value = u16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_i32(input: &[u8], cursor: &mut usize) -> Result<i32, OffsetCoordinatorError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
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

fn read_i64(input: &[u8], cursor: &mut usize) -> Result<i64, OffsetCoordinatorError> {
    if input.len() < cursor.saturating_add(8) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
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

fn read_uuid(input: &[u8], cursor: &mut usize) -> Result<[u8; 16], OffsetCoordinatorError> {
    if input.len() < cursor.saturating_add(16) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "truncated uuid".to_string(),
        });
    }
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&input[*cursor..*cursor + 16]);
    *cursor += 16;
    Ok(uuid)
}

fn read_u16_string(input: &[u8], cursor: &mut usize) -> Result<String, OffsetCoordinatorError> {
    let len = usize::from(read_u16(input, cursor)?);
    if input.len() < cursor.saturating_add(len) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "truncated string".to_string(),
        });
    }
    let bytes = &input[*cursor..*cursor + len];
    *cursor += len;
    String::from_utf8(bytes.to_vec()).map_err(|_| OffsetCoordinatorError::Io {
        operation: "decode",
        path: PathBuf::new(),
        message: "invalid utf8 string".to_string(),
    })
}

fn read_nullable_i32_string(
    input: &[u8],
    cursor: &mut usize,
) -> Result<Option<String>, OffsetCoordinatorError> {
    let len = read_i32(input, cursor)?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "invalid nullable string length".to_string(),
        });
    }
    let len = usize::try_from(len).map_err(|_| OffsetCoordinatorError::Io {
        operation: "decode",
        path: PathBuf::new(),
        message: "invalid nullable string length".to_string(),
    })?;
    if input.len() < cursor.saturating_add(len) {
        return Err(OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "truncated nullable string".to_string(),
        });
    }
    let bytes = &input[*cursor..*cursor + len];
    *cursor += len;
    String::from_utf8(bytes.to_vec())
        .map(Some)
        .map_err(|_| OffsetCoordinatorError::Io {
            operation: "decode",
            path: PathBuf::new(),
            message: "invalid utf8 nullable string".to_string(),
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
                "rafka-offsets-{label}-{millis}-{}-{counter}",
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
    fn admin_commit_and_fetch_roundtrip() {
        let temp = TempDir::new("admin-commit-fetch");
        let mut coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");

        coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 8,
                group_id: "group-a",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("topic-a"),
                partition: 0,
                committed_offset: 100,
                committed_leader_epoch: -1,
                metadata: Some("meta".to_string()),
                commit_timestamp_ms: 1_000,
            })
            .expect("commit offset");

        let fetched = coordinator
            .fetch_offset("group-a", None, None, &OffsetTopic::by_name("topic-a"), 0)
            .expect("fetch offset")
            .expect("offset present");
        assert_eq!(fetched.committed_offset, 100);
        assert_eq!(fetched.committed_leader_epoch, -1);
        assert_eq!(fetched.metadata.as_deref(), Some("meta"));
    }

    #[test]
    fn restart_recovers_offsets() {
        let temp = TempDir::new("restart-recover");
        {
            let mut coordinator =
                OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");
            coordinator
                .commit_offset(OffsetCommitInput {
                    api_version: 10,
                    group_id: "group-a",
                    member_id: "",
                    member_epoch: -1,
                    topic: OffsetTopic::by_id([9_u8; 16]),
                    partition: 2,
                    committed_offset: 222,
                    committed_leader_epoch: 5,
                    metadata: None,
                    commit_timestamp_ms: 2_000,
                })
                .expect("commit offset");
        }

        let coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("reopen");
        let fetched = coordinator
            .fetch_offset("group-a", None, None, &OffsetTopic::by_id([9_u8; 16]), 2)
            .expect("fetch recovered offset")
            .expect("recovered offset present");
        assert_eq!(fetched.committed_offset, 222);
        assert_eq!(fetched.committed_leader_epoch, 5);
    }

    #[test]
    fn non_admin_commit_requires_group_membership() {
        let temp = TempDir::new("requires-membership");
        let mut coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");

        let err = coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 8,
                group_id: "group-a",
                member_id: "member-a",
                member_epoch: 1,
                topic: OffsetTopic::by_name("topic-a"),
                partition: 0,
                committed_offset: 1,
                committed_leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
            })
            .expect_err("unknown group should fail");
        assert_eq!(err, OffsetCoordinatorError::IllegalGeneration);

        coordinator
            .join_group("group-a", "member-a", 0, 5_000)
            .expect("join member");
        coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 8,
                group_id: "group-a",
                member_id: "member-a",
                member_epoch: 1,
                topic: OffsetTopic::by_name("topic-a"),
                partition: 0,
                committed_offset: 2,
                committed_leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
            })
            .expect("commit with valid member");
    }

    #[test]
    fn stale_member_epoch_is_detected_for_v9_plus() {
        let temp = TempDir::new("stale-member-epoch");
        let mut coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");
        coordinator
            .join_group("group-a", "member-a", 0, 5_000)
            .expect("join member");

        let err = coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 9,
                group_id: "group-a",
                member_id: "member-a",
                member_epoch: 2,
                topic: OffsetTopic::by_name("topic-a"),
                partition: 0,
                committed_offset: 10,
                committed_leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
            })
            .expect_err("stale epoch should fail");
        assert_eq!(err, OffsetCoordinatorError::StaleMemberEpoch);
    }

    #[test]
    fn fetch_group_returns_sorted_entries() {
        let temp = TempDir::new("fetch-group-sorted");
        let mut coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");
        coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 8,
                group_id: "group-a",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("topic-b"),
                partition: 1,
                committed_offset: 11,
                committed_leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
            })
            .expect("commit b1");
        coordinator
            .commit_offset(OffsetCommitInput {
                api_version: 8,
                group_id: "group-a",
                member_id: "",
                member_epoch: -1,
                topic: OffsetTopic::by_name("topic-a"),
                partition: 0,
                committed_offset: 10,
                committed_leader_epoch: -1,
                metadata: None,
                commit_timestamp_ms: 0,
            })
            .expect("commit a0");

        let all = coordinator
            .fetch_group_offsets("group-a", None, None)
            .expect("fetch group");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].topic.name.as_deref(), Some("topic-a"));
        assert_eq!(all[0].partition, 0);
        assert_eq!(all[1].topic.name.as_deref(), Some("topic-b"));
        assert_eq!(all[1].partition, 1);
    }

    #[test]
    fn recovers_from_torn_tail() {
        let temp = TempDir::new("torn-tail");
        let log_path = temp.path().join(OFFSETS_LOG_FILE);
        {
            let mut coordinator =
                OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("open");
            coordinator
                .commit_offset(OffsetCommitInput {
                    api_version: 8,
                    group_id: "group-a",
                    member_id: "",
                    member_epoch: -1,
                    topic: OffsetTopic::by_name("topic-a"),
                    partition: 0,
                    committed_offset: 100,
                    committed_leader_epoch: -1,
                    metadata: None,
                    commit_timestamp_ms: 1,
                })
                .expect("commit");
        }

        let mut bytes = fs::read(&log_path).expect("read wal");
        bytes.extend_from_slice(&[0xde, 0xad, 0xbe]);
        fs::write(&log_path, bytes).expect("append torn tail");

        let coordinator =
            OffsetCoordinator::open(temp.path(), OffsetStoreConfig::default()).expect("reopen");
        let fetched = coordinator
            .fetch_offset("group-a", None, None, &OffsetTopic::by_name("topic-a"), 0)
            .expect("fetch")
            .expect("offset present");
        assert_eq!(fetched.committed_offset, 100);
    }
}
