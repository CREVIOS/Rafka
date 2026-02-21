#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const CLASSIC_GROUPS_LOG_FILE: &str = "classic_groups.wal";
const CLASSIC_RECORD_VERSION: u8 = 1;
const CLASSIC_RECORD_KIND_UPSERT: u8 = 1;
const CLASSIC_RECORD_KIND_DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClassicGroupStoreConfig {
    pub sync_on_commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassicGroupCoordinatorError {
    InvalidGroupId,
    UnknownMemberId(String),
    IllegalGeneration,
    InconsistentGroupProtocol,
    MemberIdRequired(String),
    RebalanceInProgress,
    FencedInstanceId,
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupInput {
    pub api_version: i16,
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<JoinProtocol>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupOutcome {
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
    pub skip_assignment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAssignment {
    pub member_id: String,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupInput {
    pub api_version: i16,
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignments: Vec<SyncAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupOutcome {
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatInput {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberInput {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberOutcome {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub error: Option<ClassicGroupCoordinatorError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClassicMemberState {
    member_id: String,
    group_instance_id: Option<String>,
    metadata: Vec<u8>,
    protocols: Vec<JoinProtocol>,
    last_heartbeat_ms: u64,
    pending_rejoin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClassicGroupState {
    generation_id: i32,
    protocol_type: String,
    protocol_name: String,
    rebalance_in_progress: bool,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    members: BTreeMap<String, ClassicMemberState>,
    assignments: BTreeMap<String, Vec<u8>>,
    pending_member_ids: BTreeSet<String>,
}

impl ClassicGroupState {
    fn new(
        protocol_type: String,
        protocol_name: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
    ) -> Self {
        Self {
            generation_id: 0,
            protocol_type,
            protocol_name,
            rebalance_in_progress: false,
            session_timeout_ms,
            rebalance_timeout_ms,
            members: BTreeMap::new(),
            assignments: BTreeMap::new(),
            pending_member_ids: BTreeSet::new(),
        }
    }

    fn leader_id(&self) -> String {
        self.members
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(String::new)
    }
}

#[derive(Debug)]
pub struct ClassicGroupCoordinator {
    groups: BTreeMap<String, ClassicGroupState>,
    log_file: File,
    log_path: PathBuf,
    sync_on_commit: bool,
    next_member_sequence: u64,
}

impl ClassicGroupCoordinator {
    pub fn open<P: AsRef<Path>>(
        data_dir: P,
        config: ClassicGroupStoreConfig,
    ) -> Result<Self, ClassicGroupCoordinatorError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir).map_err(|err| ClassicGroupCoordinatorError::Io {
            operation: "create_dir_all",
            path: data_dir.to_path_buf(),
            message: err.to_string(),
        })?;

        let log_path = data_dir.join(CLASSIC_GROUPS_LOG_FILE);
        let groups = load_groups_from_log(&log_path)?;
        let next_member_sequence = derive_next_member_sequence(&groups);

        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .map_err(|err| ClassicGroupCoordinatorError::Io {
                operation: "open",
                path: log_path.clone(),
                message: err.to_string(),
            })?;

        Ok(Self {
            groups,
            log_file,
            log_path,
            sync_on_commit: config.sync_on_commit,
            next_member_sequence,
        })
    }

    pub fn join_group(
        &mut self,
        input: JoinGroupInput,
        now_ms: u64,
    ) -> Result<JoinGroupOutcome, ClassicGroupCoordinatorError> {
        if input.group_id.is_empty() {
            return Err(ClassicGroupCoordinatorError::InvalidGroupId);
        }
        if input.protocol_type.is_empty() || input.protocols.is_empty() {
            return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
        }

        let selected_protocol_name = input.protocols[0].name.clone();
        if selected_protocol_name.is_empty() {
            return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
        }

        if !self.groups.contains_key(&input.group_id) {
            if input.api_version >= 4 && !input.member_id.is_empty() {
                return Err(ClassicGroupCoordinatorError::UnknownMemberId(
                    input.member_id,
                ));
            }
            self.groups.insert(
                input.group_id.clone(),
                ClassicGroupState::new(
                    input.protocol_type.clone(),
                    selected_protocol_name.clone(),
                    input.session_timeout_ms,
                    input.rebalance_timeout_ms,
                ),
            );
        }

        let mut persist_group = false;

        if input.api_version >= 4 && input.member_id.is_empty() {
            let member_id = self.next_member_id();
            {
                let group = self
                    .groups
                    .get_mut(&input.group_id)
                    .expect("group inserted above");
                ensure_join_protocol_compatible(group, &input.protocol_type, &input.protocols)?;
                group.pending_member_ids.insert(member_id.clone());
            }
            self.persist_group(&input.group_id)?;
            return Err(ClassicGroupCoordinatorError::MemberIdRequired(member_id));
        }

        let generated_member_id = if input.api_version < 4 && input.member_id.is_empty() {
            Some(self.next_member_id())
        } else {
            None
        };

        let outcome = {
            let group = self
                .groups
                .get_mut(&input.group_id)
                .expect("group inserted above");
            ensure_join_protocol_compatible(group, &input.protocol_type, &input.protocols)?;

            let member_id = if let Some(member_id) = generated_member_id {
                member_id
            } else {
                input.member_id.clone()
            };

            let mut is_new_member = false;
            let reserved_member = group.pending_member_ids.remove(&member_id);
            let member = match group.members.get_mut(&member_id) {
                Some(member) => {
                    if let Some(group_instance_id) = input.group_instance_id.as_deref() {
                        if let Some(existing_group_instance_id) =
                            member.group_instance_id.as_deref()
                        {
                            if existing_group_instance_id != group_instance_id {
                                return Err(ClassicGroupCoordinatorError::FencedInstanceId);
                            }
                        }
                    }
                    member
                }
                None => {
                    if !reserved_member && (input.api_version >= 4 || !input.member_id.is_empty()) {
                        return Err(ClassicGroupCoordinatorError::UnknownMemberId(member_id));
                    }
                    is_new_member = true;
                    group
                        .members
                        .entry(member_id.clone())
                        .or_insert(ClassicMemberState {
                            member_id: member_id.clone(),
                            group_instance_id: input.group_instance_id.clone(),
                            metadata: input.protocols[0].metadata.clone(),
                            protocols: input.protocols.clone(),
                            last_heartbeat_ms: now_ms,
                            pending_rejoin: false,
                        })
                }
            };

            member.group_instance_id = input.group_instance_id.clone();
            member.metadata = input.protocols[0].metadata.clone();
            member.protocols = input.protocols.clone();
            member.last_heartbeat_ms = now_ms;

            if is_new_member {
                if group.generation_id == 0 {
                    group.generation_id = 1;
                    group.rebalance_in_progress = false;
                } else {
                    group.generation_id = group.generation_id.saturating_add(1);
                    group.rebalance_in_progress = true;
                    for current_member in group.members.values_mut() {
                        current_member.pending_rejoin = true;
                    }
                    if let Some(current) = group.members.get_mut(&member_id) {
                        current.pending_rejoin = false;
                    }
                    group.assignments.clear();
                }
                persist_group = true;
            } else if group.rebalance_in_progress {
                member.pending_rejoin = false;
                if group.members.values().all(|item| !item.pending_rejoin) {
                    group.rebalance_in_progress = false;
                }
                persist_group = true;
            }

            let leader = group.leader_id();
            let members = if leader == member_id {
                group
                    .members
                    .values()
                    .map(|current| JoinGroupMember {
                        member_id: current.member_id.clone(),
                        group_instance_id: current.group_instance_id.clone(),
                        metadata: current.metadata.clone(),
                    })
                    .collect()
            } else {
                Vec::new()
            };

            JoinGroupOutcome {
                generation_id: group.generation_id,
                protocol_type: (input.api_version >= 7).then_some(group.protocol_type.clone()),
                protocol_name: Some(group.protocol_name.clone()),
                leader,
                member_id,
                members,
                skip_assignment: false,
            }
        };

        if persist_group {
            self.persist_group(&input.group_id)?;
        }

        Ok(outcome)
    }

    pub fn sync_group(
        &mut self,
        input: SyncGroupInput,
    ) -> Result<SyncGroupOutcome, ClassicGroupCoordinatorError> {
        if input.group_id.is_empty() {
            return Err(ClassicGroupCoordinatorError::InvalidGroupId);
        }

        let mut persist_group = false;
        let outcome = {
            let group = self.groups.get_mut(&input.group_id).ok_or_else(|| {
                ClassicGroupCoordinatorError::UnknownMemberId(input.member_id.clone())
            })?;

            if input.generation_id != group.generation_id {
                return Err(ClassicGroupCoordinatorError::IllegalGeneration);
            }

            let member = group.members.get(&input.member_id).ok_or_else(|| {
                ClassicGroupCoordinatorError::UnknownMemberId(input.member_id.clone())
            })?;

            if let Some(group_instance_id) = input.group_instance_id.as_deref() {
                if let Some(existing_group_instance_id) = member.group_instance_id.as_deref() {
                    if existing_group_instance_id != group_instance_id {
                        return Err(ClassicGroupCoordinatorError::FencedInstanceId);
                    }
                }
            }

            if input.api_version >= 5 {
                if let Some(protocol_type) = input.protocol_type.as_deref() {
                    if protocol_type != group.protocol_type {
                        return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
                    }
                }
                if let Some(protocol_name) = input.protocol_name.as_deref() {
                    if protocol_name != group.protocol_name {
                        return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
                    }
                }
            }

            if !input.assignments.is_empty() && input.member_id == group.leader_id() {
                group.assignments.clear();
                for assignment in &input.assignments {
                    if group.members.contains_key(&assignment.member_id) {
                        group
                            .assignments
                            .insert(assignment.member_id.clone(), assignment.assignment.clone());
                    }
                }
                group.rebalance_in_progress = false;
                for member in group.members.values_mut() {
                    member.pending_rejoin = false;
                }
                persist_group = true;
            } else if group.rebalance_in_progress
                && !group.assignments.contains_key(&input.member_id)
            {
                return Err(ClassicGroupCoordinatorError::RebalanceInProgress);
            }

            SyncGroupOutcome {
                protocol_type: (input.api_version >= 5).then_some(group.protocol_type.clone()),
                protocol_name: (input.api_version >= 5).then_some(group.protocol_name.clone()),
                assignment: group
                    .assignments
                    .get(&input.member_id)
                    .cloned()
                    .unwrap_or_default(),
            }
        };

        if persist_group {
            self.persist_group(&input.group_id)?;
        }

        Ok(outcome)
    }

    pub fn heartbeat(
        &mut self,
        input: HeartbeatInput,
        now_ms: u64,
    ) -> Result<(), ClassicGroupCoordinatorError> {
        if input.group_id.is_empty() {
            return Err(ClassicGroupCoordinatorError::InvalidGroupId);
        }

        let group = self.groups.get_mut(&input.group_id).ok_or_else(|| {
            ClassicGroupCoordinatorError::UnknownMemberId(input.member_id.clone())
        })?;

        if input.generation_id != group.generation_id {
            return Err(ClassicGroupCoordinatorError::IllegalGeneration);
        }

        let member = group.members.get_mut(&input.member_id).ok_or_else(|| {
            ClassicGroupCoordinatorError::UnknownMemberId(input.member_id.clone())
        })?;

        if let Some(group_instance_id) = input.group_instance_id.as_deref() {
            if let Some(existing_group_instance_id) = member.group_instance_id.as_deref() {
                if existing_group_instance_id != group_instance_id {
                    return Err(ClassicGroupCoordinatorError::FencedInstanceId);
                }
            }
        }

        if group.rebalance_in_progress {
            return Err(ClassicGroupCoordinatorError::RebalanceInProgress);
        }

        member.last_heartbeat_ms = now_ms;
        Ok(())
    }

    pub fn leave_group_legacy(
        &mut self,
        group_id: &str,
        member_id: &str,
    ) -> Result<(), ClassicGroupCoordinatorError> {
        if group_id.is_empty() {
            return Err(ClassicGroupCoordinatorError::InvalidGroupId);
        }

        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| ClassicGroupCoordinatorError::UnknownMemberId(member_id.to_string()))?;

        if !group.members.contains_key(member_id) {
            return Err(ClassicGroupCoordinatorError::UnknownMemberId(
                member_id.to_string(),
            ));
        }

        group.members.remove(member_id);
        group.assignments.remove(member_id);
        group.pending_member_ids.remove(member_id);

        if group.members.is_empty() {
            self.groups.remove(group_id);
            self.persist_group(group_id)?;
            return Ok(());
        }

        group.generation_id = group.generation_id.saturating_add(1);
        group.rebalance_in_progress = true;
        group.assignments.clear();
        for member in group.members.values_mut() {
            member.pending_rejoin = true;
        }
        self.persist_group(group_id)?;
        Ok(())
    }

    pub fn leave_group_members(
        &mut self,
        group_id: &str,
        members: &[LeaveGroupMemberInput],
    ) -> Result<Vec<LeaveGroupMemberOutcome>, ClassicGroupCoordinatorError> {
        if group_id.is_empty() {
            return Err(ClassicGroupCoordinatorError::InvalidGroupId);
        }

        let Some(group) = self.groups.get_mut(group_id) else {
            return Ok(members
                .iter()
                .map(|member| LeaveGroupMemberOutcome {
                    member_id: member.member_id.clone(),
                    group_instance_id: member.group_instance_id.clone(),
                    error: Some(ClassicGroupCoordinatorError::UnknownMemberId(
                        member.member_id.clone(),
                    )),
                })
                .collect());
        };

        let mut outcomes = Vec::with_capacity(members.len());
        let mut removed_any = false;

        for member in members {
            let removal = if let Some(group_instance_id) = member.group_instance_id.as_deref() {
                match find_member_id_by_group_instance_id(group, group_instance_id) {
                    Some(target_member_id) => {
                        if !member.member_id.is_empty() && member.member_id != target_member_id {
                            Err(ClassicGroupCoordinatorError::FencedInstanceId)
                        } else {
                            Ok(target_member_id)
                        }
                    }
                    None => Err(ClassicGroupCoordinatorError::UnknownMemberId(
                        member.member_id.clone(),
                    )),
                }
            } else if group.members.contains_key(&member.member_id) {
                Ok(member.member_id.clone())
            } else {
                Err(ClassicGroupCoordinatorError::UnknownMemberId(
                    member.member_id.clone(),
                ))
            };

            match removal {
                Ok(target_member_id) => {
                    group.members.remove(&target_member_id);
                    group.assignments.remove(&target_member_id);
                    group.pending_member_ids.remove(&target_member_id);
                    removed_any = true;
                    outcomes.push(LeaveGroupMemberOutcome {
                        member_id: member.member_id.clone(),
                        group_instance_id: member.group_instance_id.clone(),
                        error: None,
                    });
                }
                Err(err) => outcomes.push(LeaveGroupMemberOutcome {
                    member_id: member.member_id.clone(),
                    group_instance_id: member.group_instance_id.clone(),
                    error: Some(err),
                }),
            }
        }

        if removed_any {
            if group.members.is_empty() {
                self.groups.remove(group_id);
                self.persist_group(group_id)?;
            } else {
                group.generation_id = group.generation_id.saturating_add(1);
                group.rebalance_in_progress = true;
                group.assignments.clear();
                for member in group.members.values_mut() {
                    member.pending_rejoin = true;
                }
                self.persist_group(group_id)?;
            }
        }

        Ok(outcomes)
    }

    fn next_member_id(&mut self) -> String {
        let member_id = format!("rafka-member-{}", self.next_member_sequence);
        self.next_member_sequence = self.next_member_sequence.saturating_add(1);
        member_id
    }

    fn persist_group(&mut self, group_id: &str) -> Result<(), ClassicGroupCoordinatorError> {
        let bytes = if let Some(group) = self.groups.get(group_id) {
            encode_group_upsert_record(group_id, group)?
        } else {
            encode_group_delete_record(group_id)?
        };

        self.log_file
            .write_all(&bytes)
            .map_err(|err| ClassicGroupCoordinatorError::Io {
                operation: "write_all",
                path: self.log_path.clone(),
                message: err.to_string(),
            })?;

        if self.sync_on_commit {
            self.log_file
                .sync_data()
                .map_err(|err| ClassicGroupCoordinatorError::Io {
                    operation: "sync_data",
                    path: self.log_path.clone(),
                    message: err.to_string(),
                })?;
        }

        Ok(())
    }
}

fn ensure_join_protocol_compatible(
    group: &ClassicGroupState,
    protocol_type: &str,
    protocols: &[JoinProtocol],
) -> Result<(), ClassicGroupCoordinatorError> {
    if group.protocol_type != protocol_type {
        return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
    }
    if protocols.is_empty() {
        return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
    }
    if !protocols
        .iter()
        .any(|protocol| protocol.name == group.protocol_name)
    {
        return Err(ClassicGroupCoordinatorError::InconsistentGroupProtocol);
    }
    Ok(())
}

fn find_member_id_by_group_instance_id(
    group: &ClassicGroupState,
    group_instance_id: &str,
) -> Option<String> {
    for member in group.members.values() {
        if member.group_instance_id.as_deref() == Some(group_instance_id) {
            return Some(member.member_id.clone());
        }
    }
    None
}

fn derive_next_member_sequence(groups: &BTreeMap<String, ClassicGroupState>) -> u64 {
    let mut max_sequence = 0_u64;
    for group in groups.values() {
        for member_id in group.members.keys() {
            if let Some(sequence) = parse_member_sequence(member_id) {
                max_sequence = max_sequence.max(sequence.saturating_add(1));
            }
        }
        for pending_member_id in &group.pending_member_ids {
            if let Some(sequence) = parse_member_sequence(pending_member_id) {
                max_sequence = max_sequence.max(sequence.saturating_add(1));
            }
        }
    }
    max_sequence
}

fn parse_member_sequence(member_id: &str) -> Option<u64> {
    member_id
        .strip_prefix("rafka-member-")
        .and_then(|value| value.parse::<u64>().ok())
}

fn load_groups_from_log(
    path: &Path,
) -> Result<BTreeMap<String, ClassicGroupState>, ClassicGroupCoordinatorError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => {
            return Err(ClassicGroupCoordinatorError::Io {
                operation: "read",
                path: path.to_path_buf(),
                message: err.to_string(),
            });
        }
    };

    let mut groups = BTreeMap::new();
    let mut cursor = 0_usize;
    let mut valid_end = 0_usize;

    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            break;
        }
        let mut length_cursor = cursor;
        let record_len_u32 = read_u32(&bytes, &mut length_cursor, path, "read record length")?;
        let record_len =
            usize::try_from(record_len_u32).map_err(|_| ClassicGroupCoordinatorError::Io {
                operation: "read record length",
                path: path.to_path_buf(),
                message: "record length overflow".to_string(),
            })?;
        cursor = length_cursor;

        if bytes.len().saturating_sub(cursor) < record_len {
            break;
        }

        let record = &bytes[cursor..cursor + record_len];
        let (group_id, group_state) = decode_group_record(record, path)?;
        if let Some(group_state) = group_state {
            groups.insert(group_id, group_state);
        } else {
            groups.remove(&group_id);
        }

        cursor += record_len;
        valid_end = cursor;
    }

    if valid_end < bytes.len() {
        truncate_file(path, valid_end)?;
    }

    Ok(groups)
}

fn truncate_file(path: &Path, len: usize) -> Result<(), ClassicGroupCoordinatorError> {
    let len_u64 = u64::try_from(len).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation: "set_len",
        path: path.to_path_buf(),
        message: "file length overflow".to_string(),
    })?;

    OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_len(len_u64))
        .map_err(|err| ClassicGroupCoordinatorError::Io {
            operation: "set_len",
            path: path.to_path_buf(),
            message: err.to_string(),
        })
}

fn encode_group_upsert_record(
    group_id: &str,
    group: &ClassicGroupState,
) -> Result<Vec<u8>, ClassicGroupCoordinatorError> {
    let mut payload = Vec::new();
    payload.push(CLASSIC_RECORD_VERSION);
    payload.push(CLASSIC_RECORD_KIND_UPSERT);
    write_u16_string(&mut payload, group_id)?;
    payload.extend_from_slice(&group.generation_id.to_be_bytes());
    payload.push(u8::from(group.rebalance_in_progress));
    payload.extend_from_slice(&group.session_timeout_ms.to_be_bytes());
    payload.extend_from_slice(&group.rebalance_timeout_ms.to_be_bytes());
    write_u16_string(&mut payload, &group.protocol_type)?;
    write_u16_string(&mut payload, &group.protocol_name)?;

    write_u32_len(&mut payload, group.members.len())?;
    for member in group.members.values() {
        write_u16_string(&mut payload, &member.member_id)?;
        write_nullable_u16_string(&mut payload, member.group_instance_id.as_deref())?;
        write_u32_bytes(&mut payload, &member.metadata)?;
        write_u32_len(&mut payload, member.protocols.len())?;
        for protocol in &member.protocols {
            write_u16_string(&mut payload, &protocol.name)?;
            write_u32_bytes(&mut payload, &protocol.metadata)?;
        }
        payload.extend_from_slice(&member.last_heartbeat_ms.to_be_bytes());
        payload.push(u8::from(member.pending_rejoin));
    }

    write_u32_len(&mut payload, group.assignments.len())?;
    for (member_id, assignment) in &group.assignments {
        write_u16_string(&mut payload, member_id)?;
        write_u32_bytes(&mut payload, assignment)?;
    }

    write_u32_len(&mut payload, group.pending_member_ids.len())?;
    for member_id in &group.pending_member_ids {
        write_u16_string(&mut payload, member_id)?;
    }

    let mut out = Vec::new();
    write_u32_len(&mut out, payload.len())?;
    out.extend_from_slice(&payload);
    Ok(out)
}

fn encode_group_delete_record(group_id: &str) -> Result<Vec<u8>, ClassicGroupCoordinatorError> {
    let mut payload = Vec::new();
    payload.push(CLASSIC_RECORD_VERSION);
    payload.push(CLASSIC_RECORD_KIND_DELETE);
    write_u16_string(&mut payload, group_id)?;

    let mut out = Vec::new();
    write_u32_len(&mut out, payload.len())?;
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_group_record(
    input: &[u8],
    path: &Path,
) -> Result<(String, Option<ClassicGroupState>), ClassicGroupCoordinatorError> {
    let mut cursor = 0_usize;
    let record_version = read_u8(input, &mut cursor, path, "read record version")?;
    if record_version != CLASSIC_RECORD_VERSION {
        return Err(ClassicGroupCoordinatorError::Io {
            operation: "decode group record",
            path: path.to_path_buf(),
            message: format!("unsupported record version {record_version}"),
        });
    }

    let record_kind = read_u8(input, &mut cursor, path, "read record kind")?;
    let group_id = read_u16_string(input, &mut cursor, path, "read group id")?;

    if record_kind == CLASSIC_RECORD_KIND_DELETE {
        if cursor != input.len() {
            return Err(ClassicGroupCoordinatorError::Io {
                operation: "decode group record",
                path: path.to_path_buf(),
                message: "delete record has trailing bytes".to_string(),
            });
        }
        return Ok((group_id, None));
    }

    if record_kind != CLASSIC_RECORD_KIND_UPSERT {
        return Err(ClassicGroupCoordinatorError::Io {
            operation: "decode group record",
            path: path.to_path_buf(),
            message: format!("unsupported record kind {record_kind}"),
        });
    }

    let generation_id = read_i32(input, &mut cursor, path, "read generation id")?;
    let rebalance_in_progress =
        read_u8(input, &mut cursor, path, "read rebalance flag").map(|value| value != 0)?;
    let session_timeout_ms = read_i32(input, &mut cursor, path, "read session timeout")?;
    let rebalance_timeout_ms = read_i32(input, &mut cursor, path, "read rebalance timeout")?;
    let protocol_type = read_u16_string(input, &mut cursor, path, "read protocol type")?;
    let protocol_name = read_u16_string(input, &mut cursor, path, "read protocol name")?;

    let member_count_u32 = read_u32(input, &mut cursor, path, "read member count")?;
    let member_count =
        usize::try_from(member_count_u32).map_err(|_| ClassicGroupCoordinatorError::Io {
            operation: "read member count",
            path: path.to_path_buf(),
            message: "member count overflow".to_string(),
        })?;
    let mut members = BTreeMap::new();
    for _ in 0..member_count {
        let member_id = read_u16_string(input, &mut cursor, path, "read member id")?;
        let group_instance_id =
            read_nullable_u16_string(input, &mut cursor, path, "read group instance id")?;
        let metadata = read_u32_bytes(input, &mut cursor, path, "read member metadata")?;

        let protocol_count_u32 = read_u32(input, &mut cursor, path, "read protocol count")?;
        let protocol_count =
            usize::try_from(protocol_count_u32).map_err(|_| ClassicGroupCoordinatorError::Io {
                operation: "read protocol count",
                path: path.to_path_buf(),
                message: "protocol count overflow".to_string(),
            })?;
        let mut protocols = Vec::with_capacity(protocol_count);
        for _ in 0..protocol_count {
            let name = read_u16_string(input, &mut cursor, path, "read protocol name")?;
            let metadata = read_u32_bytes(input, &mut cursor, path, "read protocol metadata")?;
            protocols.push(JoinProtocol { name, metadata });
        }

        let last_heartbeat_ms = read_u64(input, &mut cursor, path, "read last heartbeat")?;
        let pending_rejoin = read_u8(input, &mut cursor, path, "read pending rejoin")? != 0;

        members.insert(
            member_id.clone(),
            ClassicMemberState {
                member_id,
                group_instance_id,
                metadata,
                protocols,
                last_heartbeat_ms,
                pending_rejoin,
            },
        );
    }

    let assignment_count_u32 = read_u32(input, &mut cursor, path, "read assignment count")?;
    let assignment_count =
        usize::try_from(assignment_count_u32).map_err(|_| ClassicGroupCoordinatorError::Io {
            operation: "read assignment count",
            path: path.to_path_buf(),
            message: "assignment count overflow".to_string(),
        })?;
    let mut assignments = BTreeMap::new();
    for _ in 0..assignment_count {
        let member_id = read_u16_string(input, &mut cursor, path, "read assignment member id")?;
        let assignment = read_u32_bytes(input, &mut cursor, path, "read assignment bytes")?;
        assignments.insert(member_id, assignment);
    }

    let pending_member_count_u32 = read_u32(input, &mut cursor, path, "read pending member count")?;
    let pending_member_count = usize::try_from(pending_member_count_u32).map_err(|_| {
        ClassicGroupCoordinatorError::Io {
            operation: "read pending member count",
            path: path.to_path_buf(),
            message: "pending member count overflow".to_string(),
        }
    })?;
    let mut pending_member_ids = BTreeSet::new();
    for _ in 0..pending_member_count {
        let member_id = read_u16_string(input, &mut cursor, path, "read pending member id")?;
        pending_member_ids.insert(member_id);
    }

    if cursor != input.len() {
        return Err(ClassicGroupCoordinatorError::Io {
            operation: "decode group record",
            path: path.to_path_buf(),
            message: "record has trailing bytes".to_string(),
        });
    }

    Ok((
        group_id,
        Some(ClassicGroupState {
            generation_id,
            protocol_type,
            protocol_name,
            rebalance_in_progress,
            session_timeout_ms,
            rebalance_timeout_ms,
            members,
            assignments,
            pending_member_ids,
        }),
    ))
}

fn write_u32_len(out: &mut Vec<u8>, len: usize) -> Result<(), ClassicGroupCoordinatorError> {
    let len = u32::try_from(len).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation: "encode length",
        path: PathBuf::new(),
        message: "length overflow".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn write_u16_string(out: &mut Vec<u8>, value: &str) -> Result<(), ClassicGroupCoordinatorError> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation: "encode string",
        path: PathBuf::new(),
        message: "string length overflow".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_nullable_u16_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), ClassicGroupCoordinatorError> {
    if let Some(value) = value {
        let bytes = value.as_bytes();
        let len = i16::try_from(bytes.len()).map_err(|_| ClassicGroupCoordinatorError::Io {
            operation: "encode nullable string",
            path: PathBuf::new(),
            message: "string length overflow".to_string(),
        })?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
        return Ok(());
    }
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    Ok(())
}

fn write_u32_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ClassicGroupCoordinatorError> {
    let len = u32::try_from(value.len()).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation: "encode bytes",
        path: PathBuf::new(),
        message: "bytes length overflow".to_string(),
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn read_u8(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<u8, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(1) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated u8".to_string(),
        });
    }
    let value = input[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u32(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<u32, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(ClassicGroupCoordinatorError::Io {
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

fn read_u64(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<u64, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(8) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated u64".to_string(),
        });
    }
    let value = u64::from_be_bytes([
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

fn read_i32(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<i32, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(4) {
        return Err(ClassicGroupCoordinatorError::Io {
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

fn read_u16_string(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<String, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated string length".to_string(),
        });
    }
    let len = u16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    let len = usize::from(len);
    if input.len() < cursor.saturating_add(len) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated string bytes".to_string(),
        });
    }
    let bytes = &input[*cursor..*cursor + len];
    *cursor += len;
    String::from_utf8(bytes.to_vec()).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation,
        path: path.to_path_buf(),
        message: "invalid utf8".to_string(),
    })
}

fn read_nullable_u16_string(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<Option<String>, ClassicGroupCoordinatorError> {
    if input.len() < cursor.saturating_add(2) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated nullable string length".to_string(),
        });
    }
    let len = i16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
    *cursor += 2;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "invalid nullable string length".to_string(),
        });
    }
    let len = usize::try_from(len).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation,
        path: path.to_path_buf(),
        message: "nullable string length overflow".to_string(),
    })?;
    if input.len() < cursor.saturating_add(len) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated nullable string".to_string(),
        });
    }
    let bytes = &input[*cursor..*cursor + len];
    *cursor += len;
    String::from_utf8(bytes.to_vec())
        .map(Some)
        .map_err(|_| ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "invalid utf8".to_string(),
        })
}

fn read_u32_bytes(
    input: &[u8],
    cursor: &mut usize,
    path: &Path,
    operation: &'static str,
) -> Result<Vec<u8>, ClassicGroupCoordinatorError> {
    let len = read_u32(input, cursor, path, operation)?;
    let len = usize::try_from(len).map_err(|_| ClassicGroupCoordinatorError::Io {
        operation,
        path: path.to_path_buf(),
        message: "bytes length overflow".to_string(),
    })?;
    if input.len() < cursor.saturating_add(len) {
        return Err(ClassicGroupCoordinatorError::Io {
            operation,
            path: path.to_path_buf(),
            message: "truncated bytes".to_string(),
        });
    }
    let value = input[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
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
                "rafka-classic-group-{label}-{millis}-{}-{counter}",
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

    fn join_input(version: i16, group_id: &str, member_id: &str) -> JoinGroupInput {
        JoinGroupInput {
            api_version: version,
            group_id: group_id.to_string(),
            session_timeout_ms: 5_000,
            rebalance_timeout_ms: 5_000,
            member_id: member_id.to_string(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinProtocol {
                name: "consumer-range".to_string(),
                metadata: vec![1, 2, 3],
            }],
        }
    }

    fn register_member(
        coordinator: &mut ClassicGroupCoordinator,
        group_id: &str,
        now_ms: u64,
    ) -> (String, i32) {
        let assigned = match coordinator
            .join_group(join_input(4, group_id, ""), now_ms)
            .expect_err("member id required")
        {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };
        let joined = coordinator
            .join_group(join_input(4, group_id, &assigned), now_ms + 1)
            .expect("join with assigned id");
        (joined.member_id, joined.generation_id)
    }

    #[test]
    fn join_v4_requires_member_id_then_rejoin_succeeds() {
        let temp = TempDir::new("join-v4-member-id-required");
        let mut coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");

        let err = coordinator
            .join_group(join_input(4, "grp", ""), 10)
            .expect_err("member id required");
        let assigned = match err {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };
        assert!(!assigned.is_empty());

        let join = coordinator
            .join_group(join_input(4, "grp", &assigned), 11)
            .expect("join with assigned id");
        assert_eq!(join.generation_id, 1);
        assert_eq!(join.member_id, assigned);
        assert_eq!(join.protocol_name.as_deref(), Some("consumer-range"));
        assert_eq!(join.members.len(), 1);
    }

    #[test]
    fn sync_persists_assignments_across_restart() {
        let temp = TempDir::new("sync-persist");
        let mut coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");

        let first_join = coordinator
            .join_group(join_input(4, "grp", ""), 10)
            .expect_err("member id required");
        let member_id = match first_join {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };

        let joined = coordinator
            .join_group(join_input(4, "grp", &member_id), 20)
            .expect("join");
        assert_eq!(joined.generation_id, 1);

        let synced = coordinator
            .sync_group(SyncGroupInput {
                api_version: 5,
                group_id: "grp".to_string(),
                generation_id: 1,
                member_id: member_id.clone(),
                group_instance_id: None,
                protocol_type: Some("consumer".to_string()),
                protocol_name: Some("consumer-range".to_string()),
                assignments: vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: vec![9, 8, 7],
                }],
            })
            .expect("sync");
        assert_eq!(synced.assignment, vec![9, 8, 7]);

        drop(coordinator);

        let mut reopened =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("reopen");
        let synced_after_restart = reopened
            .sync_group(SyncGroupInput {
                api_version: 5,
                group_id: "grp".to_string(),
                generation_id: 1,
                member_id,
                group_instance_id: None,
                protocol_type: Some("consumer".to_string()),
                protocol_name: Some("consumer-range".to_string()),
                assignments: Vec::new(),
            })
            .expect("sync after restart");
        assert_eq!(synced_after_restart.assignment, vec![9, 8, 7]);
    }

    #[test]
    fn heartbeat_returns_rebalance_during_member_churn() {
        let temp = TempDir::new("heartbeat-rebalance");
        let mut coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");

        let leader = match coordinator
            .join_group(join_input(4, "grp", ""), 10)
            .expect_err("member id required")
        {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };
        coordinator
            .join_group(join_input(4, "grp", &leader), 11)
            .expect("leader join");

        let follower = match coordinator
            .join_group(join_input(4, "grp", ""), 20)
            .expect_err("member id required for follower")
        {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };
        coordinator
            .join_group(join_input(4, "grp", &follower), 21)
            .expect("follower join");

        let err = coordinator
            .heartbeat(
                HeartbeatInput {
                    group_id: "grp".to_string(),
                    generation_id: 2,
                    member_id: leader,
                    group_instance_id: None,
                },
                30,
            )
            .expect_err("rebalance should be in progress");
        assert_eq!(err, ClassicGroupCoordinatorError::RebalanceInProgress);
    }

    #[test]
    fn leave_group_v3_reports_member_level_errors() {
        let temp = TempDir::new("leave-v3-member-errors");
        let mut coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");

        let member = match coordinator
            .join_group(
                JoinGroupInput {
                    group_instance_id: Some("instance-a".to_string()),
                    ..join_input(4, "grp", "")
                },
                10,
            )
            .expect_err("member id required")
        {
            ClassicGroupCoordinatorError::MemberIdRequired(member_id) => member_id,
            other => panic!("unexpected error: {other:?}"),
        };
        coordinator
            .join_group(
                JoinGroupInput {
                    member_id: member.clone(),
                    group_instance_id: Some("instance-a".to_string()),
                    ..join_input(4, "grp", "")
                },
                11,
            )
            .expect("join");

        let outcomes = coordinator
            .leave_group_members(
                "grp",
                &[
                    LeaveGroupMemberInput {
                        member_id: "other-member".to_string(),
                        group_instance_id: Some("instance-a".to_string()),
                    },
                    LeaveGroupMemberInput {
                        member_id: "missing-member".to_string(),
                        group_instance_id: None,
                    },
                ],
            )
            .expect("leave");
        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            outcomes[0].error,
            Some(ClassicGroupCoordinatorError::FencedInstanceId)
        );
        assert_eq!(
            outcomes[1].error,
            Some(ClassicGroupCoordinatorError::UnknownMemberId(
                "missing-member".to_string()
            ))
        );
    }

    #[test]
    #[ignore = "stress: million-scale parallel churn"]
    fn stress_parallel_consumer_group_churn_million_events() {
        const WORKERS: usize = 8;
        const EVENTS_PER_WORKER: usize = 125_000;
        const CHURN_INTERVAL: usize = 64;

        let temp = TempDir::new("stress-parallel-churn");
        let coordinator =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("open");
        let shared = Arc::new(Mutex::new(coordinator));
        let total_events = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::with_capacity(WORKERS);
        for worker in 0..WORKERS {
            let shared = Arc::clone(&shared);
            let total_events = Arc::clone(&total_events);
            handles.push(thread::spawn(move || {
                let group_id = format!("stress-grp-{worker}");
                let mut now_ms =
                    1_000_000_u64 + u64::try_from(worker).expect("worker fits") * 1_000_000;

                let (mut member_id, mut generation_id) = {
                    let mut coordinator = shared.lock().expect("lock");
                    register_member(&mut coordinator, &group_id, now_ms)
                };
                total_events.fetch_add(2, Ordering::Relaxed);

                for index in 0..EVENTS_PER_WORKER {
                    now_ms = now_ms.saturating_add(1);
                    if index % CHURN_INTERVAL == 0 {
                        let mut coordinator = shared.lock().expect("lock");
                        coordinator
                            .leave_group_legacy(&group_id, &member_id)
                            .expect("leave");
                        let (next_member_id, next_generation_id) =
                            register_member(&mut coordinator, &group_id, now_ms);
                        member_id = next_member_id;
                        generation_id = next_generation_id;
                        total_events.fetch_add(3, Ordering::Relaxed);
                    } else {
                        let mut coordinator = shared.lock().expect("lock");
                        coordinator
                            .heartbeat(
                                HeartbeatInput {
                                    group_id: group_id.clone(),
                                    generation_id,
                                    member_id: member_id.clone(),
                                    group_instance_id: None,
                                },
                                now_ms,
                            )
                            .expect("heartbeat");
                        total_events.fetch_add(1, Ordering::Relaxed);
                    }
                }

                (group_id, member_id, generation_id, now_ms.saturating_add(5))
            }));
        }

        let mut final_state = Vec::with_capacity(WORKERS);
        for handle in handles {
            final_state.push(handle.join().expect("worker"));
        }
        assert!(
            total_events.load(Ordering::Relaxed) >= 1_000_000,
            "expected at least one million membership events"
        );

        let coordinator = Arc::try_unwrap(shared)
            .expect("single strong reference remains")
            .into_inner()
            .expect("unlock");
        drop(coordinator);

        let mut reopened =
            ClassicGroupCoordinator::open(temp.path(), ClassicGroupStoreConfig::default())
                .expect("reopen");

        for (group_id, member_id, generation_id, now_ms) in final_state {
            reopened
                .heartbeat(
                    HeartbeatInput {
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id: None,
                    },
                    now_ms,
                )
                .expect("heartbeat after restart");
        }
    }
}
