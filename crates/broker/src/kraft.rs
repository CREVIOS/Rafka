#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KraftError {
    UnknownController(i32),
    ControllerOffline(i32),
    NotVoter(i32),
    QuorumUnavailable {
        online_voters: usize,
        total_voters: usize,
    },
    InvalidRecord {
        reason: String,
    },
    SnapshotIo {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    SnapshotFormat {
        line: usize,
        message: String,
    },
}

impl KraftError {
    fn snapshot_io(operation: &'static str, path: &Path, err: std::io::Error) -> Self {
        Self::SnapshotIo {
            operation,
            path: path.to_path_buf(),
            message: err.to_string(),
        }
    }

    fn snapshot_format(line: usize, message: impl Into<String>) -> Self {
        Self::SnapshotFormat {
            line,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicMetadata {
    pub partitions: BTreeMap<i32, PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataImage {
    pub brokers: BTreeMap<i32, BrokerMetadata>,
    pub topics: BTreeMap<String, TopicMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    RegisterBroker {
        broker_id: i32,
        rack: Option<String>,
    },
    UnregisterBroker {
        broker_id: i32,
    },
    CreateTopic {
        topic: String,
    },
    DeleteTopic {
        topic: String,
    },
    UpsertPartition {
        topic: String,
        partition: i32,
        leader_id: i32,
        leader_epoch: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ControllerState {
    online: bool,
    applied_offset: Option<u64>,
}

#[derive(Debug)]
pub struct KraftMetadataQuorum {
    voters: BTreeSet<i32>,
    controllers: BTreeMap<i32, ControllerState>,
    leader_id: i32,
    leader_epoch: i32,
    committed_offset: Option<u64>,
    next_offset: u64,
    image: MetadataImage,
}

impl KraftMetadataQuorum {
    pub fn new(voters: Vec<i32>, leader_id: i32) -> Result<Self, KraftError> {
        if voters.is_empty() {
            return Err(KraftError::InvalidRecord {
                reason: "voters cannot be empty".to_string(),
            });
        }

        let mut voter_set = BTreeSet::new();
        for voter in voters {
            if !voter_set.insert(voter) {
                return Err(KraftError::InvalidRecord {
                    reason: "voters cannot contain duplicates".to_string(),
                });
            }
        }
        if !voter_set.contains(&leader_id) {
            return Err(KraftError::NotVoter(leader_id));
        }

        let mut controllers = BTreeMap::new();
        for voter in &voter_set {
            controllers.insert(
                *voter,
                ControllerState {
                    online: true,
                    applied_offset: None,
                },
            );
        }

        Ok(Self {
            voters: voter_set,
            controllers,
            leader_id,
            leader_epoch: 0,
            committed_offset: None,
            next_offset: 0,
            image: MetadataImage::default(),
        })
    }

    pub fn leader_id(&self) -> i32 {
        self.leader_id
    }

    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    pub fn committed_offset(&self) -> Option<u64> {
        self.committed_offset
    }

    pub fn image(&self) -> &MetadataImage {
        &self.image
    }

    pub fn set_controller_online(
        &mut self,
        controller_id: i32,
        online: bool,
    ) -> Result<(), KraftError> {
        let Some(state) = self.controllers.get_mut(&controller_id) else {
            return Err(KraftError::UnknownController(controller_id));
        };
        state.online = online;
        if online {
            state.applied_offset = self.committed_offset;
        }
        Ok(())
    }

    pub fn elect_leader(&mut self, new_leader_id: i32) -> Result<(), KraftError> {
        if !self.voters.contains(&new_leader_id) {
            return Err(KraftError::NotVoter(new_leader_id));
        }
        let Some(state) = self.controllers.get(&new_leader_id) else {
            return Err(KraftError::UnknownController(new_leader_id));
        };
        if !state.online {
            return Err(KraftError::ControllerOffline(new_leader_id));
        }

        self.leader_id = new_leader_id;
        self.leader_epoch = self.leader_epoch.saturating_add(1);
        Ok(())
    }

    pub fn append_record(&mut self, record: MetadataRecord) -> Result<u64, KraftError> {
        self.validate_record(&record)?;
        self.ensure_leader_online()?;
        self.ensure_majority_online()?;

        let offset = self.next_offset;
        self.next_offset = self.next_offset.saturating_add(1);
        self.apply_record(&record);
        self.committed_offset = Some(offset);

        for (controller_id, state) in &mut self.controllers {
            if self.voters.contains(controller_id) && state.online {
                state.applied_offset = Some(offset);
            }
        }

        Ok(offset)
    }

    pub fn ensure_writable(&self) -> Result<(), KraftError> {
        self.ensure_leader_online()?;
        self.ensure_majority_online()
    }

    pub fn write_snapshot<P: AsRef<Path>>(&self, path: P) -> Result<(), KraftError> {
        let path = path.as_ref();
        let mut out = String::new();
        out.push_str("format_version 1\n");
        out.push_str(&format!("leader_id {}\n", self.leader_id));
        out.push_str(&format!("leader_epoch {}\n", self.leader_epoch));
        let committed = self
            .committed_offset
            .map_or_else(|| "-1".to_string(), |offset| offset.to_string());
        out.push_str(&format!("committed_offset {committed}\n"));
        out.push_str(&format!("next_offset {}\n", self.next_offset));
        out.push_str(&format!(
            "voters {}\n",
            self.voters
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));

        for (broker_id, broker) in &self.image.brokers {
            let rack = broker.rack.as_deref().unwrap_or("-");
            out.push_str(&format!("broker {broker_id} {rack}\n"));
        }

        for (topic, topic_metadata) in &self.image.topics {
            out.push_str(&format!("topic {topic}\n"));
            for (partition, partition_metadata) in &topic_metadata.partitions {
                let replicas = partition_metadata
                    .replicas
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                let isr = partition_metadata
                    .isr
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                out.push_str(&format!(
                    "partition {topic} {partition} {} {} {replicas} {isr}\n",
                    partition_metadata.leader_id, partition_metadata.leader_epoch
                ));
            }
        }

        fs::write(path, out).map_err(|err| KraftError::snapshot_io("write", path, err))
    }

    pub fn load_snapshot<P: AsRef<Path>>(&mut self, path: P) -> Result<(), KraftError> {
        let path = path.as_ref();
        let content =
            fs::read_to_string(path).map_err(|err| KraftError::snapshot_io("read", path, err))?;

        let mut leader_id = None;
        let mut leader_epoch = None;
        let mut committed_offset = None;
        let mut next_offset = None;
        let mut voters = None;
        let mut image = MetadataImage::default();

        for (index, raw_line) in content.lines().enumerate() {
            let line_number = index.saturating_add(1);
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }

            let parts = line.split_whitespace().collect::<Vec<_>>();
            match parts.as_slice() {
                ["format_version", "1"] => {}
                ["leader_id", id] => {
                    let parsed = id.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid leader_id")
                    })?;
                    leader_id = Some(parsed);
                }
                ["leader_epoch", epoch] => {
                    let parsed = epoch.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid leader_epoch")
                    })?;
                    leader_epoch = Some(parsed);
                }
                ["committed_offset", "-1"] => {
                    committed_offset = None;
                }
                ["committed_offset", offset] => {
                    let parsed = offset.parse::<u64>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid committed_offset")
                    })?;
                    committed_offset = Some(parsed);
                }
                ["next_offset", offset] => {
                    let parsed = offset.parse::<u64>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid next_offset")
                    })?;
                    next_offset = Some(parsed);
                }
                ["voters", raw_voters] => {
                    let mut voter_set = BTreeSet::new();
                    if !raw_voters.is_empty() {
                        for raw_voter in raw_voters.split(',') {
                            let voter = raw_voter.parse::<i32>().map_err(|_| {
                                KraftError::snapshot_format(line_number, "invalid voter id")
                            })?;
                            if !voter_set.insert(voter) {
                                return Err(KraftError::snapshot_format(
                                    line_number,
                                    "duplicate voter id",
                                ));
                            }
                        }
                    }
                    voters = Some(voter_set);
                }
                ["broker", broker_id, rack] => {
                    let broker_id = broker_id.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid broker id")
                    })?;
                    let rack = if *rack == "-" {
                        None
                    } else {
                        Some((*rack).to_string())
                    };
                    image.brokers.insert(broker_id, BrokerMetadata { rack });
                }
                ["topic", topic] => {
                    image.topics.entry((*topic).to_string()).or_default();
                }
                ["partition", topic, partition, leader, leader_epoch, replicas, isr] => {
                    let partition = partition.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid partition id")
                    })?;
                    let leader = leader.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid partition leader")
                    })?;
                    let leader_epoch = leader_epoch.parse::<i32>().map_err(|_| {
                        KraftError::snapshot_format(line_number, "invalid partition leader epoch")
                    })?;
                    let replicas = parse_csv_i32(replicas, line_number, "replicas")?;
                    let isr = parse_csv_i32(isr, line_number, "isr")?;

                    let record = MetadataRecord::UpsertPartition {
                        topic: (*topic).to_string(),
                        partition,
                        leader_id: leader,
                        leader_epoch,
                        replicas: replicas.clone(),
                        isr: isr.clone(),
                    };
                    self.validate_record(&record)?;

                    image
                        .topics
                        .entry((*topic).to_string())
                        .or_default()
                        .partitions
                        .insert(
                            partition,
                            PartitionMetadata {
                                leader_id: leader,
                                leader_epoch,
                                replicas,
                                isr,
                            },
                        );
                }
                _ => {
                    return Err(KraftError::snapshot_format(
                        line_number,
                        "unrecognized snapshot line",
                    ));
                }
            }
        }

        let snapshot_voters =
            voters.ok_or_else(|| KraftError::snapshot_format(0, "missing voters"))?;
        if snapshot_voters != self.voters {
            return Err(KraftError::snapshot_format(
                0,
                "snapshot voters do not match quorum voters",
            ));
        }

        let snapshot_leader_id =
            leader_id.ok_or_else(|| KraftError::snapshot_format(0, "missing leader_id"))?;
        if !self.voters.contains(&snapshot_leader_id) {
            return Err(KraftError::snapshot_format(
                0,
                "snapshot leader is not in voter set",
            ));
        }

        let snapshot_leader_epoch =
            leader_epoch.ok_or_else(|| KraftError::snapshot_format(0, "missing leader_epoch"))?;
        let snapshot_next_offset =
            next_offset.ok_or_else(|| KraftError::snapshot_format(0, "missing next_offset"))?;
        if let Some(committed) = committed_offset {
            if committed.saturating_add(1) > snapshot_next_offset {
                return Err(KraftError::snapshot_format(
                    0,
                    "next_offset must be >= committed_offset + 1",
                ));
            }
        }

        self.leader_id = snapshot_leader_id;
        self.leader_epoch = snapshot_leader_epoch;
        self.committed_offset = committed_offset;
        self.next_offset = snapshot_next_offset;
        self.image = image;

        for state in self.controllers.values_mut() {
            state.applied_offset = if state.online {
                self.committed_offset
            } else {
                None
            };
        }

        Ok(())
    }

    fn ensure_majority_online(&self) -> Result<(), KraftError> {
        let online_voters = self
            .controllers
            .iter()
            .filter(|(id, state)| self.voters.contains(*id) && state.online)
            .count();
        let majority = self.voters.len() / 2 + 1;
        if online_voters < majority {
            return Err(KraftError::QuorumUnavailable {
                online_voters,
                total_voters: self.voters.len(),
            });
        }
        Ok(())
    }

    fn ensure_leader_online(&self) -> Result<(), KraftError> {
        let Some(state) = self.controllers.get(&self.leader_id) else {
            return Err(KraftError::UnknownController(self.leader_id));
        };
        if !state.online {
            return Err(KraftError::ControllerOffline(self.leader_id));
        }
        Ok(())
    }

    fn validate_record(&self, record: &MetadataRecord) -> Result<(), KraftError> {
        match record {
            MetadataRecord::RegisterBroker { broker_id, .. }
            | MetadataRecord::UnregisterBroker { broker_id } => {
                if *broker_id < 0 {
                    return Err(KraftError::InvalidRecord {
                        reason: "broker_id cannot be negative".to_string(),
                    });
                }
            }
            MetadataRecord::CreateTopic { topic } | MetadataRecord::DeleteTopic { topic } => {
                if topic.is_empty() {
                    return Err(KraftError::InvalidRecord {
                        reason: "topic cannot be empty".to_string(),
                    });
                }
                if topic.contains(char::is_whitespace) {
                    return Err(KraftError::InvalidRecord {
                        reason: "topic cannot contain whitespace".to_string(),
                    });
                }
            }
            MetadataRecord::UpsertPartition {
                topic,
                partition,
                leader_id,
                replicas,
                isr,
                ..
            } => {
                if topic.is_empty() {
                    return Err(KraftError::InvalidRecord {
                        reason: "topic cannot be empty".to_string(),
                    });
                }
                if *partition < 0 {
                    return Err(KraftError::InvalidRecord {
                        reason: "partition cannot be negative".to_string(),
                    });
                }
                if replicas.is_empty() {
                    return Err(KraftError::InvalidRecord {
                        reason: "replicas cannot be empty".to_string(),
                    });
                }
                let replica_set: BTreeSet<i32> = replicas.iter().copied().collect();
                if replica_set.len() != replicas.len() {
                    return Err(KraftError::InvalidRecord {
                        reason: "replicas cannot contain duplicates".to_string(),
                    });
                }
                if !replica_set.contains(leader_id) {
                    return Err(KraftError::InvalidRecord {
                        reason: "leader_id must be in replicas".to_string(),
                    });
                }
                if isr.is_empty() {
                    return Err(KraftError::InvalidRecord {
                        reason: "isr cannot be empty".to_string(),
                    });
                }
                let isr_set: BTreeSet<i32> = isr.iter().copied().collect();
                if isr_set.len() != isr.len() {
                    return Err(KraftError::InvalidRecord {
                        reason: "isr cannot contain duplicates".to_string(),
                    });
                }
                if !isr_set.contains(leader_id) {
                    return Err(KraftError::InvalidRecord {
                        reason: "leader_id must be in isr".to_string(),
                    });
                }
                if !isr_set.is_subset(&replica_set) {
                    return Err(KraftError::InvalidRecord {
                        reason: "isr must be a subset of replicas".to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn apply_record(&mut self, record: &MetadataRecord) {
        match record {
            MetadataRecord::RegisterBroker { broker_id, rack } => {
                self.image
                    .brokers
                    .insert(*broker_id, BrokerMetadata { rack: rack.clone() });
            }
            MetadataRecord::UnregisterBroker { broker_id } => {
                self.image.brokers.remove(broker_id);
            }
            MetadataRecord::CreateTopic { topic } => {
                self.image.topics.entry(topic.clone()).or_default();
            }
            MetadataRecord::DeleteTopic { topic } => {
                self.image.topics.remove(topic);
            }
            MetadataRecord::UpsertPartition {
                topic,
                partition,
                leader_id,
                leader_epoch,
                replicas,
                isr,
            } => {
                self.image
                    .topics
                    .entry(topic.clone())
                    .or_default()
                    .partitions
                    .insert(
                        *partition,
                        PartitionMetadata {
                            leader_id: *leader_id,
                            leader_epoch: *leader_epoch,
                            replicas: replicas.clone(),
                            isr: isr.clone(),
                        },
                    );
            }
        }
    }
}

fn parse_csv_i32(value: &str, line: usize, field: &str) -> Result<Vec<i32>, KraftError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| {
            part.parse::<i32>().map_err(|_| {
                KraftError::snapshot_format(line, format!("invalid {field} value: {part}"))
            })
        })
        .collect()
}
