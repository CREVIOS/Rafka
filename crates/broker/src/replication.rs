#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use rafka_storage::Record;

use crate::{PartitionedBroker, PartitionedBrokerError, PersistentLogConfig};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationConfig {
    pub max_lag_records: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedPartitionState {
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub leader_next_offset: i64,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    NodeNotFound(i32),
    NodeOffline(i32),
    PartitionNotFound {
        topic: String,
        partition: i32,
    },
    PartitionAlreadyExists {
        topic: String,
        partition: i32,
    },
    InvalidReplicationPlan {
        reason: String,
    },
    FailoverTargetNotReplica {
        node_id: i32,
    },
    FailoverTargetNotInIsr {
        node_id: i32,
    },
    ReplicaOffsetMismatch {
        node_id: i32,
        expected: i64,
        actual: i64,
    },
    Storage(PartitionedBrokerError),
}

impl From<PartitionedBrokerError> for ReplicationError {
    fn from(value: PartitionedBrokerError) -> Self {
        Self::Storage(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic: String,
    partition: i32,
}

impl PartitionKey {
    fn new(topic: &str, partition: i32) -> Result<Self, ReplicationError> {
        if topic.is_empty() {
            return Err(ReplicationError::InvalidReplicationPlan {
                reason: "topic cannot be empty".to_string(),
            });
        }
        if partition < 0 {
            return Err(ReplicationError::InvalidReplicationPlan {
                reason: "partition cannot be negative".to_string(),
            });
        }
        if topic.contains('/') || topic.contains('\\') {
            return Err(ReplicationError::InvalidReplicationPlan {
                reason: "topic cannot contain path separators".to_string(),
            });
        }
        Ok(Self {
            topic: topic.to_string(),
            partition,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FollowerProgress {
    next_fetch_offset: i64,
    last_caught_up_epoch: i32,
}

#[derive(Debug, Clone)]
struct ReplicatedPartition {
    leader_id: i32,
    leader_epoch: i32,
    leader_next_offset: i64,
    replicas: Vec<i32>,
    followers: HashMap<i32, FollowerProgress>,
    isr: BTreeSet<i32>,
}

#[derive(Debug)]
struct ReplicaNode {
    broker: PartitionedBroker,
    online: bool,
}

#[derive(Debug)]
pub struct ReplicationCluster {
    config: ReplicationConfig,
    nodes: HashMap<i32, ReplicaNode>,
    partitions: HashMap<PartitionKey, ReplicatedPartition>,
}

impl ReplicationCluster {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            nodes: HashMap::new(),
            partitions: HashMap::new(),
        }
    }

    pub fn add_node<P: AsRef<Path>>(
        &mut self,
        node_id: i32,
        data_dir: P,
        log_config: PersistentLogConfig,
    ) -> Result<(), ReplicationError> {
        let broker = PartitionedBroker::open(data_dir, log_config)?;
        self.nodes.insert(
            node_id,
            ReplicaNode {
                broker,
                online: true,
            },
        );
        Ok(())
    }

    pub fn set_node_online(&mut self, node_id: i32, online: bool) -> Result<(), ReplicationError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))?;
        node.online = online;
        let keys: Vec<PartitionKey> = self.partitions.keys().cloned().collect();
        for key in keys {
            self.recompute_isr(&key)?;
        }
        Ok(())
    }

    pub fn create_partition(
        &mut self,
        topic: &str,
        partition: i32,
        leader_id: i32,
        replicas: Vec<i32>,
    ) -> Result<(), ReplicationError> {
        let key = PartitionKey::new(topic, partition)?;
        if self.partitions.contains_key(&key) {
            return Err(ReplicationError::PartitionAlreadyExists {
                topic: topic.to_string(),
                partition,
            });
        }
        if replicas.is_empty() {
            return Err(ReplicationError::InvalidReplicationPlan {
                reason: "replicas cannot be empty".to_string(),
            });
        }
        if !replicas.contains(&leader_id) {
            return Err(ReplicationError::InvalidReplicationPlan {
                reason: "leader must be part of replicas".to_string(),
            });
        }

        let mut dedupe = BTreeSet::new();
        for replica in &replicas {
            if !dedupe.insert(*replica) {
                return Err(ReplicationError::InvalidReplicationPlan {
                    reason: "replicas cannot contain duplicates".to_string(),
                });
            }
            if !self.nodes.contains_key(replica) {
                return Err(ReplicationError::NodeNotFound(*replica));
            }
        }

        let leader_next_offset = self.node_log_next_offset(leader_id, topic, partition)?;
        let mut followers = HashMap::new();
        for replica in replicas
            .iter()
            .copied()
            .filter(|replica| *replica != leader_id)
        {
            let next_fetch_offset = self.node_log_next_offset(replica, topic, partition)?;
            let last_caught_up_epoch = if next_fetch_offset == leader_next_offset {
                0
            } else {
                -1
            };
            followers.insert(
                replica,
                FollowerProgress {
                    next_fetch_offset,
                    last_caught_up_epoch,
                },
            );
        }

        self.partitions.insert(
            key.clone(),
            ReplicatedPartition {
                leader_id,
                leader_epoch: 0,
                leader_next_offset,
                replicas,
                followers,
                isr: BTreeSet::new(),
            },
        );
        self.recompute_isr(&key)?;
        Ok(())
    }

    pub fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<i64, ReplicationError> {
        let partition_key = PartitionKey::new(topic, partition)?;
        let leader_id = self
            .partitions
            .get(&partition_key)
            .ok_or(ReplicationError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?
            .leader_id;
        self.ensure_node_online(leader_id)?;

        let offset = self
            .nodes
            .get_mut(&leader_id)
            .ok_or(ReplicationError::NodeNotFound(leader_id))?
            .broker
            .produce_to(topic, partition, key, value, timestamp_ms)?;
        if let Some(state) = self.partitions.get_mut(&partition_key) {
            state.leader_next_offset = offset.saturating_add(1);
        }
        self.recompute_isr(&partition_key)?;
        Ok(offset)
    }

    pub fn tick_replication(
        &mut self,
        topic: &str,
        partition: i32,
        max_records_per_follower: usize,
    ) -> Result<usize, ReplicationError> {
        let key = PartitionKey::new(topic, partition)?;
        let (leader_id, leader_epoch, follower_ids) = {
            let partition_state =
                self.partitions
                    .get(&key)
                    .ok_or(ReplicationError::PartitionNotFound {
                        topic: topic.to_string(),
                        partition,
                    })?;
            (
                partition_state.leader_id,
                partition_state.leader_epoch,
                partition_state
                    .replicas
                    .iter()
                    .copied()
                    .filter(|replica| *replica != partition_state.leader_id)
                    .collect::<Vec<_>>(),
            )
        };

        self.ensure_node_online(leader_id)?;
        let leader_next_offset = self.node_log_next_offset(leader_id, topic, partition)?;
        if let Some(state) = self.partitions.get_mut(&key) {
            state.leader_next_offset = leader_next_offset;
        }

        let mut replicated = 0_usize;
        for follower_id in follower_ids {
            if !self
                .nodes
                .get(&follower_id)
                .ok_or(ReplicationError::NodeNotFound(follower_id))?
                .online
            {
                continue;
            }

            let next_fetch_offset = self
                .partitions
                .get(&key)
                .and_then(|state| state.followers.get(&follower_id))
                .map_or(0, |state| state.next_fetch_offset);

            let leader_records = {
                let leader_node = self
                    .nodes
                    .get_mut(&leader_id)
                    .ok_or(ReplicationError::NodeNotFound(leader_id))?;
                match leader_node.broker.fetch_from_partition(
                    topic,
                    partition,
                    next_fetch_offset,
                    max_records_per_follower,
                ) {
                    Ok(records) => records,
                    Err(PartitionedBrokerError::UnknownPartition { .. }) => Vec::new(),
                    Err(err) => return Err(err.into()),
                }
            };

            if leader_records.is_empty() {
                continue;
            }

            let mut copied = 0_usize;
            {
                let follower_node = self
                    .nodes
                    .get_mut(&follower_id)
                    .ok_or(ReplicationError::NodeNotFound(follower_id))?;
                for record in leader_records {
                    let expected =
                        next_fetch_offset.saturating_add(i64::try_from(copied).unwrap_or(i64::MAX));
                    let actual = follower_node.broker.produce_to(
                        topic,
                        partition,
                        record.key,
                        record.value,
                        record.timestamp_ms,
                    )?;
                    if actual != expected {
                        return Err(ReplicationError::ReplicaOffsetMismatch {
                            node_id: follower_id,
                            expected,
                            actual,
                        });
                    }
                    copied = copied.saturating_add(1);
                }
            }

            replicated = replicated.saturating_add(copied);
            if copied > 0 {
                let copied_i64 = i64::try_from(copied).unwrap_or(i64::MAX);
                if let Some(state) = self.partitions.get_mut(&key) {
                    if let Some(progress) = state.followers.get_mut(&follower_id) {
                        progress.next_fetch_offset =
                            progress.next_fetch_offset.saturating_add(copied_i64);
                        if progress.next_fetch_offset >= state.leader_next_offset {
                            progress.last_caught_up_epoch = leader_epoch;
                        }
                    }
                }
            }
        }

        self.recompute_isr(&key)?;
        Ok(replicated)
    }

    pub fn tick_all(&mut self, max_records_per_follower: usize) -> Result<usize, ReplicationError> {
        let keys: Vec<PartitionKey> = self.partitions.keys().cloned().collect();
        let mut total = 0_usize;
        for key in keys {
            total = total.saturating_add(self.tick_replication(
                &key.topic,
                key.partition,
                max_records_per_follower,
            )?);
        }
        Ok(total)
    }

    pub fn failover(
        &mut self,
        topic: &str,
        partition: i32,
        new_leader_id: i32,
    ) -> Result<(), ReplicationError> {
        let key = PartitionKey::new(topic, partition)?;
        let (replicas, new_leader_epoch, leader_isr_contains_target) = {
            let state = self
                .partitions
                .get(&key)
                .ok_or(ReplicationError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;
            (
                state.replicas.clone(),
                state.leader_epoch.saturating_add(1),
                state.isr.contains(&new_leader_id),
            )
        };

        if !replicas.contains(&new_leader_id) {
            return Err(ReplicationError::FailoverTargetNotReplica {
                node_id: new_leader_id,
            });
        }
        if !leader_isr_contains_target {
            return Err(ReplicationError::FailoverTargetNotInIsr {
                node_id: new_leader_id,
            });
        }
        self.ensure_node_online(new_leader_id)?;
        let new_leader_next_offset = self.node_log_next_offset(new_leader_id, topic, partition)?;

        let mut followers = HashMap::new();
        for replica in replicas
            .iter()
            .copied()
            .filter(|replica| *replica != new_leader_id)
        {
            let next_fetch_offset = self.node_log_next_offset(replica, topic, partition)?;
            let last_caught_up_epoch = if next_fetch_offset == new_leader_next_offset {
                new_leader_epoch
            } else {
                -1
            };
            followers.insert(
                replica,
                FollowerProgress {
                    next_fetch_offset,
                    last_caught_up_epoch,
                },
            );
        }

        if let Some(state) = self.partitions.get_mut(&key) {
            state.leader_id = new_leader_id;
            state.leader_epoch = new_leader_epoch;
            state.leader_next_offset = new_leader_next_offset;
            state.followers = followers;
        }

        self.recompute_isr(&key)?;
        Ok(())
    }

    pub fn partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<ReplicatedPartitionState, ReplicationError> {
        let key = PartitionKey::new(topic, partition)?;
        let state = self
            .partitions
            .get(&key)
            .ok_or(ReplicationError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?;
        Ok(ReplicatedPartitionState {
            leader_id: state.leader_id,
            leader_epoch: state.leader_epoch,
            leader_next_offset: state.leader_next_offset,
            replicas: state.replicas.clone(),
            isr: state.isr.iter().copied().collect(),
        })
    }

    pub fn node_next_offset(
        &self,
        node_id: i32,
        topic: &str,
        partition: i32,
    ) -> Result<i64, ReplicationError> {
        self.node_log_next_offset(node_id, topic, partition)
    }

    pub fn partition_keys(&self) -> Vec<(String, i32)> {
        let mut keys: Vec<(String, i32)> = self
            .partitions
            .keys()
            .map(|key| (key.topic.clone(), key.partition))
            .collect();
        keys.sort_unstable();
        keys
    }

    pub fn fetch_from_node(
        &mut self,
        node_id: i32,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>, ReplicationError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))?;
        node.broker
            .fetch_from_partition(topic, partition, offset, max_records)
            .map_err(Into::into)
    }

    fn recompute_isr(&mut self, key: &PartitionKey) -> Result<(), ReplicationError> {
        let (leader_id, leader_epoch, leader_next_offset, replicas, followers) = {
            let state = self
                .partitions
                .get(key)
                .ok_or(ReplicationError::PartitionNotFound {
                    topic: key.topic.clone(),
                    partition: key.partition,
                })?;
            (
                state.leader_id,
                state.leader_epoch,
                state.leader_next_offset,
                state.replicas.clone(),
                state.followers.clone(),
            )
        };

        let mut isr = BTreeSet::new();
        if self
            .nodes
            .get(&leader_id)
            .ok_or(ReplicationError::NodeNotFound(leader_id))?
            .online
        {
            isr.insert(leader_id);
        }

        for replica in replicas
            .iter()
            .copied()
            .filter(|replica| *replica != leader_id)
        {
            let Some(node) = self.nodes.get(&replica) else {
                return Err(ReplicationError::NodeNotFound(replica));
            };
            if !node.online {
                continue;
            }
            let Some(progress) = followers.get(&replica).copied() else {
                continue;
            };

            let lag = leader_next_offset.saturating_sub(progress.next_fetch_offset);
            if progress.last_caught_up_epoch == leader_epoch && lag <= self.config.max_lag_records {
                isr.insert(replica);
            }
        }

        if let Some(state) = self.partitions.get_mut(key) {
            state.isr = isr;
        }
        Ok(())
    }

    fn node_log_next_offset(
        &self,
        node_id: i32,
        topic: &str,
        partition: i32,
    ) -> Result<i64, ReplicationError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))?;
        match node.broker.segment_snapshots(topic, partition) {
            Ok(snapshots) => Ok(snapshots.last().map_or(0, |snapshot| snapshot.next_offset)),
            Err(PartitionedBrokerError::UnknownPartition { .. }) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    fn ensure_node_online(&self, node_id: i32) -> Result<(), ReplicationError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))?;
        if !node.online {
            return Err(ReplicationError::NodeOffline(node_id));
        }
        Ok(())
    }
}
