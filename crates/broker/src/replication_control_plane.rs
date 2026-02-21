#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};

use rafka_storage::Record;

use crate::{
    KraftError, KraftMetadataQuorum, MetadataRecord, ReplicatedPartitionState, ReplicationCluster,
    ReplicationError,
};

#[derive(Debug)]
pub enum ReplicationControlPlaneError {
    Replication(ReplicationError),
    Kraft(KraftError),
}

impl From<ReplicationError> for ReplicationControlPlaneError {
    fn from(value: ReplicationError) -> Self {
        Self::Replication(value)
    }
}

impl From<KraftError> for ReplicationControlPlaneError {
    fn from(value: KraftError) -> Self {
        Self::Kraft(value)
    }
}

#[derive(Debug)]
pub struct ReplicationControlPlane {
    cluster: ReplicationCluster,
    quorum: KraftMetadataQuorum,
    published_partitions: HashMap<(String, i32), ReplicatedPartitionState>,
}

impl ReplicationControlPlane {
    pub fn new(cluster: ReplicationCluster, quorum: KraftMetadataQuorum) -> Self {
        Self {
            cluster,
            quorum,
            published_partitions: HashMap::new(),
        }
    }

    pub fn cluster(&self) -> &ReplicationCluster {
        &self.cluster
    }

    pub fn cluster_mut(&mut self) -> &mut ReplicationCluster {
        &mut self.cluster
    }

    pub fn quorum(&self) -> &KraftMetadataQuorum {
        &self.quorum
    }

    pub fn quorum_mut(&mut self) -> &mut KraftMetadataQuorum {
        &mut self.quorum
    }

    pub fn create_partition(
        &mut self,
        topic: &str,
        partition: i32,
        leader_id: i32,
        replicas: Vec<i32>,
    ) -> Result<(), ReplicationControlPlaneError> {
        self.quorum.ensure_writable()?;
        self.cluster
            .create_partition(topic, partition, leader_id, replicas)?;
        self.publish_topic_if_missing(topic)?;
        self.publish_partition_state(topic, partition)
    }

    pub fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ms: i64,
    ) -> Result<i64, ReplicationControlPlaneError> {
        Ok(self
            .cluster
            .produce(topic, partition, key, value, timestamp_ms)?)
    }

    pub fn tick_replication(
        &mut self,
        topic: &str,
        partition: i32,
        max_records_per_follower: usize,
    ) -> Result<usize, ReplicationControlPlaneError> {
        let replicated =
            self.cluster
                .tick_replication(topic, partition, max_records_per_follower)?;
        self.publish_partition_state_if_changed(topic, partition)?;
        Ok(replicated)
    }

    pub fn tick_all(
        &mut self,
        max_records_per_follower: usize,
    ) -> Result<usize, ReplicationControlPlaneError> {
        let replicated = self.cluster.tick_all(max_records_per_follower)?;
        let _ = self.publish_all_changed_partition_states()?;
        Ok(replicated)
    }

    pub fn set_node_online(
        &mut self,
        node_id: i32,
        online: bool,
    ) -> Result<(), ReplicationControlPlaneError> {
        self.cluster.set_node_online(node_id, online)?;
        let _ = self.publish_all_changed_partition_states()?;
        Ok(())
    }

    pub fn failover(
        &mut self,
        topic: &str,
        partition: i32,
        new_leader_id: i32,
    ) -> Result<(), ReplicationControlPlaneError> {
        self.quorum.ensure_writable()?;
        self.cluster.failover(topic, partition, new_leader_id)?;
        self.publish_partition_state(topic, partition)
    }

    pub fn partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<ReplicatedPartitionState, ReplicationControlPlaneError> {
        Ok(self.cluster.partition_state(topic, partition)?)
    }

    pub fn node_next_offset(
        &self,
        node_id: i32,
        topic: &str,
        partition: i32,
    ) -> Result<i64, ReplicationControlPlaneError> {
        Ok(self.cluster.node_next_offset(node_id, topic, partition)?)
    }

    pub fn fetch_from_node(
        &mut self,
        node_id: i32,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>, ReplicationControlPlaneError> {
        Ok(self
            .cluster
            .fetch_from_node(node_id, topic, partition, offset, max_records)?)
    }

    pub fn reconcile_metadata(&mut self) -> Result<usize, ReplicationControlPlaneError> {
        self.publish_all_changed_partition_states()
    }

    fn publish_topic_if_missing(
        &mut self,
        topic: &str,
    ) -> Result<(), ReplicationControlPlaneError> {
        if self.quorum.image().topics.contains_key(topic) {
            return Ok(());
        }
        self.quorum.ensure_writable()?;
        let _ = self.quorum.append_record(MetadataRecord::CreateTopic {
            topic: topic.to_string(),
        })?;
        Ok(())
    }

    fn publish_partition_state(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<(), ReplicationControlPlaneError> {
        let state = self.cluster.partition_state(topic, partition)?;
        self.publish_partition_state_inner(topic.to_string(), partition, state)
    }

    fn publish_partition_state_if_changed(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<(), ReplicationControlPlaneError> {
        let state = self.cluster.partition_state(topic, partition)?;
        let key = (topic.to_string(), partition);
        if self.published_partitions.get(&key) == Some(&state) {
            return Ok(());
        }
        self.publish_partition_state_inner(topic.to_string(), partition, state)
    }

    fn publish_all_changed_partition_states(
        &mut self,
    ) -> Result<usize, ReplicationControlPlaneError> {
        let mut topics_to_create = BTreeSet::new();
        let mut changed = Vec::new();
        for (topic, partition) in self.cluster.partition_keys() {
            let state = self.cluster.partition_state(&topic, partition)?;
            if self.published_partitions.get(&(topic.clone(), partition)) != Some(&state) {
                changed.push((topic.clone(), partition, state));
            }
            if !self.quorum.image().topics.contains_key(&topic) {
                topics_to_create.insert(topic);
            }
        }

        if changed.is_empty() && topics_to_create.is_empty() {
            return Ok(0);
        }

        self.quorum.ensure_writable()?;
        for topic in topics_to_create {
            let _ = self
                .quorum
                .append_record(MetadataRecord::CreateTopic { topic })?;
        }

        let mut updates = 0_usize;
        for (topic, partition, state) in changed {
            self.publish_partition_state_inner(topic, partition, state)?;
            updates = updates.saturating_add(1);
        }
        Ok(updates)
    }

    fn publish_partition_state_inner(
        &mut self,
        topic: String,
        partition: i32,
        state: ReplicatedPartitionState,
    ) -> Result<(), ReplicationControlPlaneError> {
        self.publish_topic_if_missing(&topic)?;
        self.quorum.ensure_writable()?;
        let _ = self.quorum.append_record(MetadataRecord::UpsertPartition {
            topic: topic.clone(),
            partition,
            leader_id: state.leader_id,
            leader_epoch: state.leader_epoch,
            replicas: state.replicas.clone(),
            isr: state.isr.clone(),
        })?;
        self.published_partitions.insert((topic, partition), state);
        Ok(())
    }
}
