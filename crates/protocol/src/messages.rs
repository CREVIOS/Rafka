#![forbid(unsafe_code)]

use crate::ProtocolError;

include!("generated_messages.rs");

pub const API_VERSIONS_MIN_VERSION: i16 = 0;
pub const API_VERSIONS_MAX_VERSION: i16 = 4;
pub const API_VERSIONS_FIRST_FLEXIBLE_VERSION: i16 = 3;

pub const PRODUCE_MIN_VERSION: i16 = 3;
pub const PRODUCE_MAX_VERSION: i16 = 13;
pub const PRODUCE_FIRST_FLEXIBLE_VERSION: i16 = 9;

pub const OFFSET_COMMIT_MIN_VERSION: i16 = 2;
pub const OFFSET_COMMIT_MAX_VERSION: i16 = 10;
pub const OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION: i16 = 8;

pub const OFFSET_FETCH_MIN_VERSION: i16 = 1;
pub const OFFSET_FETCH_MAX_VERSION: i16 = 10;
pub const OFFSET_FETCH_FIRST_FLEXIBLE_VERSION: i16 = 6;

pub const JOIN_GROUP_MIN_VERSION: i16 = 0;
pub const JOIN_GROUP_MAX_VERSION: i16 = 9;
pub const JOIN_GROUP_FIRST_FLEXIBLE_VERSION: i16 = 6;

pub const SYNC_GROUP_MIN_VERSION: i16 = 0;
pub const SYNC_GROUP_MAX_VERSION: i16 = 5;
pub const SYNC_GROUP_FIRST_FLEXIBLE_VERSION: i16 = 4;

pub const HEARTBEAT_MIN_VERSION: i16 = 0;
pub const HEARTBEAT_MAX_VERSION: i16 = 4;
pub const HEARTBEAT_FIRST_FLEXIBLE_VERSION: i16 = 4;

pub const LEAVE_GROUP_MIN_VERSION: i16 = 0;
pub const LEAVE_GROUP_MAX_VERSION: i16 = 5;
pub const LEAVE_GROUP_FIRST_FLEXIBLE_VERSION: i16 = 4;

pub const SASL_HANDSHAKE_MIN_VERSION: i16 = 0;
pub const SASL_HANDSHAKE_MAX_VERSION: i16 = 1;
pub const SASL_HANDSHAKE_FIRST_FLEXIBLE_VERSION: i16 = i16::MAX;

pub const SASL_AUTHENTICATE_MIN_VERSION: i16 = 0;
pub const SASL_AUTHENTICATE_MAX_VERSION: i16 = 2;
pub const SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION: i16 = 2;

pub trait VersionedCodec: Sized {
    const API_NAME: &'static str;
    const MIN_VERSION: i16;
    const MAX_VERSION: i16;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError>;
    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError>;
}

impl VersionedCodec for ApiVersionsRequest {
    const API_NAME: &'static str = "ApiVersionsRequest";
    const MIN_VERSION: i16 = API_VERSIONS_MIN_VERSION;
    const MAX_VERSION: i16 = API_VERSIONS_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let mut out = Vec::new();
        if is_flexible(version, API_VERSIONS_FIRST_FLEXIBLE_VERSION) {
            write_string(
                &mut out,
                self.client_software_name.as_deref().unwrap_or_default(),
                true,
            )?;
            write_string(
                &mut out,
                self.client_software_version.as_deref().unwrap_or_default(),
                true,
            )?;
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let mut reader = Reader::new(input);
        let mut decoded = Self::default();
        if is_flexible(version, API_VERSIONS_FIRST_FLEXIBLE_VERSION) {
            decoded.client_software_name = Some(read_string(&mut reader, true)?);
            decoded.client_software_version = Some(read_string(&mut reader, true)?);
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }
        Ok((decoded, reader.cursor))
    }
}

impl VersionedCodec for ApiVersionsResponse {
    const API_NAME: &'static str = "ApiVersionsResponse";
    const MIN_VERSION: i16 = API_VERSIONS_MIN_VERSION;
    const MAX_VERSION: i16 = API_VERSIONS_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, API_VERSIONS_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_i16(&mut out, self.error_code);
        write_array_len(&mut out, self.api_keys.len(), flexible)?;
        for api in &self.api_keys {
            write_i16(&mut out, api.api_key);
            write_i16(&mut out, api.min_version);
            write_i16(&mut out, api.max_version);
            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }

        if version >= 1 {
            let throttle = self
                .throttle_time_ms
                .ok_or(ProtocolError::MissingRequiredField(
                    "ApiVersionsResponse.throttle_time_ms",
                ))?;
            write_i32(&mut out, throttle);
        }

        if flexible {
            let mut tagged_fields = Vec::new();

            if let Some(features) = &self.supported_features {
                let mut payload = Vec::new();
                write_array_len(&mut payload, features.len(), true)?;
                for feature in features {
                    write_string(&mut payload, &feature.name, true)?;
                    write_i16(&mut payload, feature.min_version);
                    write_i16(&mut payload, feature.max_version);
                    write_tagged_fields(&mut payload, Vec::new())?;
                }
                tagged_fields.push((0_u32, payload));
            }

            if let Some(epoch) = self.finalized_features_epoch {
                let mut payload = Vec::new();
                write_i64(&mut payload, epoch);
                tagged_fields.push((1_u32, payload));
            }

            if let Some(features) = &self.finalized_features {
                let mut payload = Vec::new();
                write_array_len(&mut payload, features.len(), true)?;
                for feature in features {
                    write_string(&mut payload, &feature.name, true)?;
                    write_i16(&mut payload, feature.max_version_level);
                    write_i16(&mut payload, feature.min_version_level);
                    write_tagged_fields(&mut payload, Vec::new())?;
                }
                tagged_fields.push((2_u32, payload));
            }

            if let Some(ready) = self.zk_migration_ready {
                let mut payload = Vec::new();
                write_bool(&mut payload, ready);
                tagged_fields.push((3_u32, payload));
            }

            write_tagged_fields(&mut out, tagged_fields)?;
        }

        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, API_VERSIONS_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let error_code = reader.read_i16()?;

        let api_len = read_array_len(&mut reader, flexible)?;
        let mut api_keys = Vec::with_capacity(api_len);
        for _ in 0..api_len {
            let api_key = reader.read_i16()?;
            let min_version = reader.read_i16()?;
            let max_version = reader.read_i16()?;
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            api_keys.push(ApiVersionsResponseApiVersion {
                api_key,
                min_version,
                max_version,
            });
        }

        let throttle_time_ms = if version >= 1 {
            Some(reader.read_i32()?)
        } else {
            None
        };

        let mut supported_features: Option<Vec<ApiVersionsResponseSupportedFeatureKey>> = None;
        let mut finalized_features_epoch: Option<i64> = None;
        let mut finalized_features: Option<Vec<ApiVersionsResponseFinalizedFeatureKey>> = None;
        let mut zk_migration_ready: Option<bool> = None;

        if flexible {
            read_tagged_fields(&mut reader, |tag, payload| {
                match tag {
                    0 => {
                        let mut payload_reader = Reader::new(payload);
                        let feature_len = read_array_len(&mut payload_reader, true)?;
                        let mut features = Vec::with_capacity(feature_len);
                        for _ in 0..feature_len {
                            let name = read_string(&mut payload_reader, true)?;
                            let min_version = payload_reader.read_i16()?;
                            let max_version = payload_reader.read_i16()?;
                            read_tagged_fields(
                                &mut payload_reader,
                                |_inner_tag, _inner_payload| Ok(()),
                            )?;
                            features.push(ApiVersionsResponseSupportedFeatureKey {
                                name,
                                min_version,
                                max_version,
                            });
                        }
                        supported_features = Some(features);
                    }
                    1 => {
                        let mut payload_reader = Reader::new(payload);
                        finalized_features_epoch = Some(payload_reader.read_i64()?);
                    }
                    2 => {
                        let mut payload_reader = Reader::new(payload);
                        let feature_len = read_array_len(&mut payload_reader, true)?;
                        let mut features = Vec::with_capacity(feature_len);
                        for _ in 0..feature_len {
                            let name = read_string(&mut payload_reader, true)?;
                            let max_version_level = payload_reader.read_i16()?;
                            let min_version_level = payload_reader.read_i16()?;
                            read_tagged_fields(
                                &mut payload_reader,
                                |_inner_tag, _inner_payload| Ok(()),
                            )?;
                            features.push(ApiVersionsResponseFinalizedFeatureKey {
                                name,
                                max_version_level,
                                min_version_level,
                            });
                        }
                        finalized_features = Some(features);
                    }
                    3 => {
                        let mut payload_reader = Reader::new(payload);
                        zk_migration_ready = Some(payload_reader.read_bool()?);
                    }
                    _ => {}
                }
                Ok(())
            })?;
        }

        Ok((
            Self {
                error_code,
                api_keys,
                throttle_time_ms,
                supported_features,
                finalized_features_epoch,
                finalized_features,
                zk_migration_ready,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for ProduceRequest {
    const API_NAME: &'static str = "ProduceRequest";
    const MIN_VERSION: i16 = PRODUCE_MIN_VERSION;
    const MAX_VERSION: i16 = PRODUCE_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, PRODUCE_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_nullable_string(&mut out, self.transactional_id.as_deref(), flexible)?;
        write_i16(&mut out, self.acks);
        write_i32(&mut out, self.timeout_ms);
        write_array_len(&mut out, self.topic_data.len(), flexible)?;
        for topic in &self.topic_data {
            if version <= 12 {
                let name = topic
                    .name
                    .as_deref()
                    .ok_or(ProtocolError::MissingRequiredField(
                        "ProduceRequest.topic_data.name",
                    ))?;
                write_string(&mut out, name, flexible)?;
            }
            if version >= 13 {
                let topic_id = topic.topic_id.ok_or(ProtocolError::MissingRequiredField(
                    "ProduceRequest.topic_data.topic_id",
                ))?;
                write_uuid(&mut out, &topic_id);
            }
            write_array_len(&mut out, topic.partition_data.len(), flexible)?;
            for partition in &topic.partition_data {
                write_i32(&mut out, partition.index);
                write_nullable_bytes(&mut out, partition.records.as_deref(), flexible)?;
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }
            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, PRODUCE_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let transactional_id = read_nullable_string(&mut reader, flexible)?;
        let acks = reader.read_i16()?;
        let timeout_ms = reader.read_i32()?;
        let topic_len = read_array_len(&mut reader, flexible)?;
        let mut topic_data = Vec::with_capacity(topic_len);
        for _ in 0..topic_len {
            let name = if version <= 12 {
                Some(read_string(&mut reader, flexible)?)
            } else {
                None
            };
            let topic_id = if version >= 13 {
                Some(reader.read_uuid()?)
            } else {
                None
            };

            let partition_len = read_array_len(&mut reader, flexible)?;
            let mut partition_data = Vec::with_capacity(partition_len);
            for _ in 0..partition_len {
                let index = reader.read_i32()?;
                let records = read_nullable_bytes(&mut reader, flexible)?;
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                partition_data.push(ProduceRequestPartitionProduceData { index, records });
            }
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            topic_data.push(ProduceRequestTopicProduceData {
                name,
                topic_id,
                partition_data,
            });
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                transactional_id,
                acks,
                timeout_ms,
                topic_data,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for OffsetCommitRequest {
    const API_NAME: &'static str = "OffsetCommitRequest";
    const MIN_VERSION: i16 = OFFSET_COMMIT_MIN_VERSION;
    const MAX_VERSION: i16 = OFFSET_COMMIT_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_string(&mut out, &self.group_id, flexible)?;
        write_i32(&mut out, self.generation_id_or_member_epoch);
        write_string(&mut out, &self.member_id, flexible)?;
        if version >= 7 {
            write_nullable_string(&mut out, self.group_instance_id.as_deref(), flexible)?;
        }
        if (2..=4).contains(&version) {
            write_i64(&mut out, self.retention_time_ms.unwrap_or(-1));
        }

        write_array_len(&mut out, self.topics.len(), flexible)?;
        for topic in &self.topics {
            if version <= 9 {
                let name = topic
                    .name
                    .as_deref()
                    .ok_or(ProtocolError::MissingRequiredField(
                        "OffsetCommitRequest.topics.name",
                    ))?;
                write_string(&mut out, name, flexible)?;
            }
            if version >= 10 {
                let topic_id = topic.topic_id.ok_or(ProtocolError::MissingRequiredField(
                    "OffsetCommitRequest.topics.topic_id",
                ))?;
                write_uuid(&mut out, &topic_id);
            }

            write_array_len(&mut out, topic.partitions.len(), flexible)?;
            for partition in &topic.partitions {
                write_i32(&mut out, partition.partition_index);
                write_i64(&mut out, partition.committed_offset);
                if version >= 6 {
                    write_i32(&mut out, partition.committed_leader_epoch.unwrap_or(-1));
                }
                write_nullable_string(&mut out, partition.committed_metadata.as_deref(), flexible)?;
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }

            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }

        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let group_id = read_string(&mut reader, flexible)?;
        let generation_id_or_member_epoch = reader.read_i32()?;
        let member_id = read_string(&mut reader, flexible)?;
        let group_instance_id = if version >= 7 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let retention_time_ms = if (2..=4).contains(&version) {
            Some(reader.read_i64()?)
        } else {
            None
        };

        let topic_len = read_array_len(&mut reader, flexible)?;
        let mut topics = Vec::with_capacity(topic_len);
        for _ in 0..topic_len {
            let name = if version <= 9 {
                Some(read_string(&mut reader, flexible)?)
            } else {
                None
            };
            let topic_id = if version >= 10 {
                Some(reader.read_uuid()?)
            } else {
                None
            };

            let partition_len = read_array_len(&mut reader, flexible)?;
            let mut partitions = Vec::with_capacity(partition_len);
            for _ in 0..partition_len {
                let partition_index = reader.read_i32()?;
                let committed_offset = reader.read_i64()?;
                let committed_leader_epoch = if version >= 6 {
                    Some(reader.read_i32()?)
                } else {
                    None
                };
                let committed_metadata = read_nullable_string(&mut reader, flexible)?;
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                partitions.push(OffsetCommitRequestOffsetCommitRequestPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    committed_metadata,
                });
            }

            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            topics.push(OffsetCommitRequestOffsetCommitRequestTopic {
                name,
                topic_id,
                partitions,
            });
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                generation_id_or_member_epoch,
                member_id,
                group_instance_id,
                retention_time_ms,
                topics,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for OffsetCommitResponse {
    const API_NAME: &'static str = "OffsetCommitResponse";
    const MIN_VERSION: i16 = OFFSET_COMMIT_MIN_VERSION;
    const MAX_VERSION: i16 = OFFSET_COMMIT_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 3 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "OffsetCommitResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }

        write_array_len(&mut out, self.topics.len(), flexible)?;
        for topic in &self.topics {
            if version <= 9 {
                let name = topic
                    .name
                    .as_deref()
                    .ok_or(ProtocolError::MissingRequiredField(
                        "OffsetCommitResponse.topics.name",
                    ))?;
                write_string(&mut out, name, flexible)?;
            }
            if version >= 10 {
                let topic_id = topic.topic_id.ok_or(ProtocolError::MissingRequiredField(
                    "OffsetCommitResponse.topics.topic_id",
                ))?;
                write_uuid(&mut out, &topic_id);
            }

            write_array_len(&mut out, topic.partitions.len(), flexible)?;
            for partition in &topic.partitions {
                write_i32(&mut out, partition.partition_index);
                write_i16(&mut out, partition.error_code);
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }

            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }

        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_COMMIT_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 3 {
            Some(reader.read_i32()?)
        } else {
            None
        };

        let topic_len = read_array_len(&mut reader, flexible)?;
        let mut topics = Vec::with_capacity(topic_len);
        for _ in 0..topic_len {
            let name = if version <= 9 {
                Some(read_string(&mut reader, flexible)?)
            } else {
                None
            };
            let topic_id = if version >= 10 {
                Some(reader.read_uuid()?)
            } else {
                None
            };

            let partition_len = read_array_len(&mut reader, flexible)?;
            let mut partitions = Vec::with_capacity(partition_len);
            for _ in 0..partition_len {
                let partition_index = reader.read_i32()?;
                let error_code = reader.read_i16()?;
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                partitions.push(OffsetCommitResponseOffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }

            topics.push(OffsetCommitResponseOffsetCommitResponseTopic {
                name,
                topic_id,
                partitions,
            });
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                throttle_time_ms,
                topics,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for OffsetFetchRequest {
    const API_NAME: &'static str = "OffsetFetchRequest";
    const MIN_VERSION: i16 = OFFSET_FETCH_MIN_VERSION;
    const MAX_VERSION: i16 = OFFSET_FETCH_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_FETCH_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version <= 7 {
            let group_id = self
                .group_id
                .as_deref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "OffsetFetchRequest.group_id",
                ))?;
            write_string(&mut out, group_id, flexible)?;

            let topics = self.topics.as_ref();
            if version <= 1 {
                let topics = topics.ok_or(ProtocolError::MissingRequiredField(
                    "OffsetFetchRequest.topics",
                ))?;
                write_array_len(&mut out, topics.len(), flexible)?;
                for topic in topics {
                    write_string(&mut out, &topic.name, flexible)?;
                    write_array_len(&mut out, topic.partition_indexes.len(), flexible)?;
                    for partition in &topic.partition_indexes {
                        write_i32(&mut out, *partition);
                    }
                    if flexible {
                        write_tagged_fields(&mut out, Vec::new())?;
                    }
                }
            } else {
                write_nullable_array_len(&mut out, topics.map(Vec::len), flexible)?;
                if let Some(topics) = topics {
                    for topic in topics {
                        write_string(&mut out, &topic.name, flexible)?;
                        write_array_len(&mut out, topic.partition_indexes.len(), flexible)?;
                        for partition in &topic.partition_indexes {
                            write_i32(&mut out, *partition);
                        }
                        if flexible {
                            write_tagged_fields(&mut out, Vec::new())?;
                        }
                    }
                }
            }
        } else {
            let groups = self
                .groups
                .as_ref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "OffsetFetchRequest.groups",
                ))?;
            write_array_len(&mut out, groups.len(), flexible)?;
            for group in groups {
                write_string(&mut out, &group.group_id, flexible)?;
                if version >= 9 {
                    write_nullable_string(&mut out, group.member_id.as_deref(), flexible)?;
                    write_i32(&mut out, group.member_epoch.unwrap_or(-1));
                }

                write_nullable_array_len(&mut out, group.topics.as_ref().map(Vec::len), flexible)?;
                if let Some(topics) = &group.topics {
                    for topic in topics {
                        if version <= 9 {
                            let name = topic.name.as_deref().ok_or(
                                ProtocolError::MissingRequiredField(
                                    "OffsetFetchRequest.groups.topics.name",
                                ),
                            )?;
                            write_string(&mut out, name, flexible)?;
                        }
                        if version >= 10 {
                            let topic_id =
                                topic.topic_id.ok_or(ProtocolError::MissingRequiredField(
                                    "OffsetFetchRequest.groups.topics.topic_id",
                                ))?;
                            write_uuid(&mut out, &topic_id);
                        }
                        write_array_len(&mut out, topic.partition_indexes.len(), flexible)?;
                        for partition in &topic.partition_indexes {
                            write_i32(&mut out, *partition);
                        }
                        if flexible {
                            write_tagged_fields(&mut out, Vec::new())?;
                        }
                    }
                }
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }
        }

        if version >= 7 {
            write_bool(&mut out, self.require_stable.unwrap_or(false));
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }

        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_FETCH_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let mut group_id = None;
        let mut topics = None;
        let mut groups = None;

        if version <= 7 {
            group_id = Some(read_string(&mut reader, flexible)?);
            let topic_len = if version <= 1 {
                Some(read_array_len(&mut reader, flexible)?)
            } else {
                read_nullable_array_len(&mut reader, flexible)?
            };

            topics = topic_len
                .map(|len| {
                    let mut values = Vec::with_capacity(len);
                    for _ in 0..len {
                        let name = read_string(&mut reader, flexible)?;
                        let partition_len = read_array_len(&mut reader, flexible)?;
                        let mut partition_indexes = Vec::with_capacity(partition_len);
                        for _ in 0..partition_len {
                            partition_indexes.push(reader.read_i32()?);
                        }
                        if flexible {
                            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                        }
                        values.push(OffsetFetchRequestOffsetFetchRequestTopic {
                            name,
                            partition_indexes,
                        });
                    }
                    Ok(values)
                })
                .transpose()?;
        } else {
            let group_len = read_array_len(&mut reader, flexible)?;
            let mut values = Vec::with_capacity(group_len);
            for _ in 0..group_len {
                let current_group_id = read_string(&mut reader, flexible)?;
                let member_id = if version >= 9 {
                    read_nullable_string(&mut reader, flexible)?
                } else {
                    None
                };
                let member_epoch = if version >= 9 {
                    Some(reader.read_i32()?)
                } else {
                    None
                };

                let topic_len = read_nullable_array_len(&mut reader, flexible)?;
                let group_topics = topic_len
                    .map(|len| {
                        let mut group_topics = Vec::with_capacity(len);
                        for _ in 0..len {
                            let name = if version <= 9 {
                                Some(read_string(&mut reader, flexible)?)
                            } else {
                                None
                            };
                            let topic_id = if version >= 10 {
                                Some(reader.read_uuid()?)
                            } else {
                                None
                            };
                            let partition_len = read_array_len(&mut reader, flexible)?;
                            let mut partition_indexes = Vec::with_capacity(partition_len);
                            for _ in 0..partition_len {
                                partition_indexes.push(reader.read_i32()?);
                            }
                            if flexible {
                                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                            }
                            group_topics.push(OffsetFetchRequestOffsetFetchRequestTopics {
                                name,
                                topic_id,
                                partition_indexes,
                            });
                        }
                        Ok(group_topics)
                    })
                    .transpose()?;

                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                values.push(OffsetFetchRequestOffsetFetchRequestGroup {
                    group_id: current_group_id,
                    member_id,
                    member_epoch,
                    topics: group_topics,
                });
            }
            groups = Some(values);
        }

        let require_stable = if version >= 7 {
            Some(reader.read_bool()?)
        } else {
            None
        };
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                topics,
                groups,
                require_stable,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for OffsetFetchResponse {
    const API_NAME: &'static str = "OffsetFetchResponse";
    const MIN_VERSION: i16 = OFFSET_FETCH_MIN_VERSION;
    const MAX_VERSION: i16 = OFFSET_FETCH_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_FETCH_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 3 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "OffsetFetchResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }

        if version <= 7 {
            let topics = self
                .topics
                .as_ref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "OffsetFetchResponse.topics",
                ))?;
            write_array_len(&mut out, topics.len(), flexible)?;
            for topic in topics {
                write_string(&mut out, &topic.name, flexible)?;
                write_array_len(&mut out, topic.partitions.len(), flexible)?;
                for partition in &topic.partitions {
                    write_i32(&mut out, partition.partition_index);
                    write_i64(&mut out, partition.committed_offset);
                    if version >= 5 {
                        write_i32(&mut out, partition.committed_leader_epoch.unwrap_or(-1));
                    }
                    write_nullable_string(&mut out, partition.metadata.as_deref(), flexible)?;
                    write_i16(&mut out, partition.error_code);
                    if flexible {
                        write_tagged_fields(&mut out, Vec::new())?;
                    }
                }
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }

            if version >= 2 {
                write_i16(&mut out, self.error_code.unwrap_or(0));
            }
        } else {
            let groups = self
                .groups
                .as_ref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "OffsetFetchResponse.groups",
                ))?;
            write_array_len(&mut out, groups.len(), flexible)?;
            for group in groups {
                write_string(&mut out, &group.group_id, flexible)?;
                write_array_len(&mut out, group.topics.len(), flexible)?;
                for topic in &group.topics {
                    if version <= 9 {
                        let name =
                            topic
                                .name
                                .as_deref()
                                .ok_or(ProtocolError::MissingRequiredField(
                                    "OffsetFetchResponse.groups.topics.name",
                                ))?;
                        write_string(&mut out, name, flexible)?;
                    }
                    if version >= 10 {
                        let topic_id =
                            topic.topic_id.ok_or(ProtocolError::MissingRequiredField(
                                "OffsetFetchResponse.groups.topics.topic_id",
                            ))?;
                        write_uuid(&mut out, &topic_id);
                    }

                    write_array_len(&mut out, topic.partitions.len(), flexible)?;
                    for partition in &topic.partitions {
                        write_i32(&mut out, partition.partition_index);
                        write_i64(&mut out, partition.committed_offset);
                        write_i32(&mut out, partition.committed_leader_epoch);
                        write_nullable_string(&mut out, partition.metadata.as_deref(), flexible)?;
                        write_i16(&mut out, partition.error_code);
                        if flexible {
                            write_tagged_fields(&mut out, Vec::new())?;
                        }
                    }
                    if flexible {
                        write_tagged_fields(&mut out, Vec::new())?;
                    }
                }
                write_i16(&mut out, group.error_code);
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }
        }

        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }

        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, OFFSET_FETCH_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 3 {
            Some(reader.read_i32()?)
        } else {
            None
        };

        let mut topics = None;
        let mut error_code = None;
        let mut groups = None;

        if version <= 7 {
            let topic_len = read_array_len(&mut reader, flexible)?;
            let mut values = Vec::with_capacity(topic_len);
            for _ in 0..topic_len {
                let name = read_string(&mut reader, flexible)?;
                let partition_len = read_array_len(&mut reader, flexible)?;
                let mut partitions = Vec::with_capacity(partition_len);
                for _ in 0..partition_len {
                    let partition_index = reader.read_i32()?;
                    let committed_offset = reader.read_i64()?;
                    let committed_leader_epoch = if version >= 5 {
                        Some(reader.read_i32()?)
                    } else {
                        None
                    };
                    let metadata = read_nullable_string(&mut reader, flexible)?;
                    let partition_error_code = reader.read_i16()?;
                    if flexible {
                        read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                    }
                    partitions.push(OffsetFetchResponseOffsetFetchResponsePartition {
                        partition_index,
                        committed_offset,
                        committed_leader_epoch,
                        metadata,
                        error_code: partition_error_code,
                    });
                }
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                values.push(OffsetFetchResponseOffsetFetchResponseTopic { name, partitions });
            }
            topics = Some(values);

            if version >= 2 {
                error_code = Some(reader.read_i16()?);
            }
        } else {
            let group_len = read_array_len(&mut reader, flexible)?;
            let mut values = Vec::with_capacity(group_len);
            for _ in 0..group_len {
                let group_id = read_string(&mut reader, flexible)?;
                let topic_len = read_array_len(&mut reader, flexible)?;
                let mut group_topics = Vec::with_capacity(topic_len);
                for _ in 0..topic_len {
                    let name = if version <= 9 {
                        Some(read_string(&mut reader, flexible)?)
                    } else {
                        None
                    };
                    let topic_id = if version >= 10 {
                        Some(reader.read_uuid()?)
                    } else {
                        None
                    };

                    let partition_len = read_array_len(&mut reader, flexible)?;
                    let mut partitions = Vec::with_capacity(partition_len);
                    for _ in 0..partition_len {
                        let partition_index = reader.read_i32()?;
                        let committed_offset = reader.read_i64()?;
                        let committed_leader_epoch = reader.read_i32()?;
                        let metadata = read_nullable_string(&mut reader, flexible)?;
                        let partition_error_code = reader.read_i16()?;
                        if flexible {
                            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                        }
                        partitions.push(OffsetFetchResponseOffsetFetchResponsePartitions {
                            partition_index,
                            committed_offset,
                            committed_leader_epoch,
                            metadata,
                            error_code: partition_error_code,
                        });
                    }
                    if flexible {
                        read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                    }
                    group_topics.push(OffsetFetchResponseOffsetFetchResponseTopics {
                        name,
                        topic_id,
                        partitions,
                    });
                }
                let group_error_code = reader.read_i16()?;
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                values.push(OffsetFetchResponseOffsetFetchResponseGroup {
                    group_id,
                    topics: group_topics,
                    error_code: group_error_code,
                });
            }
            groups = Some(values);
        }

        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                throttle_time_ms,
                topics,
                error_code,
                groups,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for JoinGroupRequest {
    const API_NAME: &'static str = "JoinGroupRequest";
    const MIN_VERSION: i16 = JOIN_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = JOIN_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, JOIN_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_string(&mut out, &self.group_id, flexible)?;
        write_i32(&mut out, self.session_timeout_ms);
        if version >= 1 {
            let rebalance_timeout_ms =
                self.rebalance_timeout_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "JoinGroupRequest.rebalance_timeout_ms",
                    ))?;
            write_i32(&mut out, rebalance_timeout_ms);
        }
        write_string(&mut out, &self.member_id, flexible)?;
        if version >= 5 {
            write_nullable_string(&mut out, self.group_instance_id.as_deref(), flexible)?;
        }
        write_string(&mut out, &self.protocol_type, flexible)?;
        write_array_len(&mut out, self.protocols.len(), flexible)?;
        for protocol in &self.protocols {
            write_string(&mut out, &protocol.name, flexible)?;
            write_bytes(&mut out, &protocol.metadata, flexible)?;
            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if version >= 8 {
            write_nullable_string(&mut out, self.reason.as_deref(), flexible)?;
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, JOIN_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let group_id = read_string(&mut reader, flexible)?;
        let session_timeout_ms = reader.read_i32()?;
        let rebalance_timeout_ms = if version >= 1 {
            Some(reader.read_i32()?)
        } else {
            None
        };
        let member_id = read_string(&mut reader, flexible)?;
        let group_instance_id = if version >= 5 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let protocol_type = read_string(&mut reader, flexible)?;

        let protocol_len = read_array_len(&mut reader, flexible)?;
        let mut protocols = Vec::with_capacity(protocol_len);
        for _ in 0..protocol_len {
            let name = read_string(&mut reader, flexible)?;
            let metadata = read_bytes(&mut reader, flexible)?;
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            protocols.push(JoinGroupRequestJoinGroupRequestProtocol { name, metadata });
        }
        let reason = if version >= 8 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for JoinGroupResponse {
    const API_NAME: &'static str = "JoinGroupResponse";
    const MIN_VERSION: i16 = JOIN_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = JOIN_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, JOIN_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 2 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "JoinGroupResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }
        write_i16(&mut out, self.error_code);
        write_i32(&mut out, self.generation_id);
        if version >= 7 {
            write_nullable_string(&mut out, self.protocol_type.as_deref(), flexible)?;
            write_nullable_string(&mut out, self.protocol_name.as_deref(), flexible)?;
        } else {
            let protocol_name =
                self.protocol_name
                    .as_deref()
                    .ok_or(ProtocolError::MissingRequiredField(
                        "JoinGroupResponse.protocol_name",
                    ))?;
            write_string(&mut out, protocol_name, flexible)?;
        }
        write_string(&mut out, &self.leader, flexible)?;
        if version >= 9 {
            write_bool(&mut out, self.skip_assignment.unwrap_or(false));
        }
        write_string(&mut out, &self.member_id, flexible)?;

        write_array_len(&mut out, self.members.len(), flexible)?;
        for member in &self.members {
            write_string(&mut out, &member.member_id, flexible)?;
            if version >= 5 {
                write_nullable_string(&mut out, member.group_instance_id.as_deref(), flexible)?;
            }
            write_bytes(&mut out, &member.metadata, flexible)?;
            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, JOIN_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 2 {
            Some(reader.read_i32()?)
        } else {
            None
        };
        let error_code = reader.read_i16()?;
        let generation_id = reader.read_i32()?;
        let protocol_type = if version >= 7 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let protocol_name = if version >= 7 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            Some(read_string(&mut reader, flexible)?)
        };
        let leader = read_string(&mut reader, flexible)?;
        let skip_assignment = if version >= 9 {
            Some(reader.read_bool()?)
        } else {
            None
        };
        let member_id = read_string(&mut reader, flexible)?;

        let member_len = read_array_len(&mut reader, flexible)?;
        let mut members = Vec::with_capacity(member_len);
        for _ in 0..member_len {
            let member_id = read_string(&mut reader, flexible)?;
            let group_instance_id = if version >= 5 {
                read_nullable_string(&mut reader, flexible)?
            } else {
                None
            };
            let metadata = read_bytes(&mut reader, flexible)?;
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            members.push(JoinGroupResponseJoinGroupResponseMember {
                member_id,
                group_instance_id,
                metadata,
            });
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                throttle_time_ms,
                error_code,
                generation_id,
                protocol_type,
                protocol_name,
                leader,
                skip_assignment,
                member_id,
                members,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for SyncGroupRequest {
    const API_NAME: &'static str = "SyncGroupRequest";
    const MIN_VERSION: i16 = SYNC_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = SYNC_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SYNC_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_string(&mut out, &self.group_id, flexible)?;
        write_i32(&mut out, self.generation_id);
        write_string(&mut out, &self.member_id, flexible)?;
        if version >= 3 {
            write_nullable_string(&mut out, self.group_instance_id.as_deref(), flexible)?;
        }
        if version >= 5 {
            write_nullable_string(&mut out, self.protocol_type.as_deref(), flexible)?;
            write_nullable_string(&mut out, self.protocol_name.as_deref(), flexible)?;
        }

        write_array_len(&mut out, self.assignments.len(), flexible)?;
        for assignment in &self.assignments {
            write_string(&mut out, &assignment.member_id, flexible)?;
            write_bytes(&mut out, &assignment.assignment, flexible)?;
            if flexible {
                write_tagged_fields(&mut out, Vec::new())?;
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SYNC_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let group_id = read_string(&mut reader, flexible)?;
        let generation_id = reader.read_i32()?;
        let member_id = read_string(&mut reader, flexible)?;
        let group_instance_id = if version >= 3 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let protocol_type = if version >= 5 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let protocol_name = if version >= 5 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };

        let assignment_len = read_array_len(&mut reader, flexible)?;
        let mut assignments = Vec::with_capacity(assignment_len);
        for _ in 0..assignment_len {
            let member_id = read_string(&mut reader, flexible)?;
            let assignment = read_bytes(&mut reader, flexible)?;
            if flexible {
                read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
            }
            assignments.push(SyncGroupRequestSyncGroupRequestAssignment {
                member_id,
                assignment,
            });
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for SyncGroupResponse {
    const API_NAME: &'static str = "SyncGroupResponse";
    const MIN_VERSION: i16 = SYNC_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = SYNC_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SYNC_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 1 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "SyncGroupResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }
        write_i16(&mut out, self.error_code);
        if version >= 5 {
            write_nullable_string(&mut out, self.protocol_type.as_deref(), flexible)?;
            write_nullable_string(&mut out, self.protocol_name.as_deref(), flexible)?;
        }
        write_bytes(&mut out, &self.assignment, flexible)?;
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SYNC_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 1 {
            Some(reader.read_i32()?)
        } else {
            None
        };
        let error_code = reader.read_i16()?;
        let protocol_type = if version >= 5 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let protocol_name = if version >= 5 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        let assignment = read_bytes(&mut reader, flexible)?;
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                throttle_time_ms,
                error_code,
                protocol_type,
                protocol_name,
                assignment,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for HeartbeatRequest {
    const API_NAME: &'static str = "HeartbeatRequest";
    const MIN_VERSION: i16 = HEARTBEAT_MIN_VERSION;
    const MAX_VERSION: i16 = HEARTBEAT_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, HEARTBEAT_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_string(&mut out, &self.group_id, flexible)?;
        write_i32(&mut out, self.generation_id);
        write_string(&mut out, &self.member_id, flexible)?;
        if version >= 3 {
            write_nullable_string(&mut out, self.group_instance_id.as_deref(), flexible)?;
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, HEARTBEAT_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let group_id = read_string(&mut reader, flexible)?;
        let generation_id = reader.read_i32()?;
        let member_id = read_string(&mut reader, flexible)?;
        let group_instance_id = if version >= 3 {
            read_nullable_string(&mut reader, flexible)?
        } else {
            None
        };
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for HeartbeatResponse {
    const API_NAME: &'static str = "HeartbeatResponse";
    const MIN_VERSION: i16 = HEARTBEAT_MIN_VERSION;
    const MAX_VERSION: i16 = HEARTBEAT_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, HEARTBEAT_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 1 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "HeartbeatResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }
        write_i16(&mut out, self.error_code);
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, HEARTBEAT_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 1 {
            Some(reader.read_i32()?)
        } else {
            None
        };
        let error_code = reader.read_i16()?;
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }
        Ok((
            Self {
                throttle_time_ms,
                error_code,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for LeaveGroupRequest {
    const API_NAME: &'static str = "LeaveGroupRequest";
    const MIN_VERSION: i16 = LEAVE_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = LEAVE_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, LEAVE_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_string(&mut out, &self.group_id, flexible)?;
        if version <= 2 {
            let member_id =
                self.member_id
                    .as_deref()
                    .ok_or(ProtocolError::MissingRequiredField(
                        "LeaveGroupRequest.member_id",
                    ))?;
            write_string(&mut out, member_id, flexible)?;
        } else {
            let members = self
                .members
                .as_ref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "LeaveGroupRequest.members",
                ))?;
            write_array_len(&mut out, members.len(), flexible)?;
            for member in members {
                write_string(&mut out, &member.member_id, flexible)?;
                write_nullable_string(&mut out, member.group_instance_id.as_deref(), flexible)?;
                if version >= 5 {
                    write_nullable_string(&mut out, member.reason.as_deref(), flexible)?;
                }
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, LEAVE_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let group_id = read_string(&mut reader, flexible)?;
        let mut member_id = None;
        let mut members = None;
        if version <= 2 {
            member_id = Some(read_string(&mut reader, flexible)?);
        } else {
            let member_len = read_array_len(&mut reader, flexible)?;
            let mut values = Vec::with_capacity(member_len);
            for _ in 0..member_len {
                let member_id = read_string(&mut reader, flexible)?;
                let group_instance_id = read_nullable_string(&mut reader, flexible)?;
                let reason = if version >= 5 {
                    read_nullable_string(&mut reader, flexible)?
                } else {
                    None
                };
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                values.push(LeaveGroupRequestMemberIdentity {
                    member_id,
                    group_instance_id,
                    reason,
                });
            }
            members = Some(values);
        }
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                group_id,
                member_id,
                members,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for LeaveGroupResponse {
    const API_NAME: &'static str = "LeaveGroupResponse";
    const MIN_VERSION: i16 = LEAVE_GROUP_MIN_VERSION;
    const MAX_VERSION: i16 = LEAVE_GROUP_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, LEAVE_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        if version >= 1 {
            let throttle_time_ms =
                self.throttle_time_ms
                    .ok_or(ProtocolError::MissingRequiredField(
                        "LeaveGroupResponse.throttle_time_ms",
                    ))?;
            write_i32(&mut out, throttle_time_ms);
        }
        write_i16(&mut out, self.error_code);

        if version >= 3 {
            let members = self
                .members
                .as_ref()
                .ok_or(ProtocolError::MissingRequiredField(
                    "LeaveGroupResponse.members",
                ))?;
            write_array_len(&mut out, members.len(), flexible)?;
            for member in members {
                write_string(&mut out, &member.member_id, flexible)?;
                write_nullable_string(&mut out, member.group_instance_id.as_deref(), flexible)?;
                write_i16(&mut out, member.error_code);
                if flexible {
                    write_tagged_fields(&mut out, Vec::new())?;
                }
            }
        }

        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, LEAVE_GROUP_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let throttle_time_ms = if version >= 1 {
            Some(reader.read_i32()?)
        } else {
            None
        };
        let error_code = reader.read_i16()?;
        let members = if version >= 3 {
            let member_len = read_array_len(&mut reader, flexible)?;
            let mut values = Vec::with_capacity(member_len);
            for _ in 0..member_len {
                let member_id = read_string(&mut reader, flexible)?;
                let group_instance_id = read_nullable_string(&mut reader, flexible)?;
                let error_code = reader.read_i16()?;
                if flexible {
                    read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
                }
                values.push(LeaveGroupResponseMemberResponse {
                    member_id,
                    group_instance_id,
                    error_code,
                });
            }
            Some(values)
        } else {
            None
        };
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                throttle_time_ms,
                error_code,
                members,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for SaslHandshakeRequest {
    const API_NAME: &'static str = "SaslHandshakeRequest";
    const MIN_VERSION: i16 = SASL_HANDSHAKE_MIN_VERSION;
    const MAX_VERSION: i16 = SASL_HANDSHAKE_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;

        let mut out = Vec::new();
        write_string(&mut out, &self.mechanism, false)?;
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;

        let mut reader = Reader::new(input);
        let mechanism = read_string(&mut reader, false)?;
        Ok((Self { mechanism }, reader.cursor))
    }
}

impl VersionedCodec for SaslHandshakeResponse {
    const API_NAME: &'static str = "SaslHandshakeResponse";
    const MIN_VERSION: i16 = SASL_HANDSHAKE_MIN_VERSION;
    const MAX_VERSION: i16 = SASL_HANDSHAKE_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;

        let mut out = Vec::new();
        write_i16(&mut out, self.error_code);
        write_array_len(&mut out, self.mechanisms.len(), false)?;
        for mechanism in &self.mechanisms {
            write_string(&mut out, mechanism, false)?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;

        let mut reader = Reader::new(input);
        let error_code = reader.read_i16()?;
        let mechanism_len = read_array_len(&mut reader, false)?;
        let mut mechanisms = Vec::with_capacity(mechanism_len);
        for _ in 0..mechanism_len {
            mechanisms.push(read_string(&mut reader, false)?);
        }
        Ok((
            Self {
                error_code,
                mechanisms,
            },
            reader.cursor,
        ))
    }
}

impl VersionedCodec for SaslAuthenticateRequest {
    const API_NAME: &'static str = "SaslAuthenticateRequest";
    const MIN_VERSION: i16 = SASL_AUTHENTICATE_MIN_VERSION;
    const MAX_VERSION: i16 = SASL_AUTHENTICATE_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_bytes(&mut out, &self.auth_bytes, flexible)?;
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let auth_bytes = read_bytes(&mut reader, flexible)?;
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }
        Ok((Self { auth_bytes }, reader.cursor))
    }
}

impl VersionedCodec for SaslAuthenticateResponse {
    const API_NAME: &'static str = "SaslAuthenticateResponse";
    const MIN_VERSION: i16 = SASL_AUTHENTICATE_MIN_VERSION;
    const MAX_VERSION: i16 = SASL_AUTHENTICATE_MAX_VERSION;

    fn encode(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION);

        let mut out = Vec::new();
        write_i16(&mut out, self.error_code);
        write_nullable_string(&mut out, self.error_message.as_deref(), flexible)?;
        write_bytes(&mut out, &self.auth_bytes, flexible)?;
        if version >= 1 {
            write_i64(&mut out, self.session_lifetime_ms.unwrap_or(0));
        }
        if flexible {
            write_tagged_fields(&mut out, Vec::new())?;
        }
        Ok(out)
    }

    fn decode(version: i16, input: &[u8]) -> Result<(Self, usize), ProtocolError> {
        ensure_version(
            Self::API_NAME,
            version,
            Self::MIN_VERSION,
            Self::MAX_VERSION,
        )?;
        let flexible = is_flexible(version, SASL_AUTHENTICATE_FIRST_FLEXIBLE_VERSION);

        let mut reader = Reader::new(input);
        let error_code = reader.read_i16()?;
        let error_message = read_nullable_string(&mut reader, flexible)?;
        let auth_bytes = read_bytes(&mut reader, flexible)?;
        let session_lifetime_ms = if version >= 1 {
            Some(reader.read_i64()?)
        } else {
            None
        };
        if flexible {
            read_tagged_fields(&mut reader, |_tag, _payload| Ok(()))?;
        }

        Ok((
            Self {
                error_code,
                error_message,
                auth_bytes,
                session_lifetime_ms,
            },
            reader.cursor,
        ))
    }
}

fn ensure_version(
    api: &'static str,
    version: i16,
    min_version: i16,
    max_version: i16,
) -> Result<(), ProtocolError> {
    if version < min_version || version > max_version {
        return Err(ProtocolError::InvalidVersion { api, version });
    }
    Ok(())
}

fn is_flexible(version: i16, first_flexible_version: i16) -> bool {
    version >= first_flexible_version
}

fn write_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn write_uuid(out: &mut Vec<u8>, value: &[u8; 16]) {
    out.extend_from_slice(value);
}

fn write_uvarint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn checked_len_to_u32(len: usize) -> Result<u32, ProtocolError> {
    u32::try_from(len).map_err(|_| ProtocolError::InvalidCompactLength(u32::MAX))
}

fn checked_len_to_i32(len: usize) -> Result<i32, ProtocolError> {
    i32::try_from(len).map_err(|_| ProtocolError::InvalidLength(i32::MAX))
}

fn checked_len_to_i16(len: usize) -> Result<i16, ProtocolError> {
    i16::try_from(len).map_err(|_| ProtocolError::InvalidLength(i16::MAX as i32))
}

fn write_string(out: &mut Vec<u8>, value: &str, flexible: bool) -> Result<(), ProtocolError> {
    let bytes = value.as_bytes();
    if flexible {
        let length = checked_len_to_u32(bytes.len())?;
        let length_plus_one = length
            .checked_add(1)
            .ok_or(ProtocolError::InvalidCompactLength(length))?;
        write_uvarint(out, length_plus_one);
        out.extend_from_slice(bytes);
        return Ok(());
    }

    let length = checked_len_to_i16(bytes.len())?;
    write_i16(out, length);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_nullable_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
    flexible: bool,
) -> Result<(), ProtocolError> {
    match value {
        Some(value) => write_string(out, value, flexible),
        None => {
            if flexible {
                write_uvarint(out, 0);
            } else {
                write_i16(out, -1);
            }
            Ok(())
        }
    }
}

fn write_nullable_bytes(
    out: &mut Vec<u8>,
    value: Option<&[u8]>,
    flexible: bool,
) -> Result<(), ProtocolError> {
    match value {
        Some(bytes) => {
            if flexible {
                let length = checked_len_to_u32(bytes.len())?;
                let length_plus_one = length
                    .checked_add(1)
                    .ok_or(ProtocolError::InvalidCompactLength(length))?;
                write_uvarint(out, length_plus_one);
            } else {
                let length = checked_len_to_i32(bytes.len())?;
                write_i32(out, length);
            }
            out.extend_from_slice(bytes);
            Ok(())
        }
        None => {
            if flexible {
                write_uvarint(out, 0);
            } else {
                write_i32(out, -1);
            }
            Ok(())
        }
    }
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8], flexible: bool) -> Result<(), ProtocolError> {
    write_nullable_bytes(out, Some(value), flexible)
}

fn write_array_len(out: &mut Vec<u8>, len: usize, flexible: bool) -> Result<(), ProtocolError> {
    if flexible {
        let base = checked_len_to_u32(len)?;
        let encoded = base
            .checked_add(1)
            .ok_or(ProtocolError::InvalidCompactLength(base))?;
        write_uvarint(out, encoded);
        return Ok(());
    }
    write_i32(out, checked_len_to_i32(len)?);
    Ok(())
}

fn write_nullable_array_len(
    out: &mut Vec<u8>,
    len: Option<usize>,
    flexible: bool,
) -> Result<(), ProtocolError> {
    match len {
        Some(len) => write_array_len(out, len, flexible),
        None => {
            if flexible {
                write_uvarint(out, 0);
            } else {
                write_i32(out, -1);
            }
            Ok(())
        }
    }
}

fn write_tagged_fields(
    out: &mut Vec<u8>,
    mut tagged_fields: Vec<(u32, Vec<u8>)>,
) -> Result<(), ProtocolError> {
    tagged_fields.sort_by_key(|(tag, _)| *tag);
    let field_count = checked_len_to_u32(tagged_fields.len())?;
    write_uvarint(out, field_count);
    for (tag, payload) in tagged_fields {
        write_uvarint(out, tag);
        write_uvarint(out, checked_len_to_u32(payload.len())?);
        out.extend_from_slice(&payload);
    }
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, cursor: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        if self.input.len().saturating_sub(self.cursor) < len {
            return Err(ProtocolError::Truncated);
        }
        let start = self.cursor;
        self.cursor += len;
        Ok(&self.input[start..self.cursor])
    }

    fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let bytes = self.read_exact(2)?;
        let mut buf = [0_u8; 2];
        buf.copy_from_slice(bytes);
        Ok(i16::from_be_bytes(buf))
    }

    fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let bytes = self.read_exact(4)?;
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(bytes);
        Ok(i32::from_be_bytes(buf))
    }

    fn read_i64(&mut self) -> Result<i64, ProtocolError> {
        let bytes = self.read_exact(8)?;
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(bytes);
        Ok(i64::from_be_bytes(buf))
    }

    fn read_bool(&mut self) -> Result<bool, ProtocolError> {
        let value = self.read_exact(1)?[0];
        match value {
            0 => Ok(false),
            1 => Ok(true),
            invalid => Err(ProtocolError::InvalidBoolean(invalid)),
        }
    }

    fn read_uuid(&mut self) -> Result<[u8; 16], ProtocolError> {
        let bytes = self.read_exact(16)?;
        let mut uuid = [0_u8; 16];
        uuid.copy_from_slice(bytes);
        Ok(uuid)
    }

    fn read_uvarint(&mut self) -> Result<u32, ProtocolError> {
        let mut value = 0_u32;
        for i in 0..5 {
            let byte = self.read_exact(1)?[0];
            if i == 4 && (byte & 0xf0) != 0 {
                return Err(ProtocolError::VarintTooLong);
            }
            value |= ((byte & 0x7f) as u32) << (7 * i);
            if (byte & 0x80) == 0 {
                return Ok(value);
            }
        }
        Err(ProtocolError::VarintTooLong)
    }
}

fn read_utf8_string(reader: &mut Reader<'_>, len: usize) -> Result<String, ProtocolError> {
    let bytes = reader.read_exact(len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidString)
}

fn read_string(reader: &mut Reader<'_>, flexible: bool) -> Result<String, ProtocolError> {
    let nullable = read_nullable_string(reader, flexible)?;
    nullable.ok_or({
        if flexible {
            ProtocolError::InvalidCompactLength(0)
        } else {
            ProtocolError::InvalidLength(-1)
        }
    })
}

fn read_nullable_string(
    reader: &mut Reader<'_>,
    flexible: bool,
) -> Result<Option<String>, ProtocolError> {
    if flexible {
        let encoded_len = reader.read_uvarint()?;
        if encoded_len == 0 {
            return Ok(None);
        }
        let len = encoded_len - 1;
        let len =
            usize::try_from(len).map_err(|_| ProtocolError::InvalidCompactLength(encoded_len))?;
        return Ok(Some(read_utf8_string(reader, len)?));
    }

    let len = reader.read_i16()?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidLength(i32::from(len)));
    }
    Ok(Some(read_utf8_string(reader, len as usize)?))
}

fn read_nullable_bytes(
    reader: &mut Reader<'_>,
    flexible: bool,
) -> Result<Option<Vec<u8>>, ProtocolError> {
    if flexible {
        let encoded_len = reader.read_uvarint()?;
        if encoded_len == 0 {
            return Ok(None);
        }
        let len = encoded_len - 1;
        let len =
            usize::try_from(len).map_err(|_| ProtocolError::InvalidCompactLength(encoded_len))?;
        return Ok(Some(reader.read_exact(len)?.to_vec()));
    }

    let len = reader.read_i32()?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidLength(len));
    }
    Ok(Some(reader.read_exact(len as usize)?.to_vec()))
}

fn read_bytes(reader: &mut Reader<'_>, flexible: bool) -> Result<Vec<u8>, ProtocolError> {
    let nullable = read_nullable_bytes(reader, flexible)?;
    nullable.ok_or({
        if flexible {
            ProtocolError::InvalidCompactLength(0)
        } else {
            ProtocolError::InvalidLength(-1)
        }
    })
}

fn read_array_len(reader: &mut Reader<'_>, flexible: bool) -> Result<usize, ProtocolError> {
    if flexible {
        let encoded_len = reader.read_uvarint()?;
        if encoded_len == 0 {
            return Err(ProtocolError::InvalidCompactLength(encoded_len));
        }
        let len = encoded_len - 1;
        return usize::try_from(len).map_err(|_| ProtocolError::InvalidCompactLength(encoded_len));
    }

    let len = reader.read_i32()?;
    if len < 0 {
        return Err(ProtocolError::InvalidLength(len));
    }
    Ok(len as usize)
}

fn read_nullable_array_len(
    reader: &mut Reader<'_>,
    flexible: bool,
) -> Result<Option<usize>, ProtocolError> {
    if flexible {
        let encoded_len = reader.read_uvarint()?;
        if encoded_len == 0 {
            return Ok(None);
        }
        let len = encoded_len - 1;
        let len =
            usize::try_from(len).map_err(|_| ProtocolError::InvalidCompactLength(encoded_len))?;
        return Ok(Some(len));
    }

    let len = reader.read_i32()?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(ProtocolError::InvalidLength(len));
    }
    Ok(Some(len as usize))
}

fn read_tagged_fields<F>(reader: &mut Reader<'_>, mut on_field: F) -> Result<(), ProtocolError>
where
    F: FnMut(u32, &[u8]) -> Result<(), ProtocolError>,
{
    let field_count = reader.read_uvarint()?;
    for _ in 0..field_count {
        let tag = reader.read_uvarint()?;
        let size = reader.read_uvarint()?;
        let size = usize::try_from(size).map_err(|_| ProtocolError::InvalidCompactLength(size))?;
        let payload = reader.read_exact(size)?;
        on_field(tag, payload)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_request_v0_is_empty() {
        let request = ApiVersionsRequest::default();
        let encoded = request.encode(0).expect("encode");
        assert!(encoded.is_empty());
        let (decoded, read) = ApiVersionsRequest::decode(0, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, 0);
    }

    #[test]
    fn api_versions_request_v3_known_bytes() {
        let request = ApiVersionsRequest {
            client_software_name: Some("a".to_string()),
            client_software_version: Some("1".to_string()),
        };
        let encoded = request.encode(3).expect("encode");
        assert_eq!(encoded, vec![0x02, b'a', 0x02, b'1', 0x00]);

        let (decoded, read) = ApiVersionsRequest::decode(3, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn api_versions_response_v0_known_bytes() {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersionsResponseApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: None,
            supported_features: None,
            finalized_features_epoch: None,
            finalized_features: None,
            zk_migration_ready: None,
        };
        let encoded = response.encode(0).expect("encode");
        assert_eq!(
            encoded,
            vec![
                0x00, 0x00, // error code
                0x00, 0x00, 0x00, 0x01, // api_keys length
                0x00, 0x12, // api key
                0x00, 0x00, // min version
                0x00, 0x04, // max version
            ]
        );

        let (decoded, read) = ApiVersionsResponse::decode(0, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn api_versions_response_v3_roundtrip_with_tagged_fields() {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersionsResponseApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: Some(0),
            supported_features: Some(vec![ApiVersionsResponseSupportedFeatureKey {
                name: "metadata.version".to_string(),
                min_version: 1,
                max_version: 20,
            }]),
            finalized_features_epoch: Some(7),
            finalized_features: Some(vec![ApiVersionsResponseFinalizedFeatureKey {
                name: "metadata.version".to_string(),
                max_version_level: 20,
                min_version_level: 1,
            }]),
            zk_migration_ready: Some(true),
        };

        let encoded = response.encode(3).expect("encode");
        let (decoded, read) = ApiVersionsResponse::decode(3, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn produce_request_v3_known_bytes() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: Some("t".to_string()),
                topic_id: None,
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 0,
                    records: None,
                }],
            }],
        };

        let encoded = request.encode(3).expect("encode");
        assert_eq!(
            encoded,
            vec![
                0xff, 0xff, // transactional_id = null
                0x00, 0x01, // acks
                0x00, 0x00, 0x13, 0x88, // timeout_ms
                0x00, 0x00, 0x00, 0x01, // topic_data length
                0x00, 0x01, b't', // topic name
                0x00, 0x00, 0x00, 0x01, // partition_data length
                0x00, 0x00, 0x00, 0x00, // partition index
                0xff, 0xff, 0xff, 0xff, // records = null
            ]
        );

        let (decoded, read) = ProduceRequest::decode(3, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn produce_request_v9_known_bytes() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: Some("t".to_string()),
                topic_id: None,
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 0,
                    records: None,
                }],
            }],
        };

        let encoded = request.encode(9).expect("encode");
        assert_eq!(
            encoded,
            vec![
                0x00, // transactional_id = null
                0x00, 0x01, // acks
                0x00, 0x00, 0x13, 0x88, // timeout_ms
                0x02, // topic_data compact length
                0x02, b't', // topic name
                0x02, // partition_data compact length
                0x00, 0x00, 0x00, 0x00, // partition index
                0x00, // records = null
                0x00, // partition tagged fields
                0x00, // topic tagged fields
                0x00, // top-level tagged fields
            ]
        );

        let (decoded, read) = ProduceRequest::decode(9, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn produce_request_v13_roundtrip_with_topic_id() {
        let request = ProduceRequest {
            transactional_id: Some("tx".to_string()),
            acks: -1,
            timeout_ms: 9000,
            topic_data: vec![ProduceRequestTopicProduceData {
                name: None,
                topic_id: Some([1_u8; 16]),
                partition_data: vec![ProduceRequestPartitionProduceData {
                    index: 3,
                    records: Some(vec![1, 2, 3]),
                }],
            }],
        };

        let encoded = request.encode(13).expect("encode");
        let (decoded, read) = ProduceRequest::decode(13, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_commit_request_v2_roundtrip() {
        let request = OffsetCommitRequest {
            group_id: "g".to_string(),
            generation_id_or_member_epoch: -1,
            member_id: "".to_string(),
            group_instance_id: None,
            retention_time_ms: Some(-1),
            topics: vec![OffsetCommitRequestOffsetCommitRequestTopic {
                name: Some("topic-a".to_string()),
                topic_id: None,
                partitions: vec![OffsetCommitRequestOffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 42,
                    committed_leader_epoch: None,
                    committed_metadata: Some("meta".to_string()),
                }],
            }],
        };

        let encoded = request.encode(2).expect("encode");
        let (decoded, read) = OffsetCommitRequest::decode(2, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_commit_request_v10_roundtrip_with_topic_id() {
        let request = OffsetCommitRequest {
            group_id: "group-a".to_string(),
            generation_id_or_member_epoch: 9,
            member_id: "member-a".to_string(),
            group_instance_id: Some("instance-a".to_string()),
            retention_time_ms: None,
            topics: vec![OffsetCommitRequestOffsetCommitRequestTopic {
                name: None,
                topic_id: Some([7_u8; 16]),
                partitions: vec![OffsetCommitRequestOffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 100,
                    committed_leader_epoch: Some(4),
                    committed_metadata: None,
                }],
            }],
        };

        let encoded = request.encode(10).expect("encode");
        let (decoded, read) = OffsetCommitRequest::decode(10, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_commit_response_v8_roundtrip() {
        let response = OffsetCommitResponse {
            throttle_time_ms: Some(12),
            topics: vec![OffsetCommitResponseOffsetCommitResponseTopic {
                name: Some("topic-a".to_string()),
                topic_id: None,
                partitions: vec![OffsetCommitResponseOffsetCommitResponsePartition {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        };

        let encoded = response.encode(8).expect("encode");
        let (decoded, read) = OffsetCommitResponse::decode(8, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_fetch_request_v1_roundtrip() {
        let request = OffsetFetchRequest {
            group_id: Some("group-a".to_string()),
            topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopic {
                name: "topic-a".to_string(),
                partition_indexes: vec![0, 1],
            }]),
            groups: None,
            require_stable: None,
        };

        let encoded = request.encode(1).expect("encode");
        let (decoded, read) = OffsetFetchRequest::decode(1, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_fetch_request_v10_roundtrip_with_groups() {
        let request = OffsetFetchRequest {
            group_id: None,
            topics: None,
            groups: Some(vec![OffsetFetchRequestOffsetFetchRequestGroup {
                group_id: "group-a".to_string(),
                member_id: Some("member-a".to_string()),
                member_epoch: Some(11),
                topics: Some(vec![OffsetFetchRequestOffsetFetchRequestTopics {
                    name: None,
                    topic_id: Some([3_u8; 16]),
                    partition_indexes: vec![0, 2],
                }]),
            }]),
            require_stable: Some(true),
        };

        let encoded = request.encode(10).expect("encode");
        let (decoded, read) = OffsetFetchRequest::decode(10, &encoded).expect("decode");
        assert_eq!(decoded, request);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_fetch_response_v7_roundtrip() {
        let response = OffsetFetchResponse {
            throttle_time_ms: Some(33),
            topics: Some(vec![OffsetFetchResponseOffsetFetchResponseTopic {
                name: "topic-a".to_string(),
                partitions: vec![OffsetFetchResponseOffsetFetchResponsePartition {
                    partition_index: 1,
                    committed_offset: 99,
                    committed_leader_epoch: Some(5),
                    metadata: Some("meta".to_string()),
                    error_code: 0,
                }],
            }]),
            error_code: Some(0),
            groups: None,
        };

        let encoded = response.encode(7).expect("encode");
        let (decoded, read) = OffsetFetchResponse::decode(7, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_fetch_response_v10_roundtrip() {
        let response = OffsetFetchResponse {
            throttle_time_ms: Some(4),
            topics: None,
            error_code: None,
            groups: Some(vec![OffsetFetchResponseOffsetFetchResponseGroup {
                group_id: "group-a".to_string(),
                topics: vec![OffsetFetchResponseOffsetFetchResponseTopics {
                    name: None,
                    topic_id: Some([9_u8; 16]),
                    partitions: vec![OffsetFetchResponseOffsetFetchResponsePartitions {
                        partition_index: 0,
                        committed_offset: 5,
                        committed_leader_epoch: -1,
                        metadata: None,
                        error_code: 0,
                    }],
                }],
                error_code: 0,
            }]),
        };

        let encoded = response.encode(10).expect("encode");
        let (decoded, read) = OffsetFetchResponse::decode(10, &encoded).expect("decode");
        assert_eq!(decoded, response);
        assert_eq!(read, encoded.len());
    }

    #[test]
    fn offset_commit_request_missing_topic_name_fails_for_v9() {
        let request = OffsetCommitRequest {
            group_id: "g".to_string(),
            generation_id_or_member_epoch: -1,
            member_id: "".to_string(),
            group_instance_id: None,
            retention_time_ms: None,
            topics: vec![OffsetCommitRequestOffsetCommitRequestTopic {
                name: None,
                topic_id: None,
                partitions: vec![],
            }],
        };

        let err = request.encode(9).expect_err("missing topic name");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("OffsetCommitRequest.topics.name")
        );
    }

    #[test]
    fn offset_fetch_request_missing_groups_fails_for_v8() {
        let request = OffsetFetchRequest {
            group_id: None,
            topics: None,
            groups: None,
            require_stable: Some(false),
        };

        let err = request.encode(8).expect_err("missing groups");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("OffsetFetchRequest.groups")
        );
    }

    #[test]
    fn offset_fetch_response_missing_throttle_time_fails_for_v3() {
        let response = OffsetFetchResponse {
            throttle_time_ms: None,
            topics: Some(vec![]),
            error_code: Some(0),
            groups: None,
        };

        let err = response.encode(3).expect_err("missing throttle");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("OffsetFetchResponse.throttle_time_ms")
        );
    }

    #[test]
    fn invalid_version_fails() {
        let err = ProduceRequest::default()
            .encode(2)
            .expect_err("invalid version");
        assert_eq!(
            err,
            ProtocolError::InvalidVersion {
                api: "ProduceRequest",
                version: 2
            }
        );
    }
}
