#![forbid(unsafe_code)]

use rafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, ApiVersionsResponseApiVersion,
    ApiVersionsResponseFinalizedFeatureKey, ApiVersionsResponseSupportedFeatureKey,
    OffsetCommitRequest, OffsetCommitRequestOffsetCommitRequestPartition,
    OffsetCommitRequestOffsetCommitRequestTopic, OffsetCommitResponse,
    OffsetCommitResponseOffsetCommitResponsePartition,
    OffsetCommitResponseOffsetCommitResponseTopic, OffsetFetchRequest,
    OffsetFetchRequestOffsetFetchRequestGroup, OffsetFetchRequestOffsetFetchRequestTopic,
    OffsetFetchRequestOffsetFetchRequestTopics, OffsetFetchResponse,
    OffsetFetchResponseOffsetFetchResponseGroup, OffsetFetchResponseOffsetFetchResponsePartition,
    OffsetFetchResponseOffsetFetchResponsePartitions, OffsetFetchResponseOffsetFetchResponseTopic,
    OffsetFetchResponseOffsetFetchResponseTopics, ProduceRequest,
    ProduceRequestPartitionProduceData, ProduceRequestTopicProduceData, VersionedCodec,
    API_VERSIONS_MAX_VERSION, API_VERSIONS_MIN_VERSION, OFFSET_COMMIT_MAX_VERSION,
    OFFSET_COMMIT_MIN_VERSION, OFFSET_FETCH_MAX_VERSION, OFFSET_FETCH_MIN_VERSION,
    PRODUCE_MAX_VERSION, PRODUCE_MIN_VERSION,
};
use rafka_protocol::ProtocolError;

#[derive(Debug, Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn next_u8(&mut self) -> u8 {
        (self.next_u64() & 0xff) as u8
    }

    fn next_bool(&mut self) -> bool {
        (self.next_u64() & 1) == 1
    }

    fn range_usize(&mut self, min_inclusive: usize, max_inclusive: usize) -> usize {
        assert!(min_inclusive <= max_inclusive);
        if min_inclusive == max_inclusive {
            return min_inclusive;
        }
        let span = max_inclusive - min_inclusive + 1;
        min_inclusive + (self.next_u32() as usize % span)
    }

    fn range_i16(&mut self, min_inclusive: i16, max_inclusive: i16) -> i16 {
        assert!(min_inclusive <= max_inclusive);
        if min_inclusive == max_inclusive {
            return min_inclusive;
        }
        let span = i32::from(max_inclusive) - i32::from(min_inclusive) + 1;
        (i32::from(min_inclusive) + (self.next_u32() as i32 % span)) as i16
    }

    fn range_i32(&mut self, min_inclusive: i32, max_inclusive: i32) -> i32 {
        assert!(min_inclusive <= max_inclusive);
        if min_inclusive == max_inclusive {
            return min_inclusive;
        }
        let span = i64::from(max_inclusive) - i64::from(min_inclusive) + 1;
        (i64::from(min_inclusive) + (i64::from(self.next_u32()) % span)) as i32
    }

    fn range_i64(&mut self, min_inclusive: i64, max_inclusive: i64) -> i64 {
        assert!(min_inclusive <= max_inclusive);
        if min_inclusive == max_inclusive {
            return min_inclusive;
        }
        let span = (max_inclusive - min_inclusive + 1) as u64;
        min_inclusive + (self.next_u64() % span) as i64
    }

    fn ascii_string(&mut self, min_len: usize, max_len: usize) -> String {
        let len = self.range_usize(min_len, max_len);
        let mut out = String::with_capacity(len);
        for _ in 0..len {
            let c = b'a' + (self.next_u8() % 26);
            out.push(char::from(c));
        }
        out
    }

    fn bytes(&mut self, min_len: usize, max_len: usize) -> Vec<u8> {
        let len = self.range_usize(min_len, max_len);
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(self.next_u8());
        }
        out
    }

    fn uuid(&mut self) -> [u8; 16] {
        let mut id = [0_u8; 16];
        for byte in &mut id {
            *byte = self.next_u8();
        }
        id
    }
}

fn random_api_versions_request(version: i16, rng: &mut Lcg) -> ApiVersionsRequest {
    if version >= 3 {
        ApiVersionsRequest {
            client_software_name: Some(rng.ascii_string(0, 20)),
            client_software_version: Some(rng.ascii_string(0, 20)),
        }
    } else {
        ApiVersionsRequest::default()
    }
}

fn random_api_versions_response(version: i16, rng: &mut Lcg) -> ApiVersionsResponse {
    let api_key_count = rng.range_usize(0, 6);
    let mut api_keys = Vec::with_capacity(api_key_count);
    for _ in 0..api_key_count {
        api_keys.push(ApiVersionsResponseApiVersion {
            api_key: rng.range_i16(i16::MIN, i16::MAX),
            min_version: rng.range_i16(-5, 10),
            max_version: rng.range_i16(10, 30),
        });
    }

    let throttle_time_ms = if version >= 1 {
        Some(rng.range_i32(0, 100_000))
    } else {
        None
    };

    if version < 3 {
        return ApiVersionsResponse {
            error_code: rng.range_i16(i16::MIN, i16::MAX),
            api_keys,
            throttle_time_ms,
            supported_features: None,
            finalized_features_epoch: None,
            finalized_features: None,
            zk_migration_ready: None,
        };
    }

    let supported_features = if rng.next_bool() {
        let count = rng.range_usize(0, 5);
        let mut features = Vec::with_capacity(count);
        for _ in 0..count {
            features.push(ApiVersionsResponseSupportedFeatureKey {
                name: rng.ascii_string(0, 30),
                min_version: rng.range_i16(0, 10),
                max_version: rng.range_i16(10, 30),
            });
        }
        Some(features)
    } else {
        None
    };

    let finalized_features_epoch = rng.next_bool().then(|| rng.range_i64(-10, 10_000));

    let finalized_features = if rng.next_bool() {
        let count = rng.range_usize(0, 5);
        let mut features = Vec::with_capacity(count);
        for _ in 0..count {
            features.push(ApiVersionsResponseFinalizedFeatureKey {
                name: rng.ascii_string(0, 30),
                max_version_level: rng.range_i16(0, 40),
                min_version_level: rng.range_i16(0, 20),
            });
        }
        Some(features)
    } else {
        None
    };

    ApiVersionsResponse {
        error_code: rng.range_i16(i16::MIN, i16::MAX),
        api_keys,
        throttle_time_ms,
        supported_features,
        finalized_features_epoch,
        finalized_features,
        zk_migration_ready: rng.next_bool().then(|| rng.next_bool()),
    }
}

fn random_produce_request(version: i16, rng: &mut Lcg) -> ProduceRequest {
    let topic_count = rng.range_usize(0, 4);
    let mut topic_data = Vec::with_capacity(topic_count);

    for _ in 0..topic_count {
        let partition_count = rng.range_usize(0, 3);
        let mut partition_data = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partition_data.push(ProduceRequestPartitionProduceData {
                index: rng.range_i32(0, 1_000),
                records: if rng.next_bool() {
                    Some(rng.bytes(0, 128))
                } else {
                    None
                },
            });
        }

        topic_data.push(ProduceRequestTopicProduceData {
            name: (version <= 12).then(|| rng.ascii_string(0, 24)),
            topic_id: (version >= 13).then(|| rng.uuid()),
            partition_data,
        });
    }

    ProduceRequest {
        transactional_id: if rng.next_bool() {
            Some(rng.ascii_string(0, 20))
        } else {
            None
        },
        acks: match rng.range_i32(0, 2) {
            0 => -1,
            1 => 0,
            _ => 1,
        },
        timeout_ms: rng.range_i32(0, 120_000),
        topic_data,
    }
}

fn random_offset_commit_request(version: i16, rng: &mut Lcg) -> OffsetCommitRequest {
    let topic_count = rng.range_usize(0, 4);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let partition_count = rng.range_usize(0, 4);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(OffsetCommitRequestOffsetCommitRequestPartition {
                partition_index: rng.range_i32(0, 1_024),
                committed_offset: rng.range_i64(0, 10_000_000),
                committed_leader_epoch: (version >= 6).then(|| rng.range_i32(-1, 100)),
                committed_metadata: if rng.next_bool() {
                    Some(rng.ascii_string(0, 40))
                } else {
                    None
                },
            });
        }
        topics.push(OffsetCommitRequestOffsetCommitRequestTopic {
            name: (version <= 9).then(|| rng.ascii_string(1, 30)),
            topic_id: (version >= 10).then(|| rng.uuid()),
            partitions,
        });
    }

    OffsetCommitRequest {
        group_id: rng.ascii_string(1, 20),
        generation_id_or_member_epoch: rng.range_i32(-1, 100),
        member_id: rng.ascii_string(0, 20),
        group_instance_id: if version >= 7 && rng.next_bool() {
            Some(rng.ascii_string(0, 20))
        } else {
            None
        },
        retention_time_ms: if (2..=4).contains(&version) {
            Some(rng.range_i64(-1, 100_000))
        } else {
            None
        },
        topics,
    }
}

fn random_offset_commit_response(version: i16, rng: &mut Lcg) -> OffsetCommitResponse {
    let topic_count = rng.range_usize(0, 4);
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let partition_count = rng.range_usize(0, 4);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(OffsetCommitResponseOffsetCommitResponsePartition {
                partition_index: rng.range_i32(0, 1_024),
                error_code: rng.range_i16(-1, 100),
            });
        }
        topics.push(OffsetCommitResponseOffsetCommitResponseTopic {
            name: (version <= 9).then(|| rng.ascii_string(1, 20)),
            topic_id: (version >= 10).then(|| rng.uuid()),
            partitions,
        });
    }

    OffsetCommitResponse {
        throttle_time_ms: (version >= 3).then(|| rng.range_i32(0, 120_000)),
        topics,
    }
}

fn random_offset_fetch_request(version: i16, rng: &mut Lcg) -> OffsetFetchRequest {
    if version <= 7 {
        let topics = if version <= 1 || rng.next_bool() {
            Some(vec![OffsetFetchRequestOffsetFetchRequestTopic {
                name: rng.ascii_string(1, 30),
                partition_indexes: (0..rng.range_usize(0, 4))
                    .map(|_| rng.range_i32(0, 512))
                    .collect(),
            }])
        } else {
            None
        };
        return OffsetFetchRequest {
            group_id: Some(rng.ascii_string(1, 20)),
            topics,
            groups: None,
            require_stable: (version >= 7).then(|| rng.next_bool()),
        };
    }

    let group_count = rng.range_usize(0, 3);
    let mut groups = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        let topics = if rng.next_bool() {
            None
        } else {
            let mut group_topics = Vec::with_capacity(rng.range_usize(0, 3));
            for _ in 0..group_topics.capacity() {
                group_topics.push(OffsetFetchRequestOffsetFetchRequestTopics {
                    name: (version <= 9).then(|| rng.ascii_string(1, 30)),
                    topic_id: (version >= 10).then(|| rng.uuid()),
                    partition_indexes: (0..rng.range_usize(0, 4))
                        .map(|_| rng.range_i32(0, 512))
                        .collect(),
                });
            }
            Some(group_topics)
        };

        groups.push(OffsetFetchRequestOffsetFetchRequestGroup {
            group_id: rng.ascii_string(1, 20),
            member_id: if version >= 9 && rng.next_bool() {
                Some(rng.ascii_string(0, 20))
            } else {
                None
            },
            member_epoch: (version >= 9).then(|| rng.range_i32(-1, 100)),
            topics,
        });
    }

    OffsetFetchRequest {
        group_id: None,
        topics: None,
        groups: Some(groups),
        require_stable: Some(rng.next_bool()),
    }
}

fn random_offset_fetch_response(version: i16, rng: &mut Lcg) -> OffsetFetchResponse {
    if version <= 7 {
        let topic_count = rng.range_usize(0, 3);
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let partition_count = rng.range_usize(0, 4);
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partitions.push(OffsetFetchResponseOffsetFetchResponsePartition {
                    partition_index: rng.range_i32(0, 512),
                    committed_offset: rng.range_i64(-1, 1_000_000),
                    committed_leader_epoch: (version >= 5).then(|| rng.range_i32(-1, 64)),
                    metadata: if rng.next_bool() {
                        Some(rng.ascii_string(0, 20))
                    } else {
                        None
                    },
                    error_code: rng.range_i16(-1, 64),
                });
            }
            topics.push(OffsetFetchResponseOffsetFetchResponseTopic {
                name: rng.ascii_string(1, 20),
                partitions,
            });
        }
        return OffsetFetchResponse {
            throttle_time_ms: (version >= 3).then(|| rng.range_i32(0, 100_000)),
            topics: Some(topics),
            error_code: (version >= 2).then(|| rng.range_i16(-1, 64)),
            groups: None,
        };
    }

    let group_count = rng.range_usize(0, 3);
    let mut groups = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        let topic_count = rng.range_usize(0, 3);
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let partition_count = rng.range_usize(0, 4);
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partitions.push(OffsetFetchResponseOffsetFetchResponsePartitions {
                    partition_index: rng.range_i32(0, 512),
                    committed_offset: rng.range_i64(-1, 1_000_000),
                    committed_leader_epoch: rng.range_i32(-1, 64),
                    metadata: if rng.next_bool() {
                        Some(rng.ascii_string(0, 20))
                    } else {
                        None
                    },
                    error_code: rng.range_i16(-1, 64),
                });
            }
            topics.push(OffsetFetchResponseOffsetFetchResponseTopics {
                name: (version <= 9).then(|| rng.ascii_string(1, 20)),
                topic_id: (version >= 10).then(|| rng.uuid()),
                partitions,
            });
        }
        groups.push(OffsetFetchResponseOffsetFetchResponseGroup {
            group_id: rng.ascii_string(1, 20),
            topics,
            error_code: rng.range_i16(-1, 64),
        });
    }

    OffsetFetchResponse {
        throttle_time_ms: Some(rng.range_i32(0, 100_000)),
        topics: None,
        error_code: None,
        groups: Some(groups),
    }
}

fn assert_roundtrip<T>(message: &T, version: i16)
where
    T: VersionedCodec + core::fmt::Debug + PartialEq,
{
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = T::decode(version, &encoded).expect("decode");
    assert_eq!(decoded, *message);
    assert_eq!(read, encoded.len());
}

#[test]
fn randomized_api_versions_request_roundtrip_matrix() {
    let mut rng = Lcg::new(0xA1B2_C3D4_0001);

    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        for _ in 0..256 {
            let request = random_api_versions_request(version, &mut rng);
            assert_roundtrip(&request, version);
        }
    }
}

#[test]
fn randomized_api_versions_response_roundtrip_matrix() {
    let mut rng = Lcg::new(0xA1B2_C3D4_0002);

    for version in API_VERSIONS_MIN_VERSION..=API_VERSIONS_MAX_VERSION {
        for _ in 0..192 {
            let response = random_api_versions_response(version, &mut rng);
            assert_roundtrip(&response, version);
        }
    }
}

#[test]
fn randomized_produce_request_roundtrip_matrix() {
    let mut rng = Lcg::new(0xA1B2_C3D4_0003);

    for version in PRODUCE_MIN_VERSION..=PRODUCE_MAX_VERSION {
        for _ in 0..160 {
            let request = random_produce_request(version, &mut rng);
            assert_roundtrip(&request, version);
        }
    }
}

#[test]
fn randomized_offset_commit_roundtrip_matrix() {
    let mut rng = Lcg::new(0xA1B2_C3D4_0004);

    for version in OFFSET_COMMIT_MIN_VERSION..=OFFSET_COMMIT_MAX_VERSION {
        for _ in 0..144 {
            let request = random_offset_commit_request(version, &mut rng);
            assert_roundtrip(&request, version);

            let response = random_offset_commit_response(version, &mut rng);
            assert_roundtrip(&response, version);
        }
    }
}

#[test]
fn randomized_offset_fetch_roundtrip_matrix() {
    let mut rng = Lcg::new(0xA1B2_C3D4_0005);

    for version in OFFSET_FETCH_MIN_VERSION..=OFFSET_FETCH_MAX_VERSION {
        for _ in 0..128 {
            let request = random_offset_fetch_request(version, &mut rng);
            assert_roundtrip(&request, version);

            let response = random_offset_fetch_response(version, &mut rng);
            assert_roundtrip(&response, version);
        }
    }
}

#[test]
fn produce_request_missing_required_fields_matrix() {
    for version in PRODUCE_MIN_VERSION..=12 {
        let request = ProduceRequest {
            topic_data: vec![ProduceRequestTopicProduceData {
                name: None,
                topic_id: None,
                partition_data: vec![],
            }],
            ..ProduceRequest::default()
        };

        let err = request
            .encode(version)
            .expect_err("missing name should fail for <= v12");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("ProduceRequest.topic_data.name")
        );
    }

    for version in 13..=PRODUCE_MAX_VERSION {
        let request = ProduceRequest {
            topic_data: vec![ProduceRequestTopicProduceData {
                name: Some("topic".to_string()),
                topic_id: None,
                partition_data: vec![],
            }],
            ..ProduceRequest::default()
        };

        let err = request
            .encode(version)
            .expect_err("missing topic_id should fail for >= v13");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("ProduceRequest.topic_data.topic_id")
        );
    }
}

#[test]
fn api_versions_response_missing_throttle_time_is_rejected_for_v1_plus() {
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

    for version in 1..=API_VERSIONS_MAX_VERSION {
        let err = response
            .encode(version)
            .expect_err("missing throttle_time_ms should fail for v1+");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("ApiVersionsResponse.throttle_time_ms")
        );
    }
}

#[test]
fn offset_commit_response_missing_throttle_time_is_rejected_for_v3_plus() {
    for version in 3..=OFFSET_COMMIT_MAX_VERSION {
        let response = OffsetCommitResponse {
            throttle_time_ms: None,
            topics: vec![],
        };
        let err = response
            .encode(version)
            .expect_err("missing throttle should fail");
        assert_eq!(
            err,
            ProtocolError::MissingRequiredField("OffsetCommitResponse.throttle_time_ms")
        );
    }
}
