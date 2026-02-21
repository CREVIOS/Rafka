# Rafka Test Plan

## What is tested now

- Codegen behavior:
  - Version-range parser compatibility (`none`, `A-B`, `A+`)
  - Real Kafka JSON message spec loading (`clients/src/main/resources/common/message`)
  - Generated protocol registry contains known APIs
  - Generated Rust protocol message structs for `ApiVersions`, `Produce`, `OffsetCommit`, `OffsetFetch`, and classic-group APIs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`)
- Protocol behavior:
  - Varint and varlong round-trip encoding
  - Truncation and overflow safety
  - Record batch header parse/serialize validity
  - Version-aware encode/decode for `ApiVersionsRequest`, `ApiVersionsResponse`, `ProduceRequest`, `OffsetCommit`, `OffsetFetch`, and classic-group APIs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`)
  - Flexible-version tagged-field handling for `ApiVersionsResponse`
  - Known wire-byte compatibility vectors for `ApiVersions` and `Produce`
  - Spec-translation checks against Kafka JSON schemas for version/flex bounds and field-gating rules
  - Rust-native matrix tests across all supported versions for `ApiVersions`, `Produce`, `OffsetCommit`, `OffsetFetch`, and classic-group APIs (including invalid and truncation paths)
  - Randomized encode/decode roundtrip matrices for `ApiVersionsRequest`, `ApiVersionsResponse`, `ProduceRequest`, `OffsetCommit`, and `OffsetFetch`
  - Required-field negative validation matrix (missing topic name/topic ID/throttle time)
  - Optional JVM byte-for-byte differential tests for `ApiVersions` and `Produce` against Kafka Java serializers
- Storage behavior:
  - Monotonic append offsets
  - Bounded fetch semantics
  - Out-of-range checks
  - File-backed segmented log append/fetch correctness across segment rolls
  - Kafka-style padded segment/index file naming checks
  - Large payload persistence/recovery checks
  - Mid-segment corruption truncation behavior checks
  - Segment-gap corruption detection checks
  - Index-file rebuild behavior when index files are missing or corrupted
  - Tail truncation recovery for torn/corrupt records during startup scan
  - Restart recovery consistency for persisted offsets and records
  - Stress append/fetch/restart loop coverage
  - Repeated open/close/delete cycles as file-handle leak guards
- Coordinator behavior:
  - Generation changes on group membership changes
  - Heartbeat validation
  - Session timeout eviction behavior
  - Durable offset WAL commit/fetch behavior with restart recovery and torn-tail truncation
  - Offset commit/fetch epoch/member fencing behavior parity (`IllegalGeneration`, `UnknownMemberId`, `StaleMemberEpoch`)
  - Durable classic group WAL behavior for membership + assignment state (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) with restart recovery
  - Classic group rebalance error-code parity coverage (`InvalidGroupId`, `UnknownMemberId`, `IllegalGeneration`, `InconsistentGroupProtocol`, `MemberIdRequired`, `RebalanceInProgress`, `FencedInstanceId`)
  - Durable transaction WAL behavior for producer IDs/epochs, ongoing transaction state, marker application, and restart recovery
  - Realistic coordinator workload tests for rebalance/member churn, lag catch-up, and commit storms
  - Optional million-commit coordinator stress recovery test (ignored by default)
  - Optional million-scale parallel consumer-group churn stress test with restart validation (ignored by default)
- Broker behavior:
  - End-to-end produce/fetch
  - Group join + heartbeat path
  - Error path for invalid fetch offset
  - Persistent topic/partition routing produce/fetch paths
  - Unknown partition error behavior and offset-range preservation
  - Multi-partition stress behavior and restart recovery
  - Partition discovery edge cases (topic names with dashes, non-partition dir/file ignore rules)
  - TCP wire transport request/response routing for `ApiVersions` and `Produce`
  - TCP wire transport request/response routing for `OffsetCommit`/`OffsetFetch` (v2-v10 commit, v1-v10 fetch)
  - TCP wire transport request/response routing for `Fetch` (v4-v18)
  - TCP wire transport request/response routing for group rebalance APIs (`JoinGroup` v0-v9, `SyncGroup` v0-v5, `Heartbeat` v0-v4, `LeaveGroup` v0-v5)
  - TCP wire transport request/response routing for transaction APIs (`InitProducerId` v0-v6, `EndTxn` v0-v5, `WriteTxnMarkers` v1-v2)
  - Request/response header compatibility checks (legacy and flexible versions)
  - Wire error-path coverage (unknown API, unsupported version, malformed request, oversized/negative/truncated frames)
  - TLS wire transport integration checks (`rustls`) for successful handshake + request roundtrip and handshake-failure path.
  - SASL wire transport integration checks (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) for handshake/authenticate sequencing and post-auth API dispatch.
  - ACL wire authorization checks for topic/group/cluster/transactional-id resources with Kafka-style error mapping (`TOPIC_AUTHORIZATION_FAILED`, `GROUP_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED`, `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`).
  - SASL principal + ACL integration checks to validate that authenticated principals drive authorization outcomes.
  - Combined TLS+SASL integration checks to validate layered security composition on a single listener.
  - Async transport (`tokio`) integration checks for `ApiVersions` roundtrip matrix, concurrent connection handling, and oversized-frame rejection parity.
  - SASL pre-auth request rejection checks for protected APIs before broker dispatch.
  - Prometheus transport metrics integration checks for TLS handshake counters, SASL auth counters, and API error-class counters.
  - Rust-native performance harness coverage for produce/fetch latency and achieved throughput at target load classes (`1,000`, `10,000`, `100,000` msg/sec).
  - Mixed multi-request single-connection transport flow
  - Transport-level persistence checks (wire produce -> disk -> restart -> fetch)
  - Offset transport matrix checks (legacy and flexible versions), error-code mapping matrix, and restart recovery
  - Group transport matrix checks (legacy + flexible headers, rebalance cycles, member churn, per-member leave errors, and assignment durability across restart)
  - Transport offset commit storm workload (`2,000` commits) with restart-recovery verification
  - Fetch wire behavior checks (topic name/topic ID routing, ReplicaState and tagged-field gates, out-of-range and unknown-partition mapping)
  - Fetch isolation behavior checks (`READ_COMMITTED` filtering, LSO updates, aborted transaction list encoding)
  - Fetch response top-level tag decoding checks for NodeEndpoints (v16+)
  - Rust-native replication integration checks: leader append -> follower catch-up, ISR shrink/recovery, leader failover + epoch bump
  - Rust-native metadata publishing integration checks: replication leader/ISR/epoch changes mirrored into KRaft metadata image
  - Metadata publication safety checks when quorum majority is unavailable (no partial replication metadata commits)
  - Realistic multi-topic workload checks for metadata image/replication-state convergence under failover and follower outage
  - Large payload replication roundtrip checks
  - Optional half-million-record three-node replication stress test (ignored by default)
  - Optional million-record metadata publication + failover stress test (ignored by default)
  - Kafka-ported `ApiVersions`/`Produce` parity matrix checks (version gates, header versions, record-error field gating)
  - High-volume persistent routing stress test with restart (`5,000` records per partition)
  - Optional million-scale persistent stress test (`1,000,000` records total, ignored by default)

## Commands

```bash
cd rafka
cargo test --workspace
```

```bash
cd rafka
cargo test -p rafka-broker --test security_transport_integration
```

```bash
cd rafka
cargo test -p rafka-broker --test transport_integration transport_acl_
```

```bash
cd rafka
cargo test -p rafka-broker --test async_transport_integration
```

```bash
cd rafka
./scripts/check_behavior.sh
```

```bash
cd rafka
./scripts/generate_protocol_messages.sh
```

```bash
cd rafka
./scripts/check_jvm_compat.sh
```

```bash
cd rafka
./scripts/run_perf_harness.sh --mode both --duration-secs 10 --target-rates 1000,10000,100000
```

```bash
cd rafka
cargo test -p rafka-broker --test persistent_routing -- --ignored stress_one_million_records_across_partitions
```

```bash
cd rafka
cargo test -p rafka-broker --test replication_integration
```

```bash
cd rafka
cargo test -p rafka-broker --test replication_integration -- --ignored stress_replication_half_million_records_three_nodes
```

```bash
cd rafka
cargo test -p rafka-broker --test metadata_publishing_integration
```

```bash
cd rafka
cargo test -p rafka-broker --test metadata_publishing_integration -- --ignored stress_metadata_publication_one_million_records_and_failovers
```

```bash
cd rafka
cargo test -p rafka-coordinator --test workload_integration
```

```bash
cd rafka
cargo test -p rafka-coordinator --test workload_integration -- --ignored stress_million_offset_commits_survive_restart
```

```bash
cd rafka
cargo test -p rafka-coordinator stress_parallel_consumer_group_churn_million_events -- --ignored
```

## Near-term test additions

- File-backed storage compatibility tests against real Kafka segment fixtures.
- Fuzzing for protocol and record parsing.
