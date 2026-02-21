# Rafka TODO

## Phase 0: Foundation

- [x] Create Rust workspace and core crates.
- [x] Establish research-first workflow (`RESEARCH_SOURCES.md`).
- [x] Add initial protocol primitives (varint, record batch header).
- [x] Add initial storage log behavior (append/fetch/out-of-range).
- [x] Add initial coordinator state behavior (join/heartbeat/evict).
- [x] Add behavior checks and tests running in CI-ready command form.

## Phase 1: Protocol + Codec Parity

- [x] Generate protocol registry from Kafka JSON message specs.
- [x] Generate full protocol request/response Rust data types from Kafka specs.
- [x] Add version-aware request/response encoding and decoding.
- [x] Port classic-group wire APIs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) to generated protocol codecs and remove manual transport codecs.
- [x] Differential tests against JVM serialization behavior.

## Phase 2: Broker Data Path

- [x] Implement TCP listener and request dispatcher.
- [x] Add `Fetch` API wire request/response support in transport dispatcher.
- [x] Implement Produce/Fetch request handlers with topic/partition routing.
- [x] Add file-backed segmented log with index files.
- [x] Implement broker crash recovery from log/index.

## Phase 3: Replication + KRaft

- [x] Implement leader/follower replication fetch loops.
- [x] Implement Rust-native leader epoch updates and ISR tracking with failover tests.
- [x] Implement KRaft metadata quorum and snapshotting.
- [x] Implement metadata publishing parity (post-epoch/ISR updates).

## Phase 4: Coordinators

- [x] Group coordinator offset parity (`OffsetCommit`/`OffsetFetch` wire path + durable offset state).
- [x] Offset coordinator integration matrix (legacy/flexible versions, error codes, restart recovery).
- [x] Group coordinator rebalance parity (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` wire behavior + durable assignment state).
- [x] Transaction coordinator parity (`InitProducerId`/`EndTxn`/`WriteTxnMarkers` wire path + durable txn state and marker handling).
- [x] Transactional fetch visibility parity (`READ_COMMITTED`/`READ_UNCOMMITTED`, LSO, aborted transaction list).
- [ ] Share coordinator behavior parity where needed.

## Phase 5: Security + Ops

- [ ] TLS/SASL/ACL parity.
- [x] TLS listener parity baseline (`rustls`) with handshake success/failure integration tests.
- [x] SASL parity baseline (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) with wire-level integration tests.
- [x] ACL authorization baseline (principal/resource/action enforcement with Kafka-compatible error mapping for topic/group/cluster/transactional-id paths).
- [ ] Quotas and throttling behavior.
- [x] Prometheus transport metrics baseline (API, partition, SASL, TLS counters) with integration assertions.
- [x] Rust-native performance harness for produce/fetch throughput and latency at 1K/10K/100K msg/sec classes.
- [x] Async transport baseline (`tokio`) with wire-compatible request framing and concurrent connection integration tests.
- [ ] Admin operation parity.

## Phase 6: Rust Clients

- [ ] Rust producer/consumer/admin clients.
- [ ] Interop with Java broker and Rust broker.
- [ ] Idempotent and transactional client support.

## Phase 7: Connect + Streams

- [ ] Connect runtime in Rust with priority connectors.
- [ ] Streams runtime/DSL compatibility layer.
- [ ] Migration tools for existing deployments.

## Phase 8: Cutover

- [ ] Canary and phased production migration.
- [ ] Soak, failover, and rollback drill validation.
- [ ] Full production sign-off and old-stack decommission.
