# Rafka

`rafka` is the Rust rewrite workspace for Apache Kafka replication work.

Planning docs:

- `SPRINT_PLAN.md` - sprint-by-sprint execution plan with quality gates.
- `TODO.md` - phase-level checklist.
- `TEST_PLAN.md` - test inventory and execution commands.

CI/CD workflows:

- `.github/workflows/ci.yml` - build, fmt, clippy, behavior tests, and key transport/security matrices.
- `.github/workflows/extended-tests.yml` - scheduled integration matrix plus optional manual long stress suites.
- `.github/workflows/perf.yml` - scheduled/manual release-mode perf harness run with CSV artifact upload.
- `.github/workflows/release.yml` - tag-triggered release checks, binary packaging, and GitHub release publishing.

Current status:

- Workspace and subsystem crates created.
- Initial protocol/log/coordinator behavior implemented.
- Protocol registry generated from Kafka message specs.
- Rust message structs generated from Kafka specs for `ApiVersions`, `Produce`, `OffsetCommit`, `OffsetFetch`, and classic-group APIs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`).
- Version-aware native codec implemented for those APIs (including flexible versions and tagged fields).
- Spec-translation checks validate codec/version behavior against Kafka source schemas.
- Rust-native matrix tests validate all supported protocol versions and error/truncation paths for `ApiVersions` and `Produce`.
- Optional JVM differential tests compare Rust wire bytes to Kafka Java serializer bytes for parity assurance.
- File-backed segmented storage engine implemented in Rust with `.log`/`.index` files, segment rolling, and startup recovery.
- Persistent partition routing implemented for broker produce/fetch paths (`topic` + `partition`) with restart recovery.
- TCP transport listener + request dispatcher implemented in Rust for `ApiVersions` and `Produce` wire paths.
- TCP transport `Fetch` wire path implemented in Rust (v4-v18, including flexible versions, topic IDs, replica-state tags, and NodeEndpoints support).
- TCP transport `OffsetCommit`/`OffsetFetch` wire paths implemented in Rust with version/header gates and Kafka-style error mapping.
- TCP transport classic group rebalance wire paths implemented in Rust for `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` with legacy/flexible header gates.
- TCP transport transaction wire paths implemented in Rust for `InitProducerId`/`EndTxn`/`WriteTxnMarkers` with legacy/flexible header gates.
- Durable transaction coordinator state added in Rust (producer IDs/epochs, ongoing transactions, marker application, and restart recovery).
- Transaction-aware fetch behavior implemented (`READ_COMMITTED` filtering, LSO handling, aborted transaction metadata in fetch responses).
- Durable classic group coordinator state added in Rust (membership + assignment WAL recovery across restart).
- Randomized protocol roundtrip tests and required-field negative matrices added for wire-level robustness.
- Stress and leak-guard tests added for storage and partitioned broker behavior, including restart and corruption paths.
- Transport integration tests cover wire framing, header/version compatibility, malformed frame handling, and disk persistence roundtrips.
- Rust-native transport security slice added: `rustls` TLS listener + wire-level SASL (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) with post-auth dispatch gating.
- Rust-native ACL authorization baseline added in transport dispatch (topic/group/cluster/transactional-id resource checks with Kafka-compatible error codes and SASL-principal integration).
- Prometheus transport metrics surface added for API request/error counts, per-partition events/bytes, SASL auth results, and TLS handshake outcomes.
- Rust-native performance harness added for produce/fetch throughput + latency baselines at 1K/10K/100K msg/sec classes.
- Async transport baseline added with `tokio` listener + concurrent connection handling while reusing Kafka-compatible request dispatch.
- Coordinator workload integration tests added for rebalance/member churn, lag catch-up progression, commit storms, and durable restart recovery.
- Optional million-scale parallel consumer-group churn stress test added in coordinator (ignored by default).
- Rust-native replication core added with leader/follower fetch loops, ISR tracking, leader-epoch failover, and integration tests.
- Rust-native metadata quorum added with snapshot read/write and majority commit checks for KRaft-style controller behavior.
- Replication control-plane metadata publishing added so leader/ISR/epoch changes are persisted into KRaft metadata records.
- Kafka-test-ported parity checks added for `ApiVersions` and `Produce` version behavior.
- High-volume and optional million-scale broker stress tests added for scale validation.
- Behavior checks and tests wired with `cargo test --workspace`.
- Research-first workflow documented in `RESEARCH_SOURCES.md`.

Primary command:

```bash
cd rafka
cargo test --workspace
```

Generate protocol registry from Kafka JSON specs:

```bash
cd rafka
./scripts/generate_protocol_registry.sh
```

Generate Rust protocol message structs from Kafka JSON specs:

```bash
cd rafka
./scripts/generate_protocol_messages.sh
```

For a single command wrapper:

```bash
./scripts/check_behavior.sh
```

Run JVM differential wire-compat tests (test-only oracle; runtime remains Rust-native):

```bash
./scripts/check_jvm_compat.sh
```

Run replication integration coverage:

```bash
cd rafka
cargo test -p rafka-broker --test replication_integration
```

Run long replication stress (ignored by default):

```bash
cd rafka
cargo test -p rafka-broker --test replication_integration -- --ignored stress_replication_half_million_records_three_nodes
```

Run metadata publishing integration coverage:

```bash
cd rafka
cargo test -p rafka-broker --test metadata_publishing_integration
```

Run long metadata publishing stress (ignored by default):

```bash
cd rafka
cargo test -p rafka-broker --test metadata_publishing_integration -- --ignored stress_metadata_publication_one_million_records_and_failovers
```

Run transport security integration coverage:

```bash
cd rafka
cargo test -p rafka-broker --test security_transport_integration
```

Run ACL integration coverage (topic/group/transaction paths):

```bash
cd rafka
cargo test -p rafka-broker --test transport_integration transport_acl_
```

Run async transport integration coverage:

```bash
cd rafka
cargo test -p rafka-broker --test async_transport_integration
```

Run Rust-native performance harness (release mode):

```bash
cd rafka
./scripts/run_perf_harness.sh --mode both --duration-secs 10 --target-rates 1000,10000,100000
```

The harness emits CSV rows with achieved throughput, target attainment percent, and latency percentiles (`p50/p95/p99/max` in microseconds).
