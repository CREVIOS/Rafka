# Rafka Sprint Plan

This plan converts the phase roadmap into sprint-executable work with strict
Kafka-compatibility and Rust-native implementation requirements.

## Planning Rules

- Scope for implementation track is `rafka-next/` (keep current `rafka/` as stable baseline).
- Runtime logic remains Rust-native end-to-end.
- Kafka protocol compatibility is mandatory for all shipped APIs/versions.
- Every sprint must exit with passing quality gates, not partial handoffs.

## Sprint Cadence

- Sprint length: 2 weeks.
- Planning: Day 1.
- Mid-sprint review: Day 6 or 7.
- Hardening and release candidate cut: Days 9-10.
- Demo plus retro: Day 10.

## Global Definition of Done

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `./scripts/check_behavior.sh`
- All newly added API/version matrix tests pass.
- New metrics for changed critical paths are exported and asserted in tests.
- No net regression in baseline perf classes (`1K`, `10K`, `100K` msg/sec).

## Global Non-Functional Gates

- Compatibility gate: supported API keys/versions and header versions are snapshot-tested.
- Reliability gate: restart recovery tests for any new durable state.
- Security gate: negative tests for malformed requests and unauthorized access.
- Performance gate: p95 and p99 latency regression stays within sprint budget.

## Sprint Timeline (12 Sprints)

| Sprint | Theme | Primary Output |
|---|---|---|
| S0 | Freeze + Parity Baseline | `rafka-next/` created, parity snapshot + CI guard |
| S1 | Protocol Expansion A | Generated codecs: CreateTopics/DeleteTopics/DescribeConfigs |
| S2 | Protocol Expansion B | Generated codecs: AlterConfigs/IncrementalAlterConfigs/CreatePartitions |
| S3 | Admin Service A | Broker/controller-style admin routing + Create/Delete/Describe handlers |
| S4 | Admin Service B | Alter/IncrementalAlter/CreatePartitions handlers + durability |
| S5 | ACL Parity A | ACL CRUD wire APIs + durable ACL store |
| S6 | ACL Parity B | ACL implication semantics + deny-by-default + super-users |
| S7 | Quotas A | Request/produce/fetch quota manager + throttle_time_ms |
| S8 | Quotas B | Fairness/starvation protections + quota storm hardening |
| S9 | Performance A | Async transport completion + bounded backpressure |
| S10 | Performance B | Compression parity + zero-copy fetch fast path |
| S11 | Hardening + Release | Data-path recovery hardening + operability + canary drills |

## Sprint Backlog Detail

## S0 - Freeze + Parity Baseline

### Goals

- Create isolated execution branch/folder `rafka-next/`.
- Snapshot current compatibility and performance baselines.
- Gate all future changes against this baseline.

### Stories

- `S0-1` Create `rafka-next/` from current tested state.
- `S0-2` Add parity snapshot command and artifact format (JSON + CSV).
- `S0-3` Capture baseline outputs for transport, security, and perf harness.
- `S0-4` Add CI job that fails on unintended parity drift.

### Exit Criteria

- Baseline artifact is committed and reproducible.
- Existing suites pass unchanged on `rafka-next/`.

## S1 - Protocol Expansion A

### Goals

- Add generated protocol codecs for:
- `CreateTopics` (`v2-v7`)
- `DeleteTopics` (`v1-v6`)
- `DescribeConfigs` (`v1-v4`)

### Stories

- `S1-1` Extend `scripts/generate_protocol_messages.sh` targets.
- `S1-2` Regenerate protocol types and version gates from Kafka message specs.
- `S1-3` Add encode/decode matrix tests for all versions and headers.
- `S1-4` Add malformed/truncated/flexible-tag negative tests.

### Exit Criteria

- All 3 APIs generated and tested across full supported version range.
- Transport layer uses generated codecs for these APIs.

## S2 - Protocol Expansion B

### Goals

- Add generated protocol codecs for:
- `AlterConfigs` (`v0-v2`)
- `IncrementalAlterConfigs` (`v0-v1`)
- `CreatePartitions` (`v0-v3`)

### Stories

- `S2-1` Extend codegen mappings for remaining control-plane core APIs.
- `S2-2` Add header-version and flexible-version gates from JSON specs.
- `S2-3` Add strict field-gating tests (presence/absence by version).
- `S2-4` Add cross-version wire roundtrip tests.

### Exit Criteria

- Generated codecs fully replace any manual encoding for these APIs.
- Matrix tests green for all valid versions and invalid-version paths.

## S3 - Admin Service A

### Goals

- Build Rust-native admin service boundaries with Kafka-style routing split.
- Implement wire path for CreateTopics/DeleteTopics/DescribeConfigs.

### Stories

- `S3-1` Add `AdminService` trait and module boundaries.
- `S3-2` Implement broker-side validation and controller mutation abstraction.
- `S3-3` Implement handlers for CreateTopics/DeleteTopics/DescribeConfigs.
- `S3-4` Add e2e wire tests including restart durability for created topics/config state.

### Exit Criteria

- Transport flow is decode -> authn -> authz -> service -> encode.
- Admin API wire tests pass for legacy and flexible headers.

## S4 - Admin Service B

### Goals

- Implement `AlterConfigs`, `IncrementalAlterConfigs`, and `CreatePartitions` end-to-end.
- Harden controller unavailability and retry paths.

### Stories

- `S4-1` Add handlers and state mutations for alter/create-partitions paths.
- `S4-2` Add admin error mapping matrix (invalid config, unknown topic, auth failures).
- `S4-3` Add restart and partial-failure recovery tests.
- `S4-4` Add admin API latency and error counters.

### Exit Criteria

- End-to-end matrix for all 6 admin APIs is green across supported versions.
- Restart recovery validates durable admin mutations.

## S5 - ACL Parity A

### Goals

- Add ACL management APIs and durable ACL persistence.
- Keep existing enforcement logic and extend to full CRUD surface.

### Stories

- `S5-1` Add generated protocol support for `DescribeAcls`, `CreateAcls`, `DeleteAcls`.
- `S5-2` Implement ACL durable store with startup replay/recovery.
- `S5-3` Implement API handlers and error mapping parity.
- `S5-4` Add integration matrix for literal/prefixed/wildcard resource patterns.

### Exit Criteria

- ACL CRUD works over wire and survives restart.
- Authorization decisions consume durable ACL state.

## S6 - ACL Parity B

### Goals

- Match ACL implication semantics and production policy modes.

### Stories

- `S6-1` Implement operation implication rules:
- `READ/WRITE/DELETE/ALTER -> DESCRIBE`
- `ALTER_CONFIGS -> DESCRIBE_CONFIGS`
- `S6-2` Add deny-by-default mode and bootstrap `super.users`.
- `S6-3` Add optional compatibility mode (`allow-if-no-acl`).
- `S6-4` Add precedence matrix tests: explicit deny vs broad allow, host filtering.

### Exit Criteria

- ACL semantics matrix passes for allow/deny/wildcard/implication/restart.

## S7 - Quotas A

### Goals

- Implement quota manager for request, produce bytes, and fetch bytes.
- Return and enforce `throttle_time_ms` on compatible APIs.

### Stories

- `S7-1` Implement per principal/client-id quota resolution and precedence.
- `S7-2` Implement rolling-window accounting and throttle-time calculation.
- `S7-3` Integrate delayed response path for throttled requests.
- `S7-4` Add API compatibility tests for throttle field behavior by version.

### Exit Criteria

- Quota violations produce correct throttle behavior and metrics.

## S8 - Quotas B

### Goals

- Harden quotas under high concurrency and avoid starvation.

### Stories

- `S8-1` Add fairness scheduler for throttled channels.
- `S8-2` Add storm tests with high client churn and mixed request classes.
- `S8-3` Add anti-starvation invariants and regression tests.
- `S8-4` Expand quota metrics (`violations`, `throttle_duration`, `queue_depth`).

### Exit Criteria

- Quota stress suite passes without starvation regressions.

## S9 - Performance A

### Goals

- Complete async transport migration and backpressure protections.

### Stories

- `S9-1` Remove remaining thread-per-connection paths.
- `S9-2` Add bounded per-connection queues and global admission control.
- `S9-3` Add load-shedding behavior tests for overload conditions.
- `S9-4` Verify protocol compatibility matrix remains unchanged.

### Exit Criteria

- Async path is default.
- Throughput and p99 latency improve or remain within accepted budget.

## S10 - Performance B

### Goals

- Add compression parity and optional zero-copy fetch optimizations.

### Stories

- `S10-1` Implement record batch compression: `gzip`, `snappy`, `lz4`, `zstd`.
- `S10-2` Add Linux zero-copy fetch fast path (`sendfile`) behind feature gate.
- `S10-3` Add optional `io_uring` fast path behind explicit feature flag.
- `S10-4` Add compatibility and performance regression tests for all codec paths.

### Exit Criteria

- Compression compatibility tests pass.
- Feature-gated zero-copy path is stable and benchmarked.

## S11 - Hardening + Release

### Goals

- Final reliability, security, and operability gates before production pilot.

### Stories

- `S11-1` Complete remaining txn/data-path recovery edge cases.
- `S11-2` Expand Prometheus metrics and add SLO dashboards/runbooks.
- `S11-3` Add upgrade/downgrade and canary rollback drills in KRaft mode.
- `S11-4` Execute long soak and chaos test suite.

### Exit Criteria

- Production readiness checklist passes.
- Canary drill and rollback drill pass with documented runbooks.

## Test Program by Sprint

- Unit tests for parser/codec/state-machine invariants.
- Integration wire tests for every added API and version range.
- Restart recovery tests for every new durable store.
- Negative tests for malformed payloads and unauthorized operations.
- Stress suites for churn, storms, and concurrency hot paths.
- Perf harness checks for throughput and latency at `1K/10K/100K/1M`.
- Optional long-run ignored tests for million-scale soak.

## Release Milestones

- `M1` after S2: protocol expansion complete.
- `M2` after S4: admin control-plane core complete.
- `M3` after S6: ACL production semantics complete.
- `M4` after S8: quotas/throttling production baseline complete.
- `M5` after S10: performance feature track complete.
- `M6` after S11: release candidate for production pilot.

## Risk Register

- Protocol drift risk: mitigated via generated codecs + version matrix tests.
- Behavior drift risk: mitigated via parity snapshots and CI regression guards.
- Performance regressions: mitigated via mandatory per-sprint perf gates.
- Security misconfiguration risk: mitigated via fail-fast config lint and negative tests.
- Scope creep risk: mitigated via strict sprint exit criteria and milestone freeze windows.

## Immediate Start Checklist (This Week)

- Create `rafka-next/` and copy baseline artifacts.
- Implement parity snapshot command and CI gate.
- Finalize S1 backlog tickets with API-version acceptance tests.
- Reserve benchmark hardware profile for S7-S11 repeatable measurements.
