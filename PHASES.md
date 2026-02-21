# Rafka Phases

## Phase 0: Bootstrap (done)

- Workspace setup
- Initial subsystem boundaries
- Foundational behavior tests

## Phase 1: Wire Protocol

- Kafka API schemas and versioning
- Encoder/decoder compatibility
- Differential JVM parity tests

## Phase 2: Data Plane

- Produce/Fetch handlers
- Segmented storage engine
- Recovery and correctness validation

## Phase 3: Control Plane

- KRaft quorum
- Metadata image/state updates
- ISR and leadership transitions

## Phase 4: Coordination

- Group coordinator
- Transaction coordinator
- Offset and transaction log compatibility

## Phase 5: Platform Readiness

- Security (TLS/SASL/ACL)
- Quotas, metrics, and admin ops
- Stability, soak, and performance gates

## Phase 6: Ecosystem Rewrite

- Rust clients
- Connect runtime
- Streams runtime

## Phase 7: Migration and Cutover

- Progressive rollout
- Observability-driven gates
- Rollback and decommission plan

