# Rafka Ecosystem Plan

This document describes the plan for building the Kafka Connect and Kafka Streams
equivalents in Rafka. The goal is not to clone Java implementations but to design
clean, idiomatic Rust alternatives that build on Rafka's existing foundation.

---

## Why This Is Hard

Kafka's ecosystem advantage is not just features — it is 10+ years of:
- 200+ production-tested connectors (S3, Postgres, Elasticsearch, Snowflake, etc.)
- A stable SPI that plugin authors depend on
- Streams exactly-once semantics built on top of mature transactions
- Tooling (Kafka Connect REST API, monitoring, offset management)

We cannot shortcut this. The plan below is honest about phasing and scope.

---

## Part 1: Rafka Connect

### Architecture Overview

```
┌─────────────────────────────────────────────┐
│               ConnectWorker                  │  ← runs in a thread/process
│  ┌──────────────────────────────────────┐   │
│  │         ConnectTask (per partition)   │   │
│  │  ┌─────────────┐  ┌──────────────┐  │   │
│  │  │  SourceTask │  │  SinkTask    │  │   │
│  │  └─────────────┘  └──────────────┘  │   │
│  └──────────────────────────────────────┘   │
│                                             │
│  ConnectorRegistry (plugin registry)        │
│  OffsetStore (per-connector offset WAL)     │
└─────────────────────────────────────────────┘
          │                    │
          ▼                    ▼
    Rafka broker          External system
    (produce/fetch)       (DB, S3, ES, etc.)
```

### Core Traits (to add to `crates/connect/src/lib.rs`)

```rust
/// A source connector reads from an external system and produces to Rafka.
pub trait SourceConnector: Send + 'static {
    type Config: serde::de::DeserializeOwned;

    fn name() -> &'static str;
    fn start(&mut self, config: Self::Config) -> Result<(), ConnectError>;
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError>;
    fn commit_offsets(&mut self, offsets: &ConnectorOffsets) -> Result<(), ConnectError>;
    fn stop(&mut self);
}

/// A sink connector reads from Rafka and writes to an external system.
pub trait SinkConnector: Send + 'static {
    type Config: serde::de::DeserializeOwned;

    fn name() -> &'static str;
    fn start(&mut self, config: Self::Config) -> Result<(), ConnectError>;
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), ConnectError>;
    fn flush(&mut self) -> Result<ConnectorOffsets, ConnectError>;
    fn stop(&mut self);
}

/// A record produced from an external source.
pub struct SourceRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
    pub source_offset: Vec<u8>, // opaque offset for the source system
}

/// A record to be delivered to an external sink.
pub struct SinkRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
}
```

### ConnectWorker

```rust
pub struct ConnectWorkerConfig {
    pub broker_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub max_tasks_per_connector: usize,
    pub poll_interval_ms: u64,
    pub batch_size: usize,
}

pub struct ConnectWorker {
    config: ConnectWorkerConfig,
    // per-connector persistent offset store (separate WAL per connector name)
    offset_store: ConnectorOffsetStore,
}
```

### Connector Offset Store

Each connector gets its own WAL under `data_dir/connect/<connector-name>/offsets.wal`.
The format mirrors the existing `OffsetCoordinator` WAL: binary, append-only,
torn-tail truncation on startup.

### Priority Connectors to Build

#### Phase A – File-based (no external deps, proves the model works)
- [ ] **FileSource**: reads lines from a file, produces to Rafka topic
- [ ] **FileSink**: consumes from Rafka topic, writes to a file

#### Phase B – Object storage
- [ ] **S3Sink**: batch upload to S3 using `aws-sdk-s3` (Rust SDK)
  - Parquet and JSON output formats
  - S3 prefix partitioning by date/hour
  - Exactly-once: commit offsets only after successful S3 upload
- [ ] **S3Source**: read objects from S3 prefix, produce to Rafka

#### Phase C – Databases
- [ ] **PostgresSink**: COPY or batch INSERT using `tokio-postgres`
  - Upsert mode (INSERT ON CONFLICT UPDATE)
  - Schema auto-create from record schema
- [ ] **PostgresSource** (CDC): read WAL via logical replication (pgoutput plugin)
  - Produces Debezium-compatible envelope format
- [ ] **SqliteSink**: local SQLite sink for development/test use (no deps)

#### Phase D – Search / Analytics
- [ ] **ElasticsearchSink**: bulk index to Elasticsearch using HTTP client
- [ ] **ClickHouseSink**: batch insert using `clickhouse-rs`

#### Phase E – Messaging interop
- [ ] **KafkaMirrorSource**: reads from an Apache Kafka cluster, writes to Rafka
  - Uses `rdkafka` Rust bindings as the consumer side
  - Enables progressive migration: run Rafka in parallel, mirror from Kafka

---

## Part 2: Rafka Streams

### Architecture Overview

```
┌──────────────────────────────────────────────┐
│               StreamsApp                      │
│                                              │
│  topology: DAG of processors                 │
│  ┌─────────────┐   ┌───────────────────┐    │
│  │  KStream<K,V>│──▶│  KTable<K,V>     │    │
│  └─────────────┘   └───────────────────┘    │
│         │                   │               │
│         ▼                   ▼               │
│  ┌─────────────┐   ┌───────────────────┐    │
│  │  Processor  │   │  StateStore       │    │
│  └─────────────┘   └───────────────────┘    │
└──────────────────────────────────────────────┘
        │                     │
        ▼                     ▼
   Rafka broker         local RocksDB /
   (produce/fetch)      in-memory store
```

### Core Types (to add to `crates/streams/src/lib.rs`)

```rust
/// A stream of key-value records from a Rafka topic.
pub struct KStream<K, V> {
    topology: Arc<Topology>,
    source_topic: String,
    _marker: PhantomData<(K, V)>,
}

/// A changelog-backed table of the latest value per key.
pub struct KTable<K, V> {
    topology: Arc<Topology>,
    source_topic: String,
    state_store: Arc<dyn StateStore<K, V>>,
}

/// Pluggable state store backend.
pub trait StateStore<K, V>: Send + Sync {
    fn get(&self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V);
    fn delete(&mut self, key: &K);
    fn all(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;
    fn flush(&mut self) -> Result<(), StoreError>;
}
```

### StreamsBuilder API

```rust
let builder = StreamsBuilder::new(StreamsConfig {
    broker_addr: "127.0.0.1:9092".parse()?,
    application_id: "word-count-app".to_string(),
    data_dir: "/var/rafka/streams".into(),
});

// Word count example
let stream: KStream<String, String> = builder.stream("text-input");

let counts = stream
    .flat_map(|_k, v| v.split_whitespace().map(|w| (w.to_string(), 1_i64)))
    .group_by_key()
    .count(); // returns KTable<String, i64>

counts.to_topic("word-counts");

let app = builder.build()?;
app.start()?;
```

### DSL Operations

#### KStream operations
- [ ] `map(fn)` — transform each record
- [ ] `flat_map(fn)` — produce 0-N records per input record
- [ ] `filter(predicate)` — drop records not matching predicate
- [ ] `group_by_key()` — group by the existing key → KGroupedStream
- [ ] `group_by(fn)` — re-key then group → KGroupedStream
- [ ] `merge(other)` — union two streams
- [ ] `branch(predicates)` — split stream into N branches
- [ ] `to_topic(name)` — sink to a Rafka topic

#### KGroupedStream operations
- [ ] `count()` → KTable<K, i64>
- [ ] `reduce(fn)` → KTable<K, V>
- [ ] `aggregate(init, fn)` → KTable<K, A>
- [ ] `windowed(window)` → WindowedKStream

#### KTable operations
- [ ] `filter(predicate)`
- [ ] `map_values(fn)`
- [ ] `join(other, fn)` — table-table join
- [ ] `stream()` — convert back to KStream (changelog view)
- [ ] `to_topic(name)` — materialise changelog to a topic

#### Windowing
- [ ] `TumblingWindow(size)` — non-overlapping fixed windows
- [ ] `HoppingWindow(size, advance)` — overlapping windows
- [ ] `SessionWindow(inactivity_gap)` — session-based grouping

### Exactly-Once Semantics

Streams exactly-once requires:
1. Transactional produces (already implemented in Rafka)
2. Atomic offset commits inside the same transaction
3. Fenced epoch handling on task reassignment

This is achievable because Rafka already has:
- `TransactionCoordinator` with PID/epoch management
- `OffsetCoordinator` with generation fencing
- `end_txn` + `write_txn_markers` pipeline

### State Store Backends

#### Phase A: In-memory (development)
- `BTreeMap<K, V>` — exact match, no persistence
- Replayable from source topic on startup

#### Phase B: Persistent (production)
- **RocksDB** via `rust-rocksdb` crate — log-structured, changelog-backed
- Checkpoint snapshots under `data_dir/streams/<app-id>/<store-name>/`
- Standby replication: stream changelog topic to replica node

---

## Part 3: Migration Tooling

For users migrating from Apache Kafka to Rafka, the KafkaMirrorSource connector
(Phase E above) enables a zero-downtime migration pattern:

```
Step 1: Run Rafka alongside existing Kafka cluster
Step 2: Deploy KafkaMirrorSource → copies all topics to Rafka
Step 3: Validate parity (lag, offsets, data checksums)
Step 4: Switch producers to Rafka (one topic at a time)
Step 5: Switch consumers to Rafka (one consumer group at a time)
Step 6: Decommission Kafka
```

Tools needed:
- [ ] `rafka-mirror` CLI: start/stop topic mirroring
- [ ] `rafka-verify` CLI: compare topic offsets and record hashes between clusters
- [ ] `rafka-cutover` CLI: automate producer/consumer config update

---

## Delivery Timeline (Rough Phases)

| Phase | Contents | Prerequisite |
|---|---|---|
| **Connect v0** | Trait definitions, ConnectWorker, FileSource/FileSink | Phase 5 complete |
| **Connect v1** | S3Sink, PostgresSink, offset store, REST config API | Connect v0 |
| **Connect v2** | PostgresSource (CDC), ElasticsearchSink | Connect v1 |
| **Connect v3** | KafkaMirrorSource, migration CLI | Connect v2 |
| **Streams v0** | StreamsBuilder, KStream map/filter/to_topic | Phase 6 clients |
| **Streams v1** | KGroupedStream count/reduce, in-memory stores | Streams v0 |
| **Streams v2** | KTable joins, windowing, RocksDB state | Streams v1 |
| **Streams v3** | Exactly-once, standby replicas | Streams v2 |

---

## What NOT to Build (at first)

- A Connect REST API compatible with Confluent's API — build a simpler TOML-config
  model first; REST can come later when there are real users
- A Streams SQL layer (KSQL equivalent) — this is enormous scope; DSL first
- Schema Registry — use raw bytes initially; add Avro/Protobuf support later
- Cloud-hosted connector marketplace — build the runtime first

---

## Key Decisions

1. **Async runtime**: Streams and Connect workers should use `tokio`. This is the
   right time to introduce it — isolated in new crates, not retrofitting into the
   existing blocking broker transport.

2. **Connector plugins as separate crates**: each connector is a `rafka-connect-<name>`
   crate. The core `rafka-connect` crate only contains the traits and worker.

3. **No JNI/FFI for connectors**: unlike Kafka which allows JVM connectors,
   Rafka Connect is Rust-only. Users wanting Java connectors should use a
   KafkaMirrorSource to bridge.

4. **State store is pluggable from day one**: the `StateStore` trait must be
   defined before any concrete implementation. This prevents lock-in.

5. **Tests first**: every connector must have an integration test that spins up
   a local Rafka broker (using `TransportServer::bind`) and verifies end-to-end
   record delivery. The test pattern from `transport_integration.rs` is the model.
