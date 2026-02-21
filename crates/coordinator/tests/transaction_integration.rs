#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rafka_coordinator::{
    EndTxnInput, InitProducerInput, InitProducerResult, TransactionCoordinator,
    TransactionCoordinatorError, TransactionStoreConfig, WriteTxnMarkerInput,
    WriteTxnMarkerTopicInput,
};
use rafka_storage::Record;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "rafka-txn-integration-{label}-{millis}-{}-{counter}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn open(dir: &Path) -> TransactionCoordinator {
    TransactionCoordinator::open(dir, TransactionStoreConfig::default()).expect("open coordinator")
}

fn init(coordinator: &mut TransactionCoordinator, tx_id: &str) -> InitProducerResult {
    coordinator
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some(tx_id.to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("init_producer")
}

fn make_record(offset: i64, value_byte: u8) -> Record {
    Record {
        offset,
        timestamp_ms: offset * 10,
        key: Vec::new(),
        value: vec![value_byte],
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Full commit: records become visible after end_txn(committed=true)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn committed_records_visible_in_read_committed() {
    let temp = TempDir::new("committed-visible");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-commit");
    c.record_produce("tx-commit", "events", 0, 0)
        .expect("produce 0");
    c.record_produce("tx-commit", "events", 0, 1)
        .expect("produce 1");

    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-commit".to_string(),
        producer_id: p.producer_id,
        producer_epoch: p.producer_epoch,
        committed: true,
    })
    .expect("commit");

    let records = vec![make_record(0, 0), make_record(1, 1)];
    let result = c.read_committed("events", 0, 0, 2, &records);

    // After commit, no ongoing transaction, so last_stable_offset == high_watermark
    assert_eq!(result.last_stable_offset, 2);
    assert_eq!(
        result.visible_records.len(),
        2,
        "both records should be visible"
    );
    assert!(result.aborted_transactions.is_empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Full abort: aborted records hidden, aborted_transactions list populated
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn aborted_records_hidden_in_read_committed() {
    let temp = TempDir::new("aborted-hidden");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-abort");
    c.record_produce("tx-abort", "payments", 0, 5)
        .expect("produce 5");
    c.record_produce("tx-abort", "payments", 0, 6)
        .expect("produce 6");

    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-abort".to_string(),
        producer_id: p.producer_id,
        producer_epoch: p.producer_epoch,
        committed: false,
    })
    .expect("abort");

    let records = vec![make_record(5, 5), make_record(6, 6)];
    let result = c.read_committed("payments", 0, 0, 7, &records);

    assert!(
        result.visible_records.is_empty(),
        "aborted records must be hidden"
    );
    assert_eq!(result.aborted_transactions.len(), 1);
    assert_eq!(result.aborted_transactions[0].producer_id, p.producer_id);
    assert_eq!(result.aborted_transactions[0].first_offset, 5);
}

// ──────────────────────────────────────────────────────────────────────────────
// Ongoing transaction: records beyond LSO are not visible
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn ongoing_transaction_blocks_records_beyond_lso() {
    let temp = TempDir::new("ongoing-blocks-lso");
    let mut c = open(temp.path());

    let _p = init(&mut c, "tx-ongoing");
    // Produce to offsets 10 and 11 without committing
    c.record_produce("tx-ongoing", "orders", 0, 10)
        .expect("produce 10");
    c.record_produce("tx-ongoing", "orders", 0, 11)
        .expect("produce 11");

    // Produce another committed record at offset 5 (different topic)
    // For simplicity we check the same topic: LSO should be 10 (first unstable)
    let records = vec![make_record(5, 5), make_record(10, 10), make_record(11, 11)];
    let result = c.read_committed("orders", 0, 0, 12, &records);

    // LSO = min(first_unstable_offset=10, high_watermark=12) = 10
    assert_eq!(result.last_stable_offset, 10);
    // Only record at offset 5 is below LSO and not aborted → visible
    assert_eq!(result.visible_records.len(), 1);
    assert_eq!(result.visible_records[0].offset, 5);
    assert!(result.aborted_transactions.is_empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Multi-producer: one producer's abort does not affect another's committed records
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_producer_abort_isolates_from_committed() {
    let temp = TempDir::new("multi-producer-isolation");
    let mut c = open(temp.path());

    let p1 = init(&mut c, "tx-a");
    let p2 = init(&mut c, "tx-b");

    // tx-a writes offsets 0-1 and aborts
    c.record_produce("tx-a", "log", 0, 0)
        .expect("tx-a produce 0");
    c.record_produce("tx-a", "log", 0, 1)
        .expect("tx-a produce 1");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-a".to_string(),
        producer_id: p1.producer_id,
        producer_epoch: p1.producer_epoch,
        committed: false,
    })
    .expect("tx-a abort");

    // tx-b writes offsets 2-3 and commits
    c.record_produce("tx-b", "log", 0, 2)
        .expect("tx-b produce 2");
    c.record_produce("tx-b", "log", 0, 3)
        .expect("tx-b produce 3");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-b".to_string(),
        producer_id: p2.producer_id,
        producer_epoch: p2.producer_epoch,
        committed: true,
    })
    .expect("tx-b commit");

    let records: Vec<Record> = (0..4).map(|i| make_record(i, i as u8)).collect();
    let result = c.read_committed("log", 0, 0, 4, &records);

    // Only tx-b's records (offsets 2,3) should be visible
    assert_eq!(result.visible_records.len(), 2);
    assert_eq!(result.visible_records[0].offset, 2);
    assert_eq!(result.visible_records[1].offset, 3);
    assert_eq!(result.aborted_transactions.len(), 1);
    assert_eq!(result.aborted_transactions[0].producer_id, p1.producer_id);
    assert_eq!(result.aborted_transactions[0].first_offset, 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// write_txn_markers abort: partition marked aborted via marker API
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn write_txn_markers_abort_hides_records() {
    let temp = TempDir::new("write-txn-markers-abort");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-marker");
    c.record_produce("tx-marker", "topic-x", 2, 100)
        .expect("produce");
    c.record_produce("tx-marker", "topic-x", 2, 101)
        .expect("produce");

    // Abort via write_txn_markers instead of end_txn
    let results = c
        .write_txn_markers(vec![WriteTxnMarkerInput {
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: false,
            topics: vec![WriteTxnMarkerTopicInput {
                name: "topic-x".to_string(),
                partition_indexes: vec![2],
            }],
        }])
        .expect("write_txn_markers");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].producer_id, p.producer_id);
    assert!(
        results[0].topics[0].partitions[0].error.is_none(),
        "marker should succeed"
    );

    let records = vec![make_record(100, 100), make_record(101, 101)];
    let result = c.read_committed("topic-x", 2, 0, 102, &records);
    assert!(
        result.visible_records.is_empty(),
        "marker-aborted records must be hidden"
    );
    assert_eq!(result.aborted_transactions.len(), 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// write_txn_markers commit: records remain visible
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn write_txn_markers_commit_keeps_records_visible() {
    let temp = TempDir::new("write-txn-markers-commit");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-commit-marker");
    c.record_produce("tx-commit-marker", "output", 0, 50)
        .expect("produce");

    let results = c
        .write_txn_markers(vec![WriteTxnMarkerInput {
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: true,
            topics: vec![WriteTxnMarkerTopicInput {
                name: "output".to_string(),
                partition_indexes: vec![0],
            }],
        }])
        .expect("write_txn_markers commit");
    assert!(results[0].topics[0].partitions[0].error.is_none());

    let records = vec![make_record(50, 50)];
    let result = c.read_committed("output", 0, 0, 51, &records);
    assert_eq!(result.visible_records.len(), 1);
    assert_eq!(result.visible_records[0].offset, 50);
    assert!(result.aborted_transactions.is_empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Producer fencing: stale epoch is rejected on end_txn
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn stale_epoch_rejected_on_end_txn() {
    let temp = TempDir::new("stale-epoch");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-fence");
    c.record_produce("tx-fence", "x", 0, 0).expect("produce");

    // Attempt end_txn with an incorrect epoch (original was 0, we pass 5)
    let err = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-fence".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch.wrapping_add(5), // wrong epoch
            committed: true,
        })
        .expect_err("wrong epoch must be rejected");
    assert_eq!(err, TransactionCoordinatorError::InvalidProducerEpoch);
}

// ──────────────────────────────────────────────────────────────────────────────
// Concurrent init blocked while transaction is ongoing
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn concurrent_init_blocked_while_ongoing() {
    let temp = TempDir::new("concurrent-init-block");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-concurrent");
    c.record_produce("tx-concurrent", "x", 0, 0)
        .expect("produce");

    // Second init with the same transactional_id while transaction is ongoing must fail
    let err = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-concurrent".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
        })
        .expect_err("concurrent init must be blocked");
    assert_eq!(err, TransactionCoordinatorError::ConcurrentTransactions);
}

// ──────────────────────────────────────────────────────────────────────────────
// Restart recovers aborted ranges: read_committed works after reopen
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn restart_recovers_aborted_ranges() {
    let temp = TempDir::new("restart-aborted");

    let producer_id;
    {
        let mut c = open(temp.path());
        let p = init(&mut c, "tx-persist");
        producer_id = p.producer_id;

        c.record_produce("tx-persist", "stream", 0, 200)
            .expect("produce");
        c.record_produce("tx-persist", "stream", 0, 201)
            .expect("produce");
        c.end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-persist".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: false,
        })
        .expect("abort before shutdown");
    }

    // Reopen and verify aborted range is recovered
    let c2 = open(temp.path());
    let records = vec![make_record(200, 200), make_record(201, 201)];
    let result = c2.read_committed("stream", 0, 0, 202, &records);

    assert!(
        result.visible_records.is_empty(),
        "aborted records must stay hidden after restart"
    );
    assert_eq!(result.aborted_transactions.len(), 1);
    assert_eq!(result.aborted_transactions[0].producer_id, producer_id);
    assert_eq!(result.aborted_transactions[0].first_offset, 200);
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-transactional producer: init without transactional_id works
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn non_transactional_producer_init_allocates_id() {
    let temp = TempDir::new("non-transactional");
    let mut c = open(temp.path());

    let p1 = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("non-transactional init");
    let p2 = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("second non-transactional init");

    assert!(p1.producer_id >= 1, "producer_id must be positive");
    assert!(
        p2.producer_id > p1.producer_id,
        "each init must allocate a new id"
    );
    assert_eq!(p1.producer_epoch, 0);
    assert_eq!(p2.producer_epoch, 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// ensure_transactional_id: unknown tx_id rejected, known accepted
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn ensure_transactional_id_unknown_and_known() {
    let temp = TempDir::new("ensure-tx-id");
    let mut c = open(temp.path());

    // Unknown before init
    let err = c
        .ensure_transactional_id("tx-unknown")
        .expect_err("unknown tx_id must fail");
    assert_eq!(
        err,
        TransactionCoordinatorError::UnknownTransactionalId("tx-unknown".to_string())
    );

    // Known after init
    init(&mut c, "tx-known");
    c.ensure_transactional_id("tx-known")
        .expect("known tx_id must succeed");
}

// ──────────────────────────────────────────────────────────────────────────────
// Epoch v5 auto-bumps on end_txn commit
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn end_txn_v5_bumps_epoch_on_commit() {
    let temp = TempDir::new("epoch-bump-v5");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-epoch-bump");
    c.record_produce("tx-epoch-bump", "t", 0, 0)
        .expect("produce");

    let result = c
        .end_txn(EndTxnInput {
            api_version: 5, // v5+ auto-bumps epoch
            transactional_id: "tx-epoch-bump".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: true,
        })
        .expect("end_txn v5");

    assert_eq!(result.producer_id, p.producer_id);
    assert_eq!(
        result.producer_epoch,
        p.producer_epoch.saturating_add(1),
        "v5 end_txn must auto-bump epoch"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// read_committed with fetch_offset skips aborted ranges before window
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn read_committed_fetch_offset_skips_old_aborted_ranges() {
    let temp = TempDir::new("fetch-offset-skip-aborted");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-old-abort");
    // Abort at offsets 0-1 (old, before our fetch window)
    c.record_produce("tx-old-abort", "q", 0, 0)
        .expect("produce");
    c.record_produce("tx-old-abort", "q", 0, 1)
        .expect("produce");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-old-abort".to_string(),
        producer_id: p.producer_id,
        producer_epoch: p.producer_epoch,
        committed: false,
    })
    .expect("abort old range");

    // Fresh producer commits at offsets 10-11 (our fetch window)
    let p2 = init(&mut c, "tx-new-commit");
    c.record_produce("tx-new-commit", "q", 0, 10)
        .expect("produce 10");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-new-commit".to_string(),
        producer_id: p2.producer_id,
        producer_epoch: p2.producer_epoch,
        committed: true,
    })
    .expect("commit new range");

    // Fetch from offset 10 — old abort at 0-1 is before fetch_offset, should not appear
    let records = vec![make_record(10, 10)];
    let result = c.read_committed("q", 0, 10, 11, &records);

    assert_eq!(result.visible_records.len(), 1);
    assert_eq!(result.visible_records[0].offset, 10);
    assert!(
        result.aborted_transactions.is_empty(),
        "aborted range before fetch_offset must not appear in response"
    );
}
