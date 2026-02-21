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
            "rafka-txn-edge-{label}-{millis}-{}-{counter}",
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

fn init(c: &mut TransactionCoordinator, tx_id: &str) -> InitProducerResult {
    c.init_producer(InitProducerInput {
        api_version: 3,
        transactional_id: Some(tx_id.to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
    })
    .expect("init_producer")
}

fn make_record(offset: i64) -> Record {
    Record {
        offset,
        timestamp_ms: offset * 1_000,
        key: Vec::new(),
        value: vec![offset as u8],
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Input validation: empty transactional_id
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_transactional_id_rejected_on_init() {
    let temp = TempDir::new("empty-tx-id-init");
    let mut c = open(temp.path());

    let err = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some(String::new()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect_err("empty transactional_id must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidTransactionalId);
}

// ──────────────────────────────────────────────────────────────────────────────
// Input validation: non-positive transaction timeout
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn zero_timeout_rejected_on_init() {
    let temp = TempDir::new("zero-timeout");
    let mut c = open(temp.path());

    let err = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-timeout".to_string()),
            transaction_timeout_ms: 0,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect_err("zero timeout must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidTxnState);
}

#[test]
fn negative_timeout_rejected_on_init() {
    let temp = TempDir::new("neg-timeout");
    let mut c = open(temp.path());

    let err = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-neg-timeout".to_string()),
            transaction_timeout_ms: -1,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect_err("negative timeout must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidTxnState);
}

// ──────────────────────────────────────────────────────────────────────────────
// record_produce: empty topic name rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn record_produce_empty_topic_rejected() {
    let temp = TempDir::new("empty-topic");
    let mut c = open(temp.path());

    init(&mut c, "tx-empty-topic");
    let err = c
        .record_produce("tx-empty-topic", "", 0, 0)
        .expect_err("empty topic must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidTopic);
}

// ──────────────────────────────────────────────────────────────────────────────
// record_produce: negative partition rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn record_produce_negative_partition_rejected() {
    let temp = TempDir::new("neg-partition");
    let mut c = open(temp.path());

    init(&mut c, "tx-neg-part");
    let err = c
        .record_produce("tx-neg-part", "events", -1, 0)
        .expect_err("negative partition must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidPartition(-1));
}

// ──────────────────────────────────────────────────────────────────────────────
// record_produce: unknown transactional_id rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn record_produce_unknown_tx_id_rejected() {
    let temp = TempDir::new("unknown-tx-produce");
    let mut c = open(temp.path());

    let err = c
        .record_produce("tx-ghost", "events", 0, 0)
        .expect_err("unknown tx_id must fail on record_produce");
    assert_eq!(
        err,
        TransactionCoordinatorError::UnknownTransactionalId("tx-ghost".to_string())
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// end_txn: wrong producer_id fences
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn end_txn_wrong_producer_id_fences() {
    let temp = TempDir::new("wrong-pid-fence");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-wrong-pid");
    c.record_produce("tx-wrong-pid", "topic", 0, 0)
        .expect("produce");

    let err = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-wrong-pid".to_string(),
            producer_id: p.producer_id + 999, // wrong PID
            producer_epoch: p.producer_epoch,
            committed: true,
        })
        .expect_err("wrong producer_id must fence");
    assert_eq!(err, TransactionCoordinatorError::ProducerFenced);
}

// ──────────────────────────────────────────────────────────────────────────────
// end_txn: transaction not Ongoing → InvalidTxnState
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn end_txn_on_ready_state_is_invalid() {
    let temp = TempDir::new("end-txn-ready");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-ready-end");
    // Do NOT call record_produce → status stays Ready

    let err = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-ready-end".to_string(),
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: true,
        })
        .expect_err("end_txn on Ready state must fail");
    assert_eq!(err, TransactionCoordinatorError::InvalidTxnState);
}

// ──────────────────────────────────────────────────────────────────────────────
// end_txn: unknown transactional_id rejected
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn end_txn_unknown_transactional_id_rejected() {
    let temp = TempDir::new("end-txn-unknown");
    let mut c = open(temp.path());

    let err = c
        .end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-nobody".to_string(),
            producer_id: 1,
            producer_epoch: 0,
            committed: true,
        })
        .expect_err("unknown tx_id end_txn must fail");
    assert_eq!(
        err,
        TransactionCoordinatorError::UnknownTransactionalId("tx-nobody".to_string())
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// ProducerFenced: re-init with mismatched credentials at api_version >= 3
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn reinit_with_wrong_credentials_is_fenced() {
    let temp = TempDir::new("reinit-fenced");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-reinit");

    // Re-init with same tx_id but wrong producer_id at v3+
    let err = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-reinit".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: p.producer_id + 1, // wrong PID
            producer_epoch: p.producer_epoch,
        })
        .expect_err("mismatched producer_id must fence");
    assert_eq!(err, TransactionCoordinatorError::ProducerFenced);
}

// ──────────────────────────────────────────────────────────────────────────────
// Multiple aborts on the same topic/partition accumulate aborted ranges
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn multiple_aborts_same_partition_accumulate_ranges() {
    let temp = TempDir::new("multi-abort-accumulate");
    let mut c = open(temp.path());

    // Abort 1: offsets 0-1
    let p1 = init(&mut c, "tx-cycle");
    c.record_produce("tx-cycle", "log", 0, 0).expect("produce");
    c.record_produce("tx-cycle", "log", 0, 1).expect("produce");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-cycle".to_string(),
        producer_id: p1.producer_id,
        producer_epoch: p1.producer_epoch,
        committed: false,
    })
    .expect("abort 1");

    // Abort 2: offsets 2-3 (epoch bumped after reinit)
    let p2 = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-cycle".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: p1.producer_id,
            producer_epoch: p1.producer_epoch,
        })
        .expect("reinit after abort 1");
    c.record_produce("tx-cycle", "log", 0, 2).expect("produce");
    c.record_produce("tx-cycle", "log", 0, 3).expect("produce");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-cycle".to_string(),
        producer_id: p2.producer_id,
        producer_epoch: p2.producer_epoch,
        committed: false,
    })
    .expect("abort 2");

    let records: Vec<Record> = (0..4).map(make_record).collect();
    let result = c.read_committed("log", 0, 0, 4, &records);

    assert!(
        result.visible_records.is_empty(),
        "all records from both aborts must be hidden"
    );
    assert_eq!(
        result.aborted_transactions.len(),
        2,
        "both aborted ranges must be reported"
    );
    assert_eq!(result.aborted_transactions[0].first_offset, 0);
    assert_eq!(result.aborted_transactions[1].first_offset, 2);
}

// ──────────────────────────────────────────────────────────────────────────────
// Single transaction spanning multiple partitions: abort hides all
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn same_tx_multi_partition_abort_all_hidden() {
    let temp = TempDir::new("multi-part-abort");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-multi-part");
    c.record_produce("tx-multi-part", "events", 0, 100)
        .expect("produce p0");
    c.record_produce("tx-multi-part", "events", 1, 200)
        .expect("produce p1");
    c.record_produce("tx-multi-part", "events", 2, 300)
        .expect("produce p2");

    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-multi-part".to_string(),
        producer_id: p.producer_id,
        producer_epoch: p.producer_epoch,
        committed: false,
    })
    .expect("abort");

    // All three partitions should have their records hidden
    for (partition, base_offset) in [(0i32, 100i64), (1, 200), (2, 300)] {
        let records = vec![make_record(base_offset)];
        let result = c.read_committed("events", partition, 0, base_offset + 1, &records);
        assert!(
            result.visible_records.is_empty(),
            "partition {partition} records must be hidden"
        );
        assert_eq!(
            result.aborted_transactions.len(),
            1,
            "partition {partition} must report one aborted range"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Two simultaneous ongoing transactions → LSO = minimum first unstable offset
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn two_ongoing_transactions_lso_is_minimum() {
    let temp = TempDir::new("two-ongoing-lso");
    let mut c = open(temp.path());

    let _p1 = init(&mut c, "tx-early");
    let _p2 = init(&mut c, "tx-late");

    // tx-early produces at offset 5 (earlier first-unstable)
    c.record_produce("tx-early", "shared", 0, 5)
        .expect("tx-early produce");
    // tx-late produces at offset 10 (later first-unstable)
    c.record_produce("tx-late", "shared", 0, 10)
        .expect("tx-late produce");

    let records: Vec<Record> = [5i64, 7, 10, 11].iter().map(|&o| make_record(o)).collect();
    let result = c.read_committed("shared", 0, 0, 12, &records);

    // LSO = min(first_unstable=5, hwm=12) = 5
    assert_eq!(result.last_stable_offset, 5);
    // No records below offset 5, so nothing visible
    assert!(
        result.visible_records.is_empty(),
        "nothing below LSO=5 should be visible"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// write_txn_markers: unknown producer_id reports error per partition
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn write_txn_markers_unknown_producer_id_reports_error() {
    let temp = TempDir::new("marker-unknown-pid");
    let mut c = open(temp.path());

    // producer_id 9999 was never allocated
    let results = c
        .write_txn_markers(vec![WriteTxnMarkerInput {
            producer_id: 9_999,
            producer_epoch: 0,
            committed: true,
            topics: vec![WriteTxnMarkerTopicInput {
                name: "topic-x".to_string(),
                partition_indexes: vec![0],
            }],
        }])
        .expect("write_txn_markers itself must succeed (per-partition errors)");

    assert_eq!(results.len(), 1);
    assert!(
        results[0].topics[0].partitions[0].error.is_some(),
        "unknown producer_id must produce a per-partition error"
    );
    assert_eq!(
        results[0].topics[0].partitions[0].error,
        Some(TransactionCoordinatorError::UnknownProducerId(9_999))
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// write_txn_markers: wrong producer epoch → per-partition error
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn write_txn_markers_wrong_epoch_per_partition_error() {
    let temp = TempDir::new("marker-wrong-epoch");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-marker-epoch");
    c.record_produce("tx-marker-epoch", "topic-y", 0, 50)
        .expect("produce");

    let results = c
        .write_txn_markers(vec![WriteTxnMarkerInput {
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch.wrapping_add(7), // wrong epoch
            committed: false,
            topics: vec![WriteTxnMarkerTopicInput {
                name: "topic-y".to_string(),
                partition_indexes: vec![0],
            }],
        }])
        .expect("write_txn_markers returns per-partition errors");

    assert_eq!(
        results[0].topics[0].partitions[0].error,
        Some(TransactionCoordinatorError::InvalidProducerEpoch),
        "wrong epoch must produce per-partition InvalidProducerEpoch"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// write_txn_markers: partition not part of ongoing transaction → per-partition error
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn write_txn_markers_partition_not_in_tx_returns_error() {
    let temp = TempDir::new("marker-no-partition");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-marker-nopart");
    // Produce to partition 0 only
    c.record_produce("tx-marker-nopart", "topic-z", 0, 1)
        .expect("produce");

    // Try to apply marker to partition 3 (not in the transaction)
    let results = c
        .write_txn_markers(vec![WriteTxnMarkerInput {
            producer_id: p.producer_id,
            producer_epoch: p.producer_epoch,
            committed: false,
            topics: vec![WriteTxnMarkerTopicInput {
                name: "topic-z".to_string(),
                partition_indexes: vec![3], // partition 3 never recorded
            }],
        }])
        .expect("write_txn_markers returns per-partition results");

    assert!(
        results[0].topics[0].partitions[0].error.is_some(),
        "partition not in ongoing tx must produce a per-partition error"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Producer IDs allocated across multiple inits are monotonically increasing
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn producer_ids_monotonically_increasing() {
    let temp = TempDir::new("monotonic-pid");
    let mut c = open(temp.path());

    let mut prev_id = 0i64;
    for i in 0..10 {
        let tx_id = format!("tx-mono-{i}");
        let p = init(&mut c, &tx_id);
        assert!(
            p.producer_id > prev_id,
            "producer_id {i} ({}) must be greater than previous ({prev_id})",
            p.producer_id
        );
        prev_id = p.producer_id;
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Abort then commit: second transaction on same tx_id with bumped epoch
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn abort_then_commit_second_transaction_same_tx_id() {
    let temp = TempDir::new("abort-then-commit");
    let mut c = open(temp.path());

    // First transaction: abort
    let p1 = init(&mut c, "tx-cycle2");
    c.record_produce("tx-cycle2", "orders", 0, 0)
        .expect("produce");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-cycle2".to_string(),
        producer_id: p1.producer_id,
        producer_epoch: p1.producer_epoch,
        committed: false,
    })
    .expect("abort");

    // Second transaction on same tx_id: commit at different offsets
    let p2 = c
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: Some("tx-cycle2".to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: p1.producer_id,
            producer_epoch: p1.producer_epoch,
        })
        .expect("reinit after abort");
    assert_eq!(p2.producer_id, p1.producer_id);
    assert_eq!(p2.producer_epoch, p1.producer_epoch + 1);

    c.record_produce("tx-cycle2", "orders", 0, 10)
        .expect("produce committed");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-cycle2".to_string(),
        producer_id: p2.producer_id,
        producer_epoch: p2.producer_epoch,
        committed: true,
    })
    .expect("commit second transaction");

    let records = vec![make_record(0), make_record(10)];
    let result = c.read_committed("orders", 0, 0, 11, &records);

    // offset 0 was aborted, offset 10 was committed
    assert_eq!(result.visible_records.len(), 1);
    assert_eq!(result.visible_records[0].offset, 10);
    assert_eq!(result.aborted_transactions.len(), 1);
    assert_eq!(result.aborted_transactions[0].first_offset, 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// read_committed with no records always returns empty visible_records
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn read_committed_empty_records_returns_hwm_as_lso() {
    let temp = TempDir::new("empty-records-lso");
    let c = open(temp.path());

    // No transactions at all
    let result = c.read_committed("any-topic", 0, 0, 42, &[]);

    assert_eq!(
        result.last_stable_offset, 42,
        "LSO = hwm when no ongoing tx"
    );
    assert!(result.visible_records.is_empty());
    assert!(result.aborted_transactions.is_empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Multi-topic, multi-partition: partial abort (one topic aborted, one committed)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_topic_partial_abort_one_topic_hidden() {
    let temp = TempDir::new("partial-abort");
    let mut c = open(temp.path());

    // Producer A: writes to "inputs" and aborts
    let pa = init(&mut c, "tx-input");
    c.record_produce("tx-input", "inputs", 0, 0)
        .expect("produce inputs");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-input".to_string(),
        producer_id: pa.producer_id,
        producer_epoch: pa.producer_epoch,
        committed: false,
    })
    .expect("abort inputs");

    // Producer B: writes to "outputs" and commits
    let pb = init(&mut c, "tx-output");
    c.record_produce("tx-output", "outputs", 0, 0)
        .expect("produce outputs");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-output".to_string(),
        producer_id: pb.producer_id,
        producer_epoch: pb.producer_epoch,
        committed: true,
    })
    .expect("commit outputs");

    let input_result = c.read_committed("inputs", 0, 0, 1, &[make_record(0)]);
    assert!(
        input_result.visible_records.is_empty(),
        "inputs were aborted"
    );
    assert_eq!(input_result.aborted_transactions.len(), 1);

    let output_result = c.read_committed("outputs", 0, 0, 1, &[make_record(0)]);
    assert_eq!(
        output_result.visible_records.len(),
        1,
        "outputs were committed"
    );
    assert!(output_result.aborted_transactions.is_empty());
}

// ──────────────────────────────────────────────────────────────────────────────
// Epoch bumps with saturating_add: many reinit cycles do not overflow
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn epoch_bumps_are_monotonic_through_many_cycles() {
    let temp = TempDir::new("epoch-monotonic");
    let mut c = open(temp.path());

    let p_init = init(&mut c, "tx-epoch-cycle");
    let mut prev_epoch = p_init.producer_epoch;
    let prev_pid = p_init.producer_id;

    for _ in 0..50 {
        // Need to commit or abort to leave Ongoing state before reinit
        // First get into Ongoing state
        c.record_produce("tx-epoch-cycle", "topic", 0, 0)
            .expect("produce");
        c.end_txn(EndTxnInput {
            api_version: 4,
            transactional_id: "tx-epoch-cycle".to_string(),
            producer_id: prev_pid,
            producer_epoch: prev_epoch,
            committed: true,
        })
        .expect("commit cycle");

        let p_next = c
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: Some("tx-epoch-cycle".to_string()),
                transaction_timeout_ms: 60_000,
                producer_id: prev_pid,
                producer_epoch: prev_epoch,
            })
            .expect("reinit");
        assert_eq!(p_next.producer_id, prev_pid);
        assert_eq!(p_next.producer_epoch, prev_epoch.saturating_add(1));
        prev_epoch = p_next.producer_epoch;
    }
    // After 50 reinits the epoch should be at most i16::MAX (no wrap-around)
    assert!(prev_epoch >= 50 || prev_epoch == i16::MAX);
}

// ──────────────────────────────────────────────────────────────────────────────
// Non-transactional producers get unique IDs even after restart
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn non_transactional_producer_ids_unique_after_restart() {
    let temp = TempDir::new("non-txn-restart-ids");

    let id1;
    let id2;
    {
        let mut c = open(temp.path());
        let p1 = c
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
            })
            .expect("first non-txn");
        id1 = p1.producer_id;
        let p2 = c
            .init_producer(InitProducerInput {
                api_version: 3,
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
            })
            .expect("second non-txn");
        id2 = p2.producer_id;
    }

    // After restart, new allocations must not reuse old IDs
    let mut c2 = open(temp.path());
    let p3 = c2
        .init_producer(InitProducerInput {
            api_version: 3,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
        })
        .expect("post-restart non-txn");

    assert_ne!(p3.producer_id, id1, "must not reuse id1");
    assert_ne!(p3.producer_id, id2, "must not reuse id2");
    assert!(
        p3.producer_id > id2,
        "must be strictly greater than previous max"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// read_committed: records within and outside fetch_offset window
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn read_committed_respects_fetch_offset_lower_bound() {
    let temp = TempDir::new("fetch-offset-bound");
    let mut c = open(temp.path());

    let p = init(&mut c, "tx-fetch-bound");
    // Two committed records at offsets 0 and 50
    c.record_produce("tx-fetch-bound", "stream", 0, 0)
        .expect("produce 0");
    c.record_produce("tx-fetch-bound", "stream", 0, 50)
        .expect("produce 50");
    c.end_txn(EndTxnInput {
        api_version: 4,
        transactional_id: "tx-fetch-bound".to_string(),
        producer_id: p.producer_id,
        producer_epoch: p.producer_epoch,
        committed: true,
    })
    .expect("commit");

    // read_committed does not filter by fetch_offset for visible_records; that is
    // the storage layer's job. The caller passes only the records in the physical
    // fetch window (starting at fetch_offset). Passing only the record at 50
    // simulates a storage fetch starting at offset 50.
    let records = vec![make_record(50)];
    let result = c.read_committed("stream", 0, 50, 51, &records);

    assert_eq!(result.visible_records.len(), 1);
    assert_eq!(result.visible_records[0].offset, 50);
}

// ──────────────────────────────────────────────────────────────────────────────
// Concurrent transactions on different topics do not block each other's LSO
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn concurrent_transactions_on_different_topics_independent_lso() {
    let temp = TempDir::new("concurrent-diff-topics");
    let mut c = open(temp.path());

    let _pa = init(&mut c, "tx-topic-a");
    let _pb = init(&mut c, "tx-topic-b");

    // tx-topic-a is ongoing on "alpha"
    c.record_produce("tx-topic-a", "alpha", 0, 10)
        .expect("produce alpha");
    // tx-topic-b is ongoing on "beta"
    c.record_produce("tx-topic-b", "beta", 0, 20)
        .expect("produce beta");

    // LSO for "alpha" is blocked by tx-topic-a
    let alpha_result = c.read_committed("alpha", 0, 0, 11, &[make_record(10)]);
    assert_eq!(alpha_result.last_stable_offset, 10);

    // LSO for "beta" is blocked by tx-topic-b (not by tx-topic-a)
    let beta_result = c.read_committed("beta", 0, 0, 21, &[make_record(20)]);
    assert_eq!(beta_result.last_stable_offset, 20);

    // An unrelated topic has no ongoing transactions → LSO = hwm
    let other_result = c.read_committed("gamma", 0, 0, 100, &[]);
    assert_eq!(other_result.last_stable_offset, 100);
}
