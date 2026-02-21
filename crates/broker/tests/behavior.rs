use rafka_broker::{Broker, StorageError};

#[test]
fn behavior_produce_fetch_multiple_records() {
    let mut broker = Broker::new(0);
    broker.produce(b"a".to_vec(), b"1".to_vec(), 10);
    broker.produce(b"b".to_vec(), b"2".to_vec(), 11);
    broker.produce(b"c".to_vec(), b"3".to_vec(), 12);

    let fetched = broker.fetch(1, 2).expect("fetch from offset 1");
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].offset, 1);
    assert_eq!(fetched[0].value, b"2".to_vec());
    assert_eq!(fetched[1].offset, 2);
    assert_eq!(fetched[1].value, b"3".to_vec());
}

#[test]
fn behavior_group_expiration_path() {
    let mut broker = Broker::new(0);
    broker.join_group("group-1", "member-1", 0, 100);
    broker.join_group("group-1", "member-2", 50, 100);

    let expired = broker.evict_expired_members("group-1", 120);
    assert_eq!(expired, vec![String::from("member-1")]);
}

#[test]
fn behavior_out_of_range_fetch_error() {
    let broker = Broker::new(100);
    let err = broker
        .fetch(99, 10)
        .expect_err("expected out-of-range error");
    assert_eq!(
        err,
        StorageError::OffsetOutOfRange {
            requested: 99,
            earliest: 100,
            latest: 99,
        }
    );
}
