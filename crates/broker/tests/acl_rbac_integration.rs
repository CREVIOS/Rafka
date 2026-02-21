//! Role-based access control (RBAC) tests for the Rafka ACL system.
//!
//! Covers: role separation, wildcard grants, super-user bypass, multi-tenant
//! isolation, cross-resource authorization, idempotent-write, anonymous
//! principal behaviour, and complex real-world producer/consumer role models.

#![forbid(unsafe_code)]

use rafka_broker::{
    AclAuthorizer, AclConfigError, AclOperation, AclResourceType, ANONYMOUS_PRINCIPAL,
    CLUSTER_RESOURCE_NAME,
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn producer_acl(principal: &str, topic: &str) -> AclAuthorizer {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        principal,
        AclOperation::Write,
        AclResourceType::Topic,
        topic,
    )
    .unwrap();
    acl
}

fn consumer_acl(principal: &str, topic: &str, group: &str) -> AclAuthorizer {
    let mut acl = AclAuthorizer::new();
    acl.allow(principal, AclOperation::Read, AclResourceType::Topic, topic)
        .unwrap();
    acl.allow(principal, AclOperation::Read, AclResourceType::Group, group)
        .unwrap();
    acl
}

// ── 1. Empty / no-rules baseline ─────────────────────────────────────────────

#[test]
fn empty_acl_denies_every_operation() {
    let acl = AclAuthorizer::new();
    for op in [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Describe,
        AclOperation::ClusterAction,
        AclOperation::IdempotentWrite,
    ] {
        assert!(
            !acl.authorize("alice", op, AclResourceType::Topic, "orders"),
            "empty ACL should deny {op:?}"
        );
    }
}

#[test]
fn empty_acl_authorize_any_returns_false() {
    let acl = AclAuthorizer::new();
    assert!(!acl.authorize_any("alice", AclOperation::Read, AclResourceType::Topic));
    assert!(!acl.authorize_any("alice", AclOperation::Write, AclResourceType::Cluster));
}

// ── 2. Exact-match rules ──────────────────────────────────────────────────────

#[test]
fn exact_rule_allows_correct_triple_and_rejects_neighbours() {
    let acl = producer_acl("producer-svc", "payments");

    assert!(acl.authorize(
        "producer-svc",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    // Wrong principal
    assert!(!acl.authorize(
        "consumer-svc",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    // Wrong operation
    assert!(!acl.authorize(
        "producer-svc",
        AclOperation::Read,
        AclResourceType::Topic,
        "payments"
    ));
    // Wrong topic
    assert!(!acl.authorize(
        "producer-svc",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));
    // Wrong resource type
    assert!(!acl.authorize(
        "producer-svc",
        AclOperation::Write,
        AclResourceType::Group,
        "payments"
    ));
}

// ── 3. Wildcard rules ─────────────────────────────────────────────────────────

#[test]
fn wildcard_principal_grants_all_users() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "*",
        AclOperation::Read,
        AclResourceType::Topic,
        "public-feed",
    )
    .unwrap();

    for user in ["alice", "bob", "charlie", "ANONYMOUS", "service-x"] {
        assert!(
            acl.authorize(
                user,
                AclOperation::Read,
                AclResourceType::Topic,
                "public-feed"
            ),
            "wildcard principal should grant {user}"
        );
    }
}

#[test]
fn wildcard_resource_grants_all_resources_of_type() {
    let mut acl = AclAuthorizer::new();
    acl.allow("alice", AclOperation::Write, AclResourceType::Topic, "*")
        .unwrap();

    for topic in [
        "orders",
        "payments",
        "events",
        "audit-log",
        "system-internal",
    ] {
        assert!(
            acl.authorize("alice", AclOperation::Write, AclResourceType::Topic, topic),
            "wildcard resource should grant topic {topic}"
        );
    }
    // But not a different resource type
    assert!(!acl.authorize(
        "alice",
        AclOperation::Write,
        AclResourceType::Group,
        "my-group"
    ));
}

#[test]
fn wildcard_principal_and_wildcard_resource_grants_any_combination() {
    let mut acl = AclAuthorizer::new();
    acl.allow("*", AclOperation::Describe, AclResourceType::Topic, "*")
        .unwrap();

    assert!(acl.authorize(
        "alice",
        AclOperation::Describe,
        AclResourceType::Topic,
        "orders"
    ));
    assert!(acl.authorize(
        "bob",
        AclOperation::Describe,
        AclResourceType::Topic,
        "payments"
    ));
    assert!(!acl.authorize(
        "alice",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    )); // wrong op
}

// ── 4. Super-user bypass ──────────────────────────────────────────────────────

#[test]
fn super_user_bypasses_all_resource_and_operation_checks() {
    let mut acl = AclAuthorizer::new();
    acl.add_super_user("admin-root").unwrap();

    for op in [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Describe,
        AclOperation::ClusterAction,
        AclOperation::IdempotentWrite,
    ] {
        for rtype in [
            AclResourceType::Topic,
            AclResourceType::Group,
            AclResourceType::Cluster,
            AclResourceType::TransactionalId,
        ] {
            assert!(
                acl.authorize("admin-root", op, rtype, "any-resource"),
                "super user should bypass {op:?} on {rtype:?}"
            );
        }
    }
}

#[test]
fn super_user_also_passes_authorize_any() {
    let mut acl = AclAuthorizer::new();
    acl.add_super_user("admin").unwrap();

    assert!(acl.authorize_any(
        "admin",
        AclOperation::ClusterAction,
        AclResourceType::Cluster
    ));
    assert!(acl.authorize_any("admin", AclOperation::Write, AclResourceType::Topic));
}

#[test]
fn non_super_user_is_still_restricted() {
    let mut acl = AclAuthorizer::new();
    acl.add_super_user("admin").unwrap();
    // alice has no rules
    assert!(!acl.authorize("alice", AclOperation::Write, AclResourceType::Topic, "t1"));
    // bob has a rule for a different topic
    acl.allow("bob", AclOperation::Write, AclResourceType::Topic, "t2")
        .unwrap();
    assert!(!acl.authorize("bob", AclOperation::Write, AclResourceType::Topic, "t1"));
}

#[test]
fn multiple_super_users_each_bypass_independently() {
    let mut acl = AclAuthorizer::new();
    acl.add_super_user("root1").unwrap();
    acl.add_super_user("root2").unwrap();
    acl.add_super_user("root3").unwrap();

    for su in ["root1", "root2", "root3"] {
        assert!(acl.authorize(
            su,
            AclOperation::ClusterAction,
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME
        ));
    }
    assert!(!acl.authorize(
        "regular",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
}

// ── 5. Role-based models ──────────────────────────────────────────────────────

/// Full producer role: Write to topics + Idempotent write on cluster
#[test]
fn producer_role_can_write_topics_and_idempotent_write() {
    let mut acl = AclAuthorizer::new();
    // Producer service gets write on its assigned topics
    for topic in ["orders", "payments", "inventory"] {
        acl.allow(
            "order-producer",
            AclOperation::Write,
            AclResourceType::Topic,
            topic,
        )
        .unwrap();
    }
    // Idempotent write requires Cluster-level IdempotentWrite
    acl.allow(
        "order-producer",
        AclOperation::IdempotentWrite,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
    )
    .unwrap();

    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));
    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "inventory"
    ));
    assert!(acl.authorize(
        "order-producer",
        AclOperation::IdempotentWrite,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
    // Cannot read topics
    assert!(!acl.authorize(
        "order-producer",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders"
    ));
    // Cannot write audit topic
    assert!(!acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "audit-log"
    ));
}

/// Consumer role: Read from topics and consumer groups
#[test]
fn consumer_role_can_read_topics_and_join_groups() {
    let acl = consumer_acl("analytics-consumer", "events", "analytics-group");

    assert!(acl.authorize(
        "analytics-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "events"
    ));
    assert!(acl.authorize(
        "analytics-consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "analytics-group"
    ));
    // Cannot write
    assert!(!acl.authorize(
        "analytics-consumer",
        AclOperation::Write,
        AclResourceType::Topic,
        "events"
    ));
    // Cannot access other groups
    assert!(!acl.authorize(
        "analytics-consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "other-group"
    ));
}

/// Admin role: full cluster access
#[test]
fn admin_role_has_cluster_action_access() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "kafka-admin",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
    )
    .unwrap();
    // Also Describe on all topics
    acl.allow(
        "kafka-admin",
        AclOperation::Describe,
        AclResourceType::Topic,
        "*",
    )
    .unwrap();

    assert!(acl.authorize(
        "kafka-admin",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
    assert!(acl.authorize(
        "kafka-admin",
        AclOperation::Describe,
        AclResourceType::Topic,
        "any-topic"
    ));
    // But no write (no rule for that)
    assert!(!acl.authorize(
        "kafka-admin",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));
}

// ── 6. Multi-tenant isolation ─────────────────────────────────────────────────

#[test]
fn tenant_a_cannot_access_tenant_b_topics() {
    let mut acl = AclAuthorizer::new();
    // Tenant A topics
    acl.allow(
        "tenant-a-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "tenant-a-orders",
    )
    .unwrap();
    acl.allow(
        "tenant-a-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "tenant-a-orders",
    )
    .unwrap();
    // Tenant B topics
    acl.allow(
        "tenant-b-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "tenant-b-orders",
    )
    .unwrap();
    acl.allow(
        "tenant-b-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "tenant-b-orders",
    )
    .unwrap();

    // Tenant A can access its own topic
    assert!(acl.authorize(
        "tenant-a-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "tenant-a-orders"
    ));
    assert!(acl.authorize(
        "tenant-a-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "tenant-a-orders"
    ));

    // Tenant A cannot access tenant B topic
    assert!(!acl.authorize(
        "tenant-a-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "tenant-b-orders"
    ));
    assert!(!acl.authorize(
        "tenant-a-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "tenant-b-orders"
    ));

    // Cross-contamination check
    assert!(!acl.authorize(
        "tenant-b-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "tenant-a-orders"
    ));
}

#[test]
fn tenant_isolation_with_many_tenants() {
    let mut acl = AclAuthorizer::new();
    let tenants: Vec<String> = (0..20).map(|i| format!("tenant-{i}")).collect();

    for tenant in &tenants {
        let topic = format!("{tenant}-events");
        acl.allow(tenant, AclOperation::Read, AclResourceType::Topic, &topic)
            .unwrap();
        acl.allow(tenant, AclOperation::Write, AclResourceType::Topic, &topic)
            .unwrap();
    }

    // Each tenant can access only its own topic
    for i in 0..20_usize {
        let tenant = &tenants[i];
        let my_topic = format!("{tenant}-events");
        let other_topic = format!("tenant-{}-events", (i + 1) % 20);

        assert!(acl.authorize(
            tenant,
            AclOperation::Read,
            AclResourceType::Topic,
            &my_topic
        ));
        assert!(!acl.authorize(
            tenant,
            AclOperation::Read,
            AclResourceType::Topic,
            &other_topic
        ));
    }
}

// ── 7. Transactional ID authorization ────────────────────────────────────────

#[test]
fn transactional_producer_must_have_txn_id_write_permission() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "txn-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "order-tx-001",
    )
    .unwrap();
    acl.allow(
        "txn-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders",
    )
    .unwrap();

    assert!(acl.authorize(
        "txn-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "order-tx-001"
    ));
    // Cannot use a different transactional ID
    assert!(!acl.authorize(
        "txn-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "other-tx-999"
    ));
}

#[test]
fn wildcard_txn_id_allows_any_transaction() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "power-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "*",
    )
    .unwrap();

    for tx_id in [
        "tx-001",
        "tx-002",
        "billing-tx-abc",
        "long-tx-identifier-xyzw",
    ] {
        assert!(
            acl.authorize(
                "power-producer",
                AclOperation::Write,
                AclResourceType::TransactionalId,
                tx_id
            ),
            "wildcard txn id should allow {tx_id}"
        );
    }
}

// ── 8. Anonymous principal ────────────────────────────────────────────────────

#[test]
fn anonymous_principal_is_rejected_when_no_rule() {
    let acl = AclAuthorizer::new();
    assert!(!acl.authorize(
        ANONYMOUS_PRINCIPAL,
        AclOperation::Read,
        AclResourceType::Topic,
        "events"
    ));
}

#[test]
fn anonymous_principal_allowed_by_wildcard_principal_rule() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "*",
        AclOperation::Read,
        AclResourceType::Topic,
        "public-events",
    )
    .unwrap();

    assert!(acl.authorize(
        ANONYMOUS_PRINCIPAL,
        AclOperation::Read,
        AclResourceType::Topic,
        "public-events"
    ));
}

#[test]
fn anonymous_principal_explicit_rule_works() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        ANONYMOUS_PRINCIPAL,
        AclOperation::Read,
        AclResourceType::Topic,
        "open-feed",
    )
    .unwrap();

    assert!(acl.authorize(
        ANONYMOUS_PRINCIPAL,
        AclOperation::Read,
        AclResourceType::Topic,
        "open-feed"
    ));
    // Other topics still denied
    assert!(!acl.authorize(
        ANONYMOUS_PRINCIPAL,
        AclOperation::Read,
        AclResourceType::Topic,
        "private-feed"
    ));
}

// ── 9. Invalid configuration ──────────────────────────────────────────────────

#[test]
fn empty_principal_rejected_in_add_super_user() {
    let mut acl = AclAuthorizer::new();
    assert_eq!(acl.add_super_user(""), Err(AclConfigError::EmptyPrincipal));
}

#[test]
fn empty_principal_rejected_in_allow() {
    let mut acl = AclAuthorizer::new();
    assert_eq!(
        acl.allow("", AclOperation::Read, AclResourceType::Topic, "t1"),
        Err(AclConfigError::EmptyPrincipal)
    );
}

#[test]
fn empty_resource_name_rejected_in_allow() {
    let mut acl = AclAuthorizer::new();
    assert_eq!(
        acl.allow("alice", AclOperation::Read, AclResourceType::Topic, ""),
        Err(AclConfigError::EmptyResourceName)
    );
}

#[test]
fn authorize_with_empty_principal_always_false() {
    let mut acl = AclAuthorizer::new();
    acl.allow("*", AclOperation::Write, AclResourceType::Topic, "*")
        .unwrap();
    // Even wildcard rule doesn't grant empty principal
    assert!(!acl.authorize("", AclOperation::Write, AclResourceType::Topic, "t1"));
}

#[test]
fn authorize_with_empty_resource_name_always_false() {
    let mut acl = AclAuthorizer::new();
    acl.allow("*", AclOperation::Write, AclResourceType::Topic, "*")
        .unwrap();
    assert!(!acl.authorize("alice", AclOperation::Write, AclResourceType::Topic, ""));
}

// ── 10. authorize_any scenarios ───────────────────────────────────────────────

#[test]
fn authorize_any_returns_true_when_at_least_one_resource_matches() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "alice",
        AclOperation::Write,
        AclResourceType::Topic,
        "t-specific",
    )
    .unwrap();

    assert!(acl.authorize_any("alice", AclOperation::Write, AclResourceType::Topic));
    assert!(!acl.authorize_any("alice", AclOperation::Read, AclResourceType::Topic));
    assert!(!acl.authorize_any("alice", AclOperation::Write, AclResourceType::Group));
    assert!(!acl.authorize_any("bob", AclOperation::Write, AclResourceType::Topic));
}

#[test]
fn authorize_any_with_wildcard_resource_grants_operation_type() {
    let mut acl = AclAuthorizer::new();
    acl.allow("alice", AclOperation::Describe, AclResourceType::Topic, "*")
        .unwrap();

    assert!(acl.authorize_any("alice", AclOperation::Describe, AclResourceType::Topic));
    assert!(!acl.authorize_any("alice", AclOperation::Write, AclResourceType::Topic));
}

// ── 11. Privilege escalation attempts ────────────────────────────────────────

#[test]
fn read_permission_does_not_imply_write() {
    let acl = consumer_acl("reader", "secure-topic", "secure-group");

    // Read is granted
    assert!(acl.authorize(
        "reader",
        AclOperation::Read,
        AclResourceType::Topic,
        "secure-topic"
    ));
    // Write is NOT granted
    assert!(!acl.authorize(
        "reader",
        AclOperation::Write,
        AclResourceType::Topic,
        "secure-topic"
    ));
    // Describe is NOT granted
    assert!(!acl.authorize(
        "reader",
        AclOperation::Describe,
        AclResourceType::Topic,
        "secure-topic"
    ));
    // ClusterAction is NOT granted
    assert!(!acl.authorize(
        "reader",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
}

#[test]
fn group_read_does_not_grant_topic_read() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "my-group",
    )
    .unwrap();

    assert!(acl.authorize(
        "consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "my-group"
    ));
    // Topic access NOT implied by group access
    assert!(!acl.authorize(
        "consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "my-group"
    ));
    assert!(!acl.authorize(
        "consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "events"
    ));
}

#[test]
fn cluster_action_does_not_grant_topic_operations() {
    let mut acl = AclAuthorizer::new();
    acl.allow(
        "follower",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
    )
    .unwrap();

    assert!(acl.authorize(
        "follower",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
    assert!(!acl.authorize(
        "follower",
        AclOperation::Read,
        AclResourceType::Topic,
        "any-topic"
    ));
    assert!(!acl.authorize(
        "follower",
        AclOperation::Write,
        AclResourceType::Topic,
        "any-topic"
    ));
}

// ── 12. Duplicate rule idempotency ────────────────────────────────────────────

#[test]
fn adding_same_rule_twice_is_idempotent() {
    let mut acl = AclAuthorizer::new();
    for _ in 0..5 {
        acl.allow(
            "alice",
            AclOperation::Read,
            AclResourceType::Topic,
            "events",
        )
        .unwrap();
    }
    assert!(acl.authorize(
        "alice",
        AclOperation::Read,
        AclResourceType::Topic,
        "events"
    ));
}

#[test]
fn adding_same_super_user_twice_is_idempotent() {
    let mut acl = AclAuthorizer::new();
    for _ in 0..3 {
        acl.add_super_user("sa").unwrap();
    }
    assert!(acl.authorize("sa", AclOperation::Write, AclResourceType::Topic, "t1"));
}

// ── 13. Real-world e-commerce scenario ───────────────────────────────────────

/// Order service uses 3 roles: order-producer, payment-consumer, fraud-reader.
/// Ensure strict separation.
#[test]
fn ecommerce_role_separation() {
    let mut acl = AclAuthorizer::new();

    // Order producer: write to orders, payments; use order-txn
    acl.allow(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders",
    )
    .unwrap();
    acl.allow(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments",
    )
    .unwrap();
    acl.allow(
        "order-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "order-txn",
    )
    .unwrap();

    // Payment consumer: read payments topic, join payment-group
    acl.allow(
        "payment-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "payments",
    )
    .unwrap();
    acl.allow(
        "payment-consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "payment-group",
    )
    .unwrap();

    // Fraud reader: read-only on orders + payments for auditing
    acl.allow(
        "fraud-reader",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders",
    )
    .unwrap();
    acl.allow(
        "fraud-reader",
        AclOperation::Read,
        AclResourceType::Topic,
        "payments",
    )
    .unwrap();
    acl.allow(
        "fraud-reader",
        AclOperation::Describe,
        AclResourceType::Topic,
        "*",
    )
    .unwrap();

    // Admin: full cluster control
    acl.add_super_user("platform-admin").unwrap();

    // order-producer can produce
    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));
    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    assert!(acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "order-txn"
    ));
    // order-producer cannot read
    assert!(!acl.authorize(
        "order-producer",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders"
    ));
    // order-producer cannot use different txn id
    assert!(!acl.authorize(
        "order-producer",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "rogue-txn"
    ));

    // payment-consumer can read payments, join group
    assert!(acl.authorize(
        "payment-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "payments"
    ));
    assert!(acl.authorize(
        "payment-consumer",
        AclOperation::Read,
        AclResourceType::Group,
        "payment-group"
    ));
    // payment-consumer cannot write
    assert!(!acl.authorize(
        "payment-consumer",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    // payment-consumer cannot read orders
    assert!(!acl.authorize(
        "payment-consumer",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders"
    ));

    // fraud-reader can describe any topic
    assert!(acl.authorize(
        "fraud-reader",
        AclOperation::Describe,
        AclResourceType::Topic,
        "orders"
    ));
    assert!(acl.authorize(
        "fraud-reader",
        AclOperation::Describe,
        AclResourceType::Topic,
        "any-new-topic"
    ));
    // fraud-reader cannot write
    assert!(!acl.authorize(
        "fraud-reader",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));

    // admin bypasses everything
    assert!(acl.authorize(
        "platform-admin",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
    assert!(acl.authorize(
        "platform-admin",
        AclOperation::Write,
        AclResourceType::Topic,
        "orders"
    ));
}

// ── 14. Mixed wildcard and exact rules ───────────────────────────────────────

#[test]
fn specific_rule_and_wildcard_coexist() {
    let mut acl = AclAuthorizer::new();
    // Specific write on orders
    acl.allow("svc", AclOperation::Write, AclResourceType::Topic, "orders")
        .unwrap();
    // Wildcard read
    acl.allow("svc", AclOperation::Read, AclResourceType::Topic, "*")
        .unwrap();

    assert!(acl.authorize("svc", AclOperation::Write, AclResourceType::Topic, "orders"));
    assert!(!acl.authorize(
        "svc",
        AclOperation::Write,
        AclResourceType::Topic,
        "payments"
    ));
    assert!(acl.authorize("svc", AclOperation::Read, AclResourceType::Topic, "orders"));
    assert!(acl.authorize(
        "svc",
        AclOperation::Read,
        AclResourceType::Topic,
        "payments"
    ));
}

// ── 15. authorize_any with empty principal ────────────────────────────────────

#[test]
fn authorize_any_empty_principal_is_always_false() {
    let mut acl = AclAuthorizer::new();
    acl.allow("*", AclOperation::Write, AclResourceType::Topic, "*")
        .unwrap();
    assert!(!acl.authorize_any("", AclOperation::Write, AclResourceType::Topic));
}

// ── 16. All resource types covered ───────────────────────────────────────────

#[test]
fn all_resource_types_can_be_independently_authorized() {
    let mut acl = AclAuthorizer::new();
    acl.allow("svc", AclOperation::Read, AclResourceType::Topic, "t1")
        .unwrap();
    acl.allow("svc", AclOperation::Read, AclResourceType::Group, "g1")
        .unwrap();
    acl.allow(
        "svc",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
    )
    .unwrap();
    acl.allow(
        "svc",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "tx1",
    )
    .unwrap();

    assert!(acl.authorize("svc", AclOperation::Read, AclResourceType::Topic, "t1"));
    assert!(acl.authorize("svc", AclOperation::Read, AclResourceType::Group, "g1"));
    assert!(acl.authorize(
        "svc",
        AclOperation::ClusterAction,
        AclResourceType::Cluster,
        CLUSTER_RESOURCE_NAME
    ));
    assert!(acl.authorize(
        "svc",
        AclOperation::Write,
        AclResourceType::TransactionalId,
        "tx1"
    ));

    // Each rule is scoped to its type only
    assert!(!acl.authorize("svc", AclOperation::Read, AclResourceType::Group, "t1"));
    assert!(!acl.authorize("svc", AclOperation::Read, AclResourceType::Topic, "g1"));
    assert!(!acl.authorize(
        "svc",
        AclOperation::ClusterAction,
        AclResourceType::Topic,
        CLUSTER_RESOURCE_NAME
    ));
}

// ── 17. Overprovisioning guard ────────────────────────────────────────────────

#[test]
fn adding_rules_does_not_grant_unrelated_principals() {
    let mut acl = AclAuthorizer::new();
    // Add 100 rules for different users and topics
    for i in 0..100 {
        acl.allow(
            &format!("user-{i}"),
            AclOperation::Write,
            AclResourceType::Topic,
            &format!("topic-{i}"),
        )
        .unwrap();
    }
    // user-0 can only write to topic-0, not topic-1
    assert!(acl.authorize(
        "user-0",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-0"
    ));
    assert!(!acl.authorize(
        "user-0",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-1"
    ));
    assert!(!acl.authorize(
        "user-0",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-99"
    ));
    // user-99 can only write to topic-99
    assert!(acl.authorize(
        "user-99",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-99"
    ));
    assert!(!acl.authorize(
        "user-99",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-0"
    ));
    // unknown user cannot do anything
    assert!(!acl.authorize(
        "user-unknown",
        AclOperation::Write,
        AclResourceType::Topic,
        "topic-0"
    ));
}
