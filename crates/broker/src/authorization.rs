#![forbid(unsafe_code)]

use std::collections::BTreeSet;

pub const ANONYMOUS_PRINCIPAL: &str = "ANONYMOUS";
pub const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";
const WILDCARD: &str = "*";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AclOperation {
    Read,
    Write,
    Describe,
    ClusterAction,
    IdempotentWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclConfigError {
    EmptyPrincipal,
    EmptyResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AclRule {
    principal: String,
    operation: AclOperation,
    resource_type: AclResourceType,
    resource_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct AclAuthorizer {
    super_users: BTreeSet<String>,
    rules: BTreeSet<AclRule>,
}

impl AclAuthorizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_super_user(&mut self, principal: &str) -> Result<(), AclConfigError> {
        if principal.is_empty() {
            return Err(AclConfigError::EmptyPrincipal);
        }
        self.super_users.insert(principal.to_string());
        Ok(())
    }

    pub fn allow(
        &mut self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> Result<(), AclConfigError> {
        if principal.is_empty() {
            return Err(AclConfigError::EmptyPrincipal);
        }
        if resource_name.is_empty() {
            return Err(AclConfigError::EmptyResourceName);
        }
        self.rules.insert(AclRule {
            principal: principal.to_string(),
            operation,
            resource_type,
            resource_name: resource_name.to_string(),
        });
        Ok(())
    }

    pub fn authorize(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> bool {
        if principal.is_empty() || resource_name.is_empty() {
            return false;
        }
        if self.super_users.contains(principal) {
            return true;
        }
        self.rules.iter().any(|rule| {
            rule.operation == operation
                && rule.resource_type == resource_type
                && principal_matches(&rule.principal, principal)
                && resource_matches(&rule.resource_name, resource_name)
        })
    }

    pub fn authorize_any(
        &self,
        principal: &str,
        operation: AclOperation,
        resource_type: AclResourceType,
    ) -> bool {
        if principal.is_empty() {
            return false;
        }
        if self.super_users.contains(principal) {
            return true;
        }
        self.rules.iter().any(|rule| {
            rule.operation == operation
                && rule.resource_type == resource_type
                && principal_matches(&rule.principal, principal)
        })
    }
}

fn principal_matches(rule_principal: &str, principal: &str) -> bool {
    rule_principal == WILDCARD || rule_principal == principal
}

fn resource_matches(rule_resource: &str, resource_name: &str) -> bool {
    rule_resource == WILDCARD || rule_resource == resource_name
}

#[cfg(test)]
mod tests {
    use super::{
        AclAuthorizer, AclConfigError, AclOperation, AclResourceType, CLUSTER_RESOURCE_NAME,
    };

    #[test]
    fn authorize_denies_when_no_rules() {
        let acl = AclAuthorizer::new();
        assert!(!acl.authorize(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "orders"
        ));
    }

    #[test]
    fn authorize_allows_exact_rule() {
        let mut acl = AclAuthorizer::new();
        acl.allow(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "orders",
        )
        .expect("allow rule");

        assert!(acl.authorize(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "orders"
        ));
        assert!(!acl.authorize(
            "alice",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders"
        ));
        assert!(!acl.authorize(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "payments"
        ));
    }

    #[test]
    fn authorize_allows_wildcard_resource() {
        let mut acl = AclAuthorizer::new();
        acl.allow("*", AclOperation::Describe, AclResourceType::Topic, "*")
            .expect("wildcard rule");

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
    }

    #[test]
    fn authorize_any_checks_operation_scope() {
        let mut acl = AclAuthorizer::new();
        acl.allow(
            "alice",
            AclOperation::Write,
            AclResourceType::Topic,
            "orders",
        )
        .expect("allow rule");

        assert!(acl.authorize_any("alice", AclOperation::Write, AclResourceType::Topic));
        assert!(!acl.authorize_any(
            "alice",
            AclOperation::IdempotentWrite,
            AclResourceType::Cluster
        ));
    }

    #[test]
    fn super_user_bypasses_rule_checks() {
        let mut acl = AclAuthorizer::new();
        acl.add_super_user("admin").expect("add super user");

        assert!(acl.authorize(
            "admin",
            AclOperation::ClusterAction,
            AclResourceType::Cluster,
            CLUSTER_RESOURCE_NAME
        ));
    }

    #[test]
    fn rejects_invalid_configuration_inputs() {
        let mut acl = AclAuthorizer::new();
        assert_eq!(acl.add_super_user(""), Err(AclConfigError::EmptyPrincipal));
        assert_eq!(
            acl.allow("", AclOperation::Read, AclResourceType::Group, "g1"),
            Err(AclConfigError::EmptyPrincipal)
        );
        assert_eq!(
            acl.allow("alice", AclOperation::Read, AclResourceType::Group, ""),
            Err(AclConfigError::EmptyResourceName)
        );
    }
}
