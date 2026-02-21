#![forbid(unsafe_code)]

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorError {
    UnknownMember(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResult {
    pub generation: u32,
    pub leader_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberState {
    pub member_id: String,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone)]
pub struct GroupState {
    group_id: String,
    session_timeout_ms: u64,
    generation: u32,
    members: BTreeMap<String, MemberState>,
}

impl GroupState {
    pub fn new(group_id: impl Into<String>, session_timeout_ms: u64) -> Self {
        Self {
            group_id: group_id.into(),
            session_timeout_ms,
            generation: 0,
            members: BTreeMap::new(),
        }
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn members(&self) -> Vec<String> {
        self.members.keys().cloned().collect()
    }

    pub fn contains_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    pub fn join(&mut self, member_id: impl Into<String>, now_ms: u64) -> JoinResult {
        let member_id = member_id.into();
        let was_new = !self.members.contains_key(&member_id);
        let member = MemberState {
            member_id: member_id.clone(),
            last_heartbeat_ms: now_ms,
        };
        self.members.insert(member_id.clone(), member);
        if was_new {
            self.generation += 1;
        }

        JoinResult {
            generation: self.generation,
            leader_id: self.leader_id(),
        }
    }

    pub fn heartbeat(&mut self, member_id: &str, now_ms: u64) -> Result<(), CoordinatorError> {
        let Some(member) = self.members.get_mut(member_id) else {
            return Err(CoordinatorError::UnknownMember(member_id.to_string()));
        };
        member.last_heartbeat_ms = now_ms;
        Ok(())
    }

    pub fn evict_expired(&mut self, now_ms: u64) -> Vec<String> {
        let mut expired = Vec::new();
        for (member_id, state) in &self.members {
            let age = now_ms.saturating_sub(state.last_heartbeat_ms);
            if age > self.session_timeout_ms {
                expired.push(member_id.clone());
            }
        }
        for member_id in &expired {
            self.members.remove(member_id);
        }
        if !expired.is_empty() {
            self.generation += 1;
        }
        expired
    }

    fn leader_id(&self) -> String {
        self.members
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(String::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_increments_generation_on_new_member() {
        let mut group = GroupState::new("g", 5000);
        let first = group.join("member-2", 10);
        assert_eq!(first.generation, 1);
        assert_eq!(first.leader_id, "member-2");

        let second = group.join("member-1", 11);
        assert_eq!(second.generation, 2);
        assert_eq!(second.leader_id, "member-1");
    }

    #[test]
    fn rejoin_existing_member_keeps_generation() {
        let mut group = GroupState::new("g", 5000);
        group.join("member-1", 10);
        let result = group.join("member-1", 11);
        assert_eq!(result.generation, 1);
    }

    #[test]
    fn unknown_heartbeat_fails() {
        let mut group = GroupState::new("g", 100);
        let err = group.heartbeat("x", 0).expect_err("unknown member");
        assert_eq!(err, CoordinatorError::UnknownMember(String::from("x")));
    }

    #[test]
    fn evict_expired_members() {
        let mut group = GroupState::new("g", 100);
        group.join("member-1", 0);
        group.join("member-2", 90);
        let expired = group.evict_expired(150);
        assert_eq!(expired, vec![String::from("member-1")]);
        assert_eq!(group.generation(), 3);
    }

    #[test]
    fn contains_member_reflects_membership() {
        let mut group = GroupState::new("g", 100);
        assert!(!group.contains_member("member-1"));
        group.join("member-1", 0);
        assert!(group.contains_member("member-1"));
    }
}
