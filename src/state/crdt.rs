use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// A **Last-Write-Wins (LWW) Register** — the atomic unit of state.
///
/// # Merge semantics
///
/// Given two replicas of the same key, the winner is chosen by:
/// 1. Higher `timestamp` (microseconds) wins.
/// 2. On a tie, the higher `node_id` (lexicographic) wins.
///
/// This makes `merge` **commutative**, **associative**, and **idempotent** —
/// the three properties that define a CRDT merge function. No matter which
/// order you merge replicas in, you always get the same final state.
///
/// # Tradeoff vs. Raft / consensus
///
/// LWW CRDTs sacrifice *strong consistency* (every node sees every write in
/// the exact same order) for *availability* and *partition tolerance* (nodes
/// can keep accepting writes even when disconnected; they reconcile later).
/// Concurrent writes to the same key are resolved by wall-clock time, so a
/// slight clock skew between nodes could cause one write to "win" over a
/// logically-later one.  For key-value configuration data this is acceptable;
/// for financial ledgers it is not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwValue {
    pub value: String,
    /// Microseconds since the UNIX epoch.  Microsecond resolution dramatically
    /// reduces collision probability compared to milliseconds on busy nodes.
    pub timestamp: u64,
    /// Tiebreaker: the ID of the node that last wrote this value.
    pub node_id: String,
}

impl LwwValue {
    /// Construct a new value stamped with the current wall-clock time.
    pub fn new(value: String, node_id: String) -> Self {
        Self {
            value,
            timestamp: now_micros(),
            node_id,
        }
    }

    /// Deterministic, conflict-free merge.  Always call this instead of
    /// picking a value manually — callers must never implement their own
    /// "newer wins" logic, or the CRDT invariants break.
    pub fn merge(a: &Self, b: &Self) -> Self {
        match a.timestamp.cmp(&b.timestamp) {
            std::cmp::Ordering::Greater => a.clone(),
            std::cmp::Ordering::Less => b.clone(),
            // Timestamp collision: deterministic tiebreak by node_id so that
            // all nodes converge to the same winner without communication.
            std::cmp::Ordering::Equal => {
                if a.node_id >= b.node_id {
                    a.clone()
                } else {
                    b.clone()
                }
            }
        }
    }

    /// Would `incoming` win a merge against `self`?
    /// Used by the store to decide whether to emit a state-change event.
    pub fn is_dominated_by(&self, incoming: &Self) -> bool {
        &LwwValue::merge(self, incoming) != self
    }
}

/// Current wall-clock time in microseconds since UNIX epoch.
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the UNIX epoch — check your clock")
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_higher_timestamp_wins() {
        let old = LwwValue { value: "old".into(), timestamp: 100, node_id: "a".into() };
        let new = LwwValue { value: "new".into(), timestamp: 200, node_id: "b".into() };
        assert_eq!(LwwValue::merge(&old, &new).value, "new");
        assert_eq!(LwwValue::merge(&new, &old).value, "new"); // commutative
    }

    #[test]
    fn merge_tie_broken_by_node_id() {
        let a = LwwValue { value: "a".into(), timestamp: 100, node_id: "node-z".into() };
        let b = LwwValue { value: "b".into(), timestamp: 100, node_id: "node-a".into() };
        // "node-z" > "node-a" lexicographically
        assert_eq!(LwwValue::merge(&a, &b).value, "a");
        assert_eq!(LwwValue::merge(&b, &a).value, "a"); // commutative
    }

    #[test]
    fn merge_is_idempotent() {
        let v = LwwValue::new("x".into(), "n".into());
        assert_eq!(LwwValue::merge(&v, &v), v);
    }
}
