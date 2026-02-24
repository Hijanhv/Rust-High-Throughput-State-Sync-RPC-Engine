use std::collections::HashMap;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, trace};

use crate::state::crdt::LwwValue;

/// A (key, LwwValue) pair broadcast to all active subscribers on every write.
#[derive(Clone, Debug)]
pub struct StateEvent {
    pub key: String,
    pub value: LwwValue,
}

/// A list of (key, LwwValue) pairs — used for full snapshots and partial deltas.
pub type StateDelta = Vec<(String, LwwValue)>;

/// Thread-safe, CRDT-backed key-value store.
///
/// # Why `parking_lot::RwLock` and not `tokio::sync::RwLock`?
///
/// The critical sections here are pure in-memory HashMap operations — they
/// complete in nanoseconds and **never** need to yield to the async executor.
/// Using `tokio::sync::RwLock` would add future-polling overhead for zero
/// benefit.  The async variant is only needed when the critical section itself
/// does I/O (e.g. a database call).
///
/// `parking_lot::RwLock` also eliminates *lock poisoning* — in std's
/// implementation, if a thread panics while holding a write lock the lock
/// becomes permanently poisoned, requiring every reader to `unwrap()` or
/// propagate the poison.  In async Rust this is an unnecessary footgun.
///
/// # Lock rules (never violate these)
///
/// 1. **Never hold the lock across an `.await` point.**  All critical sections
///    are scoped to a single `{ }` block that contains no async calls.
/// 2. **Lock ordering: only one lock at a time.**  The `StateStore` lock is
///    never acquired while another `StateStore` lock or `DashMap` shard lock
///    is held.  This makes deadlocks structurally impossible.
///
/// # Cloning
///
/// `StateStore` is cheaply cloneable — the clone shares the same `Arc` so no
/// data is copied.  Pass clones freely to tasks without `Arc<StateStore>`.
#[derive(Clone)]
pub struct StateStore {
    inner: std::sync::Arc<StateStoreInner>,
}

struct StateStoreInner {
    /// The actual CRDT state.  Guards: see above.
    state: RwLock<HashMap<String, LwwValue>>,
    /// Fanout channel for pushing state-change events to subscribers.
    /// `broadcast::Sender` is `Clone + Send + Sync` and does not require a lock.
    event_tx: broadcast::Sender<StateEvent>,
    node_id: String,
}

impl StateStore {
    pub fn new(node_id: String, channel_capacity: usize) -> Self {
        let (event_tx, _) = broadcast::channel(channel_capacity);
        StateStore {
            inner: std::sync::Arc::new(StateStoreInner {
                state: RwLock::new(HashMap::new()),
                event_tx,
                node_id,
            }),
        }
    }

    // ── Read operations ────────────────────────────────────────────────────

    /// Look up a key.  Acquires a read lock, clones the value, releases.
    pub fn get(&self, key: &str) -> Option<LwwValue> {
        self.inner.state.read().get(key).cloned()
    }

    /// Full snapshot of every key/value in the store.
    pub fn snapshot(&self) -> StateDelta {
        self.inner
            .state
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Entries whose timestamp is *strictly* after `since` (microseconds).
    /// Used by the gossip layer to produce minimal deltas rather than full
    /// snapshots, keeping gossip messages small even as state grows.
    pub fn delta_since(&self, since: u64) -> StateDelta {
        self.inner
            .state
            .read()
            .iter()
            .filter(|(_, v)| v.timestamp > since)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Highest timestamp in the store (used to track gossip sync progress).
    pub fn max_timestamp(&self) -> u64 {
        self.inner
            .state
            .read()
            .values()
            .map(|v| v.timestamp)
            .max()
            .unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.inner.state.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.state.read().is_empty()
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    // ── Write operations ───────────────────────────────────────────────────

    /// Write a new value from **this node**.
    ///
    /// The timestamp is assigned inside this function so that clock skew is
    /// a purely local concern — remote nodes never dictate our timestamps.
    ///
    /// Returns the winning `LwwValue` after the merge (may be the existing
    /// value if a concurrent write with a higher timestamp was already there).
    pub fn set(&self, key: String, value: String) -> LwwValue {
        let incoming = LwwValue::new(value, self.inner.node_id.clone());
        // ── critical section (sync, no .await) ────────────────────────────
        let winner = {
            let mut state = self.inner.state.write();
            let entry = state.entry(key.clone()).or_insert_with(|| incoming.clone());
            *entry = LwwValue::merge(entry, &incoming);
            entry.clone()
        };
        // ── lock released ──────────────────────────────────────────────────
        trace!(key = %key, ts = winner.timestamp, "state.set");
        // Ignore send errors — zero subscribers is fine
        let _ = self.inner.event_tx.send(StateEvent { key, value: winner.clone() });
        winner
    }

    /// Merge an incoming delta from a remote peer.
    ///
    /// For each entry in `delta`, applies `LwwValue::merge` against the local
    /// entry.  If the incoming value wins, the store is updated and a
    /// `StateEvent` is emitted.
    ///
    /// Returns the list of keys whose local value actually changed.
    pub fn merge_delta(&self, delta: StateDelta) -> Vec<String> {
        let mut updated_keys = Vec::new();
        // ── critical section ───────────────────────────────────────────────
        {
            let mut state = self.inner.state.write();
            for (key, incoming) in &delta {
                let winner = match state.get(key) {
                    None => incoming.clone(),
                    Some(existing) => LwwValue::merge(existing, incoming),
                };
                let changed = state.get(key) != Some(&winner);
                if changed {
                    state.insert(key.clone(), winner);
                    updated_keys.push(key.clone());
                }
            }
        }
        // ── lock released — safe to do async-adjacent work ─────────────────
        for key in &updated_keys {
            if let Some(val) = self.get(key) {
                let _ = self.inner.event_tx.send(StateEvent {
                    key: key.clone(),
                    value: val,
                });
            }
        }
        debug!(count = updated_keys.len(), "merged delta from peer");
        updated_keys
    }

    // ── Pub/sub ────────────────────────────────────────────────────────────

    /// Subscribe to all future state-change events.
    ///
    /// Returns a `broadcast::Receiver`.  If a subscriber falls more than
    /// `channel_capacity` events behind, `recv()` returns
    /// `Err(RecvError::Lagged(n))` — it is the caller's responsibility to
    /// handle lag (the RPC server sends a `state.lag` notification).
    pub fn subscribe(&self) -> broadcast::Receiver<StateEvent> {
        self.inner.event_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_roundtrip() {
        let store = StateStore::new("node-1".into(), 64);
        store.set("foo".into(), "bar".into());
        let v = store.get("foo").unwrap();
        assert_eq!(v.value, "bar");
        assert_eq!(v.node_id, "node-1");
    }

    #[test]
    fn merge_delta_updates_stale_entries() {
        let store = StateStore::new("node-1".into(), 64);
        store.set("k".into(), "local".into());

        // Fabricate a newer remote value
        let remote = LwwValue {
            value: "remote".into(),
            timestamp: store.get("k").unwrap().timestamp + 1_000_000,
            node_id: "node-2".into(),
        };
        let updated = store.merge_delta(vec![("k".into(), remote.clone())]);
        assert_eq!(updated, vec!["k"]);
        assert_eq!(store.get("k").unwrap().value, "remote");
    }

    #[test]
    fn merge_delta_does_not_regress() {
        let store = StateStore::new("node-1".into(), 64);
        store.set("k".into(), "current".into());
        let current_ts = store.get("k").unwrap().timestamp;

        // Fabricate an *older* remote value
        let stale = LwwValue {
            value: "stale".into(),
            timestamp: current_ts - 1,
            node_id: "node-2".into(),
        };
        let updated = store.merge_delta(vec![("k".into(), stale)]);
        assert!(updated.is_empty(), "stale value should not trigger update");
        assert_eq!(store.get("k").unwrap().value, "current");
    }

    #[test]
    fn delta_since_filters_correctly() {
        let store = StateStore::new("node-1".into(), 64);
        store.set("a".into(), "1".into());
        let mid_ts = store.get("a").unwrap().timestamp;
        std::thread::sleep(std::time::Duration::from_micros(10));
        store.set("b".into(), "2".into());

        let delta = store.delta_since(mid_ts);
        assert_eq!(delta.len(), 1);
        assert_eq!(delta[0].0, "b");
    }
}
