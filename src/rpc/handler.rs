use std::sync::Arc;

use dashmap::DashMap;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tracing::{debug, error};

use crate::gossip::PeerInfo;
use crate::rpc::types::{error_codes, RpcError, RpcRequest, RpcResponse};
use crate::state::{StateEvent, StateStore};

// ── Public API ─────────────────────────────────────────────────────────────

/// Dispatches JSON-RPC 2.0 method calls against the state store and peer table.
pub struct RpcHandler {
    store: StateStore,
    /// Live peer table shared with the `GossipManager`.
    peers: Arc<DashMap<String, PeerInfo>>,
}

/// What the server should do after dispatching a request.
pub enum DispatchResult {
    /// Encode and send this JSON string, then wait for the next request.
    Response(String),
    /// Client called `state.subscribe`: send `initial_response` first, then
    /// forward events from `receiver` as server-sent notifications.
    Subscribed {
        initial_response: String,
        receiver: broadcast::Receiver<StateEvent>,
        /// If `Some`, only forward events whose key starts with this prefix.
        prefix: Option<String>,
    },
    /// The request was a notification (no `id`); no response needed.
    Notification,
}

impl RpcHandler {
    pub fn new(store: StateStore, peers: Arc<DashMap<String, PeerInfo>>) -> Self {
        Self { store, peers }
    }

    /// Parse `raw` as a JSON-RPC 2.0 request and dispatch it.
    pub fn dispatch(&self, raw: &str) -> DispatchResult {
        let req: RpcRequest = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                let err =
                    RpcError::new(error_codes::PARSE_ERROR, e.to_string(), Value::Null);
                return DispatchResult::Response(to_json(&err));
            }
        };

        let is_notification = req.id.is_none();
        let id = req.id.clone().unwrap_or(Value::Null);

        debug!(method = %req.method, ?id, "RPC dispatch");

        // `state.subscribe` needs early return to produce DispatchResult::Subscribed
        if req.method == "state.subscribe" {
            return self.handle_subscribe(&req.params, id);
        }

        let response_json = match req.method.as_str() {
            "state.get"    => self.handle_state_get(&req.params, id),
            "state.set"    => self.handle_state_set(&req.params, id),
            "state.keys"   => self.handle_state_keys(id),
            "node.peers"   => self.handle_node_peers(id),
            "sync.status"  => self.handle_sync_status(id),
            "sync.delta"   => self.handle_sync_delta(&req.params, id),
            other => to_json(&RpcError::new(
                error_codes::METHOD_NOT_FOUND,
                format!("Method not found: {other}"),
                id,
            )),
        };

        if is_notification {
            DispatchResult::Notification
        } else {
            DispatchResult::Response(response_json)
        }
    }

    // ── Method handlers ────────────────────────────────────────────────────

    /// `state.get` — params: `{ "key": string }`
    ///
    /// Returns the current LWW value or `null` if the key doesn't exist.
    fn handle_state_get(&self, params: &Value, id: Value) -> String {
        let key = match required_str(params, "key", &id) {
            Ok(k) => k,
            Err(e) => return e,
        };

        let result = match self.store.get(key) {
            Some(v) => json!({
                "key":       key,
                "value":     v.value,
                "timestamp": v.timestamp,
                "node_id":   v.node_id,
            }),
            None => Value::Null,
        };
        to_json(&RpcResponse::ok(result, id))
    }

    /// `state.set` — params: `{ "key": string, "value": string }`
    ///
    /// Writes the value through the CRDT merge.  Returns the winning timestamp.
    fn handle_state_set(&self, params: &Value, id: Value) -> String {
        let key = match required_str(params, "key", &id) {
            Ok(k) => k.to_string(),
            Err(e) => return e,
        };
        let value = match required_str(params, "value", &id) {
            Ok(v) => v.to_string(),
            Err(e) => return e,
        };

        let lww = self.store.set(key, value);
        to_json(&RpcResponse::ok(
            json!({ "timestamp": lww.timestamp, "node_id": lww.node_id }),
            id,
        ))
    }

    /// `state.subscribe` — params: `{ "prefix"?: string }`
    ///
    /// After the initial `{"status": "subscribed"}` response, the server
    /// pushes `state.update` notifications for every matching key change.
    fn handle_subscribe(&self, params: &Value, id: Value) -> DispatchResult {
        let prefix = params
            .get("prefix")
            .and_then(Value::as_str)
            .map(String::from);

        let receiver = self.store.subscribe();
        let initial_response = to_json(&RpcResponse::ok(
            json!({ "status": "subscribed", "prefix": prefix }),
            id,
        ));

        DispatchResult::Subscribed { initial_response, receiver, prefix }
    }

    /// `state.keys` — returns all keys and total count.
    fn handle_state_keys(&self, id: Value) -> String {
        let snapshot = self.store.snapshot();
        let keys: Vec<&str> = snapshot.iter().map(|(k, _)| k.as_str()).collect();
        to_json(&RpcResponse::ok(
            json!({ "keys": keys, "count": keys.len() }),
            id,
        ))
    }

    /// `node.peers` — returns live peer info from the shared DashMap.
    fn handle_node_peers(&self, id: Value) -> String {
        let peers: Vec<Value> = self
            .peers
            .iter()
            .map(|e| {
                json!({
                    "id":             e.key(),
                    "addr":           e.value().gossip_addr,
                    "last_seen_secs": e.value().last_seen.elapsed().as_secs(),
                    "exchange_count": e.value().exchange_count,
                    "last_sync_ts":   e.value().last_sync_ts,
                })
            })
            .collect();

        to_json(&RpcResponse::ok(
            json!({ "peers": peers, "count": peers.len() }),
            id,
        ))
    }

    /// `sync.status` — overall health snapshot of this node.
    fn handle_sync_status(&self, id: Value) -> String {
        to_json(&RpcResponse::ok(
            json!({
                "node_id":       self.store.node_id(),
                "state_size":    self.store.len(),
                "max_timestamp": self.store.max_timestamp(),
                "peer_count":    self.peers.len(),
            }),
            id,
        ))
    }

    /// `sync.delta` — params: `{ "since"?: number }`
    ///
    /// Returns all entries with `timestamp > since`.  Useful for a new node
    /// bootstrapping from a known peer without waiting for gossip rounds.
    fn handle_sync_delta(&self, params: &Value, id: Value) -> String {
        let since = params.get("since").and_then(Value::as_u64).unwrap_or(0);
        let delta = self.store.delta_since(since);

        let entries: Vec<Value> = delta
            .into_iter()
            .map(|(k, v)| {
                json!({
                    "key":       k,
                    "value":     v.value,
                    "timestamp": v.timestamp,
                    "node_id":   v.node_id,
                })
            })
            .collect();

        let count = entries.len();
        to_json(&RpcResponse::ok(json!({ "entries": entries, "count": count }), id))
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn to_json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|e| {
        error!("RPC serialization failed: {e}");
        r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal serialization error"},"id":null}"#
            .to_string()
    })
}

fn required_str<'a>(params: &'a Value, field: &str, id: &Value) -> Result<&'a str, String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            to_json(&RpcError::new(
                error_codes::INVALID_PARAMS,
                format!("Missing required param: \"{field}\""),
                id.clone(),
            ))
        })
}
