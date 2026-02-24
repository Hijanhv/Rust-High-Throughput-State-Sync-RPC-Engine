use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::LwwValue;

// ── Incoming ───────────────────────────────────────────────────────────────

/// JSON-RPC 2.0 request object.
///
/// `id` is `None` for notifications (client-initiated fire-and-forget).
/// `params` defaults to `null` when absent.
#[derive(Debug, Clone, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    /// `null` | string | number — JSON-RPC 2.0 allows any non-array/object.
    pub id: Option<Value>,
}

// ── Outgoing ───────────────────────────────────────────────────────────────

/// Successful JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct RpcResponse {
    pub jsonrpc: &'static str,
    pub result: Value,
    pub id: Value,
}

/// Error JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct RpcError {
    pub jsonrpc: &'static str,
    pub error: RpcErrorBody,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcErrorBody {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Server-initiated notification (no `id` field — not a response to any request).
/// Used for subscription push and lag warnings.
#[derive(Debug, Serialize)]
pub struct RpcNotification {
    pub jsonrpc: &'static str,
    pub method: &'static str,
    pub params: Value,
}

// ── Standard error codes (JSON-RPC 2.0 spec) ──────────────────────────────

pub mod error_codes {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL_ERROR: i64 = -32603;
}

// ── Constructors ───────────────────────────────────────────────────────────

impl RpcResponse {
    pub fn ok(result: Value, id: Value) -> Self {
        Self { jsonrpc: "2.0", result, id }
    }
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>, id: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            error: RpcErrorBody { code, message: message.into(), data: None },
            id,
        }
    }
}

impl RpcNotification {
    /// Push a state-change event to a subscriber.
    pub fn state_update(key: &str, val: &LwwValue) -> Self {
        Self {
            jsonrpc: "2.0",
            method: "state.update",
            params: serde_json::json!({
                "key":       key,
                "value":     val.value,
                "timestamp": val.timestamp,
                "node_id":   val.node_id,
            }),
        }
    }

    /// Inform a slow subscriber that it missed `dropped` events.
    pub fn lag(dropped: u64) -> Self {
        Self {
            jsonrpc: "2.0",
            method: "state.lag",
            params: serde_json::json!({ "dropped": dropped }),
        }
    }
}
