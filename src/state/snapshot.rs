use crate::state::store::StateDelta;
use anyhow::Result;

/// Serialize a state delta to a compact byte representation.
///
/// We use **bincode** rather than JSON for gossip payloads because:
/// - ~2× smaller on the wire (no field name strings, no quoted values)
/// - ~5–10× faster to encode/decode (pure memory copies, no string parsing)
///
/// JSON is reserved for the human-facing RPC layer where readability matters.
pub fn serialize_delta(delta: &StateDelta) -> Result<Vec<u8>> {
    bincode::serialize(delta).map_err(|e| anyhow::anyhow!("snapshot serialize: {}", e))
}

/// Deserialize bytes back into a state delta.
pub fn deserialize_delta(bytes: &[u8]) -> Result<StateDelta> {
    bincode::deserialize(bytes).map_err(|e| anyhow::anyhow!("snapshot deserialize: {}", e))
}
