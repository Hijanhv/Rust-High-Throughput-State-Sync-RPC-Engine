use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::state::StateDelta;

/// Message pushed from one gossip node to another.
///
/// Wire format:  `[ 4-byte big-endian length ][ bincode payload ]`
///
/// The length prefix makes framing simple and allocation-safe: we know exactly
/// how many bytes to read before deserializing, and we can reject absurdly
/// large messages before touching the heap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    /// Sender's node ID.
    pub from_node_id: String,
    /// Sender's gossip address — so the receiver can register the sender as a
    /// peer without a separate discovery message.
    pub from_gossip_addr: String,
    /// Only entries newer than the receiver's last known sync timestamp.
    /// Keeping deltas small is the core reason gossip stays efficient as
    /// state grows.
    pub deltas: StateDelta,
    /// Monotonically increasing generation counter per sender.
    /// Useful for ordering debug logs; not used for correctness.
    pub generation: u64,
}

/// Acknowledgment sent back to the initiator after merging a gossip message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipAck {
    /// Receiver's node ID (for the initiator to update peer registry).
    pub from_node_id: String,
    /// How many entries actually changed local state — informational.
    pub merged_count: usize,
}

// ── Framing helpers ────────────────────────────────────────────────────────

/// Maximum accepted message size (16 MiB).  Prevents a malicious peer from
/// making us allocate arbitrarily large buffers.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Write a length-prefixed, bincode-encoded message to any async writer.
pub async fn write_message<T, W>(writer: &mut W, msg: &T) -> anyhow::Result<()>
where
    T: Serialize,
    W: AsyncWriteExt + Unpin,
{
    let payload = bincode::serialize(msg).map_err(|e| anyhow::anyhow!("gossip encode: {}", e))?;
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read and decode a length-prefixed, bincode-encoded message.
pub async fn read_message<T, R>(reader: &mut R) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_BYTES {
        anyhow::bail!(
            "gossip message too large: {} bytes (max {})",
            len,
            MAX_MESSAGE_BYTES
        );
    }

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    bincode::deserialize(&payload).map_err(|e| anyhow::anyhow!("gossip decode: {}", e))
}
