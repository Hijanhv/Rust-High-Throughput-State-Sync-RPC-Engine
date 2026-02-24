use clap::Parser;
use uuid::Uuid;

/// Configuration for a single engine node.
///
/// All values have sensible defaults so you can start a development cluster
/// with nothing but `cargo run`.
#[derive(Parser, Clone, Debug)]
#[command(
    name = "state-sync-engine",
    about = "Distributed state-sync engine — JSON-RPC 2.0 + P2P gossip + CRDT state",
    long_about = None,
)]
pub struct Config {
    /// IP address to bind both the RPC and gossip listeners on.
    #[arg(long, default_value = "127.0.0.1", env = "SSE_HOST")]
    pub host: String,

    /// TCP port for the JSON-RPC 2.0 server.
    #[arg(long, default_value_t = 7070, env = "SSE_RPC_PORT")]
    pub rpc_port: u16,

    /// TCP port for the gossip protocol listener.
    #[arg(long, default_value_t = 7071, env = "SSE_GOSSIP_PORT")]
    pub gossip_port: u16,

    /// Human-readable node identifier. Auto-generated (UUID v4) when omitted.
    #[arg(long, env = "SSE_NODE_ID")]
    pub node_id: Option<String>,

    /// Comma-separated list of seed peer gossip addresses (host:port).
    /// Example: --seed-peers 127.0.0.1:7073,127.0.0.1:7075
    #[arg(long, value_delimiter = ',', default_values_t = Vec::<String>::new(), env = "SSE_SEED_PEERS")]
    pub seed_peers: Vec<String>,

    /// How often (milliseconds) to run a gossip round.
    ///
    /// Tradeoff: lower = faster convergence, higher CPU + network usage.
    /// At fanout=3 and 1 000 ms interval, a 10-node cluster converges in
    /// roughly log₃(10) ≈ 2.1 rounds ≈ 2.1 seconds worst-case.
    #[arg(long, default_value_t = 1_000, env = "SSE_GOSSIP_INTERVAL_MS")]
    pub gossip_interval_ms: u64,

    /// Number of peers to push deltas to per gossip round (fanout).
    ///
    /// Convergence time ≈ log_fanout(N) rounds. fanout=3 is a good default:
    /// enough spread without hammering the network.
    #[arg(long, default_value_t = 3, env = "SSE_GOSSIP_FANOUT")]
    pub gossip_fanout: usize,

    /// Broadcast channel capacity for internal state-change events.
    ///
    /// Slow RPC subscribers that fall more than this many events behind will
    /// receive a `state.lag` notification instead of buffered events.
    /// Bounded channels are critical for preventing unbounded memory growth.
    #[arg(long, default_value_t = 4_096, env = "SSE_EVENT_CAPACITY")]
    pub event_channel_capacity: usize,
}

impl Config {
    /// Returns `node_id` if set, otherwise generates a fresh UUID v4.
    pub fn resolved_node_id(&self) -> String {
        self.node_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    }

    pub fn rpc_addr(&self) -> String {
        format!("{}:{}", self.host, self.rpc_port)
    }

    pub fn gossip_addr(&self) -> String {
        format!("{}:{}", self.host, self.gossip_port)
    }
}
