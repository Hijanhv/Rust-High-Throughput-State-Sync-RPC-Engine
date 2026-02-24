use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use state_sync_engine::config::Config;
use state_sync_engine::gossip::{GossipManager, PeerInfo};
use state_sync_engine::rpc::server as rpc_server;
use state_sync_engine::state::StateStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Logging ────────────────────────────────────────────────────────────
    // Override with RUST_LOG=state_sync_engine=debug for verbose output.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("state_sync_engine=info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    // ── Config ─────────────────────────────────────────────────────────────
    let config = Config::parse();
    let node_id = config.resolved_node_id();

    info!(
        node_id = %node_id,
        rpc  = %config.rpc_addr(),
        gossip = %config.gossip_addr(),
        seeds  = ?config.seed_peers,
        "State-sync engine starting"
    );

    // ── Shared state ───────────────────────────────────────────────────────
    let store = StateStore::new(node_id.clone(), config.event_channel_capacity);

    // The peer table is shared between the gossip manager (writes) and the
    // RPC handler (reads for `node.peers`).  Both get an `Arc` clone.
    let peers: Arc<DashMap<String, PeerInfo>> = Arc::new(DashMap::new());

    // ── Cancellation token ─────────────────────────────────────────────────
    // A single token propagated to all subsystems. Cancelling it triggers
    // cooperative shutdown across the RPC server, gossip listener, and
    // gossip scheduler simultaneously.
    let cancel = CancellationToken::new();

    // ── Spawn JSON-RPC server ──────────────────────────────────────────────
    let rpc_handle = {
        let config = config.clone();
        let store = store.clone();
        let peers = Arc::clone(&peers);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            rpc_server::run(config, store, peers, cancel).await;
        })
    };

    // ── Spawn gossip manager ───────────────────────────────────────────────
    let gossip_handle = {
        let config = config.clone();
        let store = store.clone();
        let peers = Arc::clone(&peers);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            GossipManager::new(config, store, peers).run(cancel).await;
        })
    };

    // ── Wait for Ctrl-C ────────────────────────────────────────────────────
    tokio::signal::ctrl_c().await?;
    info!("Ctrl-C received — initiating graceful shutdown");
    cancel.cancel();

    // Wait for all subsystems to drain and exit cleanly.
    let _ = tokio::join!(rpc_handle, gossip_handle);
    info!("Shutdown complete");

    Ok(())
}
