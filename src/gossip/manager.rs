use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rand::seq::SliceRandom;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::gossip::peer::PeerInfo;
use crate::gossip::protocol::{read_message, write_message, GossipAck, GossipMessage};
use crate::state::StateStore;

/// Runs the P2P gossip subsystem: inbound listener + periodic outbound rounds.
///
/// # Concurrency model and lock ordering
///
/// Two concurrent tasks share `peers` and `store`:
/// - **Listener task** — receives deltas from peers, merges into `store`,
///   updates `peers`.
/// - **Scheduler task** — reads `peers` to select targets, reads `store` to
///   build deltas, spawns short-lived send tasks.
///
/// **Lock ordering (strictly observed, preventing all deadlocks):**
///
/// 1. `DashMap` shard lock — fine-grained, held only while reading or updating
///    a single peer entry.  Never held while taking the StateStore lock.
/// 2. `StateStore` `RwLock` — held only for the duration of a HashMap
///    read/write operation (nanoseconds).  Never held while taking a DashMap
///    shard lock.
///
/// Because there is no code path that holds **both** locks simultaneously,
/// the system is structurally deadlock-free regardless of task scheduling.
pub struct GossipManager {
    config: Config,
    store: StateStore,
    /// Shared with the RPC handler so `node.peers` can return live data.
    pub peers: Arc<DashMap<String, PeerInfo>>,
    generation: Arc<AtomicU64>,
}

impl GossipManager {
    /// Create a manager.  `peers` is typically `Arc::new(DashMap::new())` and
    /// also handed to the RPC handler so both can read the same peer table.
    pub fn new(config: Config, store: StateStore, peers: Arc<DashMap<String, PeerInfo>>) -> Self {
        // Pre-register seed peers so the first gossip round has targets.
        for addr in config.seed_peers.iter().filter(|a| !a.is_empty()) {
            peers
                .entry(addr.clone())
                .or_insert_with(|| PeerInfo::new(addr.clone()));
        }

        Self {
            config,
            store,
            peers,
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Drives the manager until `cancel` fires.
    pub async fn run(self, cancel: CancellationToken) {
        let mgr = Arc::new(self);

        let listener_mgr = Arc::clone(&mgr);
        let listener_cancel = cancel.clone();
        let listener = tokio::spawn(async move {
            if let Err(e) = listener_mgr.run_listener(listener_cancel).await {
                error!("Gossip listener error: {e}");
            }
        });

        let scheduler_mgr = Arc::clone(&mgr);
        let scheduler_cancel = cancel.clone();
        let scheduler = tokio::spawn(async move {
            scheduler_mgr.run_scheduler(scheduler_cancel).await;
        });

        let _ = tokio::join!(listener, scheduler);
        info!("Gossip manager stopped");
    }

    // ── Inbound listener ───────────────────────────────────────────────────

    async fn run_listener(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let addr = self.config.gossip_addr();
        let listener = TcpListener::bind(&addr).await?;
        info!(addr = %addr, "Gossip listener ready");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "Inbound gossip connection");
                            let store = self.store.clone();
                            let peers = Arc::clone(&self.peers);
                            tokio::spawn(async move {
                                if let Err(e) = handle_inbound(stream, store, peers).await {
                                    debug!("Inbound gossip error from {peer_addr}: {e}");
                                }
                            });
                        }
                        Err(e) => error!("Gossip accept error: {e}"),
                    }
                }
                _ = cancel.cancelled() => {
                    info!("Gossip listener stopping");
                    break;
                }
            }
        }
        Ok(())
    }

    // ── Outbound scheduler ─────────────────────────────────────────────────

    async fn run_scheduler(&self, cancel: CancellationToken) {
        let mut ticker = interval(Duration::from_millis(self.config.gossip_interval_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => self.gossip_round().await,
                _ = cancel.cancelled() => {
                    info!("Gossip scheduler stopping");
                    break;
                }
            }
        }
    }

    async fn gossip_round(&self) {
        // ── Step 1: peer selection (brief DashMap iteration) ──────────────
        let targets = {
            let mut all: Vec<(String, PeerInfo)> = self
                .peers
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect();
            // Random selection keeps gossip load evenly distributed.
            all.shuffle(&mut rand::thread_rng());
            let fanout = self.config.gossip_fanout.min(all.len());
            all.truncate(fanout);
            all
        }; // DashMap iteration released here

        if targets.is_empty() {
            return;
        }

        let gen = self.generation.fetch_add(1, Ordering::Relaxed);

        for (peer_id, peer_info) in targets {
            // ── Step 2: build delta (brief StateStore read lock) ──────────
            let delta = self.store.delta_since(peer_info.last_sync_ts);

            // Always contact a brand-new peer (exchange_count == 0) even
            // with an empty delta — this is peer discovery / heartbeat.
            // Without this, a node that hasn't written anything yet would
            // never introduce itself, and the remote end would never learn
            // this node's gossip address, breaking bidirectional propagation.
            if delta.is_empty() && peer_info.exchange_count > 0 {
                debug!(peer = %peer_id, "Skipping: no new delta for known peer");
                continue;
            }

            // Compute the highest timestamp in the delta WE ARE ABOUT TO SEND.
            // This is used — not store.max_timestamp() — to advance
            // `last_sync_ts` after a successful exchange.
            //
            // Using store.max_timestamp() is wrong: we may receive new entries
            // from other peers WHILE awaiting the ack (the tokio runtime yields
            // during TCP I/O).  Those newer entries would raise our local max
            // without B having received them, causing us to skip gossiping them
            // in future rounds (delta_since would return empty for B).
            let max_sent_ts = delta.iter().map(|(_, v)| v.timestamp).max().unwrap_or(0); // 0 for introduction gossip (empty delta)

            let msg = GossipMessage {
                from_node_id: self.store.node_id().to_string(),
                from_gossip_addr: self.config.gossip_addr(),
                deltas: delta,
                generation: gen,
            };

            // ── Step 3: send in a fire-and-forget task (no locks held) ────
            let peers = Arc::clone(&self.peers);
            let peer_addr = peer_info.gossip_addr.clone();

            tokio::spawn(async move {
                match send_and_ack(&peer_addr, &msg).await {
                    Ok(ack) => {
                        debug!(
                            peer = %peer_addr,
                            merged = ack.merged_count,
                            gen = gen,
                            max_sent_ts,
                            "Gossip round complete"
                        );
                        // Advance only to what we SENT, not to our current max.
                        // record_exchange uses .max() internally, so this is safe
                        // to call even when max_sent_ts == 0 (introduction gossip).
                        peers
                            .entry(peer_id)
                            .or_insert_with(|| PeerInfo::new(peer_addr))
                            .record_exchange(max_sent_ts);
                    }
                    Err(e) => warn!(peer = %peer_addr, "Gossip send failed: {e}"),
                }
            });
        }
    }
}

// ── Free functions ─────────────────────────────────────────────────────────

/// Handle one inbound gossip TCP connection.
async fn handle_inbound(
    mut stream: TcpStream,
    store: StateStore,
    peers: Arc<DashMap<String, PeerInfo>>,
) -> anyhow::Result<()> {
    let msg: GossipMessage = read_message(&mut stream).await?;

    debug!(
        from = %msg.from_node_id,
        entries = msg.deltas.len(),
        gen = msg.generation,
        "Received gossip delta"
    );

    // Merge — acquires StateStore write lock briefly, then releases
    let merged = store.merge_delta(msg.deltas).len();

    // Register / update peer using mark_seen() — NOT record_exchange().
    //
    // `last_sync_ts` must only advance when WE push data to a peer and they
    // ack it (handled in gossip_round's success path).  If we used
    // record_exchange here we'd set `last_sync_ts = our_max_ts`, incorrectly
    // marking our data as already received by the sender and preventing us
    // from ever pushing it to them.
    peers
        .entry(msg.from_node_id.clone())
        .or_insert_with(|| PeerInfo::new(msg.from_gossip_addr.clone()))
        .mark_seen();

    let ack = GossipAck {
        from_node_id: store.node_id().to_string(),
        merged_count: merged,
    };
    write_message(&mut stream, &ack).await?;
    Ok(())
}

/// Open a short-lived TCP connection, send a gossip message, and receive ack.
/// Both steps have a 5-second timeout to prevent tasks from hanging on dead peers.
async fn send_and_ack(addr: &str, msg: &GossipMessage) -> anyhow::Result<GossipAck> {
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout to {addr}"))?
        .map_err(|e| anyhow::anyhow!("connect failed to {addr}: {e}"))?;

    write_message(&mut stream, msg).await?;

    tokio::time::timeout(
        Duration::from_secs(5),
        read_message::<GossipAck, _>(&mut stream),
    )
    .await
    .map_err(|_| anyhow::anyhow!("ack timeout from {addr}"))?
}
