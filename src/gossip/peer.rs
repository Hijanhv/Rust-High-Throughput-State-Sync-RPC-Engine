use std::time::Instant;

/// Everything we know about a single remote peer.
///
/// Stored in the shared `DashMap<String, PeerInfo>` where the key is the
/// peer's node ID (or gossip address before the first handshake).
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Gossip TCP address of this peer (host:gossip_port).
    pub gossip_addr: String,

    /// When we last successfully exchanged gossip with this peer.
    /// Used to detect stale / dead peers.
    pub last_seen: Instant,

    /// The highest state timestamp we have *confirmed this peer has received*.
    ///
    /// On the next gossip round we only send entries with
    /// `timestamp > last_sync_ts`, keeping deltas small.
    ///
    /// Starts at 0 so the first exchange always sends the full state.
    pub last_sync_ts: u64,

    /// Cumulative successful gossip exchanges — useful for debugging
    /// convergence and load-balancing across the peer ring.
    pub exchange_count: u64,
}

impl PeerInfo {
    pub fn new(gossip_addr: String) -> Self {
        Self {
            gossip_addr,
            last_seen: Instant::now(),
            last_sync_ts: 0,
            exchange_count: 0,
        }
    }

    /// Update internal counters after a successful two-way exchange.
    ///
    /// `sync_ts` is the highest timestamp in our local store at the time of
    /// the exchange — used as the baseline for the next delta computation.
    pub fn record_exchange(&mut self, sync_ts: u64) {
        self.last_seen = Instant::now();
        // Use max() so a delayed ack can never roll back our sync pointer.
        self.last_sync_ts = self.last_sync_ts.max(sync_ts);
        self.exchange_count += 1;
    }

    /// Record that we heard from this peer (they gossipped TO us), without
    /// touching `last_sync_ts`.
    ///
    /// `last_sync_ts` must only be advanced when **we successfully push data
    /// to the peer** (i.e. in the outbound gossip round's success handler).
    /// Updating it here — when the peer gossips to us — would incorrectly
    /// mark our data as already received by them, preventing future pushes.
    pub fn mark_seen(&mut self) {
        self.last_seen = Instant::now();
        self.exchange_count += 1;
    }

    /// Returns true if we haven't heard from this peer in `timeout_secs` seconds.
    pub fn is_stale(&self, timeout_secs: u64) -> bool {
        self.last_seen.elapsed().as_secs() >= timeout_secs
    }
}
