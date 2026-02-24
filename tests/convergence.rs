/// Multi-node gossip convergence integration tests.
///
/// Each test spins up real in-process nodes (actual TCP sockets, real Tokio
/// tasks) and verifies that writes propagate across the cluster within an
/// expected time window.
///
/// # Port allocation
///
/// Tests run in parallel inside the same process.  A global `AtomicU16`
/// counter allocates (rpc_port, gossip_port) pairs starting at 19 000,
/// preventing collisions between concurrently-running test cases.
///
/// # Gossip interval
///
/// All test nodes use a 100 ms gossip interval (vs. the 1 000 ms default)
/// so that convergence happens in well under a second and tests finish quickly.
use dashmap::DashMap;
use state_sync_engine::{
    config::Config,
    gossip::{GossipManager, PeerInfo},
    state::StateStore,
};
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

// ── Port allocation ────────────────────────────────────────────────────────

static NEXT_PORT: AtomicU16 = AtomicU16::new(19_000);

/// Returns `n` pairs of `(rpc_port, gossip_port)` that are unique within this
/// process run.
fn alloc_ports(n: usize) -> Vec<(u16, u16)> {
    let base = NEXT_PORT.fetch_add((n * 2) as u16, Ordering::Relaxed);
    (0..n)
        .map(|i| (base + i as u16 * 2, base + i as u16 * 2 + 1))
        .collect()
}

// ── Test node harness ──────────────────────────────────────────────────────

struct TestNode {
    pub store: StateStore,
    cancel: CancellationToken,
}

impl TestNode {
    /// Start a node and return once the gossip listener is spawned.
    /// The caller must `sleep` briefly after this to let the OS bind the port.
    async fn start(
        node_id: &str,
        rpc_port: u16,
        gossip_port: u16,
        seed_peers: Vec<String>,
    ) -> Self {
        let config = Config {
            host: "127.0.0.1".to_string(),
            rpc_port,
            gossip_port,
            node_id: Some(node_id.to_string()),
            seed_peers,
            gossip_interval_ms: 100, // fast for tests
            gossip_fanout: 3,
            event_channel_capacity: 256,
        };

        let store = StateStore::new(node_id.to_string(), 256);
        let peers: Arc<DashMap<String, PeerInfo>> = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();

        let mgr = GossipManager::new(config, store.clone(), Arc::clone(&peers));
        let c = cancel.clone();
        tokio::spawn(async move { mgr.run(c).await });

        TestNode { store, cancel }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ── Convergence helpers ────────────────────────────────────────────────────

/// Spin-poll `predicate` every `poll_ms` until it returns `true` or `timeout`
/// elapses.  Panics on timeout and returns elapsed time on success.
async fn converge_within<F>(predicate: F, timeout: Duration, poll_ms: u64) -> Duration
where
    F: Fn() -> bool,
{
    let start = Instant::now();
    loop {
        if predicate() {
            return start.elapsed();
        }
        assert!(
            start.elapsed() < timeout,
            "convergence timed out after {:?}",
            timeout
        );
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }
}

/// Short sleep to let all freshly-spawned listener tasks actually bind their
/// TCP ports before the test starts writing or connecting.
async fn wait_for_listeners() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// A single write on node A propagates to node B within one gossip round.
#[tokio::test]
async fn two_node_write_propagates() {
    let ports = alloc_ports(2);
    let (rpc_a, gossip_a) = ports[0];
    let (rpc_b, gossip_b) = ports[1];

    let node_a = TestNode::start("node-a", rpc_a, gossip_a, vec![]).await;
    let node_b = TestNode::start(
        "node-b",
        rpc_b,
        gossip_b,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;

    wait_for_listeners().await;

    node_a.store.set("hello".into(), "world".into());

    let elapsed = converge_within(
        || node_b
            .store
            .get("hello")
            .map(|v| v.value == "world")
            .unwrap_or(false),
        Duration::from_secs(5),
        20,
    )
    .await;

    println!("[2-node] write-propagates converged in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "should converge in < 2 s, took {elapsed:?}"
    );
}

/// A write on node A propagates to both B and C (star topology).
#[tokio::test]
async fn three_node_star_convergence() {
    let ports = alloc_ports(3);
    let (rpc_a, gossip_a) = ports[0];
    let (rpc_b, gossip_b) = ports[1];
    let (rpc_c, gossip_c) = ports[2];

    // A is the hub; B and C seed from A
    let node_a = TestNode::start("node-a", rpc_a, gossip_a, vec![]).await;
    let node_b = TestNode::start(
        "node-b",
        rpc_b,
        gossip_b,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;
    let node_c = TestNode::start(
        "node-c",
        rpc_c,
        gossip_c,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;

    wait_for_listeners().await;

    node_a.store.set("star-key".into(), "star-value".into());

    let elapsed = converge_within(
        || {
            let b_ok = node_b
                .store
                .get("star-key")
                .map(|v| v.value == "star-value")
                .unwrap_or(false);
            let c_ok = node_c
                .store
                .get("star-key")
                .map(|v| v.value == "star-value")
                .unwrap_or(false);
            b_ok && c_ok
        },
        Duration::from_secs(6),
        20,
    )
    .await;

    println!("[3-node star] converged in {elapsed:?}");
    assert!(elapsed < Duration::from_secs(4));
}

/// Five nodes, all seeded from node 0 — a write on node 0 reaches everyone.
#[tokio::test]
async fn five_node_hub_convergence() {
    let ports = alloc_ports(5);
    let gossip_0 = ports[0].1;

    let mut nodes = Vec::new();
    for (i, &(rpc_port, gossip_port)) in ports.iter().enumerate() {
        let seeds = if i == 0 {
            vec![]
        } else {
            vec![format!("127.0.0.1:{gossip_0}")]
        };
        nodes.push(TestNode::start(&format!("node-{i}"), rpc_port, gossip_port, seeds).await);
    }

    wait_for_listeners().await;

    nodes[0].store.set("hub-key".into(), "hub-value".into());

    let elapsed = converge_within(
        || {
            nodes[1..].iter().all(|n| {
                n.store
                    .get("hub-key")
                    .map(|v| v.value == "hub-value")
                    .unwrap_or(false)
            })
        },
        Duration::from_secs(10),
        20,
    )
    .await;

    println!("[5-node hub] converged in {elapsed:?}");
    assert!(elapsed < Duration::from_secs(6));
}

/// Writes on three different nodes all converge to all nodes.
/// Validates that gossip works in all directions, not just hub-and-spoke.
#[tokio::test]
async fn three_node_concurrent_writes_all_converge() {
    let ports = alloc_ports(3);
    let (rpc_a, gossip_a) = ports[0];
    let (rpc_b, gossip_b) = ports[1];
    let (rpc_c, gossip_c) = ports[2];

    let node_a = TestNode::start("node-a", rpc_a, gossip_a, vec![]).await;
    let node_b = TestNode::start(
        "node-b",
        rpc_b,
        gossip_b,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;
    let node_c = TestNode::start(
        "node-c",
        rpc_c,
        gossip_c,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;

    wait_for_listeners().await;

    // Each node writes its own unique key simultaneously
    node_a.store.set("key-a".into(), "val-a".into());
    node_b.store.set("key-b".into(), "val-b".into());
    node_c.store.set("key-c".into(), "val-c".into());

    let all_stores = [&node_a.store, &node_b.store, &node_c.store];

    let elapsed = converge_within(
        || {
            all_stores.iter().all(|s| {
                s.get("key-a").map(|v| v.value == "val-a").unwrap_or(false)
                    && s.get("key-b").map(|v| v.value == "val-b").unwrap_or(false)
                    && s.get("key-c").map(|v| v.value == "val-c").unwrap_or(false)
            })
        },
        Duration::from_secs(10),
        20,
    )
    .await;

    println!("[3-node concurrent writes] all converged in {elapsed:?}");
    assert!(elapsed < Duration::from_secs(6));
}

/// When two nodes write the same key at different times, the later write wins
/// (LWW semantics) and both nodes converge to that value.
#[tokio::test]
async fn crdt_last_write_wins_on_conflict() {
    let ports = alloc_ports(2);
    let (rpc_a, gossip_a) = ports[0];
    let (rpc_b, gossip_b) = ports[1];

    let node_a = TestNode::start("node-a", rpc_a, gossip_a, vec![]).await;
    let node_b = TestNode::start(
        "node-b",
        rpc_b,
        gossip_b,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;

    wait_for_listeners().await;

    // A writes first, then B writes slightly later → B's value should win
    node_a.store.set("conflict".into(), "from-a".into());
    // Ensure B's timestamp is strictly greater
    tokio::time::sleep(Duration::from_millis(5)).await;
    node_b.store.set("conflict".into(), "from-b".into());

    let elapsed = converge_within(
        || {
            let a_val = node_a.store.get("conflict").map(|v| v.value.clone());
            let b_val = node_b.store.get("conflict").map(|v| v.value.clone());
            // Both must agree on "from-b" (higher timestamp)
            a_val == Some("from-b".into()) && b_val == Some("from-b".into())
        },
        Duration::from_secs(5),
        20,
    )
    .await;

    println!("[LWW conflict] resolved and converged in {elapsed:?}");

    // Verify the correct winner
    assert_eq!(node_a.store.get("conflict").unwrap().value, "from-b");
    assert_eq!(node_b.store.get("conflict").unwrap().value, "from-b");
}

/// A write that arrives via gossip does not overwrite a locally-newer value.
/// Ensures the CRDT merge is monotonic (state never goes backwards).
#[tokio::test]
async fn crdt_merge_never_regresses_state() {
    let ports = alloc_ports(2);
    let (rpc_a, gossip_a) = ports[0];
    let (rpc_b, gossip_b) = ports[1];

    let node_a = TestNode::start("node-a", rpc_a, gossip_a, vec![]).await;
    let node_b = TestNode::start(
        "node-b",
        rpc_b,
        gossip_b,
        vec![format!("127.0.0.1:{gossip_a}")],
    )
    .await;

    wait_for_listeners().await;

    // B writes first (lower timestamp)
    node_b.store.set("monotone".into(), "old".into());
    tokio::time::sleep(Duration::from_millis(5)).await;

    // A writes newer value
    node_a.store.set("monotone".into(), "new".into());

    // Wait for full convergence
    let elapsed = converge_within(
        || {
            node_b
                .store
                .get("monotone")
                .map(|v| v.value == "new")
                .unwrap_or(false)
        },
        Duration::from_secs(5),
        20,
    )
    .await;

    println!("[CRDT monotone] converged in {elapsed:?}");

    // B must have updated to "new" (A's write) — never rolled back to "old"
    assert_eq!(node_b.store.get("monotone").unwrap().value, "new");
    assert_eq!(node_a.store.get("monotone").unwrap().value, "new");
}
