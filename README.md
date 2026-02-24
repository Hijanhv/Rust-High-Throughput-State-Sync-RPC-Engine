# State-Sync Engine

> A high-throughput distributed state synchronisation engine written in Rust.
> JSON-RPC 2.0 server · P2P gossip · CRDT state · zero-copy gossip codec · criterion benchmarks.

```
┌──────────────────────────────────────────────────────────────┐
│                        Node Process                          │
│                                                              │
│  ┌─────────────────────┐    ┌──────────────────────────────┐ │
│  │   JSON-RPC Server   │    │       Gossip Manager         │ │
│  │  (TCP, port 7070)   │    │  listener  │  scheduler      │ │
│  │  state.get/set      │    │  port 7071 │  every 1 000 ms │ │
│  │  state.subscribe ─────────────────────────────────────► │ │
│  │  node.peers         │    │  fanout: 3 random peers      │ │
│  └────────┬────────────┘    └─────────────┬────────────────┘ │
│           │                               │                  │
│           └───────────────┬───────────────┘                  │
│                           ▼                                  │
│              ┌────────────────────────┐                      │
│              │      State Engine      │                      │
│              │  Arc<RwLock<HashMap>>  │                      │
│              │  LWW-Register CRDT     │                      │
│              └────────────┬───────────┘                      │
│                           │                                  │
│              ┌────────────▼───────────┐                      │
│              │      Event Bus         │                      │
│              │  broadcast::channel    │                      │
│              │  → subscription push   │                      │
│              └────────────────────────┘                      │
└──────────────────────────────────────────────────────────────┘
```

---

## What Is This? (For Everyone, Including Non-Developers)

Imagine a giant sticky-note board hanging in a room. Anyone in the room can
walk up and write a note, or read a note someone else wrote. That board is the
**state store**.

Now imagine ten people in ten *different* rooms, each with their own copy of
the same board. They can't all see each other's boards in real time. Instead,
every few seconds each person whispers the newest notes they've added to a
couple of random neighbours. Those neighbours pass them along to a couple
more neighbours. Within a few seconds everyone's board looks the same. That
whispering system is **gossip**.

The rule for resolving disagreements is simple: *the most recent sticky note
wins*. If two people write to the same spot at almost the same time, whoever
has the later timestamp keeps their note. This rule is called a
**CRDT (Conflict-free Replicated Data Type)**.

Finally, you need a way for *other programs* to talk to the board. Instead of
walking into the room yourself, you pick up a phone, say
"give me the note labelled `config.timeout`", and hear the answer spoken back.
That phone system is the **JSON-RPC server**.

Put it all together and you have a self-synchronising, multi-node key-value
store that any program can query over a standard protocol — with no single
point of failure.

---

## Quick Start

### Prerequisites

- Rust 1.78+ and Cargo
- No external services required

### Build

```bash
cargo build --release
```

### Run a single node

```bash
cargo run --release -- \
  --rpc-port 7070 \
  --gossip-port 7071 \
  --node-id node-1
```

### Run a two-node cluster

```bash
# Terminal 1 — node A
cargo run --release -- \
  --rpc-port 7070 --gossip-port 7071 --node-id node-a

# Terminal 2 — node B, seeded with A's gossip address
cargo run --release -- \
  --rpc-port 7072 --gossip-port 7073 --node-id node-b \
  --seed-peers 127.0.0.1:7071
```

### Talk to a node

The server speaks newline-delimited JSON-RPC 2.0 over raw TCP.
Use `nc` (netcat) or any TCP client:

```bash
# Write a value
echo '{"jsonrpc":"2.0","method":"state.set","params":{"key":"hello","value":"world"},"id":1}' \
  | nc 127.0.0.1 7070

# Read it back
echo '{"jsonrpc":"2.0","method":"state.get","params":{"key":"hello"},"id":2}' \
  | nc 127.0.0.1 7070

# Subscribe to all changes (keep nc running; press Ctrl-C to stop)
echo '{"jsonrpc":"2.0","method":"state.subscribe","params":{},"id":3}' \
  | nc -q 0 127.0.0.1 7070

# Check node status
echo '{"jsonrpc":"2.0","method":"sync.status","params":{},"id":4}' \
  | nc 127.0.0.1 7070
```

---

## JSON-RPC 2.0 API Reference

All methods follow the [JSON-RPC 2.0 specification](https://www.jsonrpc.org/specification).

### `state.get`

Return the current value of a key, or `null` if it doesn't exist.

```json
→ {"jsonrpc":"2.0","method":"state.get","params":{"key":"foo"},"id":1}
← {"jsonrpc":"2.0","result":{"key":"foo","value":"bar","timestamp":1700000001234567,"node_id":"node-a"},"id":1}
```

### `state.set`

Write a value. The timestamp is assigned server-side (never trust the client
clock for CRDT ordering). Returns the winning timestamp.

```json
→ {"jsonrpc":"2.0","method":"state.set","params":{"key":"foo","value":"bar"},"id":2}
← {"jsonrpc":"2.0","result":{"timestamp":1700000001234567,"node_id":"node-a"},"id":2}
```

### `state.subscribe`

Keep the connection open and receive `state.update` notifications for every
future write. Optionally filter by key prefix.

```json
→ {"jsonrpc":"2.0","method":"state.subscribe","params":{"prefix":"config."},"id":3}
← {"jsonrpc":"2.0","result":{"status":"subscribed","prefix":"config."},"id":3}
← {"jsonrpc":"2.0","method":"state.update","params":{"key":"config.timeout","value":"30","timestamp":...,"node_id":"..."}}
← {"jsonrpc":"2.0","method":"state.update", ...}   ← pushed as writes happen
```

If you read too slowly, you'll get a lag warning instead of missing events silently:

```json
← {"jsonrpc":"2.0","method":"state.lag","params":{"dropped":42}}
```

### `state.keys`

List all keys currently in the store.

```json
→ {"jsonrpc":"2.0","method":"state.keys","params":{},"id":4}
← {"jsonrpc":"2.0","result":{"keys":["foo","bar"],"count":2},"id":4}
```

### `node.peers`

List all peers known to this node.

```json
→ {"jsonrpc":"2.0","method":"node.peers","params":{},"id":5}
← {"jsonrpc":"2.0","result":{"peers":[{"id":"node-b","addr":"127.0.0.1:7073","last_seen_secs":0,"exchange_count":12,"last_sync_ts":1700000001234567}],"count":1},"id":5}
```

### `sync.status`

High-level health snapshot of this node.

```json
→ {"jsonrpc":"2.0","method":"sync.status","params":{},"id":6}
← {"jsonrpc":"2.0","result":{"node_id":"node-a","state_size":42,"max_timestamp":1700000001234567,"peer_count":2},"id":6}
```

### `sync.delta`

Manually pull all entries newer than a given timestamp. Useful for bootstrapping
a freshly-started node without waiting for gossip to propagate everything.

```json
→ {"jsonrpc":"2.0","method":"sync.delta","params":{"since":1700000000000000},"id":7}
← {"jsonrpc":"2.0","result":{"entries":[...],"count":5},"id":7}
```

---

## Configuration Reference

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `SSE_HOST` | `127.0.0.1` | Bind address |
| `--rpc-port` | `SSE_RPC_PORT` | `7070` | JSON-RPC TCP port |
| `--gossip-port` | `SSE_GOSSIP_PORT` | `7071` | Gossip TCP port |
| `--node-id` | `SSE_NODE_ID` | *(UUID v4)* | Unique node name |
| `--seed-peers` | `SSE_SEED_PEERS` | *(empty)* | Comma-separated `host:port` gossip addresses |
| `--gossip-interval-ms` | `SSE_GOSSIP_INTERVAL_MS` | `1000` | Gossip round interval |
| `--gossip-fanout` | `SSE_GOSSIP_FANOUT` | `3` | Peers contacted per round |
| `--event-channel-capacity` | `SSE_EVENT_CAPACITY` | `4096` | Subscription backpressure limit |

---

## Design Decisions & Tradeoffs

This section explains *why* the code is structured the way it is. Every
decision has an alternative — understanding the tradeoffs is more important
than the choice itself.

---

### 1 · Why CRDT (LWW-Register) instead of Raft?

**The choice:** Each key stores a `(value, timestamp, node_id)` triple. When
two replicas disagree, the entry with the higher timestamp wins; ties go to
the higher `node_id`. Merge is deterministic, so all nodes converge to the
same state without any coordination.

**What we give up:** *Strong consistency* — if you write key `x` on node A and
immediately read it on node B (before gossip propagates), you might see the
old value. There is also a narrow window where two writes to the same key on
different nodes at the same microsecond can have their winner decided by
node ID rather than true recency.

**What we gain:**

- **Availability during partitions.** Nodes can keep accepting writes even when
  they can't reach each other. They reconcile automatically when the partition
  heals.
- **No leader election.** Raft requires a quorum to elect a leader before
  accepting any writes. In a 3-node cluster, if one node goes down, Raft keeps
  working; if *two* go down, the surviving node refuses writes. LWW accepts
  writes on every surviving node regardless.
- **No round-trip latency for consensus.** A Raft write must be replicated to
  a majority before it is acknowledged. LWW acknowledges locally and
  propagates asynchronously.
- **Simpler code.** Raft is ~3 000 lines of subtle state machine code. The
  CRDT core here is ~80 lines.

**When you should use Raft instead:** financial transactions, inventory
counters, anything where "the most recent wall-clock timestamp" is not a
reliable proxy for "the intended final value".

---

### 2 · Why `parking_lot::RwLock` over an actor model?

**The choice:** The state store is wrapped in a `parking_lot::RwLock<HashMap>`.
Multiple readers can hold the lock simultaneously; writers get exclusive access.
Critical sections are nanosecond-scale HashMap operations — no I/O, no
allocation beyond the HashMap itself.

**The alternative: actor model.** Each operation would be a message sent to a
single-threaded actor that owns the HashMap. No locks at all, but:

- Every read and write involves at least one `tokio::sync::mpsc` channel
  send + receive, adding ~100–300 ns per operation.
- Results have to be sent *back* to the caller via a one-shot channel, adding
  another ~100 ns.
- Total round-trip per read: ~300–600 ns vs. ~20–40 ns for a `RwLock` read.

For a **read-heavy** workload (typical for a key-value store), `RwLock` wins
decisively: multiple readers proceed in parallel with no message passing at all.

**When actors win:** when the critical section involves async I/O (database
calls, file writes), or when the state needs to span multiple unrelated
operations atomically. Actors also simplify testing because the state is owned
by one place.

**The `parking_lot` advantage over `std::sync::RwLock`:**
1. No poisoning — `std`'s lock becomes permanently unusable if a thread panics
   while holding it. In async Rust this is a footgun since panics are not
   expected to propagate.
2. Slightly faster uncontended acquisition (~5–10 ns vs ~15–20 ns).
3. No phantom `MutexGuard` lifetime issues on Rust stable.

---

### 3 · Backpressure in RPC subscriptions

**The problem:** The JSON-RPC server supports `state.subscribe`, where the
server pushes every state change to the client over a persistent TCP connection.
If the client reads slower than the server writes, what happens?

**Unbounded option:** buffer every event until the client reads them. Memory
grows without bound. A single slow client can exhaust the server's heap.

**Our choice: bounded `broadcast::channel` with explicit lag notification.**

```
broadcast channel capacity = 4 096 events (configurable)

If a subscriber falls > 4 096 events behind:
  Tokio drops the oldest queued events
  recv() returns Err(RecvError::Lagged(n))
  Server sends: {"method":"state.lag","params":{"dropped":42}}
```

The client knows exactly how many events it missed and can call `sync.delta`
to fill the gap. Memory usage is bounded at `O(capacity)` regardless of how
many subscribers there are or how slowly they read.

**The fanout cost:** `broadcast::Sender::send` clones the event once per
active receiver. For `N` subscribers with `M` keys per second, the clone
cost is `O(N × M)`. For typical cluster sizes (a handful of subscribers) this
is negligible. For >1 000 subscribers you would want a Pub/Sub system with
reference-counted payloads.

---

### 4 · Gossip fanout tuning

**The parameter:** `--gossip-fanout` controls how many peers each node
contacts per gossip round (default: 3).

**Convergence time analysis:**

In a cluster of `N` nodes, after `R` gossip rounds, the fraction of nodes
that have received a given update is approximately:

```
fraction ≈ 1 - (1 - fanout/N)^R
```

For `N=10, fanout=3`:
- After 1 round:  ~30% of nodes have the update
- After 2 rounds: ~51%  (30% × 30% + 70% × 30%)
- After 5 rounds: ~83%
- After ~log_{3/10}(0.01) ≈ 8 rounds: >99%

At a 1 000 ms interval, a 10-node cluster reaches 99% convergence in ~8 seconds
worst-case (assuming the write happened just after a gossip round fired).

**Increasing fanout** reduces convergence time at the cost of more network
traffic per node per round. `fanout=N-1` is full broadcast — all nodes get
every update in one round, but network load is `O(N²)`.

**Decreasing fanout** saves bandwidth at the cost of slower convergence.
`fanout=1` is a chain — convergence is `O(N)` rounds instead of `O(log N)`.

The default of `3` follows the academic gossip literature recommendation:
sufficient spread to make convergence fast, low enough to keep per-round
traffic predictable.

---

### 5 · Lock ordering — how deadlocks are prevented

There are two shared data structures accessed from multiple concurrent tasks:

| Structure | Lock type | Held by |
|---|---|---|
| `StateStore` inner `HashMap` | `parking_lot::RwLock` | RPC handler, gossip manager |
| Peer table (`DashMap`) | Per-shard fine-grained lock | Gossip manager, RPC handler |

**The rule, strictly enforced:** no code path holds both locks simultaneously.

```
✅ Allowed:
  1. Acquire StateStore RwLock (read)  → release → later acquire DashMap shard
  2. Acquire DashMap shard             → release → later acquire StateStore RwLock

❌ Forbidden:
  Acquire StateStore RwLock then, while still holding it, acquire DashMap shard
  (or vice versa)
```

In `gossip_round()`:

```rust
// Step 1: collect peers (DashMap iteration — brief, released at end of block)
let targets = { self.peers.iter()... };   // DashMap released here

// Step 2: build delta (StateStore read lock — brief, released at end of block)
let delta = self.store.delta_since(...);  // RwLock released here

// Never: holding DashMap shard AND RwLock at the same time
```

Because the two locks are always in separate scopes, there is no lock ordering
problem. Deadlock is structurally impossible.

**Why `DashMap` and not `Mutex<HashMap>`?** DashMap shards the map into `N`
independent buckets (default: 2× CPU count). Two threads writing to different
keys (different buckets) never block each other. For a peer table with dozens
to hundreds of entries, the probability of two tasks hitting the same shard
simultaneously is low, giving near-zero contention.

---

### 6 · Why newline-delimited JSON over TCP instead of WebSockets or HTTP/2?

**Our choice:** Raw TCP with one JSON object per line (`\n` terminated).

**Pros:**
- Testable with `nc`, `socat`, or `telnet` — no special client needed.
- Zero framing overhead beyond `\n`.
- Bidirectional: server can push notifications without the client polling.
- ~20 lines of `AsyncBufReadExt::read_line` code vs. a WebSocket or HTTP/2
  library dependency.

**Cons vs. WebSockets:**
- No built-in message fragmentation for very large payloads.
- No ping/pong keepalives (easy to add).
- Firewall/proxy unfriendly (most proxies speak HTTP, not raw TCP).

**Cons vs. gRPC (HTTP/2 + Protobuf):**
- No schema enforcement.
- No multiplexing (each subscription needs its own connection).
- No compression.

For a demonstration project and internal tooling, the simplicity wins. For a
public API, gRPC would be the right choice.

---

## Benchmarks

Run benchmarks with:

```bash
cargo bench
# Open the HTML report:
open target/criterion/report/index.html
```

### Methodology

All benchmarks use [Criterion.rs](https://github.com/bheisler/criterion.rs)
with statistical outlier detection. Each benchmark runs for at least 5 seconds
of wall time across multiple sample sizes to produce stable estimates.

### Results (Apple M2 Pro, macOS 15, `cargo bench`)

> Measured with Criterion.rs: 1 s warm-up, 3 s measurement, 20 samples.

| Benchmark | Median latency | Throughput |
|---|---|---|
| `state_set/single_thread` | **194 ns / op** | ~5.15 M ops/sec |
| `state_set/concurrent_writers/1` | 504 ns / op *(+tokio spawn)* | ~2.0 M ops/sec |
| `state_set/concurrent_writers/4` | 575 ns / op | 1.74 M ops/sec total |
| `state_set/concurrent_writers/8` | 629 ns / op | 1.59 M ops/sec total |
| `state_set/concurrent_writers/16` | 677 ns / op *(plateau)* | 1.48 M ops/sec total |
| `state_set/concurrent_writers/32` | 685 ns / op | 1.46 M ops/sec total |
| `merge_delta/by_size/100` | **10.9 µs** | — |
| `merge_delta/by_size/1_000` | 116 µs | — |
| `merge_delta/by_size/10_000` | 1.1 ms | — |
| `merge_delta/by_size/100_000` | **14.5 ms** | — |
| `lww_merge` | **32 ns** | — |
| `delta_since/state_size/1_000` | 52 µs | — |
| `delta_since/state_size/10_000` | 509 µs | — |
| `delta_since/state_size/100_000` | **6.2 ms** | — |

**Key observations:**

1. **Single-threaded `state.set` at 194 ns (5.15 M ops/sec).** The time is
   dominated by `parking_lot::RwLock` write acquisition (~15 ns), HashMap
   insert, and `broadcast::Sender::send`. Clock acquisition (`SystemTime::now`)
   adds ~10–20 ns on M2.

2. **Concurrent writes plateau around 1.5 M total ops/sec beyond 8 writers.**
   Each writer needs exclusive write-lock access; beyond ~8 concurrent writers
   the lock becomes the bottleneck regardless of core count. The +tokio-spawn
   overhead (~310 ns) visible in the 1-writer concurrent case explains why
   single-threaded throughput is higher than the concurrent/1 figure.

3. **`merge_delta` is linear in delta size.** The write lock is held for the
   entire merge loop, so a 100 k-entry gossip delta blocks readers for ~14 ms.
   In production, split large deltas into chunks of ≤ 10 k entries to keep
   read latency predictable.

4. **LWW merge at 32 ns** confirms the CRDT logic itself is not the bottleneck;
   lock acquisition, HashMap hashing, and timestamp syscalls dominate.

5. **`delta_since` is O(N) in total state size**, not in result size. Scanning
   100 k entries takes 6.2 ms on every gossip round. For stores beyond ~1 M
   entries, a secondary sorted structure (e.g. `BTreeMap<u64, Vec<String>>` by
   timestamp) would cut this to O(log N + result_size).

---

## Project Structure

```
src/
├── lib.rs              # Public re-exports (for benchmarks and integration tests)
├── main.rs             # Entry point: config parsing, task orchestration, shutdown
├── config.rs           # Clap-based CLI config with env-var fallbacks
├── error.rs            # Unified error type (thiserror)
├── state/
│   ├── mod.rs

│   ├── crdt.rs         # LwwValue: merge logic, CRDT proofs as unit tests
│   ├── store.rs        # StateStore: RwLock<HashMap> + broadcast channel
│   └── snapshot.rs     # bincode helpers for gossip serialization
├── gossip/
│   ├── mod.rs
│   ├── peer.rs         # PeerInfo: per-peer sync state
│   ├── protocol.rs     # Wire format: length-prefixed bincode frames
│   └── manager.rs      # GossipManager: listener task + scheduler task
└── rpc/
    ├── mod.rs
    ├── types.rs        # JSON-RPC 2.0 request/response/notification structs
    ├── handler.rs      # Method dispatch (state.get, state.set, …)
    └── server.rs       # TCP listener, per-connection select! loop

benches/
└── benchmarks.rs       # Criterion benchmarks for write throughput, merge, delta

tests/
└── convergence.rs      # Multi-node gossip convergence integration tests (6 tests)

.github/workflows/
└── ci.yml              # Build, test, clippy, fmt, bench-compile on every push

demo.sh                 # End-to-end demo: 2-node cluster, live gossip propagation
rust-toolchain.toml     # Pins stable Rust + rustfmt + clippy
```

---

## Logging

Set `RUST_LOG` to control verbosity:

```bash
RUST_LOG=state_sync_engine=debug cargo run --release -- ...
RUST_LOG=state_sync_engine=trace cargo run -- ...   # very verbose
```

---

## License

MIT
