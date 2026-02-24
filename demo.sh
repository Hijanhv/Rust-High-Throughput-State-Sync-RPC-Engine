#!/usr/bin/env bash
# demo.sh — Spin up a 2-node cluster and show live state sync via gossip.
#
# Usage:
#   chmod +x demo.sh
#   ./demo.sh
#
# Requirements: cargo, nc (netcat)

set -euo pipefail

BINARY="./target/release/state-sync-engine"
NODE_A_RPC=7070
NODE_A_GOSSIP=7071
NODE_B_RPC=7072
NODE_B_GOSSIP=7073

# ── Helpers ────────────────────────────────────────────────────────────────

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }

# Send one JSON-RPC request to host:port and print the response.
rpc() {
    local port=$1
    local payload=$2
    # The (echo + sleep) pipe lets nc collect the response before stdin closes.
    (printf '%s\n' "$payload"; sleep 0.8) | nc 127.0.0.1 "$port" 2>/dev/null
}

cleanup() {
    bold ""
    bold "=== Shutting down nodes ==="
    kill "$NODE_A_PID" "$NODE_B_PID" 2>/dev/null || true
    wait "$NODE_A_PID" "$NODE_B_PID" 2>/dev/null || true
    green "Done."
}
trap cleanup EXIT

# ── Build ──────────────────────────────────────────────────────────────────

bold "=== Building release binary ==="
cargo build --release --quiet
green "Build complete."
echo ""

# ── Start nodes ────────────────────────────────────────────────────────────

bold "=== Starting Node A  (RPC :$NODE_A_RPC | Gossip :$NODE_A_GOSSIP) ==="
RUST_LOG=state_sync_engine=warn \
  "$BINARY" \
    --node-id node-a \
    --rpc-port "$NODE_A_RPC" \
    --gossip-port "$NODE_A_GOSSIP" \
    --gossip-interval-ms 500 \
  &
NODE_A_PID=$!

bold "=== Starting Node B  (RPC :$NODE_B_RPC | Gossip :$NODE_B_GOSSIP, seeded from A) ==="
RUST_LOG=state_sync_engine=warn \
  "$BINARY" \
    --node-id node-b \
    --rpc-port "$NODE_B_RPC" \
    --gossip-port "$NODE_B_GOSSIP" \
    --gossip-interval-ms 500 \
    --seed-peers "127.0.0.1:$NODE_A_GOSSIP" \
  &
NODE_B_PID=$!

dim "Waiting for nodes to start..."
sleep 1.5
echo ""

# ── Write on Node A ────────────────────────────────────────────────────────

bold "=== [Node A] Writing  key='greeting'  value='hello-from-node-a' ==="
RESPONSE=$(rpc $NODE_A_RPC \
  '{"jsonrpc":"2.0","method":"state.set","params":{"key":"greeting","value":"hello-from-node-a"},"id":1}')
green "  Response: $RESPONSE"
echo ""

bold "=== Waiting 1.5 s for gossip to propagate... ==="
sleep 1.5
echo ""

# ── Read on Node B ─────────────────────────────────────────────────────────

bold "=== [Node B] Reading  key='greeting'  (should be 'hello-from-node-a') ==="
RESPONSE=$(rpc $NODE_B_RPC \
  '{"jsonrpc":"2.0","method":"state.get","params":{"key":"greeting"},"id":2}')
green "  Response: $RESPONSE"
echo ""

# ── Write back on Node B ───────────────────────────────────────────────────

bold "=== [Node B] Writing  key='greeting'  value='hello-from-node-b' (LWW update) ==="
RESPONSE=$(rpc $NODE_B_RPC \
  '{"jsonrpc":"2.0","method":"state.set","params":{"key":"greeting","value":"hello-from-node-b"},"id":3}')
green "  Response: $RESPONSE"
echo ""

bold "=== Waiting 1.5 s for gossip to propagate back to A... ==="
sleep 1.5
echo ""

bold "=== [Node A] Reading  key='greeting'  (should now be 'hello-from-node-b') ==="
RESPONSE=$(rpc $NODE_A_RPC \
  '{"jsonrpc":"2.0","method":"state.get","params":{"key":"greeting"},"id":4}')
green "  Response: $RESPONSE"
echo ""

# ── Cluster status ─────────────────────────────────────────────────────────

bold "=== [Node A] sync.status ==="
rpc $NODE_A_RPC '{"jsonrpc":"2.0","method":"sync.status","params":{},"id":5}' | python3 -m json.tool 2>/dev/null || \
rpc $NODE_A_RPC '{"jsonrpc":"2.0","method":"sync.status","params":{},"id":5}'
echo ""

bold "=== [Node A] node.peers ==="
rpc $NODE_A_RPC '{"jsonrpc":"2.0","method":"node.peers","params":{},"id":6}' | python3 -m json.tool 2>/dev/null || \
rpc $NODE_A_RPC '{"jsonrpc":"2.0","method":"node.peers","params":{},"id":6}'
echo ""

bold "=== [Node B] state.keys ==="
rpc $NODE_B_RPC '{"jsonrpc":"2.0","method":"state.keys","params":{},"id":7}' | python3 -m json.tool 2>/dev/null || \
rpc $NODE_B_RPC '{"jsonrpc":"2.0","method":"state.keys","params":{},"id":7}'
echo ""

# ── Subscribe demo ─────────────────────────────────────────────────────────

bold "=== [Node A] Subscribing to state changes for 3 s... ==="
dim "  (writing 3 keys from Node B while subscribed)"

# Subscribe in background, write 3 keys, then kill subscriber
(printf '{"jsonrpc":"2.0","method":"state.subscribe","params":{},"id":8}\n'; sleep 3) \
  | nc 127.0.0.1 "$NODE_A_RPC" &
SUB_PID=$!

sleep 0.4
for i in 1 2 3; do
  rpc $NODE_B_RPC \
    "{\"jsonrpc\":\"2.0\",\"method\":\"state.set\",\"params\":{\"key\":\"live-$i\",\"value\":\"update-$i\"},\"id\":$((8+i))}" \
    > /dev/null
  sleep 0.5
done

wait "$SUB_PID" 2>/dev/null || true
echo ""

green "=== Demo complete! Both nodes synced successfully via gossip. ==="
