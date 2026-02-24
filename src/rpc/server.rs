use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::gossip::PeerInfo;
use crate::rpc::handler::{DispatchResult, RpcHandler};
use crate::rpc::types::RpcNotification;
use crate::state::{StateEvent, StateStore};

// ── Public entry point ─────────────────────────────────────────────────────

/// Bind the JSON-RPC 2.0 TCP server and drive connections until `cancel` fires.
///
/// # Protocol
///
/// Newline-delimited JSON over a raw TCP stream:
/// - Client sends one JSON-RPC request per line (terminated by `\n`).
/// - Server responds with one JSON line per request.
/// - For `state.subscribe`, the server keeps the connection open and pushes
///   `state.update` notifications as newline-delimited JSON.
///
/// This is intentionally simpler than WebSockets or HTTP/2: it is trivially
/// testable with `nc`, `socat`, or `telnet`, and avoids protocol overhead.
///
/// # Backpressure
///
/// Subscription events flow through a bounded `broadcast::channel`.  If a
/// client reads too slowly and falls more than `event_channel_capacity` events
/// behind, Tokio drops the oldest queued events and the receiver gets a
/// `RecvError::Lagged(n)`.  The server converts this into a `state.lag`
/// notification so the client knows it missed data and can resync via
/// `sync.delta`.
///
/// This is **back-pressure by explicit signalling** rather than unbounded
/// buffering: memory usage is bounded at `O(capacity)` regardless of client
/// speed, and clients always know when they've missed events.
pub async fn run(
    config: Config,
    store: StateStore,
    peers: Arc<DashMap<String, PeerInfo>>,
    cancel: CancellationToken,
) {
    let addr = config.rpc_addr();
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %addr, "Failed to bind RPC server: {e}");
            return;
        }
    };
    info!(addr = %addr, "JSON-RPC 2.0 server listening");

    let handler = Arc::new(RpcHandler::new(store, peers));

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(client = %peer_addr, "New RPC connection");
                        let h = Arc::clone(&handler);
                        let c = cancel.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, h, c).await {
                                debug!(client = %peer_addr, "Connection closed: {e}");
                            }
                        });
                    }
                    Err(e) => error!("RPC accept error: {e}"),
                }
            }
            _ = cancel.cancelled() => {
                info!("RPC server shutting down");
                break;
            }
        }
    }
}

// ── Per-connection handler ─────────────────────────────────────────────────

async fn handle_connection(
    stream: tokio::net::TcpStream,
    handler: Arc<RpcHandler>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Per-connection subscription state
    let mut subscription: Option<broadcast::Receiver<StateEvent>> = None;
    let mut sub_prefix: Option<String> = None;

    loop {
        tokio::select! {
            // ── Arm 1: read the next request line ─────────────────────────
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        // EOF — client closed the connection cleanly
                        debug!("Client disconnected");
                        break;
                    }
                    Ok(_) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            match handler.dispatch(trimmed) {
                                DispatchResult::Response(resp) => {
                                    write_line(&mut write_half, &resp).await?;
                                }
                                DispatchResult::Subscribed { initial_response, receiver, prefix } => {
                                    subscription = Some(receiver);
                                    sub_prefix = prefix;
                                    write_line(&mut write_half, &initial_response).await?;
                                }
                                DispatchResult::Notification => {
                                    // No response for client-side notifications
                                }
                            }
                        }
                        line.clear();
                    }
                    Err(e) => {
                        warn!("RPC read error: {e}");
                        break;
                    }
                }
            }

            // ── Arm 2: forward subscription events ────────────────────────
            //
            // `poll_subscription` returns a future that never resolves when
            // `subscription` is `None`, so this arm is dormant until the
            // client calls `state.subscribe`.
            event = poll_subscription(&mut subscription) => {
                match event {
                    SubPoll::Event(ev) => {
                        let matches = sub_prefix
                            .as_deref()
                            .is_none_or(|p| ev.key.starts_with(p));
                        if matches {
                            let n = RpcNotification::state_update(&ev.key, &ev.value);
                            let json = serde_json::to_string(&n)?;
                            write_line(&mut write_half, &json).await?;
                        }
                    }
                    SubPoll::Lagged(n) => {
                        // Client was too slow — inform it so it can resync
                        let n = RpcNotification::lag(n);
                        let json = serde_json::to_string(&n)?;
                        write_line(&mut write_half, &json).await?;
                    }
                    SubPoll::Idle => {
                        // This arm is parked (subscription is None); unreachable
                    }
                }
            }

            // ── Arm 3: graceful shutdown ───────────────────────────────────
            _ = cancel.cancelled() => {
                debug!("RPC connection task cancelled");
                break;
            }
        }
    }

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────

enum SubPoll {
    Event(StateEvent),
    Lagged(u64),
    /// Returned by `poll_subscription` when no subscription is active.
    /// The future never resolves, so `select!` ignores this arm.
    Idle,
}

/// Poll the optional broadcast receiver.
///
/// When `sub` is `None`, returns `std::future::pending()` so that the
/// `select!` arm is never chosen — zero overhead when not subscribed.
async fn poll_subscription(sub: &mut Option<broadcast::Receiver<StateEvent>>) -> SubPoll {
    match sub {
        None => {
            // Park this arm permanently; other arms can still fire.
            std::future::pending::<SubPoll>().await
        }
        Some(rx) => match rx.recv().await {
            Ok(ev) => SubPoll::Event(ev),
            Err(broadcast::error::RecvError::Lagged(n)) => SubPoll::Lagged(n),
            Err(broadcast::error::RecvError::Closed) => {
                // Channel closed (store dropped) — deactivate subscription
                *sub = None;
                SubPoll::Idle
            }
        },
    }
}

async fn write_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    line: &str,
) -> anyhow::Result<()> {
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
