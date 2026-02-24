use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use state_sync_engine::state::{LwwValue, StateStore};
use std::time::Duration;

/// Shared criterion config: shorter measurement so `cargo bench` finishes in
/// ~1 minute instead of ~5 minutes.  Increase `measurement_time` for
/// publication-quality results.
fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20)
}

// ── Benchmark 1: Single-threaded state.set throughput ─────────────────────
//
// Measures raw write throughput with one caller.  Shows the cost of:
//   - wall-clock timestamp acquisition
//   - LWW merge
//   - parking_lot::RwLock write acquisition
//   - broadcast::Sender::send (O(receivers) clone, here 0 receivers)
fn bench_state_set_single(c: &mut Criterion) {
    let store = StateStore::new("bench-node".into(), 4096);

    c.bench_function("state_set/single_thread", |b| {
        let mut i = 0u64;
        b.iter(|| {
            // Rotate across 1 000 keys to measure realistic HashMap behaviour
            // (not just a single hot-path entry).
            store.set(
                black_box(format!("key:{}", i % 1_000)),
                black_box("bench-value".to_string()),
            );
            i += 1;
        });
    });
}

// ── Benchmark 2: Concurrent state.set throughput under N writers ──────────
//
// Reveals lock contention as writer count grows.  Expected pattern:
//   - 1 writer  ≈ 1× single-thread throughput
//   - N writers ≈ N× single-thread only if the lock is uncontended
//   - Heavy contention → throughput plateaus around CPU core count
fn bench_state_set_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("state_set/concurrent_writers");

    for &n in &[1usize, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(n as u64 * 100));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let store = StateStore::new("bench-node".into(), 4096);
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let s = store.clone();
                            tokio::spawn(async move {
                                for j in 0..100u64 {
                                    s.set(
                                        black_box(format!("key:{i}:{j}")),
                                        black_box("v".to_string()),
                                    );
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        let _ = h.await;
                    }
                })
            });
        });
    }
    group.finish();
}

// ── Benchmark 3: merge_delta latency as incoming delta size grows ─────────
//
// Models what happens when a gossip round arrives with N new entries.
// The write lock is held for the duration of the loop over all entries,
// so latency should grow roughly linearly with delta size.
fn bench_merge_delta_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_delta/by_size");

    for &size in &[100usize, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        // Build the incoming delta once — all entries newer than anything
        // in the (empty) store so every entry causes an update.
        let delta: Vec<_> = (0..size)
            .map(|i| {
                (
                    format!("key:{i}"),
                    LwwValue {
                        value:     format!("v{i}"),
                        timestamp: 999_999_999_999_999,
                        node_id:   "remote".into(),
                    },
                )
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let store = StateStore::new("bench-node".into(), 4096);
            b.iter(|| {
                store.merge_delta(black_box(delta.clone()));
            });
        });
    }
    group.finish();
}

// ── Benchmark 4: raw LWW merge (no I/O, pure computation) ─────────────────
//
// Establishes a lower bound on state update cost.  Anything in the actual
// store benchmarks that is slower than this is lock or allocation overhead.
fn bench_lww_merge(c: &mut Criterion) {
    let a = LwwValue { value: "a".into(), timestamp: 100, node_id: "node-z".into() };
    let b = LwwValue { value: "b".into(), timestamp: 200, node_id: "node-a".into() };

    c.bench_function("lww_merge", |b_fn| {
        b_fn.iter(|| LwwValue::merge(black_box(&a), black_box(&b)));
    });
}

// ── Benchmark 5: delta_since (read path) ──────────────────────────────────
//
// Models gossip delta generation: scan the entire store and filter by
// timestamp.  Should be O(N) in state size.
fn bench_delta_since(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_since/state_size");

    for &size in &[1_000usize, 10_000, 100_000] {
        let store = StateStore::new("bench-node".into(), 4096);
        for i in 0..size {
            store.set(format!("key:{i}"), format!("v{i}"));
        }
        let mid_ts = store.max_timestamp() / 2;

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let _ = store.delta_since(black_box(mid_ts));
            });
        });
    }
    group.finish();
}

criterion_group! {
    name    = benches;
    config  = criterion_config();
    targets =
        bench_state_set_single,
        bench_state_set_concurrent,
        bench_merge_delta_size,
        bench_lww_merge,
        bench_delta_since,
}
criterion_main!(benches);
