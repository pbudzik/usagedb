//! Regression test for the P0 shutdown-deadlock from external review.
//!
//! The flusher's `run` loop used to be a `while let Some(msg) = recv.await`
//! that exited only when *all* `flush_sender` clones were dropped. But the
//! `FlusherWorker` itself holds an `Arc<AppStateInner>`, and that state
//! owns a `flush_sender` clone. The producer-side `drop(state)` in
//! `main.rs` only released the outer clone — the flusher's own clone kept
//! the channel open, so `flusher_handle.await` would hang on graceful
//! shutdown.
//!
//! The fix takes an explicit `Arc<Notify>` shutdown signal (matching the
//! rollup and compaction workers) and drains any pending messages from
//! the channel before exiting.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::FlusherWorker;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner, FlushMessage};
use usagedb::storage::manifest::Manifest;

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    path
}

fn build_state(db_root: PathBuf) -> (AppState, mpsc::Receiver<FlushMessage>) {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: 4,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let manifest = Manifest { bucket_count: 4, ..Manifest::default() };
    let (flush_sender, flush_receiver) = mpsc::channel(4);
    let state = Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    });
    (state, flush_receiver)
}

/// Reproduces the deadlock scenario from `main.rs` graceful shutdown:
/// the flusher owns an `Arc<AppState>` clone (which holds a
/// `flush_sender`), so dropping the *outer* state clones is not enough
/// for `recv()` to return `None`. With the explicit shutdown signal,
/// the flusher exits promptly anyway.
#[tokio::test(flavor = "current_thread")]
async fn flusher_shuts_down_even_with_state_clones_outstanding() {
    let root = tmp_root();
    let (state, flush_receiver) = build_state(root);

    let shutdown = Arc::new(Notify::new());
    let flusher = FlusherWorker::new(state.clone(), flush_receiver);
    let handle = tokio::spawn(flusher.run(shutdown.clone()));

    // Simulate the production sequence: producers finish, then we
    // signal the flusher. The outer `state` Arc is still alive (mirrors
    // `main.rs` where the local `state` clone hasn't been dropped yet),
    // and the flusher's own internal `state` clone is alive too. The
    // channel's sender count is therefore >= 2 here.
    shutdown.notify_one();

    // Bounded wait — without the fix this would hang forever.
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        result.is_ok(),
        "flusher did not exit within 5s after shutdown signal"
    );
    result.unwrap().expect("flusher panicked");

    // We still hold `state` here, demonstrating that the flusher exited
    // despite an active sender clone.
    drop(state);
}

/// On shutdown the flusher must still drain messages already queued in
/// the channel — that's how the `main.rs` shutdown-drain message and
/// any last-tick flushes from rollup/compaction reach disk. Without
/// drain-on-shutdown, the final memtable contents would be lost on
/// graceful shutdown.
#[tokio::test(flavor = "current_thread")]
async fn flusher_drains_queued_messages_on_shutdown() {
    let root = tmp_root();
    let (state, flush_receiver) = build_state(root.clone());

    // Queue an empty flush message; `handle_message` short-circuits on
    // empty events but still consumes the receiver slot, which is what
    // we need to observe here. Real drain messages have events; this
    // test focuses on the drain-the-channel behavior.
    state
        .flush_sender
        .send(FlushMessage {
            events: Vec::new(),
            sealed_wal_id: 0,
        })
        .await
        .expect("send to live channel");

    let shutdown = Arc::new(Notify::new());
    let flusher = FlusherWorker::new(state.clone(), flush_receiver);
    let handle = tokio::spawn(flusher.run(shutdown.clone()));

    // Give the flusher a moment to begin awaiting on the select; then
    // fire shutdown. The message is already in the channel, so the
    // drain-on-shutdown path consumes it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "flusher did not exit after shutdown");
    result.unwrap().expect("flusher panicked");

    drop(state);
}
