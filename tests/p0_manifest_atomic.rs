//! Regression test for the P0 manifest-atomicity finding from external review.
//!
//! Before the fix, several call sites took the manifest write lock,
//! mutated the manifest in place, then called `save`. If `save` failed,
//! the in-memory state still held the mutation but the on-disk manifest
//! did not. Queries would see writes that hadn't reached disk, and
//! crash-recovery would re-derive state from an older manifest while
//! the running process believed it had committed newer state.
//!
//! The fix routes every mutation through `AppStateInner::commit_manifest`
//! (or `commit_manifest_if`), which clones the manifest, mutates the
//! clone, saves the clone, and only publishes the clone in-memory after
//! the save succeeds. A save failure leaves both disk and memory
//! untouched.
//!
//! This test forces a save failure by replacing the `manifest/`
//! directory with a regular file after the initial save, then exercises
//! the helper and asserts the in-memory manifest is unchanged.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, mpsc};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    path
}

fn build_state(db_root: PathBuf) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: 4,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let mut manifest = Manifest { bucket_count: 4, ..Manifest::default() };
    // Persist an initial generation so the manifest_dir exists on disk
    // and the in-memory generation counter is non-zero.
    manifest.save(&db_root).expect("initial manifest save");
    let (flush_sender, _flush_receiver) = mpsc::channel(4);
    Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    })
}

/// Sabotage manifest writes by replacing the `manifest/` directory
/// with a regular file. `create_dir_all` then fails on the next save
/// because the path exists but is not a directory — without touching
/// permissions, which keeps the test cross-platform.
fn break_manifest_writes(db_root: &PathBuf) {
    let manifest_dir = db_root.join("manifest");
    std::fs::remove_dir_all(&manifest_dir).expect("remove manifest dir");
    std::fs::write(&manifest_dir, b"not a directory").expect("write sentinel file");
}

#[tokio::test(flavor = "current_thread")]
async fn commit_manifest_leaves_state_unchanged_on_save_failure() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let (gen_before, bucket_before) = {
        let m = state.manifest.read().await;
        (m.current_generation, m.bucket_count)
    };
    assert!(gen_before >= 1);

    break_manifest_writes(&root);

    let result = state
        .commit_manifest(|m| {
            // Mutate something visible — bucket_count is a single field
            // and easy to inspect.
            m.bucket_count = 999;
        })
        .await;
    assert!(result.is_err(), "save should have failed");

    let m = state.manifest.read().await;
    assert_eq!(
        m.current_generation, gen_before,
        "generation must not advance when save fails"
    );
    assert_eq!(
        m.bucket_count, bucket_before,
        "in-memory mutation must be discarded when save fails"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn commit_manifest_if_skips_save_when_closure_returns_none() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let gen_before = state.manifest.read().await.current_generation;

    let returned: Option<()> = state
        .commit_manifest_if(|m| {
            // Mutate, then decide not to commit. The mutation lives only
            // in the closure's local clone — it must not be published.
            m.bucket_count = 999;
            None
        })
        .await
        .expect("no save attempted, no error");
    assert!(returned.is_none());

    let m = state.manifest.read().await;
    assert_eq!(
        m.current_generation, gen_before,
        "generation must not advance when closure returns None"
    );
    assert_eq!(m.bucket_count, 4, "mutation in skipped closure must not stick");
}

#[tokio::test(flavor = "current_thread")]
async fn commit_manifest_publishes_only_after_save_succeeds() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let gen_before = state.manifest.read().await.current_generation;

    state
        .commit_manifest(|m| {
            m.bucket_count = 8;
        })
        .await
        .expect("save should succeed");

    let m = state.manifest.read().await;
    assert_eq!(
        m.current_generation,
        gen_before + 1,
        "generation must advance on successful save"
    );
    assert_eq!(m.bucket_count, 8, "mutation must be visible after commit");
}
