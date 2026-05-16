use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppStateInner, AppState};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::wal::Wal;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::flusher::FlusherWorker;
use usagedb::storage::manifest::Manifest;
use usagedb::api::http_server::start_server;

use tokio::sync::{RwLock, Mutex, mpsc};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Starting usageDb server...");

    let config = Config::default();
    std::fs::create_dir_all(&config.db_root)?;

    let wal_path = config.db_root.join("wal.jsonl");
    let wal = Wal::new(wal_path)?;

    let (flush_sender, flush_receiver) = mpsc::channel(32);

    let state: AppState = Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new()),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(Manifest::default()),
        flush_sender,
    });

    let flusher = FlusherWorker::new(state.clone(), flush_receiver);
    let flusher_handle = tokio::spawn(flusher.run());

    start_server(state.clone()).await?;

    info!("Waiting for background tasks to finish...");
    drop(state);
    let _ = flusher_handle.await;

    info!("Server gracefully shut down.");
    Ok(())
}

