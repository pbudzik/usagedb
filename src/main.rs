use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppStateInner, AppState};
use usagedb::ingest::wal::Wal;
use usagedb::ingest::flusher::FlusherWorker;
use usagedb::api::http_server::start_server;
use usagedb::runtime::recovery::Recovery;

use tokio::sync::{RwLock, Mutex, mpsc};
use std::sync::Arc;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Starting usageDb server...");

    let config = Config::default();
    std::fs::create_dir_all(&config.db_root)?;

    // Run startup recovery: load manifest, clean tmp, replay WAL
    let recovery = Recovery::new(config.db_root.clone());
    let recovery_result = match recovery.run_startup_recovery(config.dedupe_capacity) {
        Ok(r) => r,
        Err(e) => {
            error!("Recovery failed: {}", e);
            return Err(e.into());
        }
    };

    let wal_path = config.db_root.join("wal.jsonl");
    let wal = Wal::new(wal_path)?;

    let (flush_sender, flush_receiver) = mpsc::channel(32);

    let state: AppState = Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(recovery_result.dedupe),
        wal: Mutex::new(wal),
        memtable: Mutex::new(recovery_result.memtable),
        manifest: RwLock::new(recovery_result.manifest),
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
