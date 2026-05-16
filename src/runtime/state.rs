use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use crate::runtime::config::Config;
use crate::ingest::dedupe::HotDedupe;
use crate::ingest::wal::Wal;
use crate::ingest::memtable::Memtable;
use crate::storage::manifest::Manifest;
use crate::model::event::UsageEvent;

/// Message sent from the ingest handler to the flusher worker. Contains the
/// drained memtable contents plus the WAL file id that was sealed at drain
/// time, so the flusher can delete that WAL file after segment commit.
pub struct FlushMessage {
    pub events: Vec<UsageEvent>,
    pub sealed_wal_id: u64,
}

pub struct AppStateInner {
    pub config: Config,
    pub dedupe: Mutex<HotDedupe>,
    pub wal: Mutex<Wal>,
    pub memtable: Mutex<Memtable>,
    pub manifest: RwLock<Manifest>,
    pub flush_sender: tokio::sync::mpsc::Sender<FlushMessage>,
}

pub type AppState = Arc<AppStateInner>;
