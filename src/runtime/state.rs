use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use crate::runtime::config::Config;
use crate::ingest::dedupe::HotDedupe;
use crate::ingest::wal::Wal;
use crate::ingest::memtable::Memtable;
use crate::storage::manifest::Manifest;

pub struct AppStateInner {
    pub config: Config,
    pub dedupe: Mutex<HotDedupe>,
    pub wal: Mutex<Wal>,
    pub memtable: Mutex<Memtable>,
    pub manifest: RwLock<Manifest>,
    pub flush_sender: tokio::sync::mpsc::Sender<Vec<crate::model::event::UsageEvent>>,
}

pub type AppState = Arc<AppStateInner>;
