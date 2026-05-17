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

impl AppStateInner {
    /// Atomic manifest commit: clone → mutate the clone → save to disk
    /// → publish in-memory. If the save fails, the in-memory manifest
    /// is unchanged and the error is surfaced (review P0 #2).
    ///
    /// The previous pattern — take the write lock, mutate in place,
    /// then call `save` — left the in-memory state ahead of disk on
    /// save failure. Subsequent reads would see writes that hadn't
    /// reached the on-disk manifest, and any cleanup of segment files
    /// (which several callers do on save failure) would leave the
    /// in-memory manifest pointing at files that no longer existed.
    pub async fn commit_manifest<F, T>(&self, op: F) -> std::io::Result<T>
    where
        F: FnOnce(&mut Manifest) -> T,
    {
        let mut guard = self.manifest.write().await;
        let mut next = guard.clone();
        let value = op(&mut next);
        next.save(&self.config.db_root)?;
        *guard = next;
        Ok(value)
    }

    /// Like `commit_manifest`, but only writes to disk when `op`
    /// returns `Some`. Use this when the closure decides — under the
    /// write lock, with race-safety — whether the change is actually
    /// needed (e.g. close-period rechecks whether the period was
    /// already closed by a racing caller).
    pub async fn commit_manifest_if<F, T>(&self, op: F) -> std::io::Result<Option<T>>
    where
        F: FnOnce(&mut Manifest) -> Option<T>,
    {
        let mut guard = self.manifest.write().await;
        let mut next = guard.clone();
        let value = op(&mut next);
        if value.is_some() {
            next.save(&self.config.db_root)?;
            *guard = next;
        }
        Ok(value)
    }
}

pub type AppState = Arc<AppStateInner>;
