//! Exclusive-process lock on a database directory.
//!
//! Acquires an OS file lock (`flock(2)` on Unix, `LockFileEx` on Windows)
//! on `db_root/LOCK` for the lifetime of the process. Prevents the server
//! and admin commands from running concurrently against the same DB —
//! without this, concurrent manifest writes could clobber each other.
//!
//! The lock is held as long as the `DbLock` is alive; dropping it
//! releases the OS lock. The lock file itself is left on disk (it's
//! harmless if another process is already holding it via fd, and
//! removing it racily is more trouble than it's worth).

use std::fs::{File, OpenOptions};
use std::path::Path;

use fs4::fs_std::FileExt;

/// Holds the OS file lock for a database directory. Drop to release.
pub struct DbLock {
    _file: File,
}

impl DbLock {
    /// Try to acquire an exclusive lock on `db_root/LOCK`. Returns an
    /// error (rather than blocking) if another process holds it — the
    /// caller can surface a clear "DB already in use" message.
    pub fn acquire(db_root: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(db_root)?;
        let path = db_root.join("LOCK");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(true) => Ok(Self { _file: file }),
            Ok(false) => Err(anyhow::anyhow!(
                "another usagedb process holds the lock on {:?}",
                path
            )),
            Err(e) => Err(anyhow::anyhow!(
                "failed to acquire DB lock on {:?}: {}",
                path, e
            )),
        }
    }
}
