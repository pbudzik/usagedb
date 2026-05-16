use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tracing::{error, info};

use usagedb::admin::{
    cmd_check, cmd_export_parquet, cmd_inspect_segment, cmd_rebuild_rollups, cmd_verify_period,
    open_state_for_admin,
};
use usagedb::api::http_server::start_server;
use usagedb::compact::worker::CompactionWorker;
use usagedb::ingest::flusher::FlusherWorker;
use usagedb::ingest::wal::Wal;
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::recovery::Recovery;
use usagedb::runtime::state::{AppState, AppStateInner};

#[derive(Parser, Debug)]
#[command(
    name = "usagedb",
    about = "Embedded append-only usage database for AI billing",
    version
)]
struct Cli {
    /// Database root directory.
    #[arg(long, global = true, default_value = "./data")]
    db_root: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the HTTP server (default if no subcommand is given).
    Serve,
    /// Print manifest summary. With `--deep`, also open every segment and
    /// verify checksums + structure.
    Check {
        #[arg(long, default_value_t = false)]
        deep: bool,
    },
    /// Drop rollup segments overlapping `[from, to)` and rewind the watermark
    /// to `from`. The next server tick refills the gap from raw segments.
    RebuildRollups {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    /// Read a specific segment and print its metadata + sample rows.
    InspectSegment {
        segment_id: String,
    },
    /// Compute raw vs rollup totals + drift for an account over a period.
    VerifyPeriod {
        #[arg(long)]
        account: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    /// Export every raw segment to a single Parquet file.
    ExportParquet {
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let mut config = Config::default();
    config.db_root = cli.db_root;

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => run_server(config).await?,
        Command::Check { deep } => {
            let state = open_state_for_admin(config)?;
            let output = cmd_check(state, deep).await?;
            print!("{}", output);
        }
        Command::RebuildRollups { from, to } => {
            let state = open_state_for_admin(config)?;
            let output = cmd_rebuild_rollups(state, &from, &to).await?;
            print!("{}", output);
        }
        Command::InspectSegment { segment_id } => {
            let state = open_state_for_admin(config)?;
            let output = cmd_inspect_segment(state, &segment_id).await?;
            print!("{}", output);
        }
        Command::VerifyPeriod { account, from, to } => {
            let state = open_state_for_admin(config)?;
            let output = cmd_verify_period(state, &account, &from, &to).await?;
            print!("{}", output);
        }
        Command::ExportParquet { output } => {
            let state = open_state_for_admin(config)?;
            let output_msg = cmd_export_parquet(state, &output).await?;
            print!("{}", output_msg);
        }
    }
    Ok(())
}

async fn run_server(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting usageDb server...");
    std::fs::create_dir_all(&config.db_root)?;

    // Run startup recovery: load manifest, clean tmp, replay WAL.
    let recovery = Recovery::new(config.db_root.clone());
    let mut recovery_result = match recovery.run_startup_recovery(config.dedupe_capacity) {
        Ok(r) => r,
        Err(e) => {
            error!("Recovery failed: {}", e);
            return Err(e.into());
        }
    };

    // First-run initialization: a fresh DB has bucket_count = 0 in its
    // default manifest. Set it from config and persist so subsequent runs
    // use the same value (bucket assignment is fixed for a DB's lifetime).
    if recovery_result.manifest.bucket_count == 0 {
        recovery_result.manifest.bucket_count = config.default_bucket_count;
        recovery_result.manifest.save(&config.db_root)?;
        info!("Initialized new DB with bucket_count = {}", config.default_bucket_count);
    }

    let wal_dir = config.db_root.join("wal");
    let wal = Wal::open(wal_dir, recovery_result.manifest.last_sealed_wal_id)?;

    let (flush_sender, flush_receiver) = mpsc::channel(4);

    let rollup_tick_interval = Duration::from_secs(config.rollup_tick_interval_secs);
    let rollup_safety_lag_ms = config.rollup_safety_lag_ms;
    let memtable_max_age_ms = config.memtable_max_age_ms;
    let compaction_tick_interval = Duration::from_secs(config.compaction_tick_interval_secs);
    let compaction_grace_ms = config.compaction_grace_ms;
    let compaction_max_small = config.compaction_max_small_segments;

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

    let rollup_shutdown = Arc::new(Notify::new());
    let rollup_shutdown_signal = rollup_shutdown.clone();
    let rollup_worker = RollupWorker::new(
        state.clone(),
        rollup_safety_lag_ms,
        rollup_tick_interval,
        memtable_max_age_ms,
    );
    let rollup_handle = tokio::spawn(async move {
        rollup_worker.run(rollup_shutdown_signal).await;
    });

    let compaction_shutdown = Arc::new(Notify::new());
    let compaction_shutdown_signal = compaction_shutdown.clone();
    let compaction_worker = CompactionWorker::new(
        state.clone(),
        compaction_max_small,
        compaction_grace_ms,
        compaction_tick_interval,
    );
    let compaction_handle = tokio::spawn(async move {
        compaction_worker.run(compaction_shutdown_signal).await;
    });

    start_server(state.clone()).await?;

    // Shutdown drain (review P1 #6): drain the memtable + rotate the WAL
    // so any events accumulated since the last size-based flush become
    // durable raw segments instead of staying stranded in WAL files.
    {
        let drain_msg = {
            let mut wal = state.wal.lock().await;
            let mut memtable = state.memtable.lock().await;
            if memtable.is_empty() {
                None
            } else {
                info!("Shutdown drain: {} events in memtable", memtable.len());
                let events = memtable.drain_all();
                match wal.rotate() {
                    Ok(sealed_id) => Some(usagedb::runtime::state::FlushMessage {
                        events,
                        sealed_wal_id: sealed_id,
                    }),
                    Err(e) => {
                        error!("Shutdown drain: WAL rotate failed: {} — events stay in WAL for recovery", e);
                        None
                    }
                }
            }
        };
        if let Some(msg) = drain_msg {
            if let Err(e) = state.flush_sender.send(msg).await {
                error!("Shutdown drain: flusher channel closed: {}", e);
            }
        }
    }

    info!("Waiting for background tasks to finish...");
    rollup_shutdown.notify_waiters();
    compaction_shutdown.notify_waiters();
    drop(state);
    let _ = flusher_handle.await;
    let _ = rollup_handle.await;
    let _ = compaction_handle.await;

    info!("Server gracefully shut down.");
    Ok(())
}
