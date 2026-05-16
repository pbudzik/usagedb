use axum::{
    routing::{post, get},
    Router, Json, extract::State,
    response::{IntoResponse, Response},
    http::StatusCode,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use crate::runtime::state::AppState;
use crate::api::http::{IngestBatchRequest, IngestBatchResponse, UsageQueryRequest, UsageQueryResponse};
use tracing::{info, error};

pub async fn start_server(state: AppState) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/query", post(handle_query))
        .with_state(state.clone());

    let addr: SocketAddr = state.config.http_bind_address.parse().unwrap();
    info!("Starting HTTP server on {}", addr);
    
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
}

pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let err_msg = format!("Internal server error: {}", self.0);
        error!("{}", err_msg);
        (StatusCode::INTERNAL_SERVER_ERROR, err_msg).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
    info!("Shutdown signal received, starting graceful shutdown...");
}

async fn handle_ingest(
    State(state): State<AppState>,
    Json(payload): Json<IngestBatchRequest>,
) -> Result<Json<IngestBatchResponse>, AppError> {
    let mut dedupe = state.dedupe.lock().await;
    let mut wal = state.wal.lock().await;
    let mut memtable = state.memtable.lock().await;
    
    let mut accepted = 0;
    let mut duplicates = 0;
    let mut conflicts = 0;
    
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut to_append = Vec::new();

    for event in payload.events {
        let mut s1 = DefaultHasher::new();
        event.event_id.hash(&mut s1);
        let event_id_hash = s1.finish();

        let mut s2 = DefaultHasher::new();
        event.quantity.hash(&mut s2); // Very simplified payload hash for MVP
        let payload_hash = s2.finish();

        match dedupe.check_and_insert(event_id_hash, payload_hash) {
            crate::ingest::dedupe::DedupeResult::NewEvent => {
                accepted += 1;
                to_append.push(event.clone());
                memtable.insert(event);
            }
            crate::ingest::dedupe::DedupeResult::ExactDuplicate => {
                duplicates += 1;
            }
            crate::ingest::dedupe::DedupeResult::PayloadConflict => {
                conflicts += 1;
            }
        }
    }

    if !to_append.is_empty() {
        if let Err(e) = wal.append_batch(&to_append) {
            error!("WAL append failed: {}", e);
        }
        let _ = wal.sync();
    }

    if memtable.size_bytes() > state.config.max_memtable_size_bytes {
        info!("Memtable size {} exceeds {}, triggering flush", memtable.size_bytes(), state.config.max_memtable_size_bytes);
        let drained = memtable.drain_all();
        if let Err(e) = state.flush_sender.try_send(drained) {
            error!("Failed to enqueue memtable for flushing: {}", e);
        }
    }

    Ok(Json(IngestBatchResponse {
        accepted,
        duplicates,
        conflicts,
        rejected: 0,
    }))
}

async fn handle_query(
    State(_state): State<AppState>,
    Json(payload): Json<UsageQueryRequest>,
) -> Result<Json<UsageQueryResponse>, AppError> {
    Ok(Json(UsageQueryResponse {
        account_id: payload.account_id,
        from_ms: payload.from_ms,
        to_ms: payload.to_ms,
        watermark_ms: payload.to_ms,
        lines: vec![],
    }))
}
