use axum::{
    routing::{post, get},
    Router, Json, extract::State,
    response::{IntoResponse, Response},
    http::StatusCode,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use crate::runtime::state::AppState;
use crate::api::http::{IngestBatchRequest, IngestBatchResponse, UsageQueryRequest};
use tracing::{info, error};

pub async fn start_server(state: AppState) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/query/json", post(handle_query_json))
        .route("/v1/query/sql", post(handle_query_sql))
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
        let mut ev_clone = event.clone();
        ev_clone.ingested_at_ms = 0;
        if let Ok(bytes) = bincode::serialize(&ev_clone) {
            std::hash::Hash::hash_slice(&bytes, &mut s2);
        }
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
            return Err(AppError(anyhow::anyhow!("WAL append failed: {}", e)));
        }
        if let Err(e) = wal.sync() {
            error!("WAL sync failed: {}", e);
            return Err(AppError(anyhow::anyhow!("WAL sync failed: {}", e)));
        }
    }

    if memtable.size_bytes() > state.config.max_memtable_size_bytes {
        info!("Memtable size {} exceeds {}, triggering flush", memtable.size_bytes(), state.config.max_memtable_size_bytes);
        let drained = memtable.drain_all();
        if let Err(e) = state.flush_sender.send(drained).await {
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

async fn handle_query_json(
    State(_state): State<AppState>,
    Json(payload): Json<UsageQueryRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::plan::{QueryPlan, QuerySource, QueryFilter, AggregationFunction};
    use crate::query::executor::execute_plan;
    use std::collections::HashMap;
    
    let from_ms = DateTime::parse_from_rfc3339(&payload.from)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);
        
    let to_ms = DateTime::parse_from_rfc3339(&payload.to)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(i64::MAX);

    let source = if payload.source == "usage_events" {
        QuerySource::RawEvents
    } else {
        QuerySource::RollupHourly
    };

    let mut filters = Vec::new();
    if let Some(f) = payload.filters {
        for (k, v) in f {
            filters.push(QueryFilter { field: k, values: v });
        }
    }

    let mut metrics = HashMap::new();
    if let Some(m) = payload.metrics {
        for (k, v) in m {
            if v.to_lowercase() == "sum" {
                metrics.insert(k, AggregationFunction::Sum);
            } else if v.to_lowercase() == "count" {
                metrics.insert(k, AggregationFunction::Count);
            }
        }
    }

    let plan = QueryPlan {
        source,
        account_id: Some(payload.account_id),
        from_ms,
        to_ms,
        filters,
        group_by: payload.group_by,
        metrics,
        limit: None,
    };

    let results = execute_plan(&plan);
    Ok(Json(serde_json::json!({ "data": results })))
}

async fn handle_query_sql(
    State(_state): State<AppState>,
    Json(payload): Json<crate::api::http::SqlQueryRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use crate::query::sql::parse_sql;
    use crate::query::executor::execute_plan;
    let plan = parse_sql(&payload.query).map_err(|e| AppError(anyhow::anyhow!("SQL parse error: {}", e)))?;
    let results = execute_plan(&plan);
    Ok(Json(serde_json::json!({ "data": results })))
}
