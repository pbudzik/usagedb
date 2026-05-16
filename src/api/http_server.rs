use axum::{
    routing::{post, get},
    Router, Json, extract::State,
    response::{IntoResponse, Response},
    http::StatusCode,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use crate::runtime::state::{AppState, FlushMessage};
use crate::api::http::{IngestBatchRequest, IngestBatchResponse, UsageQueryRequest};
use crate::ingest::dedupe::DedupeResult;
use crate::runtime::recovery::compute_event_hashes;
use crate::model::event::UsageEvent;
use std::collections::HashMap;
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

struct Classified {
    event: UsageEvent,
    event_id_hash: crate::ingest::dedupe::EventHash,
    payload_hash: crate::ingest::dedupe::EventHash,
}

struct IngestOutcome {
    accepted: usize,
    duplicates: usize,
    conflicts: usize,
    drained: Option<FlushMessage>,
}

async fn handle_ingest(
    State(state): State<AppState>,
    Json(payload): Json<IngestBatchRequest>,
) -> Result<Json<IngestBatchResponse>, AppError> {
    // Pre-compute hashes outside the lock.
    let mut classified: Vec<Classified> = Vec::with_capacity(payload.events.len());
    for event in payload.events {
        let (event_id_hash, payload_hash) = compute_event_hashes(&event);
        classified.push(Classified { event, event_id_hash, payload_hash });
    }

    // Critical section: classify → WAL-append-and-fsync → commit. Channel
    // send happens after locks are dropped.
    let outcome = ingest_critical_section(&state, classified).await?;

    if let Some(msg) = outcome.drained {
        if let Err(e) = state.flush_sender.send(msg).await {
            error!("Failed to enqueue flush: {}", e);
        }
    }

    Ok(Json(IngestBatchResponse {
        accepted: outcome.accepted,
        duplicates: outcome.duplicates,
        conflicts: outcome.conflicts,
        rejected: 0,
    }))
}

async fn ingest_critical_section(
    state: &AppState,
    classified: Vec<Classified>,
) -> Result<IngestOutcome, AppError> {
    let mut dedupe = state.dedupe.lock().await;
    let mut wal = state.wal.lock().await;
    let mut memtable = state.memtable.lock().await;

    // Phase 1: classify (no mutation of dedupe). In-batch dedup ensures a
    // batch carrying the same event_id twice is counted as new+duplicate.
    let mut new_events: Vec<Classified> = Vec::new();
    let mut seen_in_batch: HashMap<crate::ingest::dedupe::EventHash, crate::ingest::dedupe::EventHash> = HashMap::new();
    let mut duplicates = 0usize;
    let mut conflicts = 0usize;

    for c in classified {
        if let Some(&prior_payload) = seen_in_batch.get(&c.event_id_hash) {
            if prior_payload == c.payload_hash {
                duplicates += 1;
            } else {
                conflicts += 1;
            }
            continue;
        }
        match dedupe.classify(c.event_id_hash, c.payload_hash) {
            DedupeResult::NewEvent => {
                seen_in_batch.insert(c.event_id_hash, c.payload_hash);
                new_events.push(c);
            }
            DedupeResult::ExactDuplicate => duplicates += 1,
            DedupeResult::PayloadConflict => conflicts += 1,
        }
    }

    // Phase 2: durable WAL append. On error, dedupe and memtable are
    // untouched, so the client's retry will not see false duplicates.
    if !new_events.is_empty() {
        let events_for_wal: Vec<UsageEvent> =
            new_events.iter().map(|c| c.event.clone()).collect();
        wal.append_batch(&events_for_wal)
            .map_err(|e| AppError(anyhow::anyhow!("WAL append failed: {}", e)))?;
        wal.sync()
            .map_err(|e| AppError(anyhow::anyhow!("WAL sync failed: {}", e)))?;
    }

    // Phase 3: commit dedupe and memtable.
    let accepted = new_events.len();
    for c in new_events {
        dedupe.commit(c.event_id_hash, c.payload_hash);
        memtable.insert(c.event);
    }

    // Flush trigger: drain + rotate under lock so no concurrent ingest can
    // write into the WAL file we just sealed.
    let drained = if memtable.size_bytes() > state.config.max_memtable_size_bytes {
        info!(
            "Memtable size {} exceeds {}, rotating WAL and flushing {} events",
            memtable.size_bytes(),
            state.config.max_memtable_size_bytes,
            memtable.len()
        );
        let drained_events = memtable.drain_all();
        let sealed_id = wal
            .rotate()
            .map_err(|e| AppError(anyhow::anyhow!("WAL rotate failed: {}", e)))?;
        Some(FlushMessage { events: drained_events, sealed_wal_id: sealed_id })
    } else {
        None
    };

    Ok(IngestOutcome { accepted, duplicates, conflicts, drained })
}

async fn handle_query_json(
    State(state): State<AppState>,
    Json(payload): Json<UsageQueryRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::plan::{QueryPlan, QuerySource, QueryFilter, AggregationFunction};
    use crate::query::executor::execute_plan;

    let from_ms = DateTime::parse_from_rfc3339(&payload.from)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `from`: {}", e)))?;
    let to_ms = DateTime::parse_from_rfc3339(&payload.to)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `to`: {}", e)))?;

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

    let results = execute_plan(&state, &plan).await;
    Ok(Json(serde_json::json!({ "data": results })))
}

async fn handle_query_sql(
    State(state): State<AppState>,
    Json(payload): Json<crate::api::http::SqlQueryRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use crate::query::sql::parse_sql;
    use crate::query::executor::execute_plan;
    let plan = parse_sql(&payload.query)
        .map_err(|e| AppError(anyhow::anyhow!("SQL parse error: {}", e)))?;
    let results = execute_plan(&state, &plan).await;
    Ok(Json(serde_json::json!({ "data": results })))
}
