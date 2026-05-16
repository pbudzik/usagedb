use axum::{
    routing::{post, get},
    Router, Json,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    http::StatusCode,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tracing::{info, error};

use crate::api::http::{
    IngestBatchRequest, IngestBatchResponse, UsageQueryRequest,
};
use crate::ingest::dedupe::DedupeResult;
use crate::model::event::{EventKind, UsageEvent};
use crate::runtime::recovery::compute_event_hashes;
use crate::runtime::state::{AppState, FlushMessage};
use crate::runtime::config::DurabilityMode;

pub async fn start_server(state: AppState) -> Result<(), std::io::Error> {
    let app = build_router(state.clone());

    let addr: SocketAddr = state.config.http_bind_address.parse().unwrap();
    info!("Starting HTTP server on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
}

/// Construct the axum Router with all routes wired up. Exposed so
/// integration tests can drive endpoints via `tower::oneshot` without
/// binding a port.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "OK" }))
        // Spec §9.1 — canonical path.
        .route("/v1/usage/batch", post(handle_ingest))
        // Spec §12.2 — account usage with query params.
        .route("/v1/accounts/{account_id}/usage", get(handle_account_usage))
        // Spec §12.3 — raw audit query.
        .route("/v1/accounts/{account_id}/usage/events", get(handle_account_events))
        // Phase D operability — explain a total and verify rollup-vs-raw drift.
        .route("/v1/accounts/{account_id}/explain", get(handle_explain))
        .route("/v1/accounts/{account_id}/verify", get(handle_verify))
        // Period lifecycle (minimal: Open ↔ Closed).
        .route("/v1/accounts/{account_id}/periods/{period}", get(handle_get_period))
        .route("/v1/accounts/{account_id}/periods/{period}/close", post(handle_close_period))
        .route("/v1/accounts/{account_id}/periods/{period}/reopen", post(handle_reopen_period))
        // Flexible POST query for arbitrary filter shapes.
        .route("/v1/query/json", post(handle_query_json))
        // SQL subset endpoint.
        .route("/v1/query/sql", post(handle_query_sql))
        .with_state(state)
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

/// Maximum number of dimensions per event (spec §21).
const MAX_DIMENSIONS: usize = 16;

#[derive(Debug, PartialEq, Eq)]
enum ValidationError {
    MissingEventId,
    MissingAccountId,
    MissingProductId,
    MissingMeterId,
    NonPositiveTimestamp,
    TooManyDimensions,
    CorrectionMissingRef,
}

fn validate_event(event: &UsageEvent) -> Result<(), ValidationError> {
    if event.event_id.0.is_empty() { return Err(ValidationError::MissingEventId); }
    if event.account_id.0.is_empty() { return Err(ValidationError::MissingAccountId); }
    if event.product_id.0.is_empty() { return Err(ValidationError::MissingProductId); }
    if event.meter_id.0.is_empty() { return Err(ValidationError::MissingMeterId); }
    if event.timestamp_ms <= 0 { return Err(ValidationError::NonPositiveTimestamp); }
    if event.dimensions.inner.len() > MAX_DIMENSIONS {
        return Err(ValidationError::TooManyDimensions);
    }
    if matches!(event.kind, EventKind::Correction | EventKind::Retraction)
        && event.correction_ref.is_none()
    {
        return Err(ValidationError::CorrectionMissingRef);
    }
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
    rejected: usize,
    drained: Option<FlushMessage>,
}

async fn handle_ingest(
    State(state): State<AppState>,
    Json(payload): Json<IngestBatchRequest>,
) -> Result<Json<IngestBatchResponse>, AppError> {
    // Validate + stamp ingested_at_ms server-side (so a client with a
    // wonky clock can't poison TTL eviction). Rejected events never
    // reach the WAL or dedupe.
    let ingest_now = now_ms();

    // Snapshot the closed-periods list under the manifest read lock so
    // the per-event check below is cheap (no async lock in the loop).
    // The window between snapshot and validation is small enough that a
    // racing close_period call doesn't matter — the operator should
    // wait for expected ingest to drain before closing anyway.
    let closed_snapshot: Vec<crate::storage::manifest::ClosedPeriod> = {
        let manifest = state.manifest.read().await;
        manifest.closed_periods.clone()
    };

    let mut rejected = 0usize;
    let mut classified: Vec<Classified> = Vec::with_capacity(payload.events.len());
    for mut event in payload.events {
        if let Err(reason) = validate_event(&event) {
            rejected += 1;
            tracing::warn!(?reason, event_id = %event.event_id.0, "rejected event");
            continue;
        }
        // Period-closed check: reject `Usage` events landing in a
        // closed period. Corrections / retractions are intentionally
        // allowed through — they become post-close adjustments.
        if matches!(event.kind, EventKind::Usage) {
            if let Some((year, month)) = crate::period::period_for_ts(event.timestamp_ms) {
                if closed_snapshot
                    .iter()
                    .any(|p| p.account_id == event.account_id.0
                        && p.year == year
                        && p.month == month)
                {
                    rejected += 1;
                    tracing::warn!(
                        event_id = %event.event_id.0,
                        account = %event.account_id.0,
                        year, month,
                        "rejected: Usage event in closed period"
                    );
                    continue;
                }
            }
        }
        event.ingested_at_ms = ingest_now;
        let (event_id_hash, payload_hash) = compute_event_hashes(&event);
        classified.push(Classified { event, event_id_hash, payload_hash });
    }

    let outcome = ingest_critical_section(&state, classified, rejected).await?;

    if let Some(msg) = outcome.drained {
        if let Err(e) = state.flush_sender.send(msg).await {
            error!("Failed to enqueue flush: {}", e);
        }
    }

    Ok(Json(IngestBatchResponse {
        accepted: outcome.accepted,
        duplicates: outcome.duplicates,
        conflicts: outcome.conflicts,
        rejected: outcome.rejected,
    }))
}

async fn ingest_critical_section(
    state: &AppState,
    classified: Vec<Classified>,
    rejected_before: usize,
) -> Result<IngestOutcome, AppError> {
    let mut dedupe = state.dedupe.lock().await;
    let mut wal = state.wal.lock().await;
    let mut memtable = state.memtable.lock().await;

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

    // Phase 2: durable WAL append. Per Config.durability_mode, Strict
    // does flush+fsync, Fast only flushes the userspace buffer.
    if !new_events.is_empty() {
        // Stream refs into append_batch — no intermediate Vec, no clones.
        wal.append_batch(new_events.iter().map(|c| &c.event))
            .map_err(|e| AppError(anyhow::anyhow!("WAL append failed: {}", e)))?;
        match state.config.durability_mode {
            DurabilityMode::Strict => {
                wal.sync()
                    .map_err(|e| AppError(anyhow::anyhow!("WAL sync failed: {}", e)))?;
            }
            DurabilityMode::Fast => {
                wal.flush_buffer()
                    .map_err(|e| AppError(anyhow::anyhow!("WAL flush failed: {}", e)))?;
            }
        }
    }

    // Phase 3: commit dedupe + insert into memtable (one move per event,
    // no clone).
    let accepted = new_events.len();
    for c in new_events {
        dedupe.commit(c.event_id_hash, c.payload_hash);
        memtable.insert(c.event);
    }

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

    Ok(IngestOutcome {
        accepted,
        duplicates,
        conflicts,
        rejected: rejected_before,
        drained,
    })
}

/// Spec §12.2 — GET /v1/accounts/{account_id}/usage. Query params:
///   from, to       — RFC 3339 timestamps (required)
///   group_by       — comma-separated list of group keys (optional)
///   product_id, meter_id, model_id — equality filters (optional)
#[derive(serde::Deserialize, Default)]
struct UsageQueryParams {
    from: String,
    to: String,
    #[serde(default)]
    group_by: Option<String>,
    #[serde(default)]
    product_id: Option<String>,
    #[serde(default)]
    meter_id: Option<String>,
    #[serde(default)]
    model_id: Option<String>,
    #[serde(default)]
    source: Option<String>, // "raw" or "rollup"
}

async fn handle_account_usage(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(params): Query<UsageQueryParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::executor::execute_plan;
    use crate::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};

    let from_ms = DateTime::parse_from_rfc3339(&params.from)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `from`: {}", e)))?;
    let to_ms = DateTime::parse_from_rfc3339(&params.to)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `to`: {}", e)))?;

    let source = match params.source.as_deref() {
        Some("raw") => QuerySource::RawEvents,
        _ => QuerySource::RollupHourly,
    };

    let mut filters = Vec::new();
    for (field, value) in [
        ("product_id", params.product_id),
        ("meter_id", params.meter_id),
        ("model_id", params.model_id),
    ] {
        if let Some(v) = value {
            filters.push(QueryFilter { field: field.to_string(), values: vec![v] });
        }
    }

    let group_by: Vec<String> = params
        .group_by
        .as_deref()
        .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default();

    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);
    metrics.insert("count".to_string(), AggregationFunction::Count);

    let plan = QueryPlan {
        source,
        account_id: Some(account_id),
        from_ms,
        to_ms,
        filters,
        group_by,
        metrics,
        limit: None,
    };

    let results = execute_plan(&state, &plan).await;
    let watermark_ms = state.manifest.read().await.watermarks.hourly_rollup_ms;
    Ok(Json(serde_json::json!({
        "watermark_ms": watermark_ms,
        "lines": results,
    })))
}

/// Spec §12.3 — GET /v1/accounts/{account_id}/usage/events. Returns
/// individual events in the time range (raw audit). Filters: meter_id.
#[derive(serde::Deserialize, Default)]
struct EventsQueryParams {
    from: String,
    to: String,
    #[serde(default)]
    meter_id: Option<String>,
    #[serde(default)]
    product_id: Option<String>,
}

async fn handle_account_events(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(params): Query<EventsQueryParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::executor::execute_plan;
    use crate::query::plan::{QueryFilter, QueryPlan, QuerySource};

    let from_ms = DateTime::parse_from_rfc3339(&params.from)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `from`: {}", e)))?;
    let to_ms = DateTime::parse_from_rfc3339(&params.to)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `to`: {}", e)))?;

    let mut filters = Vec::new();
    if let Some(meter_id) = params.meter_id {
        filters.push(QueryFilter { field: "meter_id".into(), values: vec![meter_id] });
    }
    if let Some(product_id) = params.product_id {
        filters.push(QueryFilter { field: "product_id".into(), values: vec![product_id] });
    }

    let plan = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some(account_id),
        from_ms,
        to_ms,
        filters,
        group_by: vec![],     // raw events, no aggregation
        metrics: HashMap::new(),
        limit: None,
    };

    let results = execute_plan(&state, &plan).await;
    Ok(Json(serde_json::json!({ "events": results })))
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

/// Phase D operability: `GET /v1/accounts/{account_id}/explain?from&to`
///
/// Returns the breakdown that contributed to an account's total over a
/// time range — broken out by `(product, meter, model, source, unit)`,
/// plus the list of rollup and raw segment IDs that overlap the range
/// (so an operator can drill into them via `inspect-segment` later), plus
/// the corrections / retractions that affected the total separately.
///
/// This is the spec's "explain a billing total" primitive — without it,
/// a disagreement between dashboard and invoice is hard to investigate.
#[derive(serde::Deserialize, Default)]
struct ExplainParams {
    from: String,
    to: String,
}

async fn handle_explain(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(params): Query<ExplainParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::executor::execute_plan;
    use crate::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};
    use crate::model::ids::{AccountId, bucket_for_account};

    let from_ms = DateTime::parse_from_rfc3339(&params.from)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `from`: {}", e)))?;
    let to_ms = DateTime::parse_from_rfc3339(&params.to)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `to`: {}", e)))?;

    // Breakdown via the rollup path (with raw fallback for the open-period
    // tail). Group by every billing-relevant column so each row is a
    // distinct invoice line.
    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);
    metrics.insert("count".to_string(), AggregationFunction::Count);
    let plan = QueryPlan {
        source: QuerySource::RollupHourly,
        account_id: Some(account_id.clone()),
        from_ms,
        to_ms,
        filters: vec![],
        group_by: vec![
            "product_id".into(),
            "meter_id".into(),
            "model_id".into(),
            "source".into(),
            "unit".into(),
        ],
        metrics,
        limit: None,
    };
    let lines = execute_plan(&state, &plan).await;

    // Corrections + retractions in the range, returned as raw rows for
    // forensic inspection. Empty for Usage-only periods.
    let plan_corr = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some(account_id.clone()),
        from_ms,
        to_ms,
        filters: vec![QueryFilter {
            field: "kind".into(),
            values: vec!["Correction".into(), "Retraction".into()],
        }],
        group_by: vec![],
        metrics: HashMap::new(),
        limit: None,
    };
    let corrections = execute_plan(&state, &plan_corr).await;

    // Segment provenance from the manifest. Filtering by bucket here
    // matches the executor's pruning. For each overlapping rollup
    // segment we also surface its `input_segment_ids` so an operator
    // can name every raw segment that contributed to a rollup line —
    // spec §19.10 (invoice snapshots reference a watermark + source
    // segment set).
    let (watermark_ms, rollup_segments, rollup_inputs, raw_segments) = {
        let manifest = state.manifest.read().await;
        let bucket_count = manifest.bucket_count.max(1);
        let target_bucket = bucket_for_account(&AccountId(account_id.clone()), bucket_count);
        let mut rollup_ids = Vec::new();
        let mut inputs_map = serde_json::Map::new();
        for s in &manifest.rollup_segments {
            if s.bucket != target_bucket
                || s.min_timestamp_ms >= to_ms
                || s.max_timestamp_ms < from_ms
            {
                continue;
            }
            rollup_ids.push(s.segment_id.clone());
            inputs_map.insert(
                s.segment_id.clone(),
                serde_json::Value::Array(
                    s.input_segment_ids
                        .iter()
                        .map(|id| serde_json::Value::String(id.clone()))
                        .collect(),
                ),
            );
        }
        let raws: Vec<String> = manifest
            .raw_segments
            .iter()
            .filter(|s| {
                s.bucket == target_bucket
                    && s.min_timestamp_ms < to_ms
                    && s.max_timestamp_ms >= from_ms
            })
            .map(|s| s.segment_id.clone())
            .collect();
        (
            manifest.watermarks.hourly_rollup_ms,
            rollup_ids,
            inputs_map,
            raws,
        )
    };

    Ok(Json(serde_json::json!({
        "account_id": account_id,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "watermark_ms": watermark_ms,
        "lines": lines,
        "rollup_segments": rollup_segments,
        "rollup_inputs": rollup_inputs,
        "raw_segments": raw_segments,
        "corrections": corrections,
    })))
}

/// Phase D operability: `GET /v1/accounts/{account_id}/verify?from&to`
///
/// Computes the same SUM(quantity) two ways — through the rollup path
/// and through a pure raw scan — and reports both totals plus the
/// `drift = raw - rollup`. Drift of zero on a fully-sealed period
/// (where `to <= watermark_ms`) is the invariant; non-zero indicates a
/// rollup bug, a late event that landed below the watermark, or a
/// missing rollup segment that operator-driven `rebuild_rollups` should
/// fix.
#[derive(serde::Deserialize, Default)]
struct VerifyParams {
    from: String,
    to: String,
}

async fn handle_verify(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(params): Query<VerifyParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    use chrono::DateTime;
    use crate::query::executor::execute_plan;
    use crate::query::plan::{AggregationFunction, QueryPlan, QuerySource};

    let from_ms = DateTime::parse_from_rfc3339(&params.from)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `from`: {}", e)))?;
    let to_ms = DateTime::parse_from_rfc3339(&params.to)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| AppError(anyhow::anyhow!("invalid `to`: {}", e)))?;

    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);

    let plan_raw = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some(account_id.clone()),
        from_ms,
        to_ms,
        filters: vec![],
        group_by: vec![],
        metrics: metrics.clone(),
        limit: None,
    };
    let plan_rollup = QueryPlan {
        source: QuerySource::RollupHourly,
        ..plan_raw.clone()
    };

    let raw_result = execute_plan(&state, &plan_raw).await;
    let rollup_result = execute_plan(&state, &plan_rollup).await;

    let raw_total = extract_quantity_sum(&raw_result);
    let rollup_total = extract_quantity_sum(&rollup_result);
    let drift = raw_total.saturating_sub(rollup_total);
    let watermark_ms = state.manifest.read().await.watermarks.hourly_rollup_ms;
    let period_sealed = to_ms <= watermark_ms;

    Ok(Json(serde_json::json!({
        "account_id": account_id,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "watermark_ms": watermark_ms,
        "period_sealed": period_sealed,
        "raw_total": raw_total.to_string(),
        "rollup_total": rollup_total.to_string(),
        "drift": drift.to_string(),
        "matches": drift == 0,
    })))
}

/// `POST /v1/accounts/{account_id}/periods/{YYYY-MM}/close`
///
/// Marks the (account, year, month) period as closed. Subsequent `Usage`
/// events with a timestamp inside this period will be rejected at
/// ingest. `Correction` and `Retraction` events for closed periods are
/// still accepted — they become adjustments.
async fn handle_close_period(
    State(state): State<AppState>,
    Path((account_id, period)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    use crate::period::parse_period;
    use crate::storage::manifest::ClosedPeriod;

    let (year, month) = parse_period(&period)
        .map_err(|e| AppError(anyhow::anyhow!("invalid period: {}", e)))?;

    let mut manifest = state.manifest.write().await;
    if manifest.closed_periods.iter().any(|p| {
        p.account_id == account_id && p.year == year && p.month == month
    }) {
        return Ok(Json(serde_json::json!({
            "account_id": account_id,
            "period": period,
            "state": "Closed",
            "already_closed": true,
        })));
    }
    let closed_at_ms = now_ms();
    manifest.closed_periods.push(ClosedPeriod {
        account_id: account_id.clone(),
        year,
        month,
        closed_at_ms,
    });
    manifest
        .save(&state.config.db_root)
        .map_err(|e| AppError(anyhow::anyhow!("manifest save: {}", e)))?;

    Ok(Json(serde_json::json!({
        "account_id": account_id,
        "period": period,
        "state": "Closed",
        "closed_at_ms": closed_at_ms,
    })))
}

/// `POST /v1/accounts/{account_id}/periods/{YYYY-MM}/reopen`
///
/// Removes the closed marker. Future `Usage` events for the period are
/// accepted again.
async fn handle_reopen_period(
    State(state): State<AppState>,
    Path((account_id, period)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    use crate::period::parse_period;

    let (year, month) = parse_period(&period)
        .map_err(|e| AppError(anyhow::anyhow!("invalid period: {}", e)))?;

    let mut manifest = state.manifest.write().await;
    let before = manifest.closed_periods.len();
    manifest.closed_periods.retain(|p| {
        !(p.account_id == account_id && p.year == year && p.month == month)
    });
    let removed = before - manifest.closed_periods.len();
    if removed > 0 {
        manifest
            .save(&state.config.db_root)
            .map_err(|e| AppError(anyhow::anyhow!("manifest save: {}", e)))?;
    }

    Ok(Json(serde_json::json!({
        "account_id": account_id,
        "period": period,
        "state": "Open",
        "removed": removed > 0,
    })))
}

/// `GET /v1/accounts/{account_id}/periods/{YYYY-MM}`
///
/// Returns the period's current state (`Open`/`Closed`), `closed_at_ms`
/// if applicable, and the current SUM(quantity) over the period. The
/// SUM is live (not a frozen snapshot) — future snapshot semantics
/// will land alongside intermediate states (Closing/Invoiced/Adjusted).
async fn handle_get_period(
    State(state): State<AppState>,
    Path((account_id, period)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    use crate::period::{find_closed, parse_period};
    use crate::query::executor::execute_plan;
    use crate::query::plan::{AggregationFunction, QueryPlan, QuerySource};

    let (year, month) = parse_period(&period)
        .map_err(|e| AppError(anyhow::anyhow!("invalid period: {}", e)))?;

    // Period bounds: [first day of month UTC, first day of next month UTC).
    use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    let from_dt = NaiveDate::from_ymd_opt(year as i32, month as u32, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .ok_or_else(|| AppError(anyhow::anyhow!("invalid date for period {}", period)))?;
    let next_month_dt: NaiveDateTime = if month == 12 {
        NaiveDate::from_ymd_opt(year as i32 + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year as i32, month as u32 + 1, 1)
    }
    .and_then(|d| d.and_hms_opt(0, 0, 0))
    .ok_or_else(|| AppError(anyhow::anyhow!("invalid date for period {}", period)))?;
    let from_ms = Utc.from_utc_datetime(&from_dt).timestamp_millis();
    let to_ms = Utc.from_utc_datetime(&next_month_dt).timestamp_millis();

    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);
    let plan = QueryPlan {
        source: QuerySource::RollupHourly,
        account_id: Some(account_id.clone()),
        from_ms,
        to_ms,
        filters: vec![],
        group_by: vec![],
        metrics,
        limit: None,
    };
    let result = execute_plan(&state, &plan).await;
    let total = extract_quantity_sum(&result);

    let (manifest_state, closed_at_ms) = {
        let manifest = state.manifest.read().await;
        match find_closed(&manifest, &account_id, year, month) {
            Some(p) => ("Closed", Some(p.closed_at_ms)),
            None => ("Open", None),
        }
    };

    Ok(Json(serde_json::json!({
        "account_id": account_id,
        "period": period,
        "state": manifest_state,
        "closed_at_ms": closed_at_ms,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "total_quantity": total.to_string(),
    })))
}

/// Pull SUM(quantity) out of an executor result. Returns 0 when the
/// result is empty (e.g., no events in range).
fn extract_quantity_sum(result: &[serde_json::Value]) -> i128 {
    result
        .iter()
        .filter_map(|v| v.get("quantity"))
        .filter_map(|v| v.as_str())
        .filter_map(|s| s.parse().ok())
        .next()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::dimensions::SmallDimensions;
    use crate::model::ids::{
        AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
    };

    fn good_event() -> UsageEvent {
        UsageEvent {
            event_id: EventId("evt".into()),
            kind: EventKind::Usage,
            correction_ref: None,
            account_id: AccountId("acc".into()),
            subscription_id: Some(SubscriptionId("sub".into())),
            product_id: ProductId("prod".into()),
            meter_id: MeterId("meter".into()),
            timestamp_ms: 1,
            quantity: 1,
            unit: Unit("u".into()),
            source: SourceId("src".into()),
            model_id: Some(ModelId("mod".into())),
            dimensions: SmallDimensions::default(),
            ingested_at_ms: 0,
        }
    }

    #[test]
    fn validates_required_ids() {
        let mut e = good_event();
        e.event_id = EventId(String::new());
        assert_eq!(validate_event(&e), Err(ValidationError::MissingEventId));

        let mut e = good_event();
        e.account_id = AccountId(String::new());
        assert_eq!(validate_event(&e), Err(ValidationError::MissingAccountId));

        let mut e = good_event();
        e.product_id = ProductId(String::new());
        assert_eq!(validate_event(&e), Err(ValidationError::MissingProductId));

        let mut e = good_event();
        e.meter_id = MeterId(String::new());
        assert_eq!(validate_event(&e), Err(ValidationError::MissingMeterId));
    }

    #[test]
    fn rejects_non_positive_timestamp() {
        let mut e = good_event();
        e.timestamp_ms = 0;
        assert_eq!(validate_event(&e), Err(ValidationError::NonPositiveTimestamp));
        e.timestamp_ms = -1;
        assert_eq!(validate_event(&e), Err(ValidationError::NonPositiveTimestamp));
    }

    #[test]
    fn rejects_dimension_overflow() {
        let mut e = good_event();
        for i in 0..(MAX_DIMENSIONS + 1) {
            e.dimensions.inner.insert(format!("k{i}"), "v".into());
        }
        assert_eq!(validate_event(&e), Err(ValidationError::TooManyDimensions));
    }

    #[test]
    fn requires_correction_ref_on_correction() {
        let mut e = good_event();
        e.kind = EventKind::Correction;
        assert_eq!(validate_event(&e), Err(ValidationError::CorrectionMissingRef));

        e.kind = EventKind::Retraction;
        assert_eq!(validate_event(&e), Err(ValidationError::CorrectionMissingRef));
    }

    #[test]
    fn passes_for_good_event() {
        assert_eq!(validate_event(&good_event()), Ok(()));
    }
}
