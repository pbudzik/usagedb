//! Apache Parquet export of raw usage events.
//!
//! Schema is intentionally flat — no nested types — so any Parquet
//! consumer can read it without struct/map support. `correction_ref`
//! and `dimensions` are flattened/serialized:
//!
//! | Column | Arrow type | Notes |
//! | --- | --- | --- |
//! | `event_id` | Utf8 | |
//! | `kind` | Utf8 | "Usage" / "Correction" / "Retraction" |
//! | `correction_original_event_id` | Utf8 (nullable) | flattened from correction_ref |
//! | `correction_reason` | Utf8 (nullable) | flattened from correction_ref |
//! | `account_id` | Utf8 | |
//! | `subscription_id` | Utf8 (nullable) | |
//! | `product_id` | Utf8 | |
//! | `meter_id` | Utf8 | |
//! | `model_id` | Utf8 (nullable) | |
//! | `timestamp_ms` | Int64 | |
//! | `quantity` | Decimal128(38, 0) | i128 fits exactly |
//! | `unit` | Utf8 | |
//! | `source` | Utf8 | |
//! | `dimensions_canonical` | Utf8 | serde_json of `SmallDimensions` (BTreeMap → canonical order) |
//! | `ingested_at_ms` | Int64 | |
//!
//! Compression: zstd. Row group size: default (~64K rows).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::model::event::{EventKind, UsageEvent};
use crate::runtime::state::AppState;
use crate::storage::segment_reader::RawSegmentReader;

/// Stats returned by an export run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportStats {
    pub events_exported: u64,
    pub segments_read: usize,
}

/// Export every raw segment in the manifest to a single Parquet file.
///
/// This is the simplest possible export — no partitioning, no filtering,
/// no chunking. Sufficient for "dump-and-load into the warehouse" use
/// cases at MVP scale. Later phases can add per-day partitioning and
/// streaming row groups for arbitrarily large exports.
pub async fn export_raw_segments(
    state: &AppState,
    output_path: &Path,
) -> anyhow::Result<ExportStats> {
    // Snapshot raw segment paths under the manifest read lock.
    let segment_paths: Vec<std::path::PathBuf> = {
        let manifest = state.manifest.read().await;
        manifest
            .raw_segments
            .iter()
            .map(|s| state.config.db_root.join(format!("{}.seg", s.segment_id)))
            .collect()
    };

    let mut all_events: Vec<UsageEvent> = Vec::new();
    let mut segments_read = 0usize;
    for path in &segment_paths {
        if !path.exists() {
            tracing::warn!("export: manifest references missing segment {:?}", path);
            continue;
        }
        let mut reader = RawSegmentReader::new(path.clone())?;
        while let Some(event) = reader.read_next()? {
            all_events.push(event);
        }
        segments_read += 1;
    }

    write_parquet(&all_events, output_path)?;

    Ok(ExportStats {
        events_exported: all_events.len() as u64,
        segments_read,
    })
}

/// Build an Arrow RecordBatch + write it to `output_path` as a Parquet file.
/// Public for testability — callers usually go through `export_raw_segments`.
pub fn write_parquet(events: &[UsageEvent], output_path: &Path) -> anyhow::Result<()> {
    let schema = Arc::new(parquet_schema());

    // Build per-column arrays. `Vec::with_capacity` up front avoids
    // reallocations on the large segments.
    let n = events.len();
    let mut event_id = Vec::with_capacity(n);
    let mut kind = Vec::with_capacity(n);
    let mut correction_original = Vec::with_capacity(n);
    let mut correction_reason = Vec::with_capacity(n);
    let mut account_id = Vec::with_capacity(n);
    let mut subscription_id = Vec::with_capacity(n);
    let mut product_id = Vec::with_capacity(n);
    let mut meter_id = Vec::with_capacity(n);
    let mut model_id = Vec::with_capacity(n);
    let mut timestamp_ms = Vec::with_capacity(n);
    let mut quantity = Vec::with_capacity(n);
    let mut unit = Vec::with_capacity(n);
    let mut source = Vec::with_capacity(n);
    let mut dimensions_canonical = Vec::with_capacity(n);
    let mut ingested_at_ms = Vec::with_capacity(n);

    for e in events {
        event_id.push(e.event_id.0.clone());
        kind.push(match e.kind {
            EventKind::Usage => "Usage",
            EventKind::Correction => "Correction",
            EventKind::Retraction => "Retraction",
        }.to_string());
        correction_original.push(e.correction_ref.as_ref().map(|c| c.original_event_id.0.clone()));
        correction_reason.push(e.correction_ref.as_ref().map(|c| c.reason.clone()));
        account_id.push(e.account_id.0.clone());
        subscription_id.push(e.subscription_id.as_ref().map(|s| s.0.clone()));
        product_id.push(e.product_id.0.clone());
        meter_id.push(e.meter_id.0.clone());
        model_id.push(e.model_id.as_ref().map(|m| m.0.clone()));
        timestamp_ms.push(e.timestamp_ms);
        quantity.push(e.quantity);
        unit.push(e.unit.0.clone());
        source.push(e.source.0.clone());
        // BTreeMap → JSON is already canonical-by-key.
        dimensions_canonical.push(serde_json::to_string(&e.dimensions).unwrap_or_default());
        ingested_at_ms.push(e.ingested_at_ms);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(event_id)),
        Arc::new(StringArray::from(kind)),
        Arc::new(StringArray::from(correction_original)),
        Arc::new(StringArray::from(correction_reason)),
        Arc::new(StringArray::from(account_id)),
        Arc::new(StringArray::from(subscription_id)),
        Arc::new(StringArray::from(product_id)),
        Arc::new(StringArray::from(meter_id)),
        Arc::new(StringArray::from(model_id)),
        Arc::new(Int64Array::from(timestamp_ms)),
        Arc::new(
            Decimal128Array::from(quantity)
                .with_precision_and_scale(38, 0)
                .map_err(|e| anyhow::anyhow!("decimal precision: {}", e))?,
        ),
        Arc::new(StringArray::from(unit)),
        Arc::new(StringArray::from(source)),
        Arc::new(StringArray::from(dimensions_canonical)),
        Arc::new(Int64Array::from(ingested_at_ms)),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {}", e))?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| anyhow::anyhow!("ArrowWriter::try_new: {}", e))?;
    writer
        .write(&batch)
        .map_err(|e| anyhow::anyhow!("ArrowWriter::write: {}", e))?;
    writer
        .close()
        .map_err(|e| anyhow::anyhow!("ArrowWriter::close: {}", e))?;
    Ok(())
}

/// The Parquet schema. Public so tests can compare round-trip output
/// against the canonical shape.
pub fn parquet_schema() -> Schema {
    Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("correction_original_event_id", DataType::Utf8, true),
        Field::new("correction_reason", DataType::Utf8, true),
        Field::new("account_id", DataType::Utf8, false),
        Field::new("subscription_id", DataType::Utf8, true),
        Field::new("product_id", DataType::Utf8, false),
        Field::new("meter_id", DataType::Utf8, false),
        Field::new("model_id", DataType::Utf8, true),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("quantity", DataType::Decimal128(38, 0), false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("dimensions_canonical", DataType::Utf8, false),
        Field::new("ingested_at_ms", DataType::Int64, false),
    ])
}
