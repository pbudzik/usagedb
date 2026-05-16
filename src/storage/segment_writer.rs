use std::fs::File;
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::PathBuf;
use crate::model::event::{CorrectionRef, EventKind, UsageEvent};
use crate::model::dimensions::SmallDimensions;
use crate::storage::compression::{compress, CompressionCodec};
use crate::storage::segment_format::{checksum, col, Codec, Encoding, MAGIC, MAGIC_END, VERSION};

/// Columnar segment writer. Events are buffered into per-column vectors;
/// `finish()` serializes each column with bincode, compresses with zstd,
/// and writes magic + headers + columns + footer to disk in one pass.
///
/// Per spec §7.3 the recommended uncompressed segment target is 64–256 MB,
/// which fits comfortably in memory for the buffering approach.
pub struct RawSegmentWriter {
    pub path: PathBuf,
    event_id: Vec<String>,
    kind: Vec<u8>,
    correction_ref: Vec<Option<CorrectionRef>>,
    account_id: Vec<String>,
    subscription_id: Vec<Option<String>>,
    product_id: Vec<String>,
    meter_id: Vec<String>,
    timestamp_ms: Vec<i64>,
    quantity: Vec<i128>,
    unit: Vec<String>,
    source: Vec<String>,
    model_id: Vec<Option<String>>,
    dimensions: Vec<SmallDimensions>,
    ingested_at_ms: Vec<i64>,
}

impl RawSegmentWriter {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        // Touch the file early so create errors (e.g. permission, missing
        // parent dir) surface before any work is done.
        let _ = File::create(&path)?;
        Ok(Self {
            path,
            event_id: Vec::new(),
            kind: Vec::new(),
            correction_ref: Vec::new(),
            account_id: Vec::new(),
            subscription_id: Vec::new(),
            product_id: Vec::new(),
            meter_id: Vec::new(),
            timestamp_ms: Vec::new(),
            quantity: Vec::new(),
            unit: Vec::new(),
            source: Vec::new(),
            model_id: Vec::new(),
            dimensions: Vec::new(),
            ingested_at_ms: Vec::new(),
        })
    }

    pub fn write_event(&mut self, event: &UsageEvent) -> IoResult<()> {
        self.event_id.push(event.event_id.0.clone());
        self.kind.push(encode_kind(&event.kind));
        self.correction_ref.push(event.correction_ref.clone());
        self.account_id.push(event.account_id.0.clone());
        self.subscription_id.push(event.subscription_id.as_ref().map(|s| s.0.clone()));
        self.product_id.push(event.product_id.0.clone());
        self.meter_id.push(event.meter_id.0.clone());
        self.timestamp_ms.push(event.timestamp_ms);
        self.quantity.push(event.quantity);
        self.unit.push(event.unit.0.clone());
        self.source.push(event.source.0.clone());
        self.model_id.push(event.model_id.as_ref().map(|m| m.0.clone()));
        self.dimensions.push(event.dimensions.clone());
        self.ingested_at_ms.push(event.ingested_at_ms);
        Ok(())
    }

    /// Write the on-disk file. Returns (row_count, checksum). The checksum
    /// is the same u64 the manifest stores in `SegmentMeta.checksum`, so
    /// recovery and integrity checks can compare without rereading the file.
    pub fn finish(self) -> IoResult<(u64, u64)> {
        let row_count = self.event_id.len() as u32;

        // Serialize each column into a Vec<u8>.
        let mut column_chunks: Vec<(&'static str, Vec<u8>)> = Vec::with_capacity(col::ORDER.len());
        column_chunks.push((col::EVENT_ID, ser(&self.event_id)?));
        column_chunks.push((col::KIND, ser(&self.kind)?));
        column_chunks.push((col::CORRECTION_REF, ser(&self.correction_ref)?));
        column_chunks.push((col::ACCOUNT_ID, ser(&self.account_id)?));
        column_chunks.push((col::SUBSCRIPTION_ID, ser(&self.subscription_id)?));
        column_chunks.push((col::PRODUCT_ID, ser(&self.product_id)?));
        column_chunks.push((col::METER_ID, ser(&self.meter_id)?));
        column_chunks.push((col::TIMESTAMP_MS, ser(&self.timestamp_ms)?));
        column_chunks.push((col::QUANTITY, ser(&self.quantity)?));
        column_chunks.push((col::UNIT, ser(&self.unit)?));
        column_chunks.push((col::SOURCE, ser(&self.source)?));
        column_chunks.push((col::MODEL_ID, ser(&self.model_id)?));
        column_chunks.push((col::DIMENSIONS, ser(&self.dimensions)?));
        column_chunks.push((col::INGESTED_AT_MS, ser(&self.ingested_at_ms)?));

        // Compress each column independently.
        let mut compressed_chunks: Vec<(&'static str, Vec<u8>)> = Vec::with_capacity(column_chunks.len());
        for (name, raw) in column_chunks {
            let compressed = compress(&raw, CompressionCodec::Zstd)?;
            compressed_chunks.push((name, compressed));
        }

        // Assemble the file into a Vec<u8> first so we can compute a single
        // checksum over the body. (Buffered then written; segment sizes
        // top out around a couple hundred MB compressed.)
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&row_count.to_le_bytes());
        let num_columns = compressed_chunks.len() as u16;
        body.extend_from_slice(&num_columns.to_le_bytes());

        for (name, compressed) in &compressed_chunks {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            body.extend_from_slice(&name_len.to_le_bytes());
            body.extend_from_slice(name_bytes);
            body.push(Encoding::Plain as u8);
            body.push(Codec::Zstd as u8);
            let compressed_len = compressed.len() as u32;
            body.extend_from_slice(&compressed_len.to_le_bytes());
            body.extend_from_slice(compressed);
        }

        let checksum_val = checksum(&body);
        body.extend_from_slice(&checksum_val.to_le_bytes());
        body.extend_from_slice(MAGIC_END);

        let file = File::create(&self.path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&body)?;
        writer.flush()?;
        writer.into_inner()?.sync_all()?;

        Ok((row_count as u64, checksum_val))
    }
}

fn ser<T: serde::Serialize>(value: &T) -> IoResult<Vec<u8>> {
    bincode::serialize(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn encode_kind(k: &EventKind) -> u8 {
    match k {
        EventKind::Usage => 0,
        EventKind::Correction => 1,
        EventKind::Retraction => 2,
    }
}
