use std::fs::File;
use std::io::{Read, Result as IoResult};
use std::path::PathBuf;
use std::collections::HashMap;
use crate::model::event::{CorrectionRef, EventKind, UsageEvent};
use crate::model::dimensions::SmallDimensions;
use crate::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use crate::storage::compression::{decompress, CompressionCodec};
use crate::storage::segment_format::{checksum, col, Codec, Encoding, MAGIC, MAGIC_END, VERSION};

/// Columnar segment reader. On `new()` the entire file is read, validated
/// against the magic + checksum + end magic, and each column is
/// decompressed into memory. `read_next()` then yields events row-by-row.
/// Future optimization: read only the columns the caller needs by parsing
/// the header and seeking past column payloads we don't care about.
pub struct RawSegmentReader {
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
    row_count: usize,
    cursor: usize,
}

impl RawSegmentReader {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let mut file = File::open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        // Minimum file size: header + footer with no columns.
        const HEADER_LEN: usize = MAGIC.len() + 1 + 4 + 2;
        const FOOTER_LEN: usize = 8 + MAGIC_END.len();
        if bytes.len() < HEADER_LEN + FOOTER_LEN {
            return Err(corrupt("file too small to be a segment"));
        }

        // End magic.
        let end_magic_off = bytes.len() - MAGIC_END.len();
        if &bytes[end_magic_off..] != MAGIC_END {
            return Err(corrupt("missing end magic"));
        }

        // Stored checksum is the 8 bytes immediately before the end magic.
        let stored_checksum_off = end_magic_off - 8;
        let stored_checksum = u64::from_le_bytes([
            bytes[stored_checksum_off],
            bytes[stored_checksum_off + 1],
            bytes[stored_checksum_off + 2],
            bytes[stored_checksum_off + 3],
            bytes[stored_checksum_off + 4],
            bytes[stored_checksum_off + 5],
            bytes[stored_checksum_off + 6],
            bytes[stored_checksum_off + 7],
        ]);
        let body = &bytes[..stored_checksum_off];
        let computed = checksum(body);
        if computed != stored_checksum {
            return Err(corrupt(&format!(
                "checksum mismatch (stored {:#x}, computed {:#x})",
                stored_checksum, computed
            )));
        }

        // Start magic.
        if &body[..MAGIC.len()] != MAGIC {
            return Err(corrupt("missing start magic"));
        }
        let mut off = MAGIC.len();

        // Version.
        let version = body[off];
        off += 1;
        if version != VERSION {
            return Err(corrupt(&format!("unsupported segment version {}", version)));
        }

        // row_count, num_columns.
        let row_count = read_u32(body, off)?;
        off += 4;
        let num_columns = read_u16(body, off)? as usize;
        off += 2;

        // Walk columns. Build a name → raw bytes map; resolve into typed
        // vectors below. The format permits columns in any order on disk
        // (current writer uses a stable order but the reader doesn't
        // depend on it).
        let mut chunks: HashMap<String, Vec<u8>> = HashMap::with_capacity(num_columns);
        for _ in 0..num_columns {
            let name_len = read_u16(body, off)? as usize;
            off += 2;
            if off + name_len > body.len() {
                return Err(corrupt("column name overruns file"));
            }
            let name = std::str::from_utf8(&body[off..off + name_len])
                .map_err(|e| corrupt(&format!("column name not utf-8: {}", e)))?
                .to_string();
            off += name_len;

            let encoding = body[off];
            off += 1;
            let codec = body[off];
            off += 1;
            if Encoding::from_byte(encoding) != Some(Encoding::Plain) {
                return Err(corrupt(&format!("unsupported encoding {} for column {}", encoding, name)));
            }
            let codec = Codec::from_byte(codec)
                .ok_or_else(|| corrupt(&format!("unknown codec {} for column {}", codec, name)))?;

            let compressed_len = read_u32(body, off)? as usize;
            off += 4;
            if off + compressed_len > body.len() {
                return Err(corrupt(&format!("column {} payload overruns file", name)));
            }
            let compressed = &body[off..off + compressed_len];
            off += compressed_len;

            let raw = match codec {
                Codec::None => compressed.to_vec(),
                Codec::Zstd => decompress(compressed, CompressionCodec::Zstd)?,
                Codec::Lz4 => decompress(compressed, CompressionCodec::Lz4)?,
            };
            chunks.insert(name, raw);
        }

        // Decode every required column. A missing column is a hard error
        // since the writer always emits all 14.
        let event_id: Vec<String> = de(&take(&mut chunks, col::EVENT_ID)?)?;
        let kind: Vec<u8> = de(&take(&mut chunks, col::KIND)?)?;
        let correction_ref: Vec<Option<CorrectionRef>> = de(&take(&mut chunks, col::CORRECTION_REF)?)?;
        let account_id: Vec<String> = de(&take(&mut chunks, col::ACCOUNT_ID)?)?;
        let subscription_id: Vec<Option<String>> = de(&take(&mut chunks, col::SUBSCRIPTION_ID)?)?;
        let product_id: Vec<String> = de(&take(&mut chunks, col::PRODUCT_ID)?)?;
        let meter_id: Vec<String> = de(&take(&mut chunks, col::METER_ID)?)?;
        let timestamp_ms: Vec<i64> = de(&take(&mut chunks, col::TIMESTAMP_MS)?)?;
        let quantity: Vec<i128> = de(&take(&mut chunks, col::QUANTITY)?)?;
        let unit: Vec<String> = de(&take(&mut chunks, col::UNIT)?)?;
        let source: Vec<String> = de(&take(&mut chunks, col::SOURCE)?)?;
        let model_id: Vec<Option<String>> = de(&take(&mut chunks, col::MODEL_ID)?)?;
        let dimensions: Vec<SmallDimensions> = de(&take(&mut chunks, col::DIMENSIONS)?)?;
        let ingested_at_ms: Vec<i64> = de(&take(&mut chunks, col::INGESTED_AT_MS)?)?;

        // Row count consistency.
        let rc = row_count as usize;
        for (name, len) in [
            ("event_id", event_id.len()),
            ("kind", kind.len()),
            ("correction_ref", correction_ref.len()),
            ("account_id", account_id.len()),
            ("subscription_id", subscription_id.len()),
            ("product_id", product_id.len()),
            ("meter_id", meter_id.len()),
            ("timestamp_ms", timestamp_ms.len()),
            ("quantity", quantity.len()),
            ("unit", unit.len()),
            ("source", source.len()),
            ("model_id", model_id.len()),
            ("dimensions", dimensions.len()),
            ("ingested_at_ms", ingested_at_ms.len()),
        ] {
            if len != rc {
                return Err(corrupt(&format!(
                    "column {} has {} rows, header claims {}",
                    name, len, rc
                )));
            }
        }

        Ok(Self {
            event_id,
            kind,
            correction_ref,
            account_id,
            subscription_id,
            product_id,
            meter_id,
            timestamp_ms,
            quantity,
            unit,
            source,
            model_id,
            dimensions,
            ingested_at_ms,
            row_count: rc,
            cursor: 0,
        })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn read_next(&mut self) -> IoResult<Option<UsageEvent>> {
        if self.cursor >= self.row_count {
            return Ok(None);
        }
        let i = self.cursor;
        self.cursor += 1;

        let kind = match self.kind[i] {
            0 => EventKind::Usage,
            1 => EventKind::Correction,
            2 => EventKind::Retraction,
            other => return Err(corrupt(&format!("unknown event kind discriminant {}", other))),
        };

        Ok(Some(UsageEvent {
            event_id: EventId(self.event_id[i].clone()),
            kind,
            correction_ref: self.correction_ref[i].clone(),
            account_id: AccountId(self.account_id[i].clone()),
            subscription_id: self.subscription_id[i].clone().map(SubscriptionId),
            product_id: ProductId(self.product_id[i].clone()),
            meter_id: MeterId(self.meter_id[i].clone()),
            timestamp_ms: self.timestamp_ms[i],
            quantity: self.quantity[i],
            unit: Unit(self.unit[i].clone()),
            source: SourceId(self.source[i].clone()),
            model_id: self.model_id[i].clone().map(ModelId),
            dimensions: self.dimensions[i].clone(),
            ingested_at_ms: self.ingested_at_ms[i],
        }))
    }

    pub fn scan_by_account(mut self, target_account: &AccountId) -> IoResult<Vec<UsageEvent>> {
        let mut results = Vec::new();
        while let Some(event) = self.read_next()? {
            if &event.account_id == target_account {
                results.push(event);
            }
        }
        Ok(results)
    }
}

fn read_u32(buf: &[u8], off: usize) -> IoResult<u32> {
    if off + 4 > buf.len() {
        return Err(corrupt("unexpected end of file"));
    }
    Ok(u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]))
}

fn read_u16(buf: &[u8], off: usize) -> IoResult<u16> {
    if off + 2 > buf.len() {
        return Err(corrupt("unexpected end of file"));
    }
    Ok(u16::from_le_bytes([buf[off], buf[off + 1]]))
}

fn take(map: &mut HashMap<String, Vec<u8>>, name: &str) -> IoResult<Vec<u8>> {
    map.remove(name).ok_or_else(|| corrupt(&format!("missing column {}", name)))
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> IoResult<T> {
    bincode::deserialize(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn corrupt(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("corrupt segment: {}", msg))
}
