use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::PathBuf;
use crate::model::event::{CorrectionRef, EventKind, UsageEvent};
use crate::model::dimensions::SmallDimensions;
use crate::storage::compression::{compress, CompressionCodec};
use crate::storage::segment_format::{checksum, col, Codec, Encoding, MAGIC, MAGIC_END, VERSION};

/// Columnar segment writer with per-column encoding selection. Events are
/// buffered into per-column vectors; `finish()` encodes each column with
/// the best-fit scheme (dictionary for ID strings, delta for timestamps,
/// zigzag for quantities, plain for everything else), then zstd-compresses
/// the encoded bytes, then writes magic + headers + columns + footer.
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

        // Encode each column with its best-fit scheme. The first element
        // of each tuple is the column name; the second is the chosen
        // Encoding (recorded in the per-column header); the third is the
        // encoded byte buffer (pre-compression).
        let mut column_chunks: Vec<(&'static str, Encoding, Vec<u8>)> = Vec::with_capacity(col::ORDER.len());

        // event_id is high-cardinality (usually unique per row) — Dictionary
        // would expand it; Plain + zstd is the right call.
        column_chunks.push((col::EVENT_ID, Encoding::Plain, ser(&self.event_id)?));

        // kind has only 3 possible values; Plain is fine, zstd handles the
        // repetition. (RLE could squeeze more out — punted for now.)
        column_chunks.push((col::KIND, Encoding::Plain, ser(&self.kind)?));

        // correction_ref is sparse + structured; Plain.
        column_chunks.push((col::CORRECTION_REF, Encoding::Plain, ser(&self.correction_ref)?));

        // ID-style strings: heavily repeating, big Dictionary wins.
        column_chunks.push((col::ACCOUNT_ID, Encoding::Dictionary, encode_dict_strings(&self.account_id)?));
        column_chunks.push((col::SUBSCRIPTION_ID, Encoding::Dictionary, encode_dict_option_strings(&self.subscription_id)?));
        column_chunks.push((col::PRODUCT_ID, Encoding::Dictionary, encode_dict_strings(&self.product_id)?));
        column_chunks.push((col::METER_ID, Encoding::Dictionary, encode_dict_strings(&self.meter_id)?));

        // Timestamps are near-monotonic — delta encoding turns large
        // i64 values into small differences (often <1s = <1000 ms).
        column_chunks.push((col::TIMESTAMP_MS, Encoding::Delta, encode_delta_i64(&self.timestamp_ms)?));

        // i128 quantities — zigzag-varint packs small positive/negative
        // values into 1-2 bytes instead of 16.
        column_chunks.push((col::QUANTITY, Encoding::Zigzag, encode_zigzag_varint_i128(&self.quantity)));

        column_chunks.push((col::UNIT, Encoding::Dictionary, encode_dict_strings(&self.unit)?));
        column_chunks.push((col::SOURCE, Encoding::Dictionary, encode_dict_strings(&self.source)?));
        column_chunks.push((col::MODEL_ID, Encoding::Dictionary, encode_dict_option_strings(&self.model_id)?));

        // dimensions is a BTreeMap per row; bincode + zstd handles the
        // structural overhead well enough. (A future Dictionary on the
        // canonical-JSON form could compound — left as TODO.)
        column_chunks.push((col::DIMENSIONS, Encoding::Plain, ser(&self.dimensions)?));

        column_chunks.push((col::INGESTED_AT_MS, Encoding::Delta, encode_delta_i64(&self.ingested_at_ms)?));

        // Compress each encoded column independently with zstd.
        let mut compressed_chunks: Vec<(&'static str, Encoding, Vec<u8>)> = Vec::with_capacity(column_chunks.len());
        for (name, encoding, raw) in column_chunks {
            let compressed = compress(&raw, CompressionCodec::Zstd)?;
            compressed_chunks.push((name, encoding, compressed));
        }

        // Assemble the file into a Vec<u8> first so we can compute a
        // single checksum over the body.
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&row_count.to_le_bytes());
        let num_columns = compressed_chunks.len() as u16;
        body.extend_from_slice(&num_columns.to_le_bytes());

        for (name, encoding, compressed) in &compressed_chunks {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            body.extend_from_slice(&name_len.to_le_bytes());
            body.extend_from_slice(name_bytes);
            body.push(*encoding as u8);
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

/// Build a dictionary of unique strings + a `Vec<u32>` of indices into it.
/// First occurrence wins for the index ordering (so the dict order is
/// insertion order, which makes the data deterministic per writer call).
fn encode_dict_strings(values: &[String]) -> IoResult<Vec<u8>> {
    let mut dict: Vec<String> = Vec::new();
    let mut index_of: HashMap<String, u32> = HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    for v in values {
        if let Some(&i) = index_of.get(v) {
            indices.push(i);
        } else {
            let i = dict.len() as u32;
            dict.push(v.clone());
            index_of.insert(v.clone(), i);
            indices.push(i);
        }
    }
    ser(&(dict, indices))
}

/// Dictionary variant for nullable strings: `None` → no dictionary entry,
/// represented as `None` in the index column. `Some(s)` → standard
/// dictionary lookup.
fn encode_dict_option_strings(values: &[Option<String>]) -> IoResult<Vec<u8>> {
    let mut dict: Vec<String> = Vec::new();
    let mut index_of: HashMap<String, u32> = HashMap::new();
    let mut indices: Vec<Option<u32>> = Vec::with_capacity(values.len());
    for v in values {
        match v {
            None => indices.push(None),
            Some(s) => {
                let i = if let Some(&i) = index_of.get(s) {
                    i
                } else {
                    let i = dict.len() as u32;
                    dict.push(s.clone());
                    index_of.insert(s.clone(), i);
                    i
                };
                indices.push(Some(i));
            }
        }
    }
    ser(&(dict, indices))
}

/// Delta-encode an `i64` column. Stores `values[0]` then successive
/// differences. The reader reconstructs by running-sum.
fn encode_delta_i64(values: &[i64]) -> IoResult<Vec<u8>> {
    let mut deltas: Vec<i64> = Vec::with_capacity(values.len());
    let mut prev = 0i64;
    for &v in values {
        deltas.push(v.wrapping_sub(prev));
        prev = v;
    }
    ser(&deltas)
}

/// Zigzag-encode each `i128` to `u128`, then varint-encode the unsigned
/// stream. Payload layout: `u32 LE count` + concatenated varints.
/// `count` is needed because varint values are variable-width and the
/// decoder otherwise can't tell where one stops if the buffer is
/// over-allocated.
fn encode_zigzag_varint_i128(values: &[i128]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + values.len() * 2);
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for &v in values {
        let mut x = zigzag_i128(v);
        loop {
            if x < 128 {
                out.push(x as u8);
                break;
            }
            out.push(((x & 0x7F) as u8) | 0x80);
            x >>= 7;
        }
    }
    out
}

/// Zigzag for i128: signed → unsigned mapping where `-1 → 1`, `1 → 2`,
/// `-2 → 3`, etc. Small absolute values get small unsigned magnitudes,
/// which then varint-encode tightly.
fn zigzag_i128(n: i128) -> u128 {
    ((n << 1) ^ (n >> 127)) as u128
}
