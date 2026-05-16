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
/// against magic + checksum + end magic, and each column is decoded
/// (dispatching by its per-column encoding header) into memory.
/// `read_next()` then yields events row-by-row.
///
/// Future optimization: parse only the column headers up front and lazily
/// decode columns the caller actually projects.
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

        const HEADER_LEN: usize = MAGIC.len() + 1 + 4 + 2;
        const FOOTER_LEN: usize = 8 + MAGIC_END.len();
        if bytes.len() < HEADER_LEN + FOOTER_LEN {
            return Err(corrupt("file too small to be a segment"));
        }

        let end_magic_off = bytes.len() - MAGIC_END.len();
        if &bytes[end_magic_off..] != MAGIC_END {
            return Err(corrupt("missing end magic"));
        }

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

        if &body[..MAGIC.len()] != MAGIC {
            return Err(corrupt("missing start magic"));
        }
        let mut off = MAGIC.len();

        let version = body[off];
        off += 1;
        if version != VERSION {
            return Err(corrupt(&format!("unsupported segment version {}", version)));
        }

        let row_count = read_u32(body, off)?;
        off += 4;
        let num_columns = read_u16(body, off)? as usize;
        off += 2;

        // Walk columns. Each entry in the map is (encoding, decompressed bytes).
        let mut chunks: HashMap<String, (Encoding, Vec<u8>)> = HashMap::with_capacity(num_columns);
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

            let encoding_byte = body[off];
            off += 1;
            let codec_byte = body[off];
            off += 1;
            let encoding = Encoding::from_byte(encoding_byte)
                .ok_or_else(|| corrupt(&format!("unknown encoding {} for column {}", encoding_byte, name)))?;
            let codec = Codec::from_byte(codec_byte)
                .ok_or_else(|| corrupt(&format!("unknown codec {} for column {}", codec_byte, name)))?;

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
            chunks.insert(name, (encoding, raw));
        }

        // Decode each required column. Dispatch by encoding.
        let event_id: Vec<String> = decode_strings(&mut chunks, col::EVENT_ID)?;
        let kind: Vec<u8> = decode_u8(&mut chunks, col::KIND)?;
        let correction_ref: Vec<Option<CorrectionRef>> = decode_plain(&mut chunks, col::CORRECTION_REF)?;
        let account_id: Vec<String> = decode_strings(&mut chunks, col::ACCOUNT_ID)?;
        let subscription_id: Vec<Option<String>> = decode_option_strings(&mut chunks, col::SUBSCRIPTION_ID)?;
        let product_id: Vec<String> = decode_strings(&mut chunks, col::PRODUCT_ID)?;
        let meter_id: Vec<String> = decode_strings(&mut chunks, col::METER_ID)?;
        let timestamp_ms: Vec<i64> = decode_i64(&mut chunks, col::TIMESTAMP_MS)?;
        let quantity: Vec<i128> = decode_i128(&mut chunks, col::QUANTITY)?;
        let unit: Vec<String> = decode_strings(&mut chunks, col::UNIT)?;
        let source: Vec<String> = decode_strings(&mut chunks, col::SOURCE)?;
        let model_id: Vec<Option<String>> = decode_option_strings(&mut chunks, col::MODEL_ID)?;
        let dimensions: Vec<SmallDimensions> = decode_plain(&mut chunks, col::DIMENSIONS)?;
        let ingested_at_ms: Vec<i64> = decode_i64(&mut chunks, col::INGESTED_AT_MS)?;

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

fn take(map: &mut HashMap<String, (Encoding, Vec<u8>)>, name: &str) -> IoResult<(Encoding, Vec<u8>)> {
    map.remove(name).ok_or_else(|| corrupt(&format!("missing column {}", name)))
}

/// Decode a column that the writer chose Plain for. Errors if the on-disk
/// encoding is something else — writers are supposed to be stable about
/// per-column choices for a given version.
fn decode_plain<T: serde::de::DeserializeOwned>(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<T>> {
    let (encoding, bytes) = take(map, name)?;
    if encoding != Encoding::Plain {
        return Err(corrupt(&format!(
            "column {} expected Plain encoding, got {:?}",
            name, encoding
        )));
    }
    de(&bytes)
}

/// Decode a string column (`Vec<String>`). Accepts Plain or Dictionary —
/// the reader is permissive here so a future writer change isn't a
/// format-breaking event.
fn decode_strings(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<String>> {
    let (encoding, bytes) = take(map, name)?;
    match encoding {
        Encoding::Plain => de(&bytes),
        Encoding::Dictionary => {
            let (dict, indices): (Vec<String>, Vec<u32>) = de(&bytes)?;
            indices
                .into_iter()
                .map(|i| {
                    dict.get(i as usize).cloned().ok_or_else(|| {
                        corrupt(&format!("dictionary index {} out of bounds for column {}", i, name))
                    })
                })
                .collect()
        }
        _ => Err(corrupt(&format!(
            "column {} string-typed but on-disk encoding is {:?}",
            name, encoding
        ))),
    }
}

/// Decode a nullable-string column (`Vec<Option<String>>`).
fn decode_option_strings(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<Option<String>>> {
    let (encoding, bytes) = take(map, name)?;
    match encoding {
        Encoding::Plain => de(&bytes),
        Encoding::Dictionary => {
            let (dict, indices): (Vec<String>, Vec<Option<u32>>) = de(&bytes)?;
            indices
                .into_iter()
                .map(|opt| match opt {
                    None => Ok(None),
                    Some(i) => dict.get(i as usize).cloned().map(Some).ok_or_else(|| {
                        corrupt(&format!(
                            "dictionary index {} out of bounds for column {}",
                            i, name
                        ))
                    }),
                })
                .collect()
        }
        _ => Err(corrupt(&format!(
            "column {} option-string-typed but on-disk encoding is {:?}",
            name, encoding
        ))),
    }
}

/// Decode an i64 column (timestamps). Accepts Plain or Delta.
fn decode_i64(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<i64>> {
    let (encoding, bytes) = take(map, name)?;
    match encoding {
        Encoding::Plain => de(&bytes),
        Encoding::Delta => {
            let deltas: Vec<i64> = de(&bytes)?;
            let mut values = Vec::with_capacity(deltas.len());
            let mut prev = 0i64;
            for d in deltas {
                let v = prev.wrapping_add(d);
                values.push(v);
                prev = v;
            }
            Ok(values)
        }
        _ => Err(corrupt(&format!(
            "column {} i64-typed but on-disk encoding is {:?}",
            name, encoding
        ))),
    }
}

/// Decode an i128 column (quantity). Accepts Plain or Zigzag-varint.
fn decode_i128(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<i128>> {
    let (encoding, bytes) = take(map, name)?;
    match encoding {
        Encoding::Plain => de(&bytes),
        Encoding::Zigzag => decode_zigzag_varint(&bytes, name),
        _ => Err(corrupt(&format!(
            "column {} i128-typed but on-disk encoding is {:?}",
            name, encoding
        ))),
    }
}

/// Decode a `Vec<u8>` column. Accepts Plain or Rle — the writer chose
/// Rle for `kind` after Phase B, but Plain segments from before that
/// still load.
fn decode_u8(
    map: &mut HashMap<String, (Encoding, Vec<u8>)>,
    name: &str,
) -> IoResult<Vec<u8>> {
    let (encoding, bytes) = take(map, name)?;
    match encoding {
        Encoding::Plain => de(&bytes),
        Encoding::Rle => decode_rle_u8(&bytes, name),
        _ => Err(corrupt(&format!(
            "column {} u8-typed but on-disk encoding is {:?}",
            name, encoding
        ))),
    }
}

/// Expand RLE-encoded (value, run_length) pairs back to a flat Vec.
fn decode_rle_u8(bytes: &[u8], name: &str) -> IoResult<Vec<u8>> {
    let runs: Vec<(u8, u32)> = de(bytes)?;
    let total: usize = runs.iter().map(|(_, c)| *c as usize).sum();
    let mut out = Vec::with_capacity(total);
    for (v, c) in runs {
        // Guard against pathological run counts that would OOM us.
        if out.len() + c as usize > 10_000_000_000 {
            return Err(corrupt(&format!(
                "column {} RLE run count overflows reasonable bound",
                name
            )));
        }
        for _ in 0..c {
            out.push(v);
        }
    }
    Ok(out)
}

fn decode_zigzag_varint(bytes: &[u8], name: &str) -> IoResult<Vec<i128>> {
    if bytes.len() < 4 {
        return Err(corrupt(&format!("column {}: zigzag payload missing count", name)));
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut i = 4;
    for _ in 0..count {
        let mut x: u128 = 0;
        let mut shift = 0u32;
        loop {
            if i >= bytes.len() {
                return Err(corrupt(&format!("column {}: zigzag varint truncated", name)));
            }
            let b = bytes[i];
            i += 1;
            x |= ((b & 0x7F) as u128) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 128 {
                return Err(corrupt(&format!("column {}: zigzag varint overflow", name)));
            }
        }
        // Zigzag-decode: u128 → i128.
        let signed = ((x >> 1) as i128) ^ (-((x & 1) as i128));
        out.push(signed);
    }
    Ok(out)
}

fn de<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> IoResult<T> {
    bincode::deserialize(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn corrupt(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("corrupt segment: {}", msg))
}
