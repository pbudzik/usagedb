//! Columnar segment file format (spec §6.1).
//!
//! ```text
//!   header:
//!     magic:        b"UDBRAW1\n"  (8 bytes)
//!     version:      u8 = 1
//!     row_count:    u32 LE
//!     num_columns:  u16 LE
//!
//!   per column (repeated num_columns times):
//!     name_len:         u16 LE
//!     name:             utf-8 bytes
//!     encoding:         u8 (0 = Plain — bincode-serialized Vec<T>)
//!     codec:            u8 (0 = None, 1 = Zstd, 2 = Lz4)
//!     compressed_len:   u32 LE
//!     compressed_bytes: bytes
//!
//!   footer:
//!     checksum:   u64 LE  (low 8 bytes of blake3 over everything above)
//!     magic_end:  b"UDBEND01"  (8 bytes)
//! ```
//!
//! Each column payload before compression is a bincode-serialized `Vec<T>`
//! where T is the column's native type. zstd is applied to the entire
//! per-column byte buffer. This gives us:
//!  - homogeneous data co-located on disk (so zstd's history window sees
//!    repeating patterns and compresses well),
//!  - the ability to add dictionary/delta/zigzag encodings later by
//!    introducing new `Encoding` enum values without breaking the format,
//!  - per-column reads (a future query optimization — current reader
//!    decompresses all columns into memory).
//!
//! Validation on read: file must start with the magic, end with the end
//! magic, and the stored checksum must match. Any deviation is treated as
//! corruption.

pub const MAGIC: &[u8; 8] = b"UDBRAW1\n";
pub const MAGIC_END: &[u8; 8] = b"UDBEND01";
pub const VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// bincode-serialized `Vec<T>` of the column's native type.
    Plain = 0,
}

impl Encoding {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Plain),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl Codec {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            2 => Some(Self::Lz4),
            _ => None,
        }
    }
}

/// Canonical column names. Kept here so writer + reader agree exactly.
pub mod col {
    pub const EVENT_ID: &str = "event_id";
    pub const KIND: &str = "kind";
    pub const CORRECTION_REF: &str = "correction_ref";
    pub const ACCOUNT_ID: &str = "account_id";
    pub const SUBSCRIPTION_ID: &str = "subscription_id";
    pub const PRODUCT_ID: &str = "product_id";
    pub const METER_ID: &str = "meter_id";
    pub const TIMESTAMP_MS: &str = "timestamp_ms";
    pub const QUANTITY: &str = "quantity";
    pub const UNIT: &str = "unit";
    pub const SOURCE: &str = "source";
    pub const MODEL_ID: &str = "model_id";
    pub const DIMENSIONS: &str = "dimensions";
    pub const INGESTED_AT_MS: &str = "ingested_at_ms";

    /// Order in which columns are written to the file. Stable so reader
    /// can compare against expectations, though the reader is in fact
    /// order-independent (it builds a name→column map).
    pub const ORDER: &[&str] = &[
        EVENT_ID,
        KIND,
        CORRECTION_REF,
        ACCOUNT_ID,
        SUBSCRIPTION_ID,
        PRODUCT_ID,
        METER_ID,
        TIMESTAMP_MS,
        QUANTITY,
        UNIT,
        SOURCE,
        MODEL_ID,
        DIMENSIONS,
        INGESTED_AT_MS,
    ];
}

/// Compute the segment checksum: low 8 bytes of blake3 over `data`. Stored
/// in the footer and re-verified on read.
pub fn checksum(data: &[u8]) -> u64 {
    let h = blake3::hash(data);
    let bytes = h.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}
