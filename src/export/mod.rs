//! Export raw usage events to open analytical formats (Phase D).
//!
//! Currently supports Apache Parquet (`parquet` submodule). The reviewer's
//! framing was "internal format optimized for billing; external format
//! Parquet for warehouse/BI/debug" — this module is the bridge.

pub mod parquet;
