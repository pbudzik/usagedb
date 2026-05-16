# usageDb

`usageDb` is a high-performance, embedded, append-only usage database built in Rust, optimized specifically for AI billing workloads and high-throughput metric ingestion.

## Overview

Unlike general-purpose SQL databases or time-series engines, `usageDb` is explicitly designed around the invariants of **billing and usage accounting**:
- **Idempotency:** Every event has a stable `event_id` and same-payload duplicates are strictly ignored.
- **Immutability:** Raw segment storage is immutable to guarantee auditability for invoices.
- **Rollups:** Aggregates are computed hourly to allow ultra-fast invoice queries while maintaining the raw data trail.
- **High Performance:** Employs Write-Ahead Logs (WAL), dictionary encoding, Zigzag quantity encoding, and Zstd/Lz4 compression for optimal performance.

## Architecture & Milestones

The project is structured around 5 major milestones for the MVP:

1. **Append-Only Raw Store** (`src/ingest`, `src/storage`): Durable append-only WAL and flushable immutable segments.
2. **Columnar Encoding & Skipping** (`src/storage/encoding.rs`, `compression.rs`): Dictionary encoding for strings, delta timestamps, and block metadata pruning.
3. **Hourly Rollups** (`src/rollup`): Real-time builder for rollups and watermark tracking for querying aggregated invoice data.
4. **Dedupe and Corrections** (`src/ingest/dedupe.rs`): Hot `event_id` deduplication and payload conflict detection.
5. **Compaction** (`src/compact`): Background merging and deduplicating of micro-segments into larger columnar blocks.

## Setup & Building

To build the project locally, ensure you have Rust installed and run:

```bash
cargo build
```

To run the tests (once implemented):

```bash
cargo test
```

## Data Model

Events are represented as `UsageEvent` and contain essential AI tracking fields:
- `account_id`, `product_id`, `meter_id`, `model_id`
- `timestamp_ms`, `quantity`, `unit`
- Custom `SmallDimensions` for dynamic filtering.

## Contributing

Contributions are welcome. Please ensure that all invariants (as specified in the design spec) hold before opening pull requests!
