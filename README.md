# usageDb

`usageDb` is a small, embedded, append-only usage database written in Rust, designed for AI billing workloads — token / credit / tool-call metering — where strict idempotency and an immutable audit trail matter more than general-purpose query power. Developed alongside [usagebox.com](https://usagebox.com/).

> **Status:** MVP scaffold. The ingest path is durable end-to-end; the query path is functional; several spec items are still stubbed. See [Status](#status).

## Why a purpose-built DB

Unlike a general SQL database or a time-series engine, the invariants here come from billing and usage accounting:

- **Idempotency.** Every event carries a stable `event_id`. Same-payload retries are duplicates; same-id-with-different-payload is a conflict.
- **Immutability.** Raw segments are written once and never modified — the audit trail backs every invoice line.
- **Cheap account-month totals.** Hourly rollups are the planned fast path; raw scans are the correctness fallback.
- **Recoverable writes.** Every acknowledged event is durable in the WAL or a committed segment.

## Architecture

```
clients
   |
   v
HTTP ingest  ->  hot dedupe  ->  WAL (numbered files, fsynced)
                                  |
                                  v
                              memtable
                                  |
              (size threshold)    v
                       raw segment writer
                                  |
                                  v
                       manifest.json (atomic rename)
                                  |
                       WAL files <= sealed_id deleted

queries: scan raw segments (timestamp-pruned via SegmentMeta)
         + memtable snapshot, filter / group / aggregate.
```

Module map:

| Path | Role |
| --- | --- |
| `src/api/` | Axum HTTP server, request/response types |
| `src/ingest/` | WAL, memtable, hot dedupe, flusher worker |
| `src/storage/` | Manifest, segment writer/reader, encoding helpers |
| `src/query/` | SQL subset parser, plan, executor |
| `src/rollup/` | Hourly rollup builder (not yet scheduled) |
| `src/compact/` | Compaction planner + worker (not yet scheduled) |
| `src/runtime/` | Config, app state, startup recovery |
| `src/model/` | Event schema, IDs, dimensions |

## HTTP API

```
POST /v1/ingest      { "events": [UsageEvent, ...] }
POST /v1/query/json  { "source", "account_id", "from", "to", "group_by", "filters", "metrics" }
POST /v1/query/sql   { "query": "SELECT meter_id, SUM(quantity) FROM usage_events WHERE account_id = '...' GROUP BY meter_id" }
GET  /health
```

`from` / `to` are RFC 3339. Supported `metrics`: `sum`, `count`. Supported group keys: column names (`account_id`, `product_id`, `meter_id`, `model_id`, `source`, `unit`), `hour_start_ms`, `day`, or any dimension key.

## Building and running

```bash
cargo build
cargo test
cargo run     # HTTP server on 127.0.0.1:8080
```

Configuration is currently hardcoded in `Config::default()` (db_root `./data`, 64 MiB memtable, 1M dedupe entries).

## Durability contract

The ingest path runs three phases under a single critical section:

1. **Classify** every event against the dedupe cache without mutating it.
2. **Append + fsync** the WAL for events classified as new.
3. **Commit** dedupe entries and insert into the memtable.

If step 2 fails, no dedupe state is mutated — client retries do not see false duplicates. The WAL is split across numbered files under `wal/`; on memtable overflow the active file is sealed and a new one is opened, then the flusher writes a segment, persists `last_sealed_wal_id` in the manifest, and deletes WAL files up to that id. Recovery cleans up stragglers and replays any unsealed files into both the dedupe cache and the memtable, so unflushed events survive a crash.

## Status

What works end-to-end:

- Durable batch ingest with idempotent dedupe (blake3 128-bit hash, 7-day TTL)
- WAL rotation, sealing, and crash recovery (memtable rebuilt from unsealed files)
- Atomic manifest updates (tmp + rename + parent dir fsync)
- Background memtable flush → immutable columnar raw segments, partitioned by `bucket = blake3(account_id) % bucket_count`
- Columnar on-disk format with per-column zstd compression and blake3 checksum (see [Segment format](#segment-format))
- Query executor: raw segments + memtable, filters, group-by, Sum/Count
- SQL subset parser (`SELECT … FROM usage_events|usage_rollup_hourly WHERE … GROUP BY …`)
- Compaction worker (read / sort / cold-dedupe / write) — implemented, not yet scheduled

Known gaps (tracked against `rust_ai_usage_db_spec.md`):

- No per-column dictionary / delta / RLE encodings yet — only `Plain` (bincode + zstd). The format reserves `Encoding` discriminants for these so they can be added without breaking compatibility.
- No block-level metadata for fine-grained skipping inside a segment; pruning is segment-level only.
- Hourly rollup builder exists but has no background scheduler — rollup queries fall back to scanning raw events
- Compaction worker has no scheduler
- Validation is permissive — `rejected` in the ingest response is always 0
- No configurable durability mode — strict per-batch fsync only

## Segment format

`.seg` files use a custom columnar layout (`src/storage/segment_format.rs`):

```
header:
  magic        b"UDBRAW1\n"  (8 bytes)
  version      u8 = 1
  row_count    u32 LE
  num_columns  u16 LE

per column (×14):
  name_len         u16 LE
  name             utf-8 bytes
  encoding         u8  (0 = Plain)
  codec            u8  (0 = None, 1 = Zstd, 2 = Lz4)
  compressed_len   u32 LE
  compressed_bytes bytes

footer:
  checksum   u64 LE  (low 8 bytes of blake3 over everything above)
  magic_end  b"UDBEND01"  (8 bytes)
```

Each column payload before compression is a bincode-serialized `Vec<T>` of the column's native type, so adding a new column is additive and old readers fail loud (missing column → corrupt segment). The checksum is verified on every open.

## Data model

`UsageEvent` carries:

- **Identity:** `event_id`, `kind` (`Usage` / `Correction` / `Retraction`), optional `correction_ref`
- **Subject:** `account_id`, optional `subscription_id`
- **Categorization:** `product_id`, `meter_id`, optional `model_id`, `source`
- **Measurement:** `timestamp_ms`, `quantity` (`i128`), `unit`
- **Variable axis:** `dimensions` (`BTreeMap<String, String>`, canonicalized for stable hashing)
- **Provenance:** `ingested_at_ms`

## Spec

The design spec is `rust_ai_usage_db_spec.md`; §19 lists the invariants the database is meant to preserve.

## Contributing

Open a PR. Run `cargo test` first — `tests/durability.rs` covers the WAL/recovery contract.
