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
| `src/rollup/` | Hourly rollup builder + background scheduler |
| `src/compact/` | Compaction planner + worker + background scheduler |
| `src/runtime/` | Config, app state, startup recovery |
| `src/model/` | Event schema, IDs, dimensions |

## HTTP API

Spec-aligned routes (§9.1, §12.2, §12.3):

```
POST /v1/usage/batch                            { "events": [UsageEvent, ...] }
GET  /v1/accounts/{account_id}/usage            ?from&to&group_by&product_id&meter_id&model_id&source
GET  /v1/accounts/{account_id}/usage/events     ?from&to&meter_id&product_id
GET  /v1/accounts/{account_id}/explain          ?from&to       — breakdown + segment provenance + corrections
GET  /v1/accounts/{account_id}/verify           ?from&to       — raw-vs-rollup drift check
GET  /v1/accounts/{account_id}/periods/{YYYY-MM}                       — state + total
POST /v1/accounts/{account_id}/periods/{YYYY-MM}/close                 — mark closed
POST /v1/accounts/{account_id}/periods/{YYYY-MM}/reopen                — mark open
POST /v1/query/json                             { "source", "account_id", "from", "to", "group_by", "filters", "metrics" }
POST /v1/query/sql                              { "query": "SELECT meter_id, SUM(quantity) FROM usage_events WHERE account_id = '...' GROUP BY meter_id" }
GET  /health
```

`from` / `to` are RFC 3339. Supported `metrics`: `sum`, `count`. Supported group keys: column names (`account_id`, `product_id`, `meter_id`, `model_id`, `source`, `unit`), `hour_start_ms`, `day`, or any dimension key. The account-usage GET defaults `source=rollup` for fast monthly totals; pass `source=raw` to force a raw scan.

Ingest response counts `accepted`, `duplicates` (same id + same payload), `conflicts` (same id + different payload — surfaces silent collector bugs), and `rejected` (validation failures: missing required IDs, non-positive timestamp, >16 dimensions, Correction/Retraction without `correction_ref`, or `Usage` event landing in a closed period).

## Building and running

```bash
cargo build
cargo test
cargo run                   # HTTP server on 127.0.0.1:8080 (default)
cargo run -- --help         # see admin subcommands
```

Configuration is currently hardcoded in `Config::default()` (db_root `./data`, 64 MiB memtable, 1M dedupe entries). The `--db-root` flag overrides the data directory.

## Admin CLI

The `usagedb` binary doubles as an admin tool. Subcommands operate on the on-disk state without needing the HTTP server running:

```
usagedb serve                                                 # HTTP server (default)
usagedb check [--deep]                                        # manifest summary; --deep verifies every segment
usagedb rebuild-rollups --from <RFC3339> --to <RFC3339>      # drop rollups + rewind watermark
usagedb inspect-segment <segment_id>                          # metadata + sample rows
usagedb verify-period --account <id> --from <RFC3339> --to <RFC3339>   # raw vs rollup drift
usagedb export-parquet <output.parquet>                       # dump every raw segment to Parquet
```

All commands accept `--db-root <path>` (default `./data`). Admin commands assume the server is **not** running concurrently — file locking to prevent that is on the backlog.

## Durability contract

The ingest path runs three phases under a single critical section:

1. **Validate + classify.** Every event is validated (non-empty IDs, positive timestamp, dimension cap, correction-ref-when-needed) and stamped with a server-side `ingested_at_ms`. Surviving events are classified against the dedupe cache without mutating it.
2. **Append + sync.** The WAL is a `BufWriter<File>`; `append_batch` writes through the userspace buffer, and the durability mode controls the next step:
   - `Strict` (default): `flush` + `fsync` before acking — billing-safe.
   - `Fast`: `flush` only — bytes hit the page cache but no disk round-trip; acceptable for at-least-once upstream retry pipelines.
3. **Commit** dedupe entries and insert into the memtable.

If the flusher fails to write a segment or commit the manifest, the drained events are **re-inserted into the memtable** so they get retried on the next flush trigger — they don't sit invisibly in a sealed WAL file until the next process restart.

If step 2 fails, no dedupe state is mutated — client retries do not see false duplicates. The WAL is split across numbered files under `wal/`; on memtable overflow the active file is sealed and a new one is opened, then the flusher writes a segment, persists `last_sealed_wal_id` in the manifest, and deletes WAL files up to that id. Recovery cleans up stragglers, replays any unsealed files into both the dedupe cache and the memtable, **and** scans raw segments within the dedupe TTL window (7 days) to re-register their events — so retries across restart of previously-committed events are detected as duplicates, not accepted as new.

The rollup worker advances the watermark under three safety bounds: (a) `time_target = floor((now - safety_lag) / 1h) * 1h`; (b) skip the tick if a flush is in flight (a sealed WAL file hasn't yet been committed to a raw segment); (c) cap by the floor-of-hour of the oldest event still in the memtable — the watermark never crosses unflushed data. If the memtable holds events older than `memtable_max_age_ms`, the rollup worker force-drains it so the watermark can resume.

## Status

What works end-to-end:

- Durable batch ingest with idempotent dedupe (blake3 128-bit hash, 7-day TTL)
- WAL rotation, sealing, and crash recovery (memtable rebuilt from unsealed files)
- Atomic manifest updates (tmp + rename + parent dir fsync)
- Background memtable flush → immutable columnar raw segments, partitioned by `bucket = blake3(account_id) % bucket_count`, written in canonical billing order `(account, product, meter, model, ts)` (sort-on-flush) so compaction is cheaper and dictionary encoding compresses better
- Exclusive process lock (`db_root/LOCK` via `flock`) — prevents server + admin commands from racing on the same database
- Columnar on-disk format with per-column zstd compression and blake3 checksum (see [Segment format](#segment-format))
- Background hourly rollup scheduler — seals completed hours into per-bucket rollup segments, advances the manifest watermark atomically, query path routes `RollupHourly` through rollups with raw fallback for the open-period tail
- Background compaction scheduler — merges small per-bucket segments into a single output, applies the `ReplacementRecord` to the manifest, deletes old files after a configurable reader grace period (spec §15.3)
- Query executor: raw segments + memtable, filters, group-by, Sum/Count; pruning uses `bucket(account_id)`, `min_account_id`/`max_account_id`, and per-segment `product_ids`/`meter_ids`/`model_ids` from `SegmentMeta` before opening segment files
- Half-open `[from, to)` time-range semantics across all query paths so adjacent month queries don't double-count boundary events
- SQL subset parser is strict — `SUM` accepts only `quantity`, `COUNT` accepts only `*`, ranges distinguish `<`/`<=` and `>`/`>=`, `OR`/`HAVING`/aliases/`SELECT *` are rejected explicitly rather than silently mapped
- Manifest generations under `manifest/` (`CURRENT` + `manifest-NNNNNN.json`) with auto-migration from a legacy single-file `manifest.json`. Recovery rolls back to the previous valid generation if the current one is corrupt; falls fully closed only if no generation parses. The last 10 generations are retained.
- Operator-facing `RollupWorker::rebuild_rollups(from_ms, to_ms)` — drops rollup segments overlapping the range, rewinds the watermark to `from_ms`, lets the next tick refill the gap from raw events. Used when a rollup bug is fixed, late data arrives for a sealed hour, or to verify rollup-vs-raw drift.
- Correction / Retraction events work end-to-end: validation requires `correction_ref`, the rollup builder treats negative-quantity corrections as net adjustments (so SUM = original + correction), and queries can filter or group by `kind` to isolate adjustments for forensics.
- Shutdown drain — `ctrl+c` flushes the memtable before exiting
- proptest property tests for spec §19 invariants (`tests/properties.rs`): raw=rollup totals, dedupe idempotence under retry, compaction-preserves-sum, recovery-preserves-sum (with and without prior flush), rollup-tick idempotence, payload-conflict detection — 32 randomized cases per property, regenerated on every CI run

Manifest layout (Phase A — generations):

```
db_root/
  manifest/
    CURRENT                          (single line: latest valid generation u64)
    manifest-000001.json
    manifest-000002.json             (last KEEP_GENERATIONS = 10 retained)
    ...
  manifest.json                       (legacy; auto-migrated to generation 1 on first load)
```

Known gaps (tracked against `rust_ai_usage_db_spec.md`):

- No block-level metadata for fine-grained skipping inside a segment; pruning is segment-level only.
- Rollup segments still use length-prefixed bincode (not the columnar format) — they're tiny so it hasn't been a win yet
- COUNT semantics differ for `RollupHourly` queries: each rollup row counts as 1, not as the number of underlying events. Use `RawEvents` source for exact event counts.
- `DurabilityMode::Balanced` (group commit, spec §9.3) is not yet implemented — only `Strict` and `Fast`.

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

Each column payload before compression is encoded based on its declared `encoding` byte, then zstd-compressed:

| Encoding | Layout | Used for |
| --- | --- | --- |
| `Plain` (0) | bincode-serialized `Vec<T>` | `event_id`, `correction_ref`, `dimensions` |
| `Dictionary` (1) | bincode `(Vec<String>, Vec<u32>)` — unique values + index per row | `account_id`, `product_id`, `meter_id`, `model_id`, `source`, `unit`, `subscription_id` |
| `Delta` (2) | bincode `Vec<i64>` of running differences | `timestamp_ms`, `ingested_at_ms` |
| `Zigzag` (3) | `u32 count` + concatenated zigzag-varints | `quantity` |
| `Rle` (4) | bincode `Vec<(u8, u32)>` of (value, run_length) | `kind` |

Dictionary collapses ID columns from O(rows × string size) to O(unique values × string size + 4 bytes/row); for ID-heavy workloads this is a 1000× shrink on the column. Delta encoding turns near-monotonic timestamps into small differences that zstd compresses dramatically better. Zigzag-varint packs small i128 quantities into 1–2 bytes instead of 16.

The reader is permissive about per-column encoding choices (the writer is allowed to change its mind), so future encoder improvements don't break older segments. Adding a column is additive and old readers fail loud (missing column → corrupt segment). The checksum is verified on every open.

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
