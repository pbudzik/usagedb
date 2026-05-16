# usageDb — Rust AI Usage Database Implementation Spec

## 1. Purpose

Build a small, purpose-built embedded/server-side usage database optimized for AI billing workloads:

- Massive append-only writes
- AI token, credit, request, and tool-call usage
- Strong idempotency
- Immutable raw event audit trail
- Compressed columnar storage
- Fast account/month usage retrieval
- Rollups for billing/invoicing
- Simple analytical queries, not full SQL

This is not a graph database, document database, or general-purpose OLAP system.

---

## 2. Core assumptions

Typical usage events:

- LLM input tokens
- LLM output tokens
- cached input tokens
- embedding tokens
- image generation credits
- tool calls
- agent runtime seconds
- model-specific credits
- custom metered features

Typical queries:

- Monthly usage for one account
- Usage by product/meter/model/day
- Invoice line generation
- Raw event audit for an invoice line
- Late correction handling
- Top accounts by usage for a period
- Rebuild rollups from raw events

Non-goals for v1:

- General SQL support
- Arbitrary joins
- Complex JSON querying
- Distributed consensus
- Mutable row updates
- Graph traversal
- Multi-table relational modeling

---

## 3. High-level architecture

```text
clients / collectors
        |
        v
+-------------------+
| ingest API         |
| batch validation   |
+---------+---------+
          |
          v
+-------------------+
| WAL               |
| durable append log |
+---------+---------+
          |
          v
+-------------------+
| memtable / buffer  |
| column builders    |
+---------+---------+
          |
          v
+-----------------------------+
| immutable raw segments       |
| sorted + compressed columns  |
+--------------+--------------+
               |
               v
+-----------------------------+
| background workers           |
| compaction + rollup builder  |
+--------------+--------------+
               |
               v
+-----------------------------+
| immutable rollup segments    |
| billing query fast path      |
+-----------------------------+
```

---

## 4. Data model

### 4.1 Raw usage event

Canonical event schema:

```rust
pub struct UsageEvent {
    pub event_id: EventId,
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub timestamp_ms: i64,
    pub quantity: i128,
    pub unit: Unit,
    pub source: SourceId,
    pub model_id: Option<ModelId>,
    pub dimensions: SmallDimensions,
    pub ingested_at_ms: i64,
}
```

### 4.2 AI-specific meters

Recommended base meters:

```text
tokens.input
tokens.output
tokens.cached_input
tokens.reasoning
tokens.embedding
requests.llm
requests.embedding
requests.image
tool.calls
agent.runtime_ms
credits.ai
```

Example event:

```json
{
  "event_id": "evt_01J...",
  "account_id": "acc_123",
  "subscription_id": "sub_456",
  "product_id": "ai_gateway",
  "meter_id": "tokens.input",
  "timestamp_ms": 1778954400123,
  "quantity": 1240,
  "unit": "token",
  "source": "agentcore_gateway",
  "model_id": "anthropic.claude-sonnet-4",
  "dimensions": {
    "provider": "anthropic",
    "agent": "support-agent",
    "tool": "search_customer"
  }
}
```

### 4.3 Event ID / idempotency

Every write must have a stable idempotency key.

Preferred:

```text
event_id = source + source_event_id
```

Fallback:

```text
event_id = hash(account_id, source, source_event_id, meter_id, timestamp_ms)
```

Rules:

- Same `event_id` with same payload: duplicate, ignore.
- Same `event_id` with different payload: conflict, reject or quarantine.
- Retry must never double-bill.

---

## 5. Storage layout

### 5.1 Directory layout

```text
/db_root/
  manifest.json
  wal/
    wal-000001.log
    wal-000002.log
  raw/
    date=2026-05-16/
      bucket=042/
        part-000001.useg
        part-000002.useg
  rollup_hourly/
    date=2026-05-16/
      bucket=042/
        part-000001.rseg
  tmp/
  compacted/
```

### 5.2 Partitioning

Default:

```text
partition = UTC day from timestamp_ms
bucket = hash(account_id) % bucket_count
sort = account_id, product_id, meter_id, model_id, timestamp_ms
```

Default bucket count:

```text
small install: 64
medium install: 256
large install: 512 or 1024
```

The bucket count is fixed per database generation.

### 5.3 Segment immutability

A segment is written once and never modified.

Compaction creates replacement segments and atomically updates the manifest.

Old segments are deleted only after:

- New compacted segment is fully written and fsynced
- Manifest is updated and fsynced
- No active readers reference old segments

---

## 6. Segment file format

Use a custom simple format first. Keep Parquet export as an integration option, not the internal v1 requirement.

### 6.1 Raw segment structure

```text
magic:       "UDBRAW1"
header_len:  u32
header:      json or postcard-encoded metadata
columns:     column chunks
footer_len:  u32
footer:      metadata + checksums
```

### 6.2 Columns

Raw segment columns:

```text
event_id_hash:      u128 or [u64; 2]
event_id_raw:       optional dictionary/string block
account_id:         dictionary-encoded
subscription_id:    dictionary-encoded nullable
product_id:         dictionary-encoded
meter_id:           dictionary-encoded
model_id:           dictionary-encoded nullable
timestamp_ms:       delta encoded i64
quantity:           i128 or scaled decimal/int varint
unit:               dictionary-encoded
source:             dictionary-encoded
dimensions_key:     dictionary-encoded
ingested_at_ms:     delta encoded i64
```

Dimension dictionary:

```text
dimensions_key -> canonical sorted key/value map
```

Canonicalization:

```text
{ "tool": "x", "provider": "y" }
```

must hash the same as:

```text
{ "provider": "y", "tool": "x" }
```

### 6.3 Rollup segment columns

```text
account_id
subscription_id
product_id
meter_id
model_id
hour_start_ms
dimensions_key
quantity_sum
event_count
first_event_ms
last_event_ms
```

Optional later:

```text
cost_estimate_minor_units
currency
pricing_version
```

Cost calculation may be kept outside storage v1. Usage DB should produce usage facts; pricing can be a separate layer.

---

## 7. Encoding and compression

### 7.1 Encoding strategy

Use column-specific encodings:

```text
IDs / strings:       dictionary encoding
timestamps:          delta-of-delta or simple delta + varint
quantities:          zigzag varint or fixed i128 blocks
booleans/enums:      bit-packed / dictionary
repeated values:     run-length encoding where useful
```

### 7.2 Compression

Use block compression:

```text
zstd default level 1-3 for balanced CPU/storage
lz4 optional for very high ingest speed
```

Each column chunk is independently compressed so queries can read only needed columns.

### 7.3 Block size

Recommended logical block size:

```text
raw segment target:      64 MB to 256 MB uncompressed
column block target:      64 KB to 1 MB compressed chunks
rollup segment target:    8 MB to 64 MB uncompressed
```

---

## 8. Metadata and skipping indexes

Each segment stores metadata:

```rust
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub kind: SegmentKind,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub bucket: u32,
    pub row_count: u64,
    pub min_account_id: Option<AccountId>,
    pub max_account_id: Option<AccountId>,
    pub product_ids: SmallSet<ProductId>,
    pub meter_ids: SmallSet<MeterId>,
    pub model_ids: SmallSet<ModelId>,
    pub quantity_sum: Option<i128>,
    pub checksum: u64,
}
```

Block-level metadata:

```rust
pub struct BlockMeta {
    pub row_start: u32,
    pub row_count: u32,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub min_account_id: AccountId,
    pub max_account_id: AccountId,
    pub product_ids: SmallSet<ProductId>,
    pub meter_ids: SmallSet<MeterId>,
    pub offset: u64,
    pub len: u32,
}
```

Optional Bloom filters:

- `event_id_hash` for dedupe/audit
- `account_id` if block min/max is weak
- `dimensions_key` for dimension-heavy queries

---

## 9. Write path

### 9.1 Ingest API

Rust API:

```rust
pub trait UsageWriter {
    fn ingest_batch(&self, batch: Vec<UsageEvent>) -> Result<IngestResult, UsageError>;
}
```

HTTP API:

```text
POST /v1/usage/batch
```

Request:

```json
{
  "events": [ ... ]
}
```

Response:

```json
{
  "accepted": 1000,
  "duplicates": 12,
  "conflicts": 0,
  "rejected": 2
}
```

### 9.2 Write sequence

```text
1. Validate batch shape
2. Canonicalize dimensions
3. Compute partition/bucket
4. Check hot dedupe cache
5. Append original normalized events to WAL
6. fsync according to durability policy
7. Add events to in-memory column buffers
8. Acknowledge accepted events
9. Flush buffers to immutable raw segments in background
10. Mark WAL range as sealed after segment commit
```

### 9.3 Durability policy

Modes:

```text
strict: fsync before ack
balanced: group commit every N ms or N bytes
fast: OS-buffered, at-least-once external retry expected
```

Default for billing:

```text
balanced group commit, e.g. 10-50 ms
```

---

## 10. Deduplication

### 10.1 Hot dedupe

Maintain recent event IDs:

```text
event_id_hash -> payload_hash, first_seen_ms
```

Implementation options:

- in-memory LRU/TTL map
- optional persistent local hash set
- WAL replay rebuilds recent dedupe on startup

TTL should cover retry windows, e.g. 7-35 days depending on upstream behavior.

### 10.2 Cold dedupe

During compaction:

```text
sort by event_id_hash
remove exact duplicates
quarantine conflicts
```

Cold dedupe is necessary because retries or replay can bypass hot cache after restart or TTL expiry.

### 10.3 Billing-safe rule

Rollup builder must be idempotent.

It should process committed raw segment IDs and record:

```text
rollup_job_id
input_segment_ids
output_segment_ids
watermark
```

Never blindly aggregate the same raw segment twice.

---

## 11. Rollups

### 11.1 Hourly rollups

Primary rollup grain:

```text
account_id
subscription_id
product_id
meter_id
model_id
hour_start_ms
dimensions_key
```

Aggregate:

```text
quantity_sum
event_count
first_event_ms
last_event_ms
```

### 11.2 Daily/monthly queries

Monthly usage is produced by summing hourly rollups.

No monthly physical rollup is required in v1, but can be added later for very large tenants.

### 11.3 Open period handling

For current, not-yet-sealed periods:

```text
answer = sealed hourly rollups
       + recent unsealed raw segments
       + in-memory buffer/WAL tail if needed
```

For invoices:

```text
invoice = frozen snapshot at watermark
```

Late events after invoice close become adjustment lines.

---

## 12. Query model

### 12.1 Query request

```rust
pub struct UsageQuery {
    pub account_id: Option<AccountId>,
    pub from_ms: i64,
    pub to_ms: i64,
    pub product_id: Option<ProductId>,
    pub meter_id: Option<MeterId>,
    pub model_id: Option<ModelId>,
    pub dimensions: DimensionFilter,
    pub group_by: Vec<GroupKey>,
    pub source: QuerySource,
}
```

`QuerySource`:

```rust
pub enum QuerySource {
    Auto,
    RollupsOnly,
    RawOnly,
}
```

`GroupKey`:

```rust
pub enum GroupKey {
    Account,
    Subscription,
    Product,
    Meter,
    Model,
    Day,
    Hour,
    Dimension(String),
}
```

### 12.2 Main query: monthly usage for account

API:

```text
GET /v1/accounts/{account_id}/usage?from=2026-05-01&to=2026-06-01&group_by=product_id,meter_id,model_id
```

Physical retrieval:

```text
1. bucket = hash(account_id) % bucket_count
2. date partitions = days in range
3. read rollup_hourly/date=*/bucket={bucket}
4. skip segments by timestamp/product/meter/model metadata
5. scan columns: account_id, product_id, meter_id, model_id, quantity_sum
6. filter account_id
7. aggregate by requested group keys
8. optionally merge hot/unsealed data
```

Result:

```json
{
  "account_id": "acc_123",
  "from": "2026-05-01T00:00:00Z",
  "to": "2026-06-01T00:00:00Z",
  "watermark_ms": 1778954400000,
  "lines": [
    {
      "product_id": "ai_gateway",
      "meter_id": "tokens.input",
      "model_id": "anthropic.claude-sonnet-4",
      "quantity": "9823000",
      "unit": "token"
    }
  ]
}
```

### 12.3 Raw audit query

API:

```text
GET /v1/accounts/{account_id}/usage/events?from=...&to=...&meter_id=tokens.input
```

Purpose:

- Explain invoice line
- Debug collector issue
- Export usage evidence

This scans raw segments and returns paginated events.

---

## 13. Corrections and late data

Never mutate or delete committed usage events.

Correction event examples:

```text
+1000 tokens original
-1000 tokens correction/retraction
+800 tokens replacement
```

Correction fields:

```rust
pub enum EventKind {
    Usage,
    Correction,
    Retraction,
}

pub struct CorrectionRef {
    pub original_event_id: EventId,
    pub reason: String,
}
```

Billing behavior:

- Before invoice finalization: corrections affect current invoice.
- After invoice finalization: corrections generate credit/debit adjustment in next invoice.

---

## 14. Manifest and atomicity

Manifest tracks committed segments:

```rust
pub struct Manifest {
    pub db_version: u32,
    pub bucket_count: u32,
    pub raw_segments: Vec<SegmentMeta>,
    pub rollup_segments: Vec<SegmentMeta>,
    pub compacted_replacements: Vec<ReplacementRecord>,
    pub watermarks: Watermarks,
}
```

Atomic update pattern:

```text
1. write new segment to tmp/
2. fsync segment
3. rename to final path
4. write manifest.new
5. fsync manifest.new
6. rename manifest.new -> manifest.json
7. fsync parent directory
```

Startup recovery:

```text
1. load manifest
2. remove tmp files
3. ignore unmanifested segment files unless recovery mode enabled
4. replay WAL after last sealed offset
5. rebuild in-memory buffers and hot dedupe
```

---

## 15. Compaction

### 15.1 Goals

- Merge many small segments
- Improve compression
- Remove exact duplicates
- Produce better sorted order
- Reduce query metadata overhead

### 15.2 Compaction policy

Per partition/bucket:

```text
if small_segment_count > threshold
or total_small_segment_size > threshold
then compact
```

Suggested thresholds:

```text
small segment < 32 MB
compact when > 16 small segments in same date/bucket
output target 128-512 MB raw segment
```

### 15.3 Compaction sequence

```text
1. choose input segments
2. read and merge rows
3. sort by account_id, product_id, meter_id, model_id, timestamp_ms
4. dedupe exact event_id duplicates
5. write output segment
6. atomically update manifest with replacement record
7. delete old files after reader grace period
```

---

## 16. Rust module layout

```text
src/
  lib.rs
  api/
    mod.rs
    http.rs
    grpc.rs
  model/
    ids.rs
    event.rs
    query.rs
    dimensions.rs
  ingest/
    writer.rs
    validator.rs
    wal.rs
    memtable.rs
    dedupe.rs
  storage/
    segment.rs
    segment_reader.rs
    segment_writer.rs
    manifest.rs
    columns.rs
    encoding.rs
    compression.rs
  rollup/
    builder.rs
    hourly.rs
    watermark.rs
  query/
    planner.rs
    executor.rs
    aggregate.rs
    audit.rs
  compact/
    planner.rs
    worker.rs
  runtime/
    config.rs
    metrics.rs
    errors.rs
```

---

## 17. Suggested Rust crates

Core:

```text
serde
serde_json
postcard or bincode
thiserror
anyhow
uuid or ulid
chrono or time
parking_lot
tokio
bytes
```

Compression/encoding:

```text
zstd
lz4_flex
byteorder
varint encoding crate or custom
```

Hashing:

```text
blake3
xxhash-rust
ahash
```

Data structures:

```text
hashbrown
roaring
smallvec
indexmap
```

HTTP:

```text
axum
tower
hyper
```

Observability:

```text
tracing
tracing-subscriber
metrics
opentelemetry optional
```

Testing:

```text
proptest
tempfile
criterion
insta optional
```

---

## 18. MVP milestones

### Milestone 1 — append-only raw store

Deliver:

- Rust data model
- Batch ingest API
- WAL
- Raw segment writer
- Manifest
- Startup recovery
- Raw event scan by account/time

Acceptance:

- Can ingest events
- Restart does not lose acknowledged events under chosen durability mode
- Can scan raw usage for account/month

### Milestone 2 — columnar encoding and skipping

Deliver:

- Dictionary encoding for IDs
- Timestamp delta encoding
- Quantity encoding
- zstd/lz4 compression
- Segment/block metadata
- Account/time/product/meter pruning

Acceptance:

- Raw scan reads only relevant date/bucket/segments
- Basic benchmark shows compressed storage and faster scans than JSONL baseline

### Milestone 3 — hourly rollups

Deliver:

- Rollup builder
- Rollup segment writer
- Rollup query path
- Monthly account usage endpoint
- Watermark tracking

Acceptance:

- Monthly usage reads rollups by default
- Raw and rollup totals match in tests
- Rollup builder is idempotent

### Milestone 4 — dedupe and corrections

Deliver:

- Hot event_id dedupe
- Payload conflict detection
- Cold dedupe during compaction
- Correction/retraction event support

Acceptance:

- Retried batch does not double-count
- Duplicate event with different payload is detected
- Correction events affect rollups correctly

### Milestone 5 — compaction

Deliver:

- Compaction planner
- Segment merge/sort
- Manifest replacement records
- Reader-safe deletion of old segments

Acceptance:

- Many small segments compact into fewer larger segments
- Query results unchanged before/after compaction
- Recovery safe across crash points

---

## 19. Critical invariants

These must hold:

```text
1. Acknowledged events are recoverable from WAL or committed segments.
2. Raw committed events are immutable.
3. Rollups are derived from specific raw segment IDs.
4. A raw segment is never counted twice in rollups.
5. Duplicate event_id with same payload is not double-counted.
6. Duplicate event_id with different payload is visible as conflict.
7. Manifest update is atomic.
8. Query over rollups can be reconciled with raw audit scan.
9. Compaction does not change logical results.
10. Invoice snapshots reference a watermark and source segment set.
```

---

## 20. Benchmark targets

Initial local benchmark dimensions:

```text
1M events
10M events
100M events
1K accounts
100K accounts
10 meters
100 models
```

Measure:

```text
ingest events/sec
bytes per event raw JSONL baseline
bytes per event compressed segment
monthly account query p50/p95
rollup build throughput
startup recovery time
compaction throughput
```

Useful benchmark queries:

```text
monthly usage for one account
monthly usage by account/product/meter/model
daily usage for one account
raw audit scan for one invoice line
top 100 accounts by tokens.output for month
```

---

## 21. Practical v1 simplifications

To avoid overbuilding:

- Use one process, local disk first.
- Use UTC only.
- Use integer quantities only.
- Use hourly rollups only.
- Support limited dimensions, e.g. max 16 keys per event.
- Require dimensions keys to be declared or whitelisted for filtering.
- Keep pricing outside the storage engine.
- Export to Parquet later, not first.
- No distributed clustering in v1.

---

## 22. Later extensions

Possible v2/v3 additions:

- Parquet export/import
- Object storage backend
- Distributed shard ownership
- Monthly materialized rollups
- Pricing-version-aware invoice snapshots
- Tenant-level retention policies
- Encrypted segments
- Column-level checksums
- Query cache
- Roaring bitmap indexes for high-value filters
- External DuckDB/Arrow integration
- S3-compatible cold storage tier

---

## 23. Recommended implementation stance

Start with correctness and simple physical layout:

```text
WAL -> immutable raw segments -> hourly rollups -> monthly query
```

Do not start with:

```text
full SQL engine
custom distributed system
complex cost-based planner
pricing engine inside storage
arbitrary schemaless dimensions
```

The product value is billing-safe usage retrieval:

```text
fast writes
compressed immutable storage
idempotent accounting
quick account/month totals
raw auditability
```

