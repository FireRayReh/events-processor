# Audit Pipeline — Design

Captures every business action across the .NET 10 microservices, carries it through Kafka, stores it
immutably in MySQL and serves it to a React dashboard with filtering and export.

| Goal | How it is met | Section |
|---|---|---|
| Zero event loss, even if a service crashes mid-request | Transactional outbox in the service's own DB, at-least-once delivery from there on, hourly reconciliation | [2](#2-producer-side-shared-library), [3](#3-kafka), [8](#8-verifying-no-loss) |
| No visible duplicates | Deterministic `eventId` set once at record time + idempotent sink (`PRIMARY KEY (occurred_at, event_id)`) | [4](#4-ingestor) |
| p95 < 1 s over 12 months | Monthly partitions, narrow hot table, filter-shaped composite indexes, keyset pagination, no `COUNT(*)`, rollup tables, read replica | [5](#5-storage-mysql), [6](#6-query-api) |
| 1M-row exports without memory spikes | Async export job that streams keyset-chunked reads to a gzip file on object storage | [7](#7-export) |
| Services emit events with no Kafka knowledge | `Audit.Client` NuGet package: `IAuditRecorder` + EF Core outbox + built-in relay | [2](#2-producer-side-shared-library) |

Contracts (JSON Schema 2020-12, with examples) are in [`/contracts`](../contracts). DDL is in
[`/db/mysql`](../db/mysql).

---

## 0. Sizing: what the hard part actually is

| Quantity | Value |
|---|---|
| Events/day | 2,000,000 |
| Average rate | ~23 events/s |
| Assumed peak (10× average) | ~250 events/s |
| Replay/backfill target | 5,000 events/s |
| Rows after 12 months | ~730M |
| Hot table (narrow row + 4 secondary indexes, ~400 B/row) | ~290 GB/yr |
| Detail table (full JSON, ~1 KB avg, ~3× page compression) | ~250 GB/yr |

**Throughput is not the problem.** One Kafka partition and one consumer could handle 250/s.
The problems are **correctness at the edges** (crashes, retries, redelivery) and **querying ~730M rows
in MySQL** when the working set is far larger than the buffer pool. Most of the design is about those two.

---

## 1. Architecture

```
┌────────────── each microservice (.NET 10) ──────────────┐
│ Request handler                                         │
│   ├─ business writes ─┐                                 │
│   └─ audit.Record()  ─┴─► SaveChanges()  (ONE MySQL tx) │
│                              │                          │
│                     audit_outbox table                  │
│                              │                          │
│  OutboxRelay (BackgroundService, in Audit.Client)       │
│   lease rows → produce (acks=all, idempotent) → delete  │
└──────────────────────────────┼──────────────────────────┘
                               ▼
           Kafka  audit.events.v1  (RF=3, min.isr=2, 12 partitions, 7d retention)
                               │                      └──► audit.events.dlq.v1
                               ▼
        Audit.Ingestor (consumer group "audit-ingestor", manual commit)
          validate → batch INSERT IGNORE (dedupe on PK) → commit offsets
                               │
                               ▼
        MySQL audit store (primary)  ── replication ──► read replica
          audit_event (narrow, indexed, partitioned by month)
          audit_event_detail (full JSON, same PK/partitions)
          audit_rollup_hourly, dim_*, audit_seal
                                                          │
                       Audit.Api (ASP.NET Core) ◄─────────┘
                         /events (keyset), /events/{id}, /stats, /exports
                               │                  │
                               ▼                  ▼
                       React dashboard     Export worker → object storage (gzip CSV/NDJSON)
```

---

## 2. Producer side: shared library

### 2.1 What a service developer writes

```csharp
// Program.cs
builder.Services
    .AddAuditing(o =>
    {
        o.ServiceName = "orders-service";
        o.Kafka.BootstrapServers = builder.Configuration["Audit:Kafka"]; // ops config, not dev concern
    })
    .UseEntityFrameworkOutbox<OrdersDbContext>(); // adds audit_outbox entity + migration + relay

// OrdersDbContext.OnModelCreating
modelBuilder.AddAuditOutbox();

// Handler
public async Task CancelAsync(Guid orderId, string reason, CancellationToken ct)
{
    var order = await _db.Orders.FindAsync([orderId], ct);
    var before = order!.Status;
    order.Cancel(reason);

    _audit.Record(new AuditEntry
    {
        Action  = "order.cancelled",
        Entity  = new("order", orderId.ToString()),
        Changes = [AuditChange.Of("status", before, order.Status)],
        Reason  = reason,
    });

    await _db.SaveChangesAsync(ct);   // business row + outbox row commit atomically
}
```

The library fills in everything else: `eventId` (UUIDv7), `occurredAt`, `source.*`, `actor` (from
`HttpContext.User` / `IAuditActorAccessor`), `correlation.traceId` (from `Activity.Current`),
`correlationId` (from an `X-Correlation-Id` header or the trace). Developers never see Kafka types.

### 2.2 Why a transactional outbox

"Zero loss even when a service crashes mid-request" can only be guaranteed if the audit record
and the business change commit **atomically**. Publishing straight to Kafka from the request leaves two gaps:

- Business tx commits, then the process crashes before `Produce` returns, so the event is **lost**.
- `Produce` succeeds, then the business tx rolls back, so the audit shows an action that **never happened**.

With the outbox, both are one MySQL transaction. Crash before commit: neither exists, which is correct.
Crash after commit: the outbox row is durable and the relay publishes it after restart.

`Record()` only adds an `AuditOutboxMessage` to the `DbContext` change tracker, so it costs nothing
until `SaveChanges`.

### 2.3 Events that must survive a rollback

**Pitfall:** failures and denials (`outcome = failure | denied`) usually happen on paths where the
business transaction is rolled back. If they go into that transaction, they are rolled back with it,
which loses the audit events you are most likely to need.

```csharp
await _audit.RecordImmediateAsync(entry with { Outcome = AuditOutcome.Denied }, ct);
```

`RecordImmediateAsync` writes the outbox row on a **separate connection and transaction** and awaits
the commit. Use it for denials, failures, logins and read-only actions ("report.exported") that have no
business transaction. Middleware does this automatically for `403` responses and unhandled exceptions
when `o.AuditFailures = true`.

### 2.4 Outbox relay

Lease-based, so no DB transaction is held open while waiting on Kafka:

```sql
-- 1. claim a batch (safe across multiple service instances)
UPDATE audit_outbox
   SET lease_owner = @instance, lease_until = NOW(6) + INTERVAL 30 SECOND
 WHERE lease_until IS NULL OR lease_until < NOW(6)
 ORDER BY id
 LIMIT 500;

-- 2. read it
SELECT id, event_id, message_key, payload FROM audit_outbox
 WHERE lease_owner = @instance AND lease_until > NOW(6) ORDER BY id;

-- 3. produce all, await delivery reports, then for acked ids:
DELETE FROM audit_outbox WHERE id IN (...);
```

Producer config:

```csharp
new ProducerConfig
{
    Acks = Acks.All,
    EnableIdempotence = true,           // prevents duplicates from producer-internal retries
    MaxInFlight = 5,                    // max allowed with idempotence; ordering still kept
    CompressionType = CompressionType.Zstd,
    LingerMs = 20,
    MessageTimeoutMs = 60_000,          // finite: on timeout the lease expires and the outbox retries
};
```

- If Kafka is down, rows accumulate in the outbox and **nothing is lost**. The relay backs off
  exponentially. Expose `audit_outbox_pending` and `audit_outbox_oldest_age_seconds` as metrics and alert
  on age, not count.
- **Pitfall:** Kafka idempotence lasts only for one producer session. A relay that crashes after Kafka acked
  but before `DELETE` will publish the batch again after restart. This is expected; the sink deduplicates
  (section 4).
- **Pitfall:** delete acknowledged rows; don't just flag them as sent. A "sent" flag on a hot table grows
  without limit and makes the claim query slower over time.
- Polling interval is 200 ms when the last batch was full and 1 s when it was empty. That gives about 1 s
  end-to-end latency at a negligible query cost. **Tradeoff:** CDC (Debezium on the outbox binlog) lowers
  latency and removes polling, but adds Kafka Connect, binlog retention and per-database connector
  operations. At 250 events/s polling is enough. Move to CDC only if a DBA team already runs Debezium.

### 2.5 Event ID and timestamps

- `eventId` is **UUIDv7**, generated once in `Record()` and stored in the outbox. Every retry carries
  the same ID. UUIDv7 is time-ordered, so it appends to InnoDB B-trees instead of splitting pages at random
  like v4 does.
- If a caller supplies `IdempotencyKey`, the library derives `eventId = UUIDv5(service + key)`. A client
  that retries an HTTP request that was actually processed twice then still produces one audit row.
- `occurredAt` is set **once, at record time**, and **truncated to microseconds**.
  **Pitfall:** .NET `DateTime` has 100 ns precision but MySQL `DATETIME(6)` has 1 µs. MySQL rounds unless
  `TIME_TRUNCATE_FRACTIONAL` is set. Truncating in the library makes the value identical everywhere, and
  the dedupe key depends on that (section 5.2).
- `contentHash` is SHA-256 over the RFC 8785 (JCS) canonical JSON of the event without the `contentHash`
  field. It is used for tamper evidence (section 5.5).

### 2.6 Guardrails in the library

- **Redaction:** properties marked `[AuditSensitive]` are masked and `[AuditIgnore]` are skipped when
  diffing entities. A default deny list (`password`, `token`, `secret`, `cardNumber`, ...) applies to
  `changes[].path`.
- **Size cap:** events over 64 KB have `changes` replaced by `{ "truncated": true, "originalSize": n }`
  and a warning is logged. The event is never dropped. Kafka's 1 MB default `max.message.bytes` is a
  hidden failure mode otherwise.
- **Schema validation** of the event against `audit-event.v1` runs in Debug and in tests (a library test
  helper), so contract mistakes fail CI and not production.
- Uses `System.Text.Json` source generation, so no reflection on the hot path and it is AOT-friendly.

### 2.7 Services without a MySQL transaction

The outbox requires a MySQL connection in the emitting service. For a service with no database, such as
a stateless gateway, `Audit.Client` offers `UseDurableSpool()`. It writes to a local append-only file with
`fsync` before acking, and the relay drains it. **Tradeoff:** this is only as durable as the pod's
volume. It needs a PersistentVolume, not `emptyDir`, or the "zero loss" guarantee does not hold for that
service. Document this explicitly per service.

---

## 3. Kafka

| Setting | Value | Why |
|---|---|---|
| Topic | `audit.events.v1` | Version in the name allows a breaking v2 to run side by side |
| Partitions | 12 | Room for 12 consumer instances; changing it later re-maps keys |
| Key | `{entity.type}:{entity.id}` | Keeps per-entity ordering in Kafka |
| `replication.factor` / `min.insync.replicas` | 3 / 2 | With `acks=all`, survives the loss of one broker without data loss |
| `unclean.leader.election.enable` | `false` | Otherwise an out-of-sync replica can become leader and **silently discard acked events** |
| `retention.ms` | 7 days | Replay buffer: must exceed the longest plausible audit-DB outage plus time to notice |
| `cleanup.policy` | `delete` | **Never `compact`**: compaction keeps only the latest record per key, so it would delete audit history |
| Value format | JSON (`audit-event.v1`) | As required; see the tradeoff below |
| Headers | `audit-event-id`, `audit-schema-version`, `content-type`, `traceparent` | Lets the ingestor route or deduplicate without parsing the body |

DLQ: `audit.events.dlq.v1` (30-day retention). Contract: `contracts/audit-dlq-message.schema.json`.

**Tradeoff: JSON vs Avro/Protobuf.** JSON is readable, needs no registry and is easy for the React and
ops tooling. In exchange, messages are about 2–3× larger before compression (zstd recovers most of that),
and **the broker does not enforce the schema**. Enforcement therefore happens at both ends: in the library
(tests) and in the ingestor (every message is validated, and invalid ones go to the DLQ). Schema changes
follow these rules:

- Additive, optional fields only within `v1`. Consumers ignore unknown fields.
- `schemaVersion` is `"1.x"`. A breaking change means a new topic `audit.events.v2` and a dual-reading ingestor.

**Ordering note:** the outbox relay can reorder events across instances, and keys only order events
within an entity. The audit store never relies on arrival order. It sorts by `(occurred_at, event_id)`.

---

## 4. Ingestor

A .NET 10 worker in consumer group `audit-ingestor`.

```csharp
new ConsumerConfig
{
    GroupId = "audit-ingestor",
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,   // a new group must start from the beginning, never "latest"
    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
    MaxPollIntervalMs = 300_000,
};
```

Loop:

1. Consume up to **1,000 messages or 200 ms**, whichever comes first.
2. Validate each message against `audit-event.v1`. Send invalid messages to the DLQ (with error details),
   and alert. **Never skip a message without writing it to the DLQ first.**
3. Resolve dimension IDs (`dim_service`, `dim_action`, `dim_entity_type`) from an in-memory cache. On a miss,
   insert with `INSERT IGNORE` and re-read.
4. In **one MySQL transaction**, run a multi-row `INSERT IGNORE INTO audit_event ...` and
   `INSERT IGNORE INTO audit_event_detail ...`, then commit.
5. Only then store and commit the Kafka offsets.

This is at-least-once delivery into an idempotent sink, which in practice behaves as exactly-once. If
the ingestor crashes between steps 4 and 5, the batch is redelivered and the duplicates are ignored by the PK.

**Pitfall: `INSERT IGNORE` hides more than duplicates.** It turns *every* error into a warning,
including truncation and invalid values, and inserts damaged data. Mitigations:

- Strict validation in step 2 makes data errors impossible in practice, because every length and enum is checked against the schema.
- After each batch, read `SHOW WARNINGS`. Any warning other than `1062 Duplicate entry` fails the batch
  and the batch goes to the bisect-and-DLQ path.
- Why not `ON DUPLICATE KEY UPDATE`? It needs the `UPDATE` privilege and fires `BEFORE UPDATE`
  triggers. Both conflict with the immutability controls in section 5.5.

**Poison messages.** If a batch fails, the ingestor retries it 3 times with backoff. If it still fails,
it bisects the batch to find the failing message, sends that message to the DLQ and inserts the rest.
If MySQL itself is unavailable, the ingestor **pauses the partitions**, sends nothing to the DLQ and
retries indefinitely. Kafka retention is the buffer.

**Late and dirty hours.** The ingestor records each `(hour)` touched by the batch in `audit_dirty_hour`,
so rollups can be recomputed (section 5.4).

**Lag** (`kafka_consumergroup_lag`) is the main health metric. Alert at more than 5 minutes behind.

---

## 5. Storage (MySQL 8.0+/8.4 LTS)

Full DDL: [`db/mysql/001_audit_schema.sql`](../db/mysql/001_audit_schema.sql).

### 5.1 Hot and detail tables

- **`audit_event`** is narrow (~150 B per row), indexed, and used for every list and filter query.
  Dimensions are dictionary-encoded to `SMALLINT`, IDs are `BINARY(16)`, and there is no JSON here. This
  keeps indexes small enough that the recent months stay in the buffer pool.
- **`audit_event_detail`** holds the full original event JSON. It has the same PK and partitions and uses
  `ROW_FORMAT=COMPRESSED`. It is read only for the detail view and for exports that ask for `changes`.

**Tradeoff:** two inserts per event and a join for the detail view, in exchange for list and filter
indexes that are 5–10× smaller. At 730M rows that difference decides whether the data fits in memory.

### 5.2 Partitioning and the dedupe key

```sql
PRIMARY KEY (occurred_at, event_id)
PARTITION BY RANGE COLUMNS (occurred_at) (PARTITION p2026_11 VALUES LESS THAN ('2026-12-01'), ...)
```

- **Pitfall: in MySQL, every unique key on a partitioned table must include the partitioning column.**
  You cannot have `UNIQUE(event_id)` alone. Dedupe therefore relies on the **pair** `(occurred_at, event_id)`.
  That is only correct because `occurred_at` is fixed in the outbox and identical on every retry (2.5).
  **Do not partition by `ingested_at`.** Retries would have different ingest times, both rows would be
  inserted, and dedupe would silently stop working.
- Monthly partitions give 12–13 partitions per year. A query with a time range only touches the relevant partitions (pruning).
- **Pitfall: an insert into a range with no partition fails.** A daily job keeps **at least 3 future
  months** of partitions and alerts if fewer exist. A `pMAX` catch-all partition exists only as a safety
  net, with an alert if it holds any rows, because `REORGANIZE` on a full `pMAX` is a long blocking rebuild.
- Retention means `ALTER TABLE ... DROP PARTITION`, which is instant. `DELETE` of 60M rows would cause
  replication lag and undo bloat. Before dropping, the partition is archived (section 5.6).

### 5.3 Indexes: designed for filter shapes

InnoDB secondary indexes already include the PK columns, so `(actor_id, occurred_at)` effectively becomes
`(actor_id, occurred_at, event_id)`. That supports equality on the leading column plus time-ordered keyset
scans in either direction.

| Index | Serves |
|---|---|
| PK `(occurred_at, event_id)` | Time-range-only browsing, export |
| `ix_entity (entity_type_id, entity_id, occurred_at)` | "History of order 123" |
| `ix_actor (actor_id, occurred_at)` | "What did user X do" |
| `ix_service_action (service_id, action_id, occurred_at)` | "All refunds from billing" |
| `ix_correlation (correlation_id)` | "Everything in this request or flow" |

**Pitfall: index count vs write cost and filter coverage.** Every extra index adds cost to each insert and
about 40–60 GB per year. Do not add one per dashboard checkbox. Filters that are not in the leading
position (`outcome`, `actor_type`, `source_ip`) are applied as **residual predicates** on top of the most
selective indexed filter.

### 5.4 Rollups for counts and charts

`COUNT(*)` over 12 months of a 730M-row table cannot meet the 1 s target. The dashboard's totals, charts
and facet counts come from `audit_rollup_hourly (hour, service_id, action_id, outcome, event_count)`.

**Pitfall: incrementing counters in the ingestor double-counts redelivered batches.** Instead, a job
**recomputes** rollups from the base table, which is idempotent:

```sql
REPLACE INTO audit_rollup_hourly (hour, service_id, action_id, outcome, event_count)
SELECT DATE_FORMAT(occurred_at, '%Y-%m-%d %H:00:00'), service_id, action_id, outcome, COUNT(*)
  FROM audit_event
 WHERE occurred_at >= @h AND occurred_at < @h + INTERVAL 1 HOUR
 GROUP BY 1, 2, 3, 4;
```

It runs every 5 minutes for every hour listed in `audit_dirty_hour`, which includes late arrivals for
old hours, and then clears those entries.

### 5.5 Immutability and tamper evidence

Defense in depth, because no single control is enough:

1. **Grants:** `audit_ingestor` has `INSERT, SELECT` only. `audit_api` has `SELECT` on the replica
   (plus `INSERT/UPDATE` on `audit_export_job` only). No application account has `UPDATE` or `DELETE` on the event tables.
2. **Triggers:** `BEFORE UPDATE` and `BEFORE DELETE` triggers `SIGNAL SQLSTATE '45000'` on both event tables.
   This catches mistakes by humans with broad grants. **Limitation:** triggers do **not** fire on
   `DROP PARTITION`, `TRUNCATE` or `DROP TABLE`, so DDL rights must stay with a separate, monitored break-glass account.
3. **Hourly seal:** after an hour closes, plus a 1-hour late-arrival grace period, a job computes
   `SHA-256` over the ordered `(event_id || content_hash)` list for that hour. It stores the result in
   `audit_seal` and **also writes it to WORM object storage** (S3 Object Lock / Azure immutable blob).
   A verifier recomputes the seals nightly. A mismatch means rows were altered or removed. Events that
   arrive after the hour is sealed produce a `seal_version + 1` amendment instead of rewriting the seal.

**Tradeoff: why not a per-row hash chain?** A chain (`hash_n = H(hash_{n-1} || row)`) needs a global write
order, which forces a single serialized writer and creates gaps under parallel consumers. Hourly
seals give the same tamper evidence at hour granularity and keep ingestion parallel.

**Conflict: immutability vs right-to-erasure (GDPR).** If audit data contains personal data that must
be erasable, keep it out of the immutable row. Use pseudonymous `actor.id` values and keep display names
and IPs in a mutable lookup, or encrypt PII fields with a per-subject key and delete the key
("crypto-shredding"). Legal should decide this **before** launch, because changing the approach later
means rewriting immutable data.

### 5.6 Retention and archive

- 13 months online, so a 12-month query always has complete data.
- Before `DROP PARTITION`, the partition is exported as Parquet to WORM object storage, together with
  its seals. Old data can be queried there through Athena, DuckDB or similar, not through the dashboard.

### 5.7 Replica

The API and export read from a **read replica**, so heavy dashboard queries never compete with ingestion.
Replication lag of a few seconds is acceptable for audit. The UI shows "data as of `<replica lag>`" when
the lag is more than 30 s.

---

## 6. Query API

Contracts: `contracts/audit-query-request.schema.json`, `contracts/audit-query-response.schema.json`.

```
GET  /api/audit/events          ?from&to&actorId&entityType&entityId&service&action&outcome&correlationId&cursor&limit
GET  /api/audit/events/{eventId}?occurredAt=   (occurredAt allows PK lookup and partition pruning)
GET  /api/audit/stats/hourly    ?from&to&service&action&outcome      (rollup table)
GET  /api/audit/facets          (dim tables: services, actions, entity types — never SELECT DISTINCT on the big table)
POST /api/audit/exports         GET /api/audit/exports/{exportId}     GET /api/audit/exports/{exportId}/download
```

Rules that make p95 < 1 s achievable:

1. **A time range is required**, with a default of the last 24 h and a maximum of 13 months. This enables partition pruning.
2. **Keyset pagination only.** `OFFSET 100000` reads and discards 100k rows.
   The cursor is an opaque base64 of `{occurredAt, eventId, direction, filterHash}`. If the filter
   hash does not match, the API returns `400`, so a cursor cannot be reused with different filters.
3. **Pitfall: row-constructor comparisons.** Write the keyset predicate in expanded form. MySQL
   does not always use an index range for `(a, b) < (?, ?)`:
   ```sql
   WHERE occurred_at BETWEEN @from AND @to
     AND (occurred_at < @cAt OR (occurred_at = @cAt AND event_id < @cId))
   ORDER BY occurred_at DESC, event_id DESC
   LIMIT 51          -- one extra row to compute hasMore
   ```
4. **The API chooses the index explicitly** based on filter precedence:
   `correlationId > entity > actor > service+action > time`. It uses `FORCE INDEX`, because at this size
   and skew the optimizer's statistics regularly choose the time-PK scan for "actor = X over 12 months".
   The plan for each filter shape is checked in CI with `EXPLAIN` against a seeded dataset.
5. **No total count** in list responses. `hasMore` plus counts from the rollup table (`/stats`) cover the UI's needs.
6. **Time-boxed queries.** Every query carries `/*+ MAX_EXECUTION_TIME(1500) */`.
   **Pitfall: the worst case is a selective filter on a large range with a non-indexed residual**,
   for example `actor = svc-batch` (40M events/yr) `AND outcome = denied` (12 rows). This scans the
   whole actor index range. Instead of timing out, the API runs the scan in **time slices**
   (newest month first) and returns what it found with `partial: true` and a `scanCursor`. The UI
   shows "Searched back to 2026-03 — continue". **The tradeoff stated plainly:** p95 < 1 s holds for indexed
   filter shapes. For arbitrary combinations over 12 months, you get a fast first response with
   progressive results, not a full answer within 1 s.
7. **Detail payloads are loaded on demand**, never joined into list queries.

**Free-text search** (`summary contains "refund"`) is **out of scope for MySQL** at this size. `LIKE '%x%'`
scans everything, and InnoDB does not support FULLTEXT indexes on partitioned tables. If it is needed, have the ingestor also write to OpenSearch, at
the cost of a second store to run.

---

## 7. Export

Contracts: `contracts/audit-export-request.schema.json`, `contracts/audit-export-job.schema.json`.

1. `POST /api/audit/exports` validates the filters (same rules as the query API), creates a row in
   `audit_export_job` with `status=queued` and returns `202` with `exportId`.
2. An **export worker** (a separate hosted service, so a 1M-row export never ties up API request threads):
   - Fixes the snapshot bounds at request time: `occurred_at <= to` **and** `ingested_at <= requestedAt`.
     **Pitfall:** without the `ingested_at` bound, late events with an old `occurred_at` that arrive during
     the export can show up in some chunks and not others, so the file would not match any single point in time.
   - Reads in **keyset chunks of 10,000 rows** from the replica with an unbuffered `MySqlDataReader`, not EF
     `ToListAsync`. **Pitfall:** one long `SELECT` over 1M rows keeps a consistent-read snapshot open for
     minutes. That holds back InnoDB purge on the replica and grows the undo log. Chunks use short, separate
     transactions.
   - Streams rows to `CsvWriter`/`Utf8JsonWriter`, then to a `GZipStream`, then to a multipart upload to object storage.
     Memory is bounded by chunk size plus the upload part buffer (~8 MB), whether the export is 1k or 1M rows.
   - Updates `rowsWritten` every chunk for the progress bar, and checks for cancellation between chunks.
3. When done, the job is `completed` with a short-lived pre-signed `downloadUrl` (15 min) that is
   re-issued on request. Files expire after 7 days.

Pitfalls specific to exports:

- **CSV injection:** cells that start with `=`, `+`, `-`, `@`, tab or CR are prefixed with `'`. Audit data
  includes user-controlled strings such as entity names and reasons.
- **Excel's limit is 1,048,576 rows.** 1M rows only just fits. Larger exports, or exports with `changes`,
  should use NDJSON.
- Including `changes` means joining `audit_event_detail` for each chunk (a PK lookup for each row in the
  chunk, batched with `IN (...)`). This is about 3× slower. It is optional and off by default.
- Limit concurrency: at most 2 running exports globally and 1 per user. A queue is better than 10 parallel 1M-row scans on the replica.
- Every export is itself audited (`audit.exported`, using `RecordImmediateAsync`).

Synchronous streaming (`IAsyncEnumerable` written to the response `PipeWriter`) is allowed only for
exports estimated at 50k rows or fewer (estimated from rollups). Long-lived HTTP downloads break on
proxies and load-balancer idle timeouts, and they cannot be resumed.

---

## 8. Verifying no loss

"Zero loss" is a claim. This is how it is checked:

- **Outbox age alert** (producer side) and **consumer lag alert** (ingest side) catch stalls.
- **Hourly reconciliation:** every relay instance publishes an `audit.relay.heartbeat` control record
  (a separate topic, not stored as an audit event; contract `audit-relay-heartbeat.v1`). It contains
  `{service, instance, hour, distinctEventCount, reclaimedCount}`, bucketed by the events' `occurredAt`
  hour. The reconciler checks, for each `(service, hour)`, that
  `stored_rows >= Σ distinctEventCount − Σ reclaimedCount`. `reclaimedCount` covers rows that another
  instance may also have published after its lease expired. Retries can therefore only cause a false alarm,
  never hide a real loss. A violation pages someone.
- **DLQ depth > 0** pages someone. A DLQ message is an event that has not reached the store yet.
- **Chaos tests in CI:** kill the service between `SaveChanges` and relay publish, kill the relay
  between Kafka ack and outbox `DELETE`, kill the ingestor between DB commit and offset commit,
  and restart a Kafka broker during produce. After each, assert that `count(distinct eventId)` equals the
  number of emitted events and that the store has no duplicate rows.

---

## 9. React dashboard

- **Server state:** TanStack Query with `useInfiniteQuery` and cursor pagination. **Table:** TanStack Table
  with row virtualization, because 1,000+ DOM rows freeze the page.
- **Filters live in the URL** (search params) so views can be shared and bookmarked, and the back button works.
  Text inputs are debounced by 300 ms, and in-flight requests are cancelled with `AbortController`
  when filters change.
- **The time range is always visible and required.** Presets are 1h, 24h, 7d, 30d, 12m.
- Facet dropdowns come from `/facets`, and the chart above the table comes from `/stats/hourly`.
- `partial: true` responses render a "Searched back to X — continue" row instead of an empty state that would look like "no results".
- Detail drawer: lazy `GET /events/{id}`, a `changes` diff view, and "show related" (same `correlationId`).
- Export button: creates the job, shows a progress toast that polls `GET /exports/{id}`, and downloads when ready.

---

## 10. Pitfalls and tradeoffs (summary)

| # | Pitfall / tradeoff | Consequence if ignored | Decision |
|---|---|---|---|
| 1 | Publishing to Kafka directly from the request | Lost or phantom events on crash | Transactional outbox |
| 2 | Failures and denials inside a rolled-back tx | The most important events disappear | `RecordImmediateAsync`, separate tx |
| 3 | Kafka idempotence is per session only | Duplicates after relay restart | Stable `eventId` + idempotent sink |
| 4 | Partitioned unique key must include the partition column | `UNIQUE(event_id)` impossible | PK `(occurred_at, event_id)` with `occurred_at` fixed at record time |
| 5 | Partitioning by `ingested_at` | Dedupe silently stops working | Partition by `occurred_at` |
| 6 | 100 ns vs 1 µs timestamp precision | Retry gets a different key, so a duplicate | Truncate to µs in the library |
| 7 | `INSERT IGNORE` hides data errors | Truncated audit data | Strict validation + `SHOW WARNINGS` check |
| 8 | `cleanup.policy=compact` / unclean leader election | Silent deletion of acked events | `delete`, `unclean=false`, `min.isr=2` |
| 9 | `AutoOffsetReset=Latest` on a new group | Skips everything already in the topic | `Earliest` |
| 10 | Missing future partitions | Inserts fail and ingestion stops | Create 3 months ahead + alert |
| 11 | `OFFSET` pagination / `COUNT(*)` | Multi-second queries | Keyset + rollup tables |
| 12 | Optimizer choosing the time-PK scan for selective filters | p95 blown on skewed data | `FORCE INDEX` by filter precedence, EXPLAIN tests |
| 13 | Residual filters over 12 months | Unbounded scans | Time-boxed, time-sliced partial results |
| 14 | Counters incremented by an at-least-once consumer | Wrong totals | Recompute rollups from the base table |
| 15 | Single long export `SELECT` | Purge lag, undo growth on replica | 10k-row keyset chunks, short transactions |
| 16 | Late events during export | Inconsistent file | `ingested_at <= requestedAt` bound |
| 17 | CSV injection | Code execution in the analyst's Excel | Prefix dangerous leading characters |
| 18 | Triggers don't stop `DROP PARTITION` | False sense of immutability | Grants + WORM-anchored hourly seals |
| 19 | Immutability vs GDPR erasure | Legal non-compliance or rewriting history | Pseudonymous IDs / crypto-shredding, decided up front |
| 20 | JSON on Kafka has no broker-side schema enforcement | Bad events reach the store | Validate in library tests + ingestor, DLQ |
| 21 | Free text in MySQL | Full scans | Not supported; OpenSearch if required |
| 22 | Event size growth (`changes` snapshots) | Kafka 1 MB rejections, storage growth | 64 KB cap with truncation marker |
| 23 | Durable spool on `emptyDir` | Loss on pod reschedule | PersistentVolume, or the service must use the outbox |
| 24 | Polling relay vs CDC | ~1 s latency vs operational complexity | Polling now; CDC if Debezium is already operated |
| 25 | Per-row hash chain | Serialized ingest | Hourly digest seals, chained hour-to-hour |

---

## 11. Solution layout (proposed)

```
src/
  Audit.Contracts/        # C# records generated from / tested against /contracts JSON Schemas
  Audit.Client/           # NuGet: IAuditRecorder, EF outbox, relay, middleware, redaction
  Audit.Ingestor/         # Kafka → MySQL worker
  Audit.Api/              # Query + export endpoints
  Audit.ExportWorker/     # Async export jobs
  Audit.Maintenance/      # Partition creation, rollups, seals, archive, reconciliation
web/audit-dashboard/      # React + TanStack Query/Table
contracts/                # JSON Schemas + examples (source of truth)
db/mysql/                 # DDL / migrations
tests/
  Audit.ContractTests/    # examples validate against schemas; C# serialization round-trips
  Audit.ChaosTests/       # crash-point tests from section 8 (Testcontainers: MySQL + Kafka)
```
