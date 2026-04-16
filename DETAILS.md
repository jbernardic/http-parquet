# http-parquet — What This Project Does

## Overview

This is a high-throughput JSON ingestion service. It accepts arbitrary JSON over HTTP, buffers it in memory, writes it to disk as JSONL (newline-delimited JSON), and periodically converts those files to Parquet format. The goal is to receive a large volume of events quickly without blocking callers on disk I/O, while producing efficient columnar Parquet files for downstream analytics.

## The Pipeline

### 1. HTTP Ingestion (`POST /ingest`)

Clients send JSON to `POST /ingest`. The body can be a single object `{"event": "click"}` or a batch array `[{"a": 1}, {"b": 2}]`. The controller parses the body, validates it, and puts the whole batch onto an in-memory queue as a single operation. It immediately returns HTTP 202 — the data is accepted but not yet on disk. This keeps latency low for the caller.

### 2. In-Memory Queue

The queue holds `List<String>` batches. A single POST with 500 events is one queue entry, not 500. This keeps queue overhead low and preserves the natural batching that clients already do.

### 3. JSONL Writer (background virtual thread)

A single virtual thread runs continuously, polling the queue every 100ms. It writes each record as one line to a `.jsonl.tmp` file named after the current UTC hour, e.g. `data/2026-04-16T14.jsonl.tmp`. The `.tmp` suffix signals that the file is still being written to.

Flushing to disk is controlled by two configurable thresholds — whichever is hit first triggers a flush:
- **Time**: flush if more than `ingestion.flush.max-interval-ms` milliseconds have passed since the last flush (default 1s)
- **Size**: flush if unflushed bytes exceed `ingestion.flush.max-bytes` (default 64KB)

This avoids both the latency of per-record flushing and the data loss risk of never flushing.

### 4. Hourly File Rolling

The writer checks the current UTC hour on every loop iteration. When the hour changes, it:
1. Flushes and closes the current `.jsonl.tmp` file
2. Atomically renames it to `.jsonl` (e.g. `2026-04-16T14.jsonl`) — this signals the file is complete
3. Enqueues the completed `.jsonl` path for Parquet conversion
4. Opens a new `.jsonl.tmp` for the new hour

Files are partitioned by UTC hour, so one Parquet file covers exactly one hour of data.

### 5. Parquet Converter (background virtual thread)

A second virtual thread blocks waiting for completed `.jsonl` files to appear on the conversion queue. When it gets one, it uses DuckDB (via JDBC) to convert it:

```sql
COPY (SELECT * FROM read_ndjson_auto('data/2026-04-16T14.jsonl'))
  TO 'data/2026-04-16T14.parquet' (FORMAT PARQUET)
```

DuckDB's `read_ndjson_auto` infers the schema from the data automatically — there is no fixed schema required. Fields with consistent types get typed columns; mixed or missing fields become nullable. The result is a standard Parquet file readable by any analytics tool (DuckDB, Spark, Pandas, etc.).

### 6. Crash Recovery

On startup, the converter scans the output directory for any `.jsonl` files that have no matching `.parquet` file. These are files that were completed (renamed from `.tmp`) before a crash but never converted. They are queued for conversion immediately. `.jsonl.tmp` files (mid-hour at crash time) are left alone — the writer will resume appending to them on restart.

### 7. Graceful Shutdown

On shutdown, Spring calls `@PreDestroy` on the writer first (it depends on the converter being alive). The writer thread is interrupted, flushes and renames the current `.tmp` file to `.jsonl`, and enqueues it. The converter then processes that final file before receiving a poison-pill sentinel and exiting cleanly.

## Output Layout

```
data/
  2026-04-16T13.jsonl       # completed hour, already converted
  2026-04-16T13.parquet     # converted Parquet file
  2026-04-16T14.jsonl       # completed hour, pending conversion (or just converted)
  2026-04-16T14.parquet
  2026-04-16T15.jsonl.tmp   # current hour, still being written
```

## Key Design Choices

- **No fixed schema** — the service accepts any JSON structure. Schema is inferred at conversion time by DuckDB. This makes it suitable for event streams where the shape of events evolves over time.
- **Virtual threads** — both background workers use Java 21 virtual threads. This means blocking on I/O or queue operations is cheap, and the approach scales without a thread pool.
- **DuckDB for conversion** — avoids Hadoop and Avro dependencies. DuckDB is a single JAR and handles schema inference, type coercion, and Parquet writing with one SQL statement.
- **Batched queue entries** — the in-memory queue holds batches, not individual records, to avoid per-event overhead when clients send arrays.
- **Injected Clock** — the writer uses an injected `java.time.Clock` rather than calling `Instant.now()` directly, making hour-rollover logic testable without waiting for a real hour to pass.
