# ADR 0006: Daily ingestion with per-day MERGE replacement

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

Airflow needs a task that loads one day and is safe to retry and backfill. The existing
loader truncates whole tables. The BigQuery sandbox has no GCS and no date partitions that
could be dropped per day, but DML works (checked in the author's project).

## Options considered

1. **Append only.** Reruns duplicate rows.
2. **Delete then insert as two statements.** A failure between them loses the day.
3. **Partition overwrite.** Needs partitioned tables; partitioning is off in the sandbox.
4. **One MERGE that deletes the day and inserts the new rows.**

## Decision

Option 4, using an `_ingestion_date` column and `ON FALSE` with `NOT MATCHED BY SOURCE`.
It is atomic, needs no partitioning, and also handles days that shrink or become empty.
Dimensions are small, so they are replaced in full each day. The day boundary is Japan
time, matching the business date used in dbt (`jst_date`).

The data goes to a separate dataset (`raw_daily`) so the full-reload tables stay as a
reference; dbt switches with `BQ_RAW_DATASET`.

## Consequences

- Reruns and out-of-order backfills give identical tables (tested in DuckDB).
- Tables are not partitioned, so a query for one day scans clustered but unpartitioned
  data; fine at this size, revisit with billing enabled (Phase 4).
- Late-arriving changes to an old day are handled by re-ingesting that day.
