# ADR 0002: Stage files in GCS and ingest with BigQuery load jobs

- **Status:** Accepted
- **Date:** 2026-11-02

## Context

The generated Parquet files have to reach the BigQuery `raw` dataset. The same files will
later be read by Spark (Phase 4), and the ingestion step will be an Airflow task (Phase 3),
so the approach should be repeatable, cheap and easy to schedule.

## Options considered

1. **Streaming inserts or `load_table_from_dataframe` straight from pandas** — few moving
   parts, but streaming is billed, large frames need to fit in memory, and nothing is left
   behind for Spark to read.
2. **Local files uploaded directly to BigQuery** — free, but one job per file (the clickstream
   has one file per day) and no shared copy of the raw data.
3. **Local Parquet -> GCS -> one BigQuery load job per table** — load jobs are free, one
   wildcard URI covers all partition files, and GCS keeps an immutable copy that Spark and
   backfills can reuse.

## Decision

Option 3. Explicit schemas (`fashion_ec.schemas`) instead of auto-detection; every run
replaces the GCS prefix and the table (`WRITE_TRUNCATE`); the BigQuery row count is checked
against the Parquet metadata. Authentication is Application Default Credentials, with no key
files. Large tables are partitioned and clustered (`clickstream`, `orders`,
`inventory_snapshots`).

## Consequences

- Reruns are idempotent, which is what Airflow retries and backfills need.
- Timestamps must be written as UTC microsecond Parquet timestamps; otherwise BigQuery reads
  them as `DATETIME` or rejects them. Tests pin this down.
- A GCS bucket is one more resource to manage; it is created once by hand and later by
  Terraform (Phase 5).
- Full replacement is fine for synthetic data. Incremental loads by date partition are a
  later refinement in the Airflow DAG.
