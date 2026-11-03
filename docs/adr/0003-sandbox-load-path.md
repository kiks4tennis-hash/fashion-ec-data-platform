# ADR 0003: Add a direct-load path for the BigQuery sandbox

- **Status:** Accepted
- **Date:** 2026-11-03

## Context

ADR 0002 stages files in GCS. The author's GCP project has no active billing account (the
attached account is closed), and Cloud Storage needs billing. The BigQuery sandbox works
without billing but limits what can be done: tables and partitions expire after 60 days,
storage and queries are capped, and DML and streaming are reported as unsupported.

## Options considered

1. **Open a billing account.** Unlocks everything, including Dataproc, but needs a payment
   method and exposes the project to charges.
2. **Stay on GCS staging and wait.** Blocks all progress on Phase 2 and 3.
3. **Add a second load path that skips GCS** (`--sandbox`): load local Parquet straight into
   BigQuery, without date partitioning.

## Decision

Option 3, kept as a mode of the same loader so that the schemas, validation, idempotent
replacement and row-count check are shared. The multiple partition files of a table are
merged into one Parquet file first, so each table needs one load job. Date partitioning is
off in this mode because the synthetic data covers past dates and partitions expire by date;
clustering stays. The GCS path stays the default and remains the target for Phase 4.

## Consequences

- Phases 2 and 3 can proceed with no billing account and no cost risk.
- Tables disappear after 60 days; re-running the loader recreates them.
- **Open question for Phase 2:** if DML is not available, dbt `incremental` (merge) and
  `snapshot` models cannot run in the sandbox. The setup guide has a one-minute check;
  its result decides whether the price-history snapshot is built with dbt or with a
  full-refresh table model, with the trade-off recorded in a later ADR.
- Spark on Dataproc (Phase 4) needs a billing account.
