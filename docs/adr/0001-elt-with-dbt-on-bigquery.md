# ADR 0001: Use ELT with dbt on BigQuery

- **Status:** Accepted
- **Date:** 2026-10-29

## Context

The platform ingests orders, returns, inventory snapshots, price history and clickstream
events for a fashion e-commerce business. Analysts need trustworthy, documented marts
(net revenue, sell-through, return rate). The project is built by one engineer, so
operational overhead must stay low, and BigQuery's free tier keeps cost near zero.

## Options considered

1. **ETL in Python/Spark before loading** — full control over transformations, but business
   logic ends up in application code, is harder to test and document, and is harder for
   analysts to read.
2. **ELT: load raw data into BigQuery, transform with dbt** — transformations are SQL under
   version control, with built-in tests, lineage and documentation. The warehouse does the
   heavy lifting.
3. **Spark for everything** — scales well, but is overkill for order-level volumes and adds
   cluster operations cost.

## Decision

Load raw data into BigQuery unchanged, then model it with dbt in three layers
(staging, intermediate, marts). Use Spark only where data volume justifies it
(clickstream sessionization), and compare it with the BigQuery SQL equivalent.

## Consequences

- Business logic is testable (`dbt test`) and reviewable in pull requests.
- Raw data is preserved, so models can be rebuilt after logic changes.
- BigQuery scan costs must be managed with partitioning and clustering.
- The Spark/BigQuery comparison will be documented in a later ADR.
