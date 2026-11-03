# ADR 0004: dbt staging conventions and checks that run without a warehouse

- **Status:** Accepted
- **Date:** 2026-11-03

## Context

The staging layer is the contract between raw data and everything built on it. Two details
decide whether later numbers can be trusted: where UTC timestamps become Japan-time business
dates, and how mistakes are caught. In addition, `dbt compile` and `dbt build` need a
BigQuery connection, which a pull-request check should not depend on (no credentials in CI,
and the sandbox has no billing).

## Options considered

1. **Convert time zones where they are needed.** Every query calls `date()` on its own:
   simple, but one forgotten conversion shifts nine hours of sales onto the wrong day.
2. **Convert once, in staging, through a macro.** Every model exposes `_at` (UTC) and
   `_on_jst` (business date); later layers never touch time zones.
3. **Only rely on `dbt build` against the warehouse.** Complete, but needs credentials and
   gives feedback only after queries ran.

## Decision

Option 2 for time zones: `jst_date()` is the single conversion point, and a test fails if a
model calls `date()` directly. Staging models are views with no joins, with unit suffixes in
column names. Instead of option 3 alone, CI also runs static checks: every model is rendered,
parsed as BigQuery SQL with sqlglot and checked against the raw schema definitions, and
`dbt parse --warn-error` validates the project structure. Reusable generic tests
(`is_positive`, `is_non_negative`, `unique_combination`) are written in the project instead
of pulling in the dbt-utils package.

## Consequences

- Renaming a raw column or misspelling one in SQL fails in CI, before any query is billed.
- Data quality is still proven only by `dbt build` against BigQuery; a CI job that does this
  (workload identity federation, no key files) is planned for the CD phase.
- The static checker depends on sqlglot's BigQuery dialect and can reject valid SQL that
  sqlglot does not support; the escape hatch is to simplify the SQL or adjust the check.
- No external dbt packages yet, so `dbt deps` is not needed.
