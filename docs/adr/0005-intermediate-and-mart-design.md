# ADR 0005: Intermediate and mart design choices

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

With the staging layer in place, the intermediate models and marts have to answer the questions
a fashion EC business asks: sales, returns, price history, web conversion and sell-through.
Four choices were not obvious and are recorded here.

## Decisions

### 1. Price history is a validity-period table built with a window function, not a `snapshot`

The raw `price_changes` table is already a change log with explicit `effective_date` values.
A dbt `snapshot` records when *dbt noticed* that a row of a mutable table changed, so it would
add nothing here and would stamp the wrong date (the run date, not the effective date).
`int_price_periods` derives `valid_from` / `valid_to` with `lead()`, and the test
`assert_order_price_matches_price_period` proves that every order line was sold at the price
valid on its order date. A snapshot becomes the right tool when the source is a master table
that is overwritten in place, which is common for product masters in production systems.
DML works in the BigQuery sandbox (checked on 2026-11-05), so snapshots and incremental
models remain available.

### 2. Sessions are derived in SQL from a visitor-level gap rule

A session ends when a visitor has no event for more than `session_gap_minutes` (30). The rule
is strict (exactly 30 minutes stays in one session), it lives in one variable, and the
boundaries are tested with hand-made events. The same definition will be implemented in Spark
in Phase 4 so that results and cost can be compared. `int_sessions` is a table because the
window functions over all events are the expensive step.

### 3. Metrics that are biased by timing are published with a flag, not hidden

Return rates of recent months are too low because returns are still arriving; sell-through is
meaningless for styles that launched before the data starts or whose window is not over.
`fct_returns_by_cohort.is_mature` and `fct_style_sell_through.is_complete` mark the rows that
are final. Hiding the rows would lose information; publishing them without a flag would invite
wrong conclusions.

### 4. The models are executed and cross-checked without BigQuery

The BigQuery SQL is translated to DuckDB and run in CI on generated data. All data tests run
there, and the marts are compared with an independent pandas implementation. A deliberate-
breakage check (time zone dropped, a `>` changed to `>=`, cancelled orders let in, a price
period widened) confirmed that these tests fail when the logic is wrong.

## Consequences

- A wrong join, a time-zone slip or an off-by-one in a window fails in CI before any query is
  billed.
- The translation layer (sqlglot, DuckDB) can differ from BigQuery in details, for example the
  rounding of `timestamp_diff`. The tests tolerate the known difference, and `dbt build` on
  BigQuery stays the final check.
- Two small additional facts are accepted as simplifications: an order with items from two
  categories counts once in each category in `fct_sales_daily`, and a session that crosses
  midnight in Japan belongs to its start day.
