# Daily ingestion

The first load (`fashion_ec.load`) replaces whole tables. A real pipeline instead loads one
business day at a time and must survive reruns and backfills. This is the part Airflow will
schedule in the next phase.

```
data/source  --extract-->  data/landing/dt=YYYY-MM-DD/  --ingest-->  BigQuery raw_daily
(generator output)         (one day, Japan time)                      (day replaced via MERGE)
```

## Commands (PowerShell)

```powershell
python -m fashion_ec.generate --out data/source
python -m fashion_ec.extract --source data/source --show-window     # available days
python -m fashion_ec.extract --source data/source --from 2026-01-01 --to 2026-01-07
python -m fashion_ec.ingest --project YOUR_PROJECT_ID --from 2026-01-01 --to 2026-01-07
```

`extract` defaults to `--source data/source` and `--out data/landing`. `ingest` defaults to
dataset `raw_daily`. Run the same command again, or days in any order: the result is the same.

To build the dbt models on top of it:

```powershell
$env:BQ_RAW_DATASET = "raw_daily"
cd dbt
dbt build --profiles-dir .
```

(Fact tables then also carry `_ingestion_date`; staging selects columns explicitly, so models
are unaffected.)

## Rules

| Table type | Tables | Slice | Load |
|---|---|---|---|
| Dimension | products, price_changes, customers | full copy every day | table replaced |
| Fact | orders, returns, clickstream, inventory_snapshots | Japan-time day of the event | MERGE: delete the day, insert the day |
| Fact | order_items | the lines of that day's orders | same MERGE |

- **Japan-time days.** A day is 00:00 to 24:00 JST, i.e. 15:00 UTC to 15:00 UTC. Clickstream
  partition folders are UTC dates, so one JST day reads two of them.
- **Empty days.** A day with no rows still produces a file with the right schema, so a
  corrected (emptied) day replaces what was loaded before.
- **Returns** are sliced by the day the return was registered, not the order day. A late
  return lands in a later slice, as in a real source system.

## How a day is replaced

```sql
MERGE `target` AS target
USING `staging` AS source
ON FALSE
WHEN NOT MATCHED BY SOURCE AND target._ingestion_date = DATE '2026-02-03' THEN DELETE
WHEN NOT MATCHED BY TARGET THEN INSERT (...) VALUES (..., DATE '2026-02-03')
```

`ON FALSE` matches nothing, so all staging rows are inserted and all target rows of that day
are deleted, in a single atomic statement. The staging table (`_stg_<table>_<yyyymmdd>`) is
dropped afterwards, even if the check fails. After the MERGE the rows of the day are counted
and compared with the file.

## What is tested

- The daily slices add up exactly to the full tables, for every fact table.
- Every file has the declared schema, also when empty.
- The generated SQL parses as BigQuery (sqlglot).
- The MERGE is executed in DuckDB (translated from BigQuery): replaying days in any order
  does not change the table, and a corrected day replaces only itself.
- The call flow with a fake BigQuery client, including "staging is dropped on failure".

Not verified here: a run against real BigQuery. Run the commands above for a few days and
compare `SELECT _ingestion_date, COUNT(*) FROM raw_daily.orders GROUP BY 1`.
