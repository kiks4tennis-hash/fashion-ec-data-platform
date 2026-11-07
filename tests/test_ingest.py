"""Ingest tests: SQL shape (sqlglot), behaviour (DuckDB), and call flow (fake client)."""

from datetime import date
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest
import sqlglot
from sqlglot import exp

from fashion_ec.extract import (
    DIMENSION_TABLES,
    FACT_TABLES,
    days_between,
    extract_day,
    landing_path,
)
from fashion_ec.generate import GenerationConfig, generate_all, write_tables
from fashion_ec.ingest import (
    INGESTION_COLUMN,
    count_sql,
    create_table_sql,
    ingest_day,
    main,
    merge_sql,
    staging_name,
)
from fashion_ec.load import LoadError
from fashion_ec.schemas import TABLES

DAY = date(2026, 2, 3)


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    source = tmp_path_factory.mktemp("source")
    write_tables(generate_all(GenerationConfig.small()), source)
    out = tmp_path_factory.mktemp("landing")
    first = date.fromisoformat(
        str(
            pd.read_parquet(source / "inventory_snapshots" / "inventory_snapshots.parquet")[
                "snapshot_date"
            ].min()
        )
    )
    for day in days_between(first, first.replace(day=first.day + 6)):
        extract_day(source, out, day)
    return out, source, first


# --- SQL shape ----------------------------------------------------------------


@pytest.mark.parametrize("table", FACT_TABLES)
def test_generated_sql_parses_as_bigquery(table):
    spec = TABLES[table]
    merge = sqlglot.parse_one(merge_sql(spec, "p.d.t", "p.d.s", DAY), read="bigquery")
    assert isinstance(merge, exp.Merge)
    whens = merge.args["whens"].expressions
    assert len(whens) == 2
    create = sqlglot.parse_one(create_table_sql(spec, "p.d.t"), read="bigquery")
    assert isinstance(create, exp.Create)
    assert sqlglot.parse_one(count_sql("p.d.t", DAY), read="bigquery")


def test_merge_lists_every_column_once():
    spec = TABLES["orders"]
    sql = merge_sql(spec, "p.d.t", "p.d.s", DAY)
    for name in spec.column_names:
        assert sql.count(f"source.`{name}`") == 1
    assert INGESTION_COLUMN in sql


def test_create_table_has_cluster_and_ingestion_column():
    sql = create_table_sql(TABLES["clickstream"], "p.d.clickstream")
    assert f"`{INGESTION_COLUMN}` DATE" in sql
    assert "CLUSTER BY" in sql


# --- behaviour: run the MERGE in DuckDB -----------------------------------------


def duck_merge(con, table, spec, day, path):
    con.execute(f"CREATE OR REPLACE TABLE stg AS SELECT * FROM read_parquet('{path}')")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS tgt_{table} AS "
        f"SELECT *, DATE '1900-01-01' AS {INGESTION_COLUMN} FROM stg LIMIT 0"
    )
    sql = merge_sql(spec, f"tgt_{table}", "stg", day)
    con.execute(sqlglot.transpile(sql, read="bigquery", write="duckdb")[0])


@pytest.mark.parametrize("table", FACT_TABLES)
def test_merge_is_idempotent_and_equals_full_table(landing, table):
    out, source, first = landing
    days = list(days_between(first, first.replace(day=first.day + 6)))
    con = duckdb.connect()
    spec = TABLES[table]

    for day in days:
        duck_merge(con, table, spec, day, landing_path(out, day, table))
    snapshot = con.execute(f"SELECT * FROM tgt_{table} ORDER BY ALL").df()

    # replay: same day again, then an earlier day again: nothing may change
    for day in (days[3], days[0], days[3]):
        duck_merge(con, table, spec, day, landing_path(out, day, table))
    again = con.execute(f"SELECT * FROM tgt_{table} ORDER BY ALL").df()
    pd.testing.assert_frame_equal(snapshot, again)

    expected = sum(len(pd.read_parquet(landing_path(out, d, table))) for d in days)
    assert len(again) == expected


def test_merge_replaces_only_that_day(landing):
    out, _, first = landing
    day, other = first, first.replace(day=first.day + 1)
    con = duckdb.connect()
    spec = TABLES["orders"]
    duck_merge(con, "orders", spec, day, landing_path(out, day, "orders"))
    duck_merge(con, "orders", spec, other, landing_path(out, other, "orders"))
    # a corrected file for `day` (one order less) replaces the earlier load of that day
    frame = pd.read_parquet(landing_path(out, day, "orders")).iloc[1:]
    fixed = out / "fixed.parquet"
    frame.to_parquet(fixed, index=False)
    duck_merge(con, "orders", spec, day, fixed)
    counts = dict(
        con.execute(f"SELECT {INGESTION_COLUMN}, COUNT(*) FROM tgt_orders GROUP BY ALL").fetchall()
    )
    assert counts[day] == len(frame)
    assert counts[other] == len(pd.read_parquet(landing_path(out, other, "orders")))


# --- call flow with a fake client ---------------------------------------------------


def fake_client(rows_for_day=None):
    """Fake BigQuery client. Loads report the file's row count unless overridden."""
    import pyarrow.parquet as pq

    client = MagicMock()
    queries = []

    def load(handle, destination, job_config, location):
        job = MagicMock()
        job.output_rows = pq.read_metadata(handle.name).num_rows
        return job

    def query(sql, location=None):
        queries.append(sql)
        result = MagicMock()
        if sql.startswith("SELECT COUNT"):
            result.result.return_value = [{"n": rows_for_day}]
        return result

    client.load_table_from_file.side_effect = load
    client.query.side_effect = query
    return client, queries


def expected_rows(out, day, table):
    return len(pd.read_parquet(landing_path(out, day, table)))


def test_ingest_day_flow(landing):
    out, _, first = landing
    client = MagicMock()
    queries = []
    import pyarrow.parquet as pq

    def load(handle, destination, job_config, location):
        job = MagicMock()
        job.output_rows = pq.read_metadata(handle.name).num_rows
        return job

    def query(sql, location=None):
        queries.append(sql)
        result = MagicMock()
        if sql.startswith("SELECT COUNT"):
            table = sql.split("`")[1].rsplit(".", 1)[1]
            result.result.return_value = [{"n": expected_rows(out, first, table)}]
        return result

    client.load_table_from_file.side_effect = load
    client.query.side_effect = query

    loaded = ingest_day("p", out, first, client=client)

    assert set(loaded) == set(DIMENSION_TABLES) | set(FACT_TABLES)
    assert sum(q.startswith("MERGE") for q in queries) == len(FACT_TABLES)
    assert client.delete_table.call_count == len(FACT_TABLES)
    dropped = {c.args[0] for c in client.delete_table.call_args_list}
    assert f"p.raw_daily.{staging_name('orders', first)}" in dropped


def test_count_mismatch_fails_and_still_drops_staging(landing):
    out, _, first = landing
    client, _ = fake_client(rows_for_day=-1)
    with pytest.raises(LoadError, match="expected"):
        ingest_day("p", out, first, client=client)
    client.delete_table.assert_called_once()


def test_missing_landing_file_is_a_clear_error(landing, tmp_path):
    with pytest.raises(LoadError, match="fashion_ec.extract"):
        ingest_day("p", tmp_path, DAY, client=MagicMock())


def test_main_requires_project(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    with pytest.raises(SystemExit):
        main(["--date", "2026-02-03"])
