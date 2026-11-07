from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest

from fashion_ec.extract import (
    DIMENSION_TABLES,
    FACT_TABLES,
    days_between,
    extract_day,
    jst_window,
    landing_path,
    main,
    source_window,
)
from fashion_ec.generate import GenerationConfig, generate_all, write_tables
from fashion_ec.schemas import TABLES


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    path = tmp_path_factory.mktemp("source")
    write_tables(generate_all(GenerationConfig.small()), path)
    return path


@pytest.fixture(scope="module")
def landing(source, tmp_path_factory):
    out = tmp_path_factory.mktemp("landing")
    first, last = source_window(source)
    for day in days_between(first, last):
        extract_day(source, out, day)
    return out


def read_source(source, table) -> pd.DataFrame:
    return pd.concat(
        [pd.read_parquet(f) for f in sorted((source / table).rglob("*.parquet"))],
        ignore_index=True,
    )


def read_landing(landing, table, days) -> pd.DataFrame:
    frames = [pd.read_parquet(landing_path(landing, d, table)) for d in days]
    return pd.concat(frames, ignore_index=True)


def test_jst_window_is_nine_hours_before_utc_midnight():
    start, end = jst_window(date(2026, 2, 3))
    assert start.isoformat() == "2026-02-02T15:00:00+00:00"
    assert end.isoformat() == "2026-02-03T15:00:00+00:00"


def test_days_between_rejects_reversed_range():
    with pytest.raises(ValueError):
        list(days_between(date(2026, 2, 3), date(2026, 2, 1)))


@pytest.mark.parametrize("table", FACT_TABLES)
def test_daily_slices_add_up_to_the_full_table(source, landing, table):
    first, last = source_window(source)
    days = list(days_between(first, last))
    full = read_source(source, table)
    sliced = read_landing(landing, table, days)
    assert len(sliced) == len(full)
    sort_cols = list(full.columns)
    a = full.sort_values(sort_cols, ignore_index=True)
    b = sliced[full.columns].sort_values(sort_cols, ignore_index=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False)


@pytest.mark.parametrize("table", DIMENSION_TABLES)
def test_dimensions_are_full_copies(source, landing, table):
    first, _ = source_window(source)
    assert pq.ParquetFile(landing_path(landing, first, table)).metadata.num_rows == len(
        read_source(source, table)
    )


def test_orders_belong_to_their_japan_time_day(source, landing):
    first, last = source_window(source)
    for day in days_between(first, last):
        frame = pd.read_parquet(landing_path(landing, day, "orders"))
        if frame.empty:
            continue
        local = (frame["order_ts"] + pd.Timedelta(hours=9)).dt.date
        assert (local == day).all()


def test_order_items_follow_their_orders(source, landing):
    first, last = source_window(source)
    for day in list(days_between(first, last))[:10]:
        orders = pd.read_parquet(landing_path(landing, day, "orders"))
        items = pd.read_parquet(landing_path(landing, day, "order_items"))
        assert set(items["order_id"]) == set(orders["order_id"]) & set(items["order_id"])
        assert set(items["order_id"]) <= set(orders["order_id"])


def test_every_file_has_the_declared_schema_even_when_empty(source, landing):
    first, last = source_window(source)
    for day in (first, last):
        for table in FACT_TABLES:
            schema = pq.read_schema(landing_path(landing, day, table))
            assert schema.equals(TABLES[table].arrow_schema, check_metadata=False), table


def test_rerun_gives_identical_files(source, tmp_path):
    first, _ = source_window(source)
    day = first.replace(day=first.day + 5)
    extract_day(source, tmp_path, day)
    before = {t: pq.read_table(landing_path(tmp_path, day, t)) for t in FACT_TABLES}
    extract_day(source, tmp_path, day)
    for t in FACT_TABLES:
        assert pq.read_table(landing_path(tmp_path, day, t)).equals(before[t])


def test_day_outside_source_is_rejected(source, tmp_path):
    with pytest.raises(ValueError, match="outside the source data"):
        extract_day(source, tmp_path, date(2000, 1, 1))


def test_cli_range(source, tmp_path, capsys):
    first, _ = source_window(source)
    last = first.replace(day=first.day + 1)
    main(["--source", str(source), "--out", str(tmp_path), "--from", str(first), "--to", str(last)])
    assert capsys.readouterr().out.count("orders=") == 2
    assert landing_path(tmp_path, last, "orders").exists()
