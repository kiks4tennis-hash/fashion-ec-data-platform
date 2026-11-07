"""Cut one Japan-time business day out of the "source system" files.

In a real company the source systems (shop database, web analytics) are queried for one day at
a time. Here the source system is the output of `fashion_ec.generate` (the whole period at
once), and this module plays the nightly export:

    data/source/<table>/...   --extract one day-->   data/landing/dt=YYYY-MM-DD/<table>.parquet

* Facts are sliced by their Japan-time business day: orders and clickstream events by their
  timestamp, order lines with their order, returns by the day the return was registered, and
  inventory by snapshot date. A day with no rows still produces a file with the right schema,
  because "nothing happened" must replace whatever was loaded for that day before.
* Dimensions (products, price_changes, customers) are exported in full every day. A full copy
  is the same whichever day is loaded, so loading days in any order gives the same result.

Usage:
    python -m fashion_ec.extract --source data/source --out data/landing --date 2026-02-03
    python -m fashion_ec.extract --date 2026-02-03
"""

from __future__ import annotations

import argparse
import shutil
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fashion_ec.schemas import TABLES

DIMENSION_TABLES = ("products", "price_changes", "customers")
FACT_TABLES = ("orders", "order_items", "returns", "inventory_snapshots", "clickstream")

JST = timezone(timedelta(hours=9))
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")


def jst_window(day: date) -> tuple[datetime, datetime]:
    """[start, end) of a Japan-time day as UTC datetimes: 00:00 JST to the next 00:00 JST."""
    start = datetime(day.year, day.month, day.day, tzinfo=JST)
    return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)


def days_between(first: date, last: date) -> Iterator[date]:
    if last < first:
        raise ValueError(f"end date {last} is before start date {first}")
    for offset in range((last - first).days + 1):
        yield first + timedelta(days=offset)


def landing_path(landing_dir: Path, day: date, table: str) -> Path:
    return landing_dir / f"dt={day.isoformat()}" / f"{table}.parquet"


def source_window(source_dir: Path) -> tuple[date, date]:
    """First and last day of the source data (inventory has a snapshot on every day)."""
    snapshot_dates = pq.read_table(
        source_dir / "inventory_snapshots" / "inventory_snapshots.parquet",
        columns=["snapshot_date"],
    ).column("snapshot_date")
    return pc.min(snapshot_dates).as_py(), pc.max(snapshot_dates).as_py()


def _single_file(source_dir: Path, table: str) -> Path:
    return source_dir / table / f"{table}.parquet"


def _in_window(column: str, day: date) -> pc.Expression:
    start, end = jst_window(day)
    field = ds.field(column)
    return (field >= pa.scalar(start, UTC_TIMESTAMP)) & (field < pa.scalar(end, UTC_TIMESTAMP))


def _clickstream_files(source_dir: Path, day: date) -> list[Path]:
    """Partition folders are UTC dates; a Japan-time day touches two of them."""
    start, end = jst_window(day)
    utc_days = {start.date(), (end - timedelta(microseconds=1)).date()}
    files: list[Path] = []
    for utc_day in sorted(utc_days):
        files += sorted(
            (source_dir / "clickstream" / f"event_date={utc_day.isoformat()}").glob("*.parquet")
        )
    return files


def _read_day(source_dir: Path, table: str, day: date) -> pa.Table:
    schema = TABLES[table].arrow_schema
    if table == "orders":
        data = ds.dataset(_single_file(source_dir, table)).to_table(
            filter=_in_window("order_ts", day)
        )
    elif table == "returns":
        data = ds.dataset(_single_file(source_dir, table)).to_table(
            filter=_in_window("return_ts", day)
        )
    elif table == "inventory_snapshots":
        data = ds.dataset(_single_file(source_dir, table)).to_table(
            filter=ds.field("snapshot_date") == pa.scalar(day, pa.date32())
        )
    elif table == "clickstream":
        files = _clickstream_files(source_dir, day)
        data = (
            ds.dataset([str(f) for f in files]).to_table(filter=_in_window("event_ts", day))
            if files
            else schema.empty_table()
        )
    else:
        raise ValueError(f"'{table}' is not a day-sliced table")
    return data.select(schema.names).cast(schema)


def extract_day(source_dir: Path, landing_dir: Path, day: date) -> dict[str, int]:
    """Write one day's landing files. Returns the number of rows per table.

    Running it again for the same day overwrites the files with identical content.
    """
    first, last = source_window(source_dir)
    if not first <= day <= last:
        raise ValueError(f"{day} is outside the source data ({first} to {last})")

    rows: dict[str, int] = {}
    for table in DIMENSION_TABLES:
        target = landing_path(landing_dir, day, table)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_single_file(source_dir, table), target)
        rows[table] = pq.ParquetFile(target).metadata.num_rows

    orders = _read_day(source_dir, "orders", day)
    day_tables = {
        "orders": orders,
        "order_items": ds.dataset(_single_file(source_dir, "order_items"))
        .to_table(filter=pc.field("order_id").isin(orders.column("order_id")))
        .select(TABLES["order_items"].arrow_schema.names)
        .cast(TABLES["order_items"].arrow_schema),
        "returns": _read_day(source_dir, "returns", day),
        "inventory_snapshots": _read_day(source_dir, "inventory_snapshots", day),
        "clickstream": _read_day(source_dir, "clickstream", day),
    }
    for table, data in day_tables.items():
        pq.write_table(data, landing_path(landing_dir, day, table))
        rows[table] = data.num_rows
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export one Japan-time day from the source files.")
    parser.add_argument(
        "--source", default="data/source", help="generator output (default data/source)"
    )
    parser.add_argument(
        "--out", default="data/landing", help="landing folder (default data/landing)"
    )
    parser.add_argument("--date", type=date.fromisoformat, help="one day, YYYY-MM-DD")
    parser.add_argument(
        "--from", dest="first", type=date.fromisoformat, help="first day of a range"
    )
    parser.add_argument("--to", dest="last", type=date.fromisoformat, help="last day of a range")
    parser.add_argument("--show-window", action="store_true", help="print the available days")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    source, out = Path(args.source), Path(args.out)
    first, last = source_window(source)
    if args.show_window:
        print(f"source data covers {first} to {last}")
        return
    if args.date:
        days = [args.date]
    elif args.first and args.last:
        days = list(days_between(args.first, args.last))
    else:
        raise SystemExit("give --date, or both --from and --to (or --show-window)")
    for day in days:
        rows = extract_day(source, out, day)
        facts = ", ".join(f"{t}={rows[t]:,}" for t in FACT_TABLES)
        print(f"{day}: {facts}")


if __name__ == "__main__":
    main()
