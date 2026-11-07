"""Load one day of landing files into BigQuery, safely repeatable.

    data/landing/dt=D/<table>.parquet  ->  <project>.<dataset>.<table>

* Dimensions (products, price_changes, customers) are full copies: the table is replaced.
* Fact tables get an ``_ingestion_date`` column. A day is loaded into a temporary table and
  then swapped in with one MERGE that deletes the target's rows for that day and inserts the
  new ones. Running the same day twice, or days in any order, gives the same table.
* After every table the rows for the day are counted and compared with the file.

The default dataset is ``raw_daily`` so the full-reload tables in ``raw`` stay untouched.
Point dbt at it with ``BQ_RAW_DATASET=raw_daily``.

Usage:
    python -m fashion_ec.ingest --project my-project --date 2026-02-03
    python -m fashion_ec.ingest --project my-project --from 2026-01-01 --to 2026-01-31
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery

from fashion_ec.extract import DIMENSION_TABLES, FACT_TABLES, days_between, landing_path
from fashion_ec.load import DEFAULT_LOCATION, LoadError, build_job_config, validate_files
from fashion_ec.schemas import TABLES, TableSpec

DEFAULT_DATASET = "raw_daily"
INGESTION_COLUMN = "_ingestion_date"


def table_id(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def staging_name(table: str, day: date) -> str:
    return f"_stg_{table}_{day:%Y%m%d}"


def create_table_sql(spec: TableSpec, target: str) -> str:
    columns = [f"`{name}` {kind}" for name, kind in spec.columns]
    columns.append(f"`{INGESTION_COLUMN}` DATE")
    sql = f"CREATE TABLE IF NOT EXISTS `{target}` ({', '.join(columns)})"
    if spec.cluster_fields:
        sql += " CLUSTER BY " + ", ".join(f"`{c}`" for c in spec.cluster_fields)
    return sql


def merge_sql(spec: TableSpec, target: str, staging: str, day: date) -> str:
    """Replace the target's rows of ``day`` with the staging rows, in one statement.

    ``ON FALSE`` matches nothing, so every staging row is "not matched by target" (inserted)
    and every target row of that day is "not matched by source" (deleted). Rows of other days
    are not touched.
    """
    names = [f"`{c}`" for c in spec.column_names]
    insert_cols = ", ".join([*names, f"`{INGESTION_COLUMN}`"])
    insert_vals = ", ".join([*[f"source.{n}" for n in names], f"DATE '{day.isoformat()}'"])
    return (
        f"MERGE `{target}` AS target\n"
        f"USING `{staging}` AS source\n"
        "ON FALSE\n"
        f"WHEN NOT MATCHED BY SOURCE AND target.`{INGESTION_COLUMN}` = DATE '{day.isoformat()}'"
        " THEN DELETE\n"
        f"WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def count_sql(target: str, day: date) -> str:
    return (
        f"SELECT COUNT(*) AS n FROM `{target}` "
        f"WHERE `{INGESTION_COLUMN}` = DATE '{day.isoformat()}'"
    )


def _load_file(client, path: Path, destination: str, spec: TableSpec, location: str, **config):
    job_config = build_job_config(spec, partitioned=False)
    for key, value in config.items():
        setattr(job_config, key, value)
    with path.open("rb") as handle:
        job = client.load_table_from_file(
            handle, destination, job_config=job_config, location=location
        )
    job.result()
    return job


def ingest_dimension(client, project, dataset, location, landing_dir, day, table) -> int:
    spec = TABLES[table]
    path = landing_path(landing_dir, day, table)
    expected = pq.ParquetFile(path).metadata.num_rows
    job = _load_file(client, path, table_id(project, dataset, table), spec, location)
    if job.output_rows != expected:
        raise LoadError(f"{table}: loaded {job.output_rows} rows but the file has {expected}")
    return expected


def ingest_fact(client, project, dataset, location, landing_dir, day, table) -> int:
    spec = TABLES[table]
    path = landing_path(landing_dir, day, table)
    expected = pq.ParquetFile(path).metadata.num_rows
    target = table_id(project, dataset, table)
    staging = table_id(project, dataset, staging_name(table, day))

    client.query(create_table_sql(spec, target), location=location).result()
    job = _load_file(client, path, staging, spec, location)
    if job.output_rows != expected:
        raise LoadError(f"{table}: staged {job.output_rows} rows but the file has {expected}")
    try:
        client.query(merge_sql(spec, target, staging, day), location=location).result()
        rows = list(client.query(count_sql(target, day), location=location).result())
        found = rows[0]["n"] if rows else None
        if found != expected:
            raise LoadError(
                f"{table} {day}: table has {found} rows for the day, expected {expected}"
            )
    finally:
        client.delete_table(staging, not_found_ok=True)
    return expected


def ingest_day(
    project: str,
    landing_dir: Path,
    day: date,
    dataset: str = DEFAULT_DATASET,
    location: str = DEFAULT_LOCATION,
    client: bigquery.Client | None = None,
) -> dict[str, int]:
    """Load one day. Returns the row count per table."""
    for table in (*DIMENSION_TABLES, *FACT_TABLES):
        path = landing_path(landing_dir, day, table)
        if not path.exists():
            raise LoadError(f"{path} is missing: run fashion_ec.extract for {day} first")
        validate_files(TABLES[table], [path])

    client = client or bigquery.Client(project=project)
    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = location
    client.create_dataset(ds, exists_ok=True)

    loaded: dict[str, int] = {}
    for table in DIMENSION_TABLES:
        loaded[table] = ingest_dimension(
            client, project, dataset, location, landing_dir, day, table
        )
    for table in FACT_TABLES:
        loaded[table] = ingest_fact(client, project, dataset, location, landing_dir, day, table)
    return loaded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load landing files of one day into BigQuery.")
    parser.add_argument("--project", help="GCP project id (env: GCP_PROJECT_ID)")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="default raw_daily")
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument("--landing", default="data/landing", help="default data/landing")
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--from", dest="first", type=date.fromisoformat)
    parser.add_argument("--to", dest="last", type=date.fromisoformat)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    project = args.project or os.environ.get("GCP_PROJECT_ID")
    if not project:
        raise SystemExit("GCP project is required: pass --project or set GCP_PROJECT_ID")
    if args.date:
        days = [args.date]
    elif args.first and args.last:
        days = list(days_between(args.first, args.last))
    else:
        raise SystemExit("give --date, or both --from and --to")
    client = bigquery.Client(project=project)
    for day in days:
        loaded = ingest_day(project, Path(args.landing), day, args.dataset, args.location, client)
        print(f"{day}: " + ", ".join(f"{t}={loaded[t]:,}" for t in FACT_TABLES))


if __name__ == "__main__":
    main()
