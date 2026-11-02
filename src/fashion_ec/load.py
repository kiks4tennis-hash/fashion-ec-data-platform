"""Load the generated Parquet files into BigQuery through Cloud Storage.

    local Parquet  ->  gs://<bucket>/raw/<table>/...  ->  BigQuery <project>.<dataset>.<table>

Design notes (see docs/adr/0002-gcs-staging-and-load-jobs.md):

* Load jobs are free, unlike streaming inserts, and read straight from GCS.
* Schemas are explicit (``fashion_ec.schemas``), never auto-detected.
* Every run replaces the table (WRITE_TRUNCATE) and the GCS prefix, so reruns are idempotent.
* After each load the BigQuery row count is compared with the Parquet row count.
* Authentication uses Application Default Credentials (``gcloud auth application-default
  login``). No key files are used.

Usage:
    python -m fashion_ec.load --project my-gcp-project --dry-run
    python -m fashion_ec.load --project my-gcp-project
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery, storage

from fashion_ec.schemas import TABLES, TableSpec

DEFAULT_LOCATION = "asia-northeast1"
DEFAULT_DATASET = "raw"
DEFAULT_PREFIX = "raw"


class LoadError(RuntimeError):
    """Raised when the loaded data does not match what was written."""


@dataclass(frozen=True)
class Settings:
    project_id: str
    bucket: str
    location: str = DEFAULT_LOCATION
    dataset: str = DEFAULT_DATASET
    prefix: str = DEFAULT_PREFIX

    @classmethod
    def build(
        cls,
        project: str | None = None,
        bucket: str | None = None,
        location: str | None = None,
        dataset: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Settings:
        """Explicit values win, then environment variables, then defaults."""
        env = os.environ if env is None else env
        project_id = project or env.get("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP project is required: pass --project or set GCP_PROJECT_ID")
        return cls(
            project_id=project_id,
            bucket=bucket or env.get("GCS_BUCKET") or f"{project_id}-fashion-ec",
            location=location or env.get("BQ_LOCATION") or DEFAULT_LOCATION,
            dataset=dataset or env.get("BQ_RAW_DATASET") or DEFAULT_DATASET,
        )

    def gcs_prefix(self, table: str) -> str:
        return f"{self.prefix}/{table}/"

    def gcs_uri(self, table: str) -> str:
        return f"gs://{self.bucket}/{self.gcs_prefix(table)}*"

    def table_id(self, table: str) -> str:
        return f"{self.project_id}.{self.dataset}.{table}"


def local_files(data_dir: Path, table: str) -> list[Path]:
    """All Parquet files of a table (the clickstream has one per day partition)."""
    files = sorted((data_dir / table).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet files for '{table}' under {data_dir / table}. "
            "Run `python -m fashion_ec.generate` first."
        )
    return files


def count_rows(files: list[Path]) -> int:
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def upload_table(bucket: storage.Bucket, settings: Settings, data_dir: Path, table: str) -> int:
    """Replace the table's GCS prefix with the local files. Returns the number of files."""
    files = local_files(data_dir, table)
    prefix = settings.gcs_prefix(table)
    stale = list(bucket.list_blobs(prefix=prefix))
    if stale:
        bucket.delete_blobs(stale)
    for path in files:
        name = prefix + path.relative_to(data_dir / table).as_posix()
        bucket.blob(name).upload_from_filename(str(path))
    return len(files)


def build_job_config(spec: TableSpec) -> bigquery.LoadJobConfig:
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[bigquery.SchemaField(name, kind) for name, kind in spec.columns],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if spec.partition_field:
        config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=spec.partition_field
        )
    if spec.cluster_fields:
        config.clustering_fields = list(spec.cluster_fields)
    return config


def ensure_dataset(client: bigquery.Client, settings: Settings) -> None:
    dataset = bigquery.Dataset(f"{settings.project_id}.{settings.dataset}")
    dataset.location = settings.location
    client.create_dataset(dataset, exists_ok=True)


def load_table(
    client: bigquery.Client, settings: Settings, spec: TableSpec, expected_rows: int
) -> int:
    """Run the load job and verify the row count. Returns the loaded row count."""
    job = client.load_table_from_uri(
        settings.gcs_uri(spec.name),
        settings.table_id(spec.name),
        job_config=build_job_config(spec),
        location=settings.location,
    )
    job.result()
    if job.output_rows != expected_rows:
        raise LoadError(
            f"{spec.name}: loaded {job.output_rows} rows but the Parquet files have {expected_rows}"
        )
    return job.output_rows


def plan(settings: Settings, data_dir: Path, tables: list[str]) -> list[str]:
    lines = [
        f"project   : {settings.project_id}",
        f"location  : {settings.location}",
        f"bucket    : gs://{settings.bucket}/{settings.prefix}/",
        f"dataset   : {settings.dataset}",
        "",
    ]
    for name in tables:
        files = local_files(data_dir, name)
        lines.append(
            f"{name:<22}{count_rows(files):>12,} rows  {len(files):>4} file(s)  "
            f"-> {settings.table_id(name)}"
        )
    return lines


def run(
    settings: Settings,
    data_dir: Path,
    tables: list[str],
    storage_client: storage.Client | None = None,
    bq_client: bigquery.Client | None = None,
) -> dict[str, int]:
    """Upload and load the given tables. Returns loaded row counts per table."""
    storage_client = storage_client or storage.Client(project=settings.project_id)
    bq_client = bq_client or bigquery.Client(project=settings.project_id)
    bucket = storage_client.bucket(settings.bucket)
    ensure_dataset(bq_client, settings)

    loaded: dict[str, int] = {}
    for name in tables:
        expected = count_rows(local_files(data_dir, name))
        n_files = upload_table(bucket, settings, data_dir, name)
        loaded[name] = load_table(bq_client, settings, TABLES[name], expected)
        print(f"{name:<22}{loaded[name]:>12,} rows  ({n_files} file(s) uploaded)", flush=True)
    return loaded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load generated Parquet into BigQuery via GCS.")
    parser.add_argument("--project", help="GCP project id (env: GCP_PROJECT_ID)")
    parser.add_argument(
        "--bucket", help="GCS bucket (env: GCS_BUCKET, default <project>-fashion-ec)"
    )
    parser.add_argument("--location", help=f"BigQuery location (default {DEFAULT_LOCATION})")
    parser.add_argument("--dataset", help=f"BigQuery dataset (default {DEFAULT_DATASET})")
    parser.add_argument("--data-dir", default="data/raw", help="generated data (default data/raw)")
    parser.add_argument("--tables", nargs="+", choices=list(TABLES), help="default: all tables")
    parser.add_argument("--dry-run", action="store_true", help="show the plan, touch nothing")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = Settings.build(args.project, args.bucket, args.location, args.dataset)
    data_dir = Path(args.data_dir)
    tables = args.tables or list(TABLES)

    if args.dry_run:
        print("\n".join(plan(settings, data_dir, tables)))
        return
    run(settings, data_dir, tables)
    print(f"\nDone. Query it: SELECT COUNT(*) FROM `{settings.table_id('orders')}`")


if __name__ == "__main__":
    main()
