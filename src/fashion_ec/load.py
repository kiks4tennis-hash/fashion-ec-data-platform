"""Load the generated Parquet files into BigQuery through Cloud Storage.

    local Parquet  ->  gs://<bucket>/raw/<table>/...  ->  BigQuery <project>.<dataset>.<table>

Without a billing account (BigQuery sandbox) Cloud Storage is not available. ``--sandbox``
loads the local files straight into BigQuery instead, without date partitioning:

    local Parquet  ->  (merged into one file per table)  ->  BigQuery <project>.<dataset>.<table>

Design notes (see docs/adr/0002-gcs-staging-and-load-jobs.md and 0003-sandbox-load-path.md):

* Load jobs are free, unlike streaming inserts, and read straight from GCS.
* Schemas are explicit (``fashion_ec.schemas``), never auto-detected.
* Every run replaces the table (WRITE_TRUNCATE) and the GCS prefix, so reruns are idempotent.
* After each load the BigQuery row count is compared with the Parquet row count.
* Authentication uses Application Default Credentials (``gcloud auth application-default
  login``). No key files are used.

Usage:
    python -m fashion_ec.load --project my-gcp-project --dry-run
    python -m fashion_ec.load --project my-gcp-project
    python -m fashion_ec.load --project my-gcp-project --sandbox      # no billing account
"""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
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


def validate_files(spec: TableSpec, files: list[Path]) -> None:
    """Fail early, with a clear message, if the files do not match the table schema.

    Files written by an older generator (timezone-less timestamps, different columns) would
    otherwise fail later with a confusing BigQuery error, or load with the wrong types.
    ``large_string`` is accepted for ``string``: BigQuery reads both as STRING.
    """
    expected = spec.arrow_schema
    for path in files:
        actual = pq.read_schema(path)
        problems = [
            f"{field.name}: expected {field.type}, found "
            f"{actual.field(field.name).type if field.name in actual.names else 'missing'}"
            for field in expected
            if field.name not in actual.names
            or not (
                actual.field(field.name).type == field.type
                or (
                    pa.types.is_string(field.type)
                    and pa.types.is_large_string(actual.field(field.name).type)
                )
            )
        ]
        if problems or len(actual.names) != len(expected.names):
            detail = "; ".join(problems) or "unexpected extra columns"
            raise LoadError(
                f"{spec.name}: {path.name} does not match the expected schema ({detail}). "
                "It was probably written by an older version: re-run "
                "`python -m fashion_ec.generate` and try again."
            )


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


def build_job_config(spec: TableSpec, partitioned: bool = True) -> bigquery.LoadJobConfig:
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[bigquery.SchemaField(name, kind) for name, kind in spec.columns],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if spec.partition_field and partitioned:
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
    return _verified_rows(job, spec.name, expected_rows)


def _verified_rows(job: bigquery.LoadJob, table: str, expected_rows: int) -> int:
    job.result()
    if job.output_rows != expected_rows:
        raise LoadError(
            f"{table}: loaded {job.output_rows} rows but the Parquet files have {expected_rows}"
        )
    return job.output_rows


def merge_parquet(files: list[Path], destination: Path, schema: pa.Schema) -> Path:
    """Concatenate Parquet files into one, one file at a time (memory stays small).

    The generator already writes every file with the table's exact schema, so rows are copied
    unchanged. One file means one load job instead of one per day partition.
    """
    with pq.ParquetWriter(destination, schema) as writer:
        for path in files:
            writer.write_table(pq.read_table(path).cast(schema))
    return destination


def load_table_from_local(
    client: bigquery.Client,
    settings: Settings,
    spec: TableSpec,
    files: list[Path],
    work_dir: Path,
) -> int:
    """Sandbox path: load local Parquet directly (no GCS, no date partitioning)."""
    expected = count_rows(files)
    source = files[0] if len(files) == 1 else work_dir / f"{spec.name}.parquet"
    if len(files) > 1:
        merge_parquet(files, source, spec.arrow_schema)
    with source.open("rb") as handle:
        job = client.load_table_from_file(
            handle,
            settings.table_id(spec.name),
            job_config=build_job_config(spec, partitioned=False),
            location=settings.location,
        )
        return _verified_rows(job, spec.name, expected)


def plan(settings: Settings, data_dir: Path, tables: list[str], sandbox: bool = False) -> list[str]:
    lines = [
        f"project   : {settings.project_id}",
        f"location  : {settings.location}",
        "mode      : sandbox (direct load, no GCS, no date partitioning)"
        if sandbox
        else f"mode      : GCS staging at gs://{settings.bucket}/{settings.prefix}/",
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
    sandbox: bool = False,
) -> dict[str, int]:
    """Load the given tables. Returns loaded row counts per table.

    With ``sandbox=True`` nothing touches Cloud Storage (it needs a billing account).
    """
    for name in tables:  # validate all files first: nothing is uploaded if any file is stale
        validate_files(TABLES[name], local_files(data_dir, name))

    bq_client = bq_client or bigquery.Client(project=settings.project_id)
    ensure_dataset(bq_client, settings)

    loaded: dict[str, int] = {}
    if sandbox:
        with tempfile.TemporaryDirectory() as work:
            for name in tables:
                files = local_files(data_dir, name)
                loaded[name] = load_table_from_local(
                    bq_client, settings, TABLES[name], files, Path(work)
                )
                print(f"{name:<22}{loaded[name]:>12,} rows  (loaded from {len(files)} file(s))")
        return loaded

    storage_client = storage_client or storage.Client(project=settings.project_id)
    bucket = storage_client.bucket(settings.bucket)
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
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="no billing account: load local files directly, skip GCS and date partitioning",
    )
    parser.add_argument("--dry-run", action="store_true", help="show the plan, touch nothing")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = Settings.build(args.project, args.bucket, args.location, args.dataset)
    data_dir = Path(args.data_dir)
    tables = args.tables or list(TABLES)

    if args.dry_run:
        print("\n".join(plan(settings, data_dir, tables, args.sandbox)))
        return
    run(settings, data_dir, tables, sandbox=args.sandbox)
    print(f"\nDone. Query it: SELECT COUNT(*) FROM `{settings.table_id('orders')}`")


if __name__ == "__main__":
    main()
