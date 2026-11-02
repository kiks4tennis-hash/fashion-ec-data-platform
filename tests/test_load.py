"""Loader tests with fake GCP clients. No network and no credentials are used."""

from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from fashion_ec.generate import GenerationConfig, generate_all, write_tables
from fashion_ec.load import (
    LoadError,
    Settings,
    build_job_config,
    count_rows,
    load_table,
    local_files,
    main,
    plan,
    run,
    upload_table,
)
from fashion_ec.schemas import TABLES

SETTINGS = Settings(project_id="demo-project", bucket="demo-bucket")


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    write_tables(generate_all(GenerationConfig.small()), out)
    return out


def fake_clients(output_rows=None, stale_blobs=()):
    """Fake storage and BigQuery clients. ``output_rows`` maps table name -> loaded rows."""
    bucket = MagicMock()
    bucket.list_blobs.return_value = list(stale_blobs)
    storage_client = MagicMock()
    storage_client.bucket.return_value = bucket

    bq_client = MagicMock()

    def load_from_uri(uri, table_id, job_config, location):
        job = MagicMock()
        job.output_rows = (output_rows or {}).get(table_id.rsplit(".", 1)[1])
        return job

    bq_client.load_table_from_uri.side_effect = load_from_uri
    return storage_client, bucket, bq_client


# --- settings ---------------------------------------------------------------


def test_settings_defaults_and_derived_bucket():
    settings = Settings.build(project="p1", env={})
    assert settings.project_id == "p1"
    assert settings.bucket == "p1-fashion-ec"
    assert settings.location == "asia-northeast1"
    assert settings.dataset == "raw"


def test_settings_read_environment_and_explicit_values_win():
    env = {"GCP_PROJECT_ID": "from-env", "GCS_BUCKET": "env-bucket", "BQ_LOCATION": "US"}
    from_env = Settings.build(env=env)
    assert (from_env.project_id, from_env.bucket, from_env.location) == (
        "from-env",
        "env-bucket",
        "US",
    )
    explicit = Settings.build(project="cli", bucket="cli-bucket", env=env)
    assert (explicit.project_id, explicit.bucket) == ("cli", "cli-bucket")


def test_settings_require_a_project():
    with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
        Settings.build(env={})


def test_uris_and_table_ids():
    assert SETTINGS.gcs_uri("orders") == "gs://demo-bucket/raw/orders/*"
    assert SETTINGS.table_id("orders") == "demo-project.raw.orders"


# --- local files ------------------------------------------------------------


def test_local_files_and_row_counts(data_dir):
    assert len(local_files(data_dir, "clickstream")) > 1  # one file per day partition
    assert len(local_files(data_dir, "products")) == 1
    assert count_rows(local_files(data_dir, "orders")) == GenerationConfig.small().n_orders


def test_missing_data_gives_a_helpful_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="fashion_ec.generate"):
        local_files(tmp_path, "orders")


# --- upload -----------------------------------------------------------------


def test_upload_replaces_prefix_and_keeps_partition_folders(data_dir):
    _, bucket, _ = fake_clients(stale_blobs=["old-blob"])
    n_files = upload_table(bucket, SETTINGS, data_dir, "clickstream")

    bucket.delete_blobs.assert_called_once_with(["old-blob"])
    names = [call.args[0] for call in bucket.blob.call_args_list]
    assert len(names) == n_files > 1
    assert all(name.startswith("raw/clickstream/event_date=") for name in names)
    assert all("\\" not in name for name in names)  # object names always use '/'
    assert bucket.blob.return_value.upload_from_filename.call_count == n_files


def test_upload_skips_delete_when_prefix_is_empty(data_dir):
    _, bucket, _ = fake_clients()
    upload_table(bucket, SETTINGS, data_dir, "products")
    bucket.delete_blobs.assert_not_called()
    bucket.blob.assert_called_once_with("raw/products/products.parquet")


# --- load jobs --------------------------------------------------------------


def test_job_config_for_partitioned_table():
    config = build_job_config(TABLES["clickstream"])
    assert config.source_format == bigquery.SourceFormat.PARQUET
    assert config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert config.time_partitioning.field == "event_ts"
    assert config.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert config.clustering_fields == ["event_type", "anonymous_id"]
    assert [(f.name, f.field_type) for f in config.schema][:2] == [
        ("event_id", "STRING"),
        ("event_ts", "TIMESTAMP"),
    ]


def test_job_config_for_plain_table():
    config = build_job_config(TABLES["products"])
    assert config.time_partitioning is None
    assert config.clustering_fields is None
    assert len(config.schema) == len(TABLES["products"].columns)


def test_load_table_checks_row_count():
    _, _, bq_client = fake_clients(output_rows={"orders": 10})
    assert load_table(bq_client, SETTINGS, TABLES["orders"], expected_rows=10) == 10

    _, _, bq_client = fake_clients(output_rows={"orders": 9})
    with pytest.raises(LoadError, match="orders.*9.*10"):
        load_table(bq_client, SETTINGS, TABLES["orders"], expected_rows=10)


def test_load_table_uses_gcs_wildcard_and_location():
    _, _, bq_client = fake_clients(output_rows={"orders": 1})
    load_table(bq_client, SETTINGS, TABLES["orders"], expected_rows=1)
    args, kwargs = bq_client.load_table_from_uri.call_args
    assert args == ("gs://demo-bucket/raw/orders/*", "demo-project.raw.orders")
    assert kwargs["location"] == "asia-northeast1"


# --- end to end with fakes --------------------------------------------------


def test_run_loads_every_table_and_returns_counts(data_dir):
    expected = {name: count_rows(local_files(data_dir, name)) for name in TABLES}
    storage_client, _, bq_client = fake_clients(output_rows=expected)

    loaded = run(SETTINGS, data_dir, list(TABLES), storage_client, bq_client)

    assert loaded == expected
    bq_client.create_dataset.assert_called_once()
    assert bq_client.load_table_from_uri.call_count == len(TABLES)


def test_run_stops_on_a_row_count_mismatch(data_dir):
    storage_client, _, bq_client = fake_clients(output_rows={"products": 1})
    with pytest.raises(LoadError):
        run(SETTINGS, data_dir, ["products"], storage_client, bq_client)


# --- CLI --------------------------------------------------------------------


def test_dry_run_prints_plan_without_touching_gcp(data_dir, capsys, monkeypatch):
    # Any attempt to build a real client would fail here, proving dry-run is offline.
    monkeypatch.setattr("fashion_ec.load.storage.Client", MagicMock(side_effect=AssertionError))
    monkeypatch.setattr("fashion_ec.load.bigquery.Client", MagicMock(side_effect=AssertionError))

    main(["--project", "demo-project", "--data-dir", str(data_dir), "--dry-run"])

    out = capsys.readouterr().out
    assert "demo-project.raw.orders" in out
    assert "gs://demo-project-fashion-ec/raw/" in out


def test_plan_lists_only_requested_tables(data_dir):
    lines = plan(SETTINGS, data_dir, ["orders", "returns"])
    text = "\n".join(lines)
    assert "demo-project.raw.orders" in text
    assert "demo-project.raw.returns" in text
    assert "clickstream" not in text
