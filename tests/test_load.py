"""Loader tests with fake GCP clients. No network and no credentials are used."""

from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest
from google.cloud import bigquery

from fashion_ec.generate import GenerationConfig, generate_all, write_tables
from fashion_ec.load import (
    LoadError,
    Settings,
    build_job_config,
    count_rows,
    load_table,
    load_table_from_local,
    local_files,
    main,
    merge_parquet,
    plan,
    run,
    upload_table,
    validate_files,
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


# --- sandbox (no billing account, no GCS) -----------------------------------


def fake_sandbox_client():
    """BigQuery fake whose load job reports the rows actually present in the uploaded file."""
    client = MagicMock()
    uploaded = []

    def load_from_file(handle, table_id, job_config, location):
        table = pq.read_table(handle)  # read now: the merged temp file is deleted later
        uploaded.append((table_id, table.num_rows, job_config))
        job = MagicMock()
        job.output_rows = table.num_rows
        return job

    client.load_table_from_file.side_effect = load_from_file
    return client, uploaded


def test_sandbox_job_config_drops_partitioning_but_keeps_clustering():
    config = build_job_config(TABLES["clickstream"], partitioned=False)
    assert config.time_partitioning is None
    assert config.clustering_fields == ["event_type", "anonymous_id"]
    assert len(config.schema) == len(TABLES["clickstream"].columns)


def test_merge_parquet_keeps_every_row_in_order(data_dir, tmp_path):
    files = local_files(data_dir, "clickstream")
    merged = merge_parquet(files, tmp_path / "merged.parquet", TABLES["clickstream"].arrow_schema)

    table = pq.read_table(merged)
    assert table.num_rows == count_rows(files)
    assert table.schema.equals(TABLES["clickstream"].arrow_schema, check_metadata=False)
    event_ids = table.column("event_id").to_pylist()
    assert event_ids == sorted(event_ids)  # day partitions are concatenated chronologically


def test_sandbox_load_merges_partition_files_into_one_job(data_dir, tmp_path):
    client, uploaded = fake_sandbox_client()
    files = local_files(data_dir, "clickstream")
    assert len(files) > 1

    rows = load_table_from_local(client, SETTINGS, TABLES["clickstream"], files, tmp_path)

    assert rows == count_rows(files)
    assert client.load_table_from_file.call_count == 1
    assert uploaded[0][0] == "demo-project.raw.clickstream"
    assert uploaded[0][2].time_partitioning is None


def test_sandbox_load_uses_a_single_file_as_is(data_dir, tmp_path):
    client, uploaded = fake_sandbox_client()
    load_table_from_local(
        client, SETTINGS, TABLES["products"], local_files(data_dir, "products"), tmp_path
    )
    assert uploaded[0][1] == len(generate_all(GenerationConfig.small())["products"])
    assert list(tmp_path.iterdir()) == []  # nothing had to be merged


def test_sandbox_load_checks_row_count(data_dir, tmp_path):
    client = MagicMock()
    client.load_table_from_file.return_value.output_rows = 1
    with pytest.raises(LoadError, match="products"):
        load_table_from_local(
            client, SETTINGS, TABLES["products"], local_files(data_dir, "products"), tmp_path
        )


def test_run_in_sandbox_mode_never_touches_cloud_storage(data_dir, monkeypatch):
    monkeypatch.setattr("fashion_ec.load.storage.Client", MagicMock(side_effect=AssertionError))
    client, uploaded = fake_sandbox_client()

    loaded = run(SETTINGS, data_dir, list(TABLES), bq_client=client, sandbox=True)

    assert set(loaded) == set(TABLES)
    assert loaded["orders"] == GenerationConfig.small().n_orders
    assert [table_id for table_id, _, _ in uploaded] == [SETTINGS.table_id(t) for t in TABLES]
    client.load_table_from_uri.assert_not_called()
    client.create_dataset.assert_called_once()


def test_sandbox_dry_run_describes_the_mode(data_dir, capsys, monkeypatch):
    monkeypatch.setattr("fashion_ec.load.storage.Client", MagicMock(side_effect=AssertionError))
    monkeypatch.setattr("fashion_ec.load.bigquery.Client", MagicMock(side_effect=AssertionError))

    main(["--project", "demo-project", "--data-dir", str(data_dir), "--sandbox", "--dry-run"])

    out = capsys.readouterr().out
    assert "sandbox" in out
    assert "gs://" not in out


# --- stale files from an older generator ------------------------------------


def test_validate_accepts_current_files(data_dir):
    for name, spec in TABLES.items():
        validate_files(spec, local_files(data_dir, name))


def test_validate_accepts_large_string_columns(tmp_path):
    import pyarrow as pa

    spec = TABLES["price_changes"]
    schema = pa.schema(
        [
            pa.field(f.name, pa.large_string() if pa.types.is_string(f.type) else f.type)
            for f in spec.arrow_schema
        ]
    )
    path = tmp_path / "x.parquet"
    pq.write_table(schema.empty_table(), path)
    validate_files(spec, [path])


def test_validate_rejects_old_naive_timestamps_with_a_regenerate_hint(tmp_path):
    import pyarrow as pa

    spec = TABLES["returns"]
    schema = pa.schema(
        [
            pa.field(f.name, pa.timestamp("ns") if f.name == "return_ts" else f.type)
            for f in spec.arrow_schema
        ]
    )
    path = tmp_path / "old.parquet"
    pq.write_table(schema.empty_table(), path)

    with pytest.raises(LoadError, match=r"return_ts.*fashion_ec\.generate"):
        validate_files(spec, [path])


def test_validate_rejects_missing_and_extra_columns(tmp_path):
    import pyarrow as pa

    spec = TABLES["customers"]
    missing = pa.schema(list(spec.arrow_schema)[:-1])
    extra = pa.schema([*spec.arrow_schema, pa.field("surprise", pa.int64())])
    for name, schema in {"missing": missing, "extra": extra}.items():
        path = tmp_path / f"{name}.parquet"
        pq.write_table(schema.empty_table(), path)
        with pytest.raises(LoadError, match="customers"):
            validate_files(spec, [path])


def test_run_stops_before_loading_anything_if_a_file_is_stale(data_dir, tmp_path):
    import shutil

    import pyarrow as pa

    broken = tmp_path / "raw"
    shutil.copytree(data_dir, broken)
    stale = pa.schema([pa.field("sku", pa.string())])
    pq.write_table(stale.empty_table(), broken / "returns" / "returns.parquet")

    client, _ = fake_sandbox_client()
    with pytest.raises(LoadError, match="returns"):
        run(SETTINGS, broken, list(TABLES), bq_client=client, sandbox=True)
    client.create_dataset.assert_not_called()
    client.load_table_from_file.assert_not_called()
