"""The generator output and the BigQuery schema definitions must agree."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fashion_ec.generate import GenerationConfig, generate_all, write_tables
from fashion_ec.schemas import TABLES


@pytest.fixture(scope="module")
def parquet_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    write_tables(generate_all(GenerationConfig.small()), out)
    return out


def bigquery_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "STRING"
    if pa.types.is_integer(arrow_type):
        return "INT64"
    if pa.types.is_date32(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        # BigQuery reads UTC-adjusted microsecond timestamps as TIMESTAMP, others as DATETIME.
        assert arrow_type.tz == "UTC", "timestamps must be timezone-aware UTC"
        assert arrow_type.unit == "us", "timestamps must have microsecond precision"
        return "TIMESTAMP"
    raise AssertionError(f"unmapped Arrow type: {arrow_type}")


def test_every_generated_table_has_a_schema(parquet_dir):
    written = {p.name for p in parquet_dir.iterdir()}
    assert written == set(TABLES)


@pytest.mark.parametrize("table", list(TABLES))
def test_parquet_columns_match_schema(parquet_dir, table):
    spec = TABLES[table]
    for path in (parquet_dir / table).rglob("*.parquet"):
        schema = pq.read_schema(path)
        actual = [(field.name, bigquery_type(field.type)) for field in schema]
        assert actual == list(spec.columns), path


@pytest.mark.parametrize("table", list(TABLES))
def test_partition_and_cluster_columns_exist(table):
    spec = TABLES[table]
    names = spec.column_names
    if spec.partition_field:
        assert spec.partition_field in names
        assert dict(spec.columns)[spec.partition_field] in {"DATE", "TIMESTAMP"}
    assert set(spec.cluster_fields) <= set(names)
    assert len(spec.cluster_fields) <= 4  # BigQuery limit


def test_all_null_column_keeps_its_declared_type(tmp_path):
    """A day with no logged-in visitors must not turn customer_id into the Arrow null type.

    BigQuery would reject a null-typed Parquet column for a STRING field.
    """
    tables = generate_all(GenerationConfig.small())
    tables["clickstream"]["customer_id"] = None
    write_tables(tables, tmp_path)

    for path in (tmp_path / "clickstream").rglob("*.parquet"):
        assert bigquery_type(pq.read_schema(path).field("customer_id").type) == "STRING"
