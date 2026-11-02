"""Single source of truth for the raw tables: column names, BigQuery types, layout.

Used by the Parquet writer (to pick date/timestamp conversions) and by the BigQuery loader
(explicit schema, partitioning, clustering). Tests check that generated data matches it, so
the generator and the warehouse cannot drift apart silently.

All columns are NULLABLE on purpose: Parquet nullability does not map cleanly to REQUIRED
columns, and "not null" is enforced by dbt tests instead.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

ARROW_TYPES: dict[str, pa.DataType] = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "DATE": pa.date32(),
    # UTC-adjusted microseconds: BigQuery reads this as TIMESTAMP (naive would be DATETIME).
    "TIMESTAMP": pa.timestamp("us", tz="UTC"),
}


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[tuple[str, str], ...]  # (column name, BigQuery type)
    partition_field: str | None = None  # DAY partitioning on a DATE/TIMESTAMP column
    cluster_fields: tuple[str, ...] = ()

    @property
    def column_names(self) -> list[str]:
        return [name for name, _ in self.columns]

    @property
    def arrow_schema(self) -> pa.Schema:
        """Explicit Parquet schema, so types never depend on pandas inference (e.g. an
        all-null column would otherwise be written as the Arrow ``null`` type)."""
        return pa.schema([pa.field(name, ARROW_TYPES[kind]) for name, kind in self.columns])

    def columns_of_type(self, bq_type: str) -> list[str]:
        return [name for name, kind in self.columns if kind == bq_type]


TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TableSpec(
            "products",
            (
                ("sku", "STRING"),
                ("style_id", "STRING"),
                ("category", "STRING"),
                ("color", "STRING"),
                ("size", "STRING"),
                ("season", "STRING"),
                ("launch_date", "DATE"),
                ("base_price", "INT64"),
                ("initial_stock", "INT64"),
            ),
        ),
        TableSpec(
            "price_changes",
            (
                ("style_id", "STRING"),
                ("effective_date", "DATE"),
                ("price", "INT64"),
                ("change_reason", "STRING"),
            ),
        ),
        TableSpec(
            "customers",
            (
                ("customer_id", "STRING"),
                ("signup_date", "DATE"),
                ("region", "STRING"),
                ("acquisition_channel", "STRING"),
            ),
        ),
        TableSpec(
            "orders",
            (
                ("order_id", "STRING"),
                ("customer_id", "STRING"),
                ("order_ts", "TIMESTAMP"),
                ("status", "STRING"),
                ("payment_method", "STRING"),
                ("shipping_fee", "INT64"),
            ),
            partition_field="order_ts",
            cluster_fields=("customer_id",),
        ),
        TableSpec(
            "order_items",
            (
                ("order_item_id", "STRING"),
                ("order_id", "STRING"),
                ("sku", "STRING"),
                ("quantity", "INT64"),
                ("unit_price", "INT64"),
            ),
        ),
        TableSpec(
            "returns",
            (
                ("return_id", "STRING"),
                ("order_item_id", "STRING"),
                ("return_ts", "TIMESTAMP"),
                ("reason", "STRING"),
                ("refund_amount", "INT64"),
            ),
        ),
        TableSpec(
            "inventory_snapshots",
            (
                ("snapshot_date", "DATE"),
                ("sku", "STRING"),
                ("on_hand", "INT64"),
            ),
            partition_field="snapshot_date",
            cluster_fields=("sku",),
        ),
        TableSpec(
            "clickstream",
            (
                ("event_id", "STRING"),
                ("event_ts", "TIMESTAMP"),
                ("anonymous_id", "STRING"),
                ("customer_id", "STRING"),
                ("event_type", "STRING"),
                ("sku", "STRING"),
                ("device", "STRING"),
            ),
            partition_field="event_ts",
            cluster_fields=("event_type", "anonymous_id"),
        ),
    )
}
