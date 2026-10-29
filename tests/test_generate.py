import pandas as pd
import pytest

from fashion_ec.generate import (
    CATEGORY_SPECS,
    JST_OFFSET,
    GenerationConfig,
    generate_all,
    write_tables,
)

CFG = GenerationConfig.small()


@pytest.fixture(scope="module")
def tables() -> dict[str, pd.DataFrame]:
    return generate_all(CFG)


def local_date(timestamps: pd.Series) -> pd.Series:
    """UTC timestamps -> JST calendar date (the business date)."""
    return (timestamps + JST_OFFSET).dt.normalize()


# --- reproducibility --------------------------------------------------------


def test_same_seed_gives_identical_data(tables):
    again = generate_all(CFG)
    for name, frame in tables.items():
        pd.testing.assert_frame_equal(frame, again[name], obj=name)


def test_different_seed_gives_different_data(tables):
    other = generate_all(GenerationConfig.small(seed=7))
    assert not tables["orders"]["customer_id"].equals(other["orders"]["customer_id"])


@pytest.mark.parametrize("field", ["days", "n_styles", "n_customers", "n_orders", "n_sessions"])
def test_config_rejects_non_positive_sizes(field):
    with pytest.raises(ValueError, match=field):
        GenerationConfig(**{field: 0})


# --- keys -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("table", "key"),
    [
        ("products", ["sku"]),
        ("customers", ["customer_id"]),
        ("orders", ["order_id"]),
        ("order_items", ["order_item_id"]),
        ("order_items", ["order_id", "sku"]),
        ("returns", ["return_id"]),
        ("returns", ["order_item_id"]),
        ("inventory_snapshots", ["snapshot_date", "sku"]),
        ("clickstream", ["event_id"]),
        ("price_changes", ["style_id", "effective_date"]),
    ],
)
def test_keys_are_unique_and_not_null(tables, table, key):
    frame = tables[table]
    assert frame[key].notna().all().all()
    assert not frame.duplicated(key).any()


@pytest.mark.parametrize(
    ("child", "column", "parent", "parent_column"),
    [
        ("orders", "customer_id", "customers", "customer_id"),
        ("order_items", "order_id", "orders", "order_id"),
        ("order_items", "sku", "products", "sku"),
        ("returns", "order_item_id", "order_items", "order_item_id"),
        ("inventory_snapshots", "sku", "products", "sku"),
        ("clickstream", "sku", "products", "sku"),
        ("price_changes", "style_id", "products", "style_id"),
    ],
)
def test_foreign_keys_resolve(tables, child, column, parent, parent_column):
    assert tables[child][column].isin(tables[parent][parent_column]).all()


def test_clickstream_customer_id_is_optional_but_valid(tables):
    customer_id = tables["clickstream"]["customer_id"]
    assert customer_id.isna().any()
    assert customer_id.dropna().isin(tables["customers"]["customer_id"]).all()


# --- orders and prices ------------------------------------------------------


def test_orders_fall_inside_window_in_jst(tables):
    days = local_date(tables["orders"]["order_ts"])
    assert days.min() >= pd.Timestamp(CFG.start_date)
    assert days.max() <= pd.Timestamp(CFG.end_date)


def test_order_count_matches_config(tables):
    assert len(tables["orders"]) == CFG.n_orders


def test_nothing_sells_before_launch_or_signup(tables):
    lines = (
        tables["order_items"]
        .merge(tables["orders"], on="order_id")
        .merge(tables["products"][["sku", "launch_date"]], on="sku")
        .merge(tables["customers"][["customer_id", "signup_date"]], on="customer_id")
    )
    order_day = local_date(lines["order_ts"])
    assert (order_day >= lines["launch_date"]).all()
    assert (order_day >= lines["signup_date"]).all()


def test_unit_price_is_the_price_in_effect_on_order_date(tables):
    items = tables["order_items"].merge(tables["orders"], on="order_id")
    items = items.merge(tables["products"][["sku", "style_id"]], on="sku")
    history = tables["price_changes"]
    for row in items.sample(80, random_state=0).itertuples():
        order_day = (row.order_ts + JST_OFFSET).normalize()
        past = history[
            (history["style_id"] == row.style_id) & (history["effective_date"] <= order_day)
        ]
        expected = past.sort_values("effective_date").iloc[-1]["price"]
        assert row.unit_price == expected


def test_markdowns_lower_the_price(tables):
    history = tables["price_changes"].sort_values(["style_id", "effective_date"])
    previous = history.groupby("style_id")["price"].shift()
    markdowns = history["change_reason"] == "markdown"
    assert markdowns.any()
    assert (history.loc[markdowns, "price"] < previous[markdowns]).all()


def test_some_orders_are_cancelled(tables):
    status = tables["orders"]["status"]
    assert set(status) == {"completed", "cancelled"}
    assert status.eq("cancelled").mean() < 0.15


def test_free_shipping_above_threshold(tables):
    subtotal = (
        (tables["order_items"]["unit_price"] * tables["order_items"]["quantity"])
        .groupby(tables["order_items"]["order_id"])
        .sum()
    )
    orders = tables["orders"].assign(subtotal=lambda df: df["order_id"].map(subtotal))
    assert (orders.loc[orders["subtotal"] >= 5_000, "shipping_fee"] == 0).all()
    assert (orders.loc[orders["subtotal"] < 5_000, "shipping_fee"] > 0).all()


# --- returns ----------------------------------------------------------------


def test_returns_come_after_completed_orders_only(tables):
    joined = (
        tables["returns"]
        .merge(tables["order_items"], on="order_item_id")
        .merge(tables["orders"], on="order_id")
    )
    assert (joined["status"] == "completed").all()
    assert (joined["return_ts"] > joined["order_ts"]).all()


def test_refund_equals_line_amount(tables):
    joined = tables["returns"].merge(tables["order_items"], on="order_item_id")
    assert (joined["refund_amount"] == joined["unit_price"] * joined["quantity"]).all()


def test_returns_are_cut_off_at_window_end(tables):
    cutoff = pd.Timestamp(CFG.end_date) + pd.Timedelta(days=1) - JST_OFFSET
    assert (tables["returns"]["return_ts"] < cutoff).all()


def test_return_rate_is_plausible_and_varies_by_category():
    # A larger sample than the shared fixture, so the category comparison is stable.
    # The window is long because returns arrive 3-30 days after the order: in a short window
    # many returns fall outside it (right-censoring) and the observed rate is understated.
    big = generate_all(
        GenerationConfig.small(days=120, n_orders=20_000, n_customers=2_000, n_styles=60)
    )
    lines = big["order_items"].merge(big["products"][["sku", "category"]], on="sku")
    returned = lines["order_item_id"].isin(big["returns"]["order_item_id"])
    rate = returned.groupby(lines["category"]).mean()
    assert 0.08 < returned.mean() < 0.30
    assert rate["accessories"] < rate["dresses"]
    assert set(rate.index) == set(CATEGORY_SPECS)


def test_one_size_items_have_no_size_return_reasons(tables):
    joined = (
        tables["returns"]
        .merge(tables["order_items"][["order_item_id", "sku"]], on="order_item_id")
        .merge(tables["products"][["sku", "size"]], on="sku")
    )
    one_size = joined[joined["size"] == "F"]
    assert not one_size["reason"].isin(["size_too_small", "size_too_large"]).any()


# --- inventory --------------------------------------------------------------


def test_inventory_is_never_negative(tables):
    assert (tables["inventory_snapshots"]["on_hand"] >= 0).all()


def test_inventory_starts_at_initial_stock_and_only_exists_after_launch(tables):
    inventory = tables["inventory_snapshots"].merge(tables["products"], on="sku")
    assert (inventory["snapshot_date"] >= inventory["launch_date"]).all()
    first_day = inventory.sort_values("snapshot_date").groupby("sku").first()
    # Stock on a SKU's first day is initial stock minus that day's net sales, so never above it.
    assert (first_day["on_hand"] <= first_day["initial_stock"]).all()


def test_inventory_covers_every_day_for_each_live_sku(tables):
    snapshots = tables["inventory_snapshots"]
    per_sku_days = snapshots.groupby("sku")["snapshot_date"].agg(["min", "max", "count"])
    span = (per_sku_days["max"] - per_sku_days["min"]).dt.days + 1
    assert (span == per_sku_days["count"]).all()


# --- clickstream ------------------------------------------------------------


def test_clickstream_funnel_narrows(tables):
    counts = tables["clickstream"]["event_type"].value_counts()
    assert set(counts.index) == {"page_view", "add_to_cart", "purchase"}
    assert counts["page_view"] > counts["add_to_cart"] > counts["purchase"] > 0


def test_clickstream_is_time_ordered_and_has_no_session_id(tables):
    events = tables["clickstream"]
    assert events["event_ts"].is_monotonic_increasing
    assert "session_id" not in events.columns


def test_visitors_return_in_multiple_sessions(tables):
    events = tables["clickstream"].sort_values(["anonymous_id", "event_ts"])
    gap = events.groupby("anonymous_id")["event_ts"].diff()
    assert (gap > pd.Timedelta(minutes=30)).any()


# --- output -----------------------------------------------------------------


def test_write_tables_round_trip(tables, tmp_path):
    write_tables(tables, tmp_path)

    products = pd.read_parquet(tmp_path / "products" / "products.parquet")
    assert len(products) == len(tables["products"])
    assert str(products["launch_date"].dtype).startswith("object") or "date" in str(
        products["launch_date"].dtype
    )

    partitions = sorted((tmp_path / "clickstream").glob("event_date=*"))
    assert partitions
    total = sum(len(pd.read_parquet(p)) for p in partitions)
    assert total == len(tables["clickstream"])


def test_write_tables_replaces_previous_output(tables, tmp_path):
    write_tables(tables, tmp_path)
    stale = tmp_path / "clickstream" / "event_date=1999-01-01"
    stale.mkdir()
    write_tables(tables, tmp_path)
    assert not stale.exists()
