"""Run the whole dbt project on DuckDB and check it against independent calculations.

`dbt build` needs BigQuery. These tests run the same models without it: each model's BigQuery
SQL is rendered like dbt does, translated to DuckDB with sqlglot and executed on freshly
generated data. Then:

1. every dbt data test (the generic ones in the yml files and the business-rule ones in
   `dbt/tests/`) is executed and must return no rows;
2. the marts are compared with a second implementation written in pandas, so a mistake in
   the SQL logic (a wrong join, a time-zone slip, an off-by-one in a window) is caught even
   when all data tests pass;
3. the session rules are probed at their exact boundaries with hand-made events.

Differences between DuckDB and BigQuery can still exist; `dbt build` on BigQuery is the final
word. What these tests rule out is a wrong *logic*, which is the bug that hides longest.
"""

import re
from datetime import date

import duckdb
import jinja2
import pandas as pd
import pytest
import sqlglot
import yaml

from dbt_render import (
    MODELS_DIR,
    TESTS_DIR,
    model_files,
    render_sql,
    singular_test_files,
)
from fashion_ec.generate import JST_OFFSET, GenerationConfig, generate_all, write_tables

CLICKSTREAM_DDL = """
CREATE TABLE raw.clickstream (
    event_id VARCHAR, event_ts TIMESTAMPTZ, anonymous_id VARCHAR, customer_id VARCHAR,
    event_type VARCHAR, sku VARCHAR, device VARCHAR
)
"""
REF = re.compile(r"ref\(\s*'(\w+)'\s*\)")
CONFIG = GenerationConfig.small(
    days=120, n_orders=3_000, n_customers=500, n_styles=40, n_sessions=6_000
)
SESSION_GAP = pd.Timedelta(minutes=30)
RETURN_WINDOW = pd.Timedelta(days=30)


# --- running the project on DuckDB ------------------------------------------


def new_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA raw")
    return con


def to_duckdb(bigquery_sql: str) -> str:
    return sqlglot.transpile(bigquery_sql, read="bigquery", write="duckdb")[0]


def run_order(only: list[str] | None = None) -> list[str]:
    """Model names in dependency order; with `only`, just those and what they depend on."""
    files = model_files()
    dependencies = {name: set(REF.findall(path.read_text())) for name, path in files.items()}
    wanted = set(dependencies) if only is None else set()
    for name in only or []:
        stack = [name]
        while stack:
            current = stack.pop()
            if current not in wanted:
                wanted.add(current)
                stack.extend(dependencies[current])
    order: list[str] = []
    while len(order) < len(wanted):
        ready = [n for n in sorted(wanted) if n not in order and dependencies[n] <= set(order)]
        assert ready, "circular model references"
        order.extend(ready)
    return order


def build_models(con, only=None, extra_vars=None) -> None:
    files = model_files()
    for name in run_order(only):
        sql = render_sql(
            files[name],
            ref=lambda model: f'"{model}"',
            source=lambda source, table: f"{source}.{table}",
            extra_vars=extra_vars,
        )
        con.execute(f'CREATE OR REPLACE TABLE "{name}" AS {to_duckdb(sql)}')


@pytest.fixture(scope="module")
def tables():
    return generate_all(CONFIG)


@pytest.fixture(scope="module")
def warehouse(tables, tmp_path_factory):
    raw_dir = tmp_path_factory.mktemp("raw")
    write_tables(tables, raw_dir)
    con = new_connection()
    for name in tables:
        pattern = "*/*.parquet" if name == "clickstream" else f"{name}.parquet"
        con.execute(
            f"CREATE VIEW raw.{name} AS SELECT * FROM read_parquet('{raw_dir}/{name}/{pattern}')"
        )
    build_models(con)
    return con


def frame(con, name: str) -> pd.DataFrame:
    """A model as a DataFrame; dates become timestamps and timestamps lose their time zone."""
    df = con.execute(f'SELECT * FROM "{name}"').df()
    for column in df.columns:
        if (
            df[column].dtype == object
            and df[column].dropna().map(lambda v: isinstance(v, date)).all()
        ):
            df[column] = pd.to_datetime(df[column])
        elif isinstance(df[column].dtype, pd.DatetimeTZDtype):
            df[column] = df[column].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


# --- 1. every dbt data test runs and passes ----------------------------------

GENERIC_MACROS = "\n".join(
    path.read_text().replace("{% test ", "{% macro ").replace("{% endtest %}", "{% endmacro %}")
    for path in sorted((TESTS_DIR / "generic").glob("*.sql"))
)


def parse_test(entry) -> tuple[str, dict]:
    if isinstance(entry, str):
        return entry, {}
    name, config = next(iter(entry.items()))
    config = config or {}
    return name, config.get("arguments", {k: v for k, v in config.items() if k != "config"})


def collect_generic_tests() -> list[tuple[str, str | None, str, dict]]:
    found = []
    for path in sorted(MODELS_DIR.rglob("*.yml")):
        for model in (yaml.safe_load(path.read_text()) or {}).get("models", []):
            for entry in model.get("data_tests", []):
                found.append((model["name"], None, *parse_test(entry)))
            for column in model.get("columns", []):
                for entry in column.get("data_tests", []):
                    found.append((model["name"], column["name"], *parse_test(entry)))
    return found


def quote(value) -> str:
    return f"'{value}'" if isinstance(value, str) else str(value)


def generic_test_sql(model: str, column: str | None, test: str, args: dict) -> str:
    target = f'"{model}"'
    if test == "unique":
        return f"SELECT {column} FROM {target} GROUP BY {column} HAVING count(*) > 1"
    if test == "not_null":
        return f"SELECT * FROM {target} WHERE {column} IS NULL"
    if test == "accepted_values":
        allowed = ", ".join(quote(v) for v in args["values"])
        return (
            f"SELECT {column} FROM {target} WHERE {column} IS NOT NULL "
            f"AND {column} NOT IN ({allowed})"
        )
    if test == "relationships":
        parent = REF.search(args["to"]).group(1)
        return (
            f"SELECT {column} FROM {target} WHERE {column} IS NOT NULL "
            f'AND {column} NOT IN (SELECT {args["field"]} FROM "{parent}")'
        )
    # project-defined generic tests: render the real macro from dbt/tests/generic
    arguments = ", ".join(f"{key}={value!r}" for key, value in args.items())
    call = (
        f"{test}({target!r}"
        + (f", {column!r}" if column else "")
        + (f", {arguments}" if arguments else "")
    )
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    sql = env.from_string(GENERIC_MACROS + "{{ " + call + ") }}").render().strip()
    return to_duckdb(sql)


GENERIC_TESTS = collect_generic_tests()
GENERIC_IDS = [f"{m}.{c or '*'}.{t}" for m, c, t, _ in GENERIC_TESTS]


def test_the_project_has_a_lot_of_data_tests():
    assert len(GENERIC_TESTS) >= 150
    assert len(singular_test_files()) >= 10


@pytest.mark.parametrize(("model", "column", "test", "args"), GENERIC_TESTS, ids=GENERIC_IDS)
def test_generic_data_test_passes(warehouse, model, column, test, args):
    failing = warehouse.execute(generic_test_sql(model, column, test, args)).df()
    assert failing.empty, f"{len(failing)} rows fail:\n{failing.head()}"


@pytest.mark.parametrize("name", list(singular_test_files()))
def test_business_rule_test_passes(warehouse, name):
    sql = render_sql(
        singular_test_files()[name],
        ref=lambda model: f'"{model}"',
        source=lambda source, table: f"{source}.{table}",
    )
    failing = warehouse.execute(to_duckdb(sql)).df()
    assert failing.empty, f"{len(failing)} rows fail:\n{failing.head()}"


# --- 2. marts equal an independent pandas implementation ---------------------


def jst_day(timestamps: pd.Series) -> pd.Series:
    return (timestamps + JST_OFFSET).dt.normalize()


def order_lines(tables) -> pd.DataFrame:
    """Order lines with order, product and return, computed straight from the raw frames."""
    returns = tables["returns"][["order_item_id", "return_ts", "refund_amount"]]
    lines = (
        tables["order_items"]
        .merge(tables["orders"], on="order_id")
        .merge(tables["products"], on="sku")
        .merge(returns, on="order_item_id", how="left")
    )
    lines["ordered_on_jst"] = jst_day(lines["order_ts"])
    lines["line_amount"] = lines["quantity"] * lines["unit_price"]
    lines["is_returned"] = lines["return_ts"].notna()
    return lines


def completed(lines: pd.DataFrame) -> pd.DataFrame:
    return lines[lines["status"] == "completed"]


def assert_same(actual: pd.DataFrame, expected: pd.DataFrame, keys: list[str]) -> None:
    columns = list(expected.columns)
    actual = actual[columns].copy()
    expected = expected.copy()
    for column in columns:  # DuckDB and pandas disagree on the datetime unit; compare on one
        if "datetime64" in str(actual[column].dtype) or "datetime64" in str(expected[column].dtype):
            actual[column] = actual[column].astype("datetime64[ns]")
            expected[column] = expected[column].astype("datetime64[ns]")
    actual = actual.sort_values(keys).reset_index(drop=True)
    expected = expected.sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, check_exact=False)


def test_daily_sales_match_pandas(warehouse, tables):
    lines = completed(order_lines(tables))
    expected = (
        lines.groupby(["ordered_on_jst", "category"])
        .agg(
            order_count=("order_id", "nunique"),
            sold_units=("quantity", "sum"),
            gross_sales_jpy=("line_amount", "sum"),
        )
        .reset_index()
    )
    assert_same(frame(warehouse, "fct_sales_daily"), expected, ["ordered_on_jst", "category"])


def test_japan_time_dates_differ_from_utc_dates_and_follow_jst(warehouse, tables):
    """Orders placed 15:00-23:59 UTC are the next day in Japan: the mart must follow Japan."""
    orders = tables["orders"].merge(
        frame(warehouse, "stg_orders"), left_on="order_id", right_on="order_id"
    )
    utc_day = orders["order_ts"].dt.normalize()
    assert (orders["ordered_on_jst"] != utc_day).any(), "test data has no late-evening UTC orders"
    assert (orders["ordered_on_jst"] == jst_day(orders["order_ts"])).all()


def test_customer_totals_match_pandas(warehouse, tables):
    lines = completed(order_lines(tables))
    lines = lines.assign(refund=lines["refund_amount"].fillna(0))
    per_customer = lines.groupby("customer_id").agg(
        completed_orders=("order_id", "nunique"),
        first_order_on_jst=("ordered_on_jst", "min"),
        last_order_on_jst=("ordered_on_jst", "max"),
        gross_sales_jpy=("line_amount", "sum"),
        refunded_jpy=("refund", "sum"),
    )
    expected = tables["customers"][["customer_id"]].merge(
        per_customer.reset_index(), on="customer_id", how="left"
    )
    for column in ["completed_orders", "gross_sales_jpy", "refunded_jpy"]:
        expected[column] = expected[column].fillna(0)
    expected["net_sales_jpy"] = expected["gross_sales_jpy"] - expected["refunded_jpy"]
    expected["is_repeat_customer"] = expected["completed_orders"] >= 2

    actual = frame(warehouse, "dim_customers")
    assert expected["is_repeat_customer"].any() and (~expected["is_repeat_customer"]).any()
    assert_same(actual, expected, ["customer_id"])


def test_return_rate_by_cohort_and_maturity_match_pandas(warehouse, tables):
    lines = completed(order_lines(tables))
    lines = lines.assign(order_month=lines["ordered_on_jst"].dt.to_period("M").dt.to_timestamp())
    cohorts = (
        lines.groupby(["order_month", "category"])
        .apply(
            lambda g: pd.Series(
                {
                    "sold_units": g["quantity"].sum(),
                    "returned_units": g.loc[g["is_returned"], "quantity"].sum(),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    cohorts["return_rate"] = cohorts["returned_units"] / cohorts["sold_units"]
    as_of = jst_day(tables["orders"]["order_ts"]).max()
    month_end = cohorts["order_month"] + pd.offsets.MonthEnd(0)
    cohorts["is_mature"] = month_end + RETURN_WINDOW <= as_of

    actual = frame(warehouse, "fct_returns_by_cohort")
    assert cohorts["is_mature"].any() and (~cohorts["is_mature"]).any()
    assert_same(actual, cohorts, ["order_month", "category"])


def test_recent_cohorts_look_better_only_because_returns_are_still_arriving(warehouse):
    """The reason is_mature exists: the newest month has an artificially low return rate."""
    cohorts = frame(warehouse, "fct_returns_by_cohort")
    by_month = cohorts.groupby("order_month").agg(
        sold=("sold_units", "sum"), returned=("returned_units", "sum"), mature=("is_mature", "all")
    )
    rates = by_month["returned"] / by_month["sold"]
    assert rates[by_month["mature"]].mean() > rates[~by_month["mature"]].iloc[-1]


def test_refunds_by_return_day_match_pandas(warehouse, tables):
    lines = order_lines(tables)
    returned = lines[lines["is_returned"]].assign(returned_on_jst=lambda d: jst_day(d["return_ts"]))
    returned = returned.merge(tables["returns"][["order_item_id", "reason"]], on="order_item_id")
    expected = (
        returned.groupby(["returned_on_jst", "reason"])
        .agg(
            return_count=("order_item_id", "count"),
            returned_units=("quantity", "sum"),
            refunded_jpy=("refund_amount", "sum"),
        )
        .reset_index()
        .rename(columns={"reason": "return_reason"})
    )
    assert_same(
        frame(warehouse, "fct_refunds_daily"), expected, ["returned_on_jst", "return_reason"]
    )


def pandas_sessions(tables) -> pd.DataFrame:
    events = tables["clickstream"].sort_values(["anonymous_id", "event_ts", "event_id"]).copy()
    # The generator works in nanoseconds, but the files (and BigQuery) keep microseconds.
    events["event_ts"] = events["event_ts"].astype("datetime64[us]")
    gap = events.groupby("anonymous_id")["event_ts"].diff()
    starts = gap.isna() | (gap > SESSION_GAP)
    number = starts.groupby(events["anonymous_id"]).cumsum()
    events["session_id"] = events["anonymous_id"] + "-" + number.astype(str)
    return (
        events.groupby("session_id")
        .agg(
            started_at=("event_ts", "min"),
            ended_at=("event_ts", "max"),
            page_views=("event_type", lambda s: (s == "page_view").sum()),
            add_to_carts=("event_type", lambda s: (s == "add_to_cart").sum()),
            purchases=("event_type", lambda s: (s == "purchase").sum()),
            device_type=("device", "first"),
        )
        .reset_index()
    )


def test_sessions_match_pandas(warehouse, tables):
    expected = pandas_sessions(tables)
    actual = frame(warehouse, "fct_sessions")

    assert len(actual) == len(expected)
    columns = ["session_id", "started_at", "ended_at", "page_views", "add_to_carts", "purchases"]
    assert_same(actual, expected[columns], ["session_id"])

    seconds = (
        expected.set_index("session_id")["ended_at"]
        - expected.set_index("session_id")["started_at"]
    ).dt.total_seconds()
    drift = (actual.set_index("session_id")["duration_seconds"] - seconds).abs()
    assert drift.max() <= 1  # BigQuery truncates to whole seconds, DuckDB counts boundaries

    merged = actual.merge(
        expected[["session_id", "device_type"]], on="session_id", suffixes=("", "_expected")
    )
    assert (merged["device_type"] == merged["device_type_expected"]).all()


def test_funnel_matches_pandas(warehouse, tables):
    sessions = pandas_sessions(tables)
    sessions["started_on_jst"] = jst_day(sessions["started_at"])
    expected = (
        sessions.groupby(["started_on_jst", "device_type"])
        .agg(
            sessions=("session_id", "count"),
            sessions_with_view=("page_views", lambda s: (s > 0).sum()),
            sessions_with_cart=("add_to_carts", lambda s: (s > 0).sum()),
            sessions_with_purchase=("purchases", lambda s: (s > 0).sum()),
        )
        .reset_index()
    )
    expected["conversion_rate"] = expected["sessions_with_purchase"] / expected["sessions"]
    assert_same(frame(warehouse, "fct_funnel_daily"), expected, ["started_on_jst", "device_type"])


def test_style_sell_through_matches_pandas(warehouse, tables):
    lines = completed(order_lines(tables))
    products = tables["products"]
    styles = products.groupby("style_id").agg(
        category=("category", "first"),
        launch_date=("launch_date", "min"),
        initial_units=("initial_stock", "sum"),
    )
    window = lines[lines["ordered_on_jst"] < lines["launch_date"] + pd.Timedelta(days=30)]
    styles["sold_units_in_window"] = window.groupby("style_id")["quantity"].sum()
    styles["sold_units_in_window"] = styles["sold_units_in_window"].fillna(0)
    styles["sell_through_rate"] = styles["sold_units_in_window"] / styles["initial_units"]
    all_days = jst_day(tables["orders"]["order_ts"])
    styles["is_complete"] = (styles["launch_date"] >= all_days.min()) & (
        styles["launch_date"] + pd.Timedelta(days=30) <= all_days.max()
    )
    expected = styles.reset_index()

    assert expected["is_complete"].any() and (~expected["is_complete"]).any()
    assert_same(frame(warehouse, "fct_style_sell_through"), expected, ["style_id"])


def test_current_price_is_the_latest_price_change(warehouse, tables):
    latest = (
        tables["price_changes"].sort_values("effective_date").groupby("style_id").last()["price"]
    )
    products = frame(warehouse, "dim_products").set_index("sku")
    style_of = tables["products"].set_index("sku")["style_id"]
    assert (products["current_price_jpy"] == style_of.map(latest).reindex(products.index)).all()
    assert products["is_on_markdown"].any() and (~products["is_on_markdown"]).any()


# --- 3. session rules at their exact boundaries --------------------------------


def sessions_for(events: list[tuple[str, str, str]], extra_vars=None) -> list[tuple[str, int]]:
    """Build the session models on hand-made events: (visitor, UTC timestamp, event type)."""
    con = new_connection()
    con.execute(CLICKSTREAM_DDL)
    for number, (visitor, timestamp, event_type) in enumerate(events):
        con.execute(
            "INSERT INTO raw.clickstream VALUES (?, ?, ?, NULL, ?, 'S1-RED-M', 'mobile')",
            [f"E{number:03d}", timestamp, visitor, event_type],
        )
    build_models(con, only=["fct_sessions"], extra_vars=extra_vars)
    rows = con.execute(
        "SELECT session_id, page_views + add_to_carts + purchases FROM fct_sessions ORDER BY 1"
    ).fetchall()
    return [(session_id, int(n)) for session_id, n in rows]


def test_exactly_the_gap_stays_in_one_session_and_one_second_more_splits():
    events = [
        ("v1", "2026-03-01 10:00:00+00", "page_view"),
        ("v1", "2026-03-01 10:30:00+00", "page_view"),  # exactly 30 min after: same session
        ("v1", "2026-03-01 11:00:01+00", "page_view"),  # 30 min 1 s after: new session
    ]
    assert sessions_for(events) == [("v1-1", 2), ("v1-2", 1)]


def test_sessions_are_per_visitor_and_independent_of_input_order():
    events = [
        ("v2", "2026-03-01 10:05:00+00", "page_view"),
        ("v1", "2026-03-01 10:00:00+00", "page_view"),
        ("v2", "2026-03-01 23:00:00+00", "page_view"),
        ("v1", "2026-03-01 10:10:00+00", "add_to_cart"),
    ]
    assert sessions_for(events) == [("v1-1", 2), ("v2-1", 1), ("v2-2", 1)]


def test_the_session_gap_comes_from_the_project_variable():
    events = [
        ("v1", "2026-03-01 10:00:00+00", "page_view"),
        ("v1", "2026-03-01 10:20:00+00", "page_view"),
    ]
    assert sessions_for(events) == [("v1-1", 2)]
    assert sessions_for(events, extra_vars={"session_gap_minutes": 10}) == [
        ("v1-1", 1),
        ("v1-2", 1),
    ]


def test_a_session_crossing_midnight_japan_time_belongs_to_its_start_day():
    con = new_connection()
    con.execute(CLICKSTREAM_DDL)
    # 14:50 and 15:10 UTC = 23:50 JST on Mar 1 and 00:10 JST on Mar 2.
    for number, timestamp in enumerate(["2026-03-01 14:50:00+00", "2026-03-01 15:10:00+00"]):
        con.execute(
            "INSERT INTO raw.clickstream VALUES (?, ?, 'v1', NULL, 'page_view', 'S1', 'mobile')",
            [f"E{number}", timestamp],
        )
    build_models(con, only=["fct_sessions"])
    assert con.execute("SELECT count(*), min(started_on_jst) FROM fct_sessions").fetchone() == (
        1,
        date(2026, 3, 1),
    )
