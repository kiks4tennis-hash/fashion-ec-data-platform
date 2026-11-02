"""Synthetic fashion e-commerce data generator.

Generates eight related tables with a fixed random seed, so the same configuration always
produces the same data:

    products, price_changes, customers, orders, order_items, returns,
    inventory_snapshots, clickstream

Conventions (chosen on purpose, so downstream dbt/Spark work has realistic things to handle):

* Business dates are Japan time (JST), but every timestamp column is stored in **UTC**.
* Orders can be cancelled; cancelled orders never have returns and do not reduce inventory.
* Returns are right-censored: returns that would happen after the observation window are
  not in the data.
* Clickstream events have **no session_id**. Sessions must be derived (e.g. 30-minute
  inactivity), which is the Spark exercise in a later phase.

Usage:
    python -m fashion_ec.generate --out data/raw
    fashion-ec-generate --out data/raw --days 365 --orders 200000 --sessions 2000000
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

JST_OFFSET = pd.Timedelta(hours=9)

CANCEL_RATE = 0.03
FREE_SHIPPING_THRESHOLD = 5_000
SHIPPING_FEE = 550

COLORS = ["black", "white", "navy", "beige", "gray", "khaki", "red", "green"]
REGIONS = ["Kanto", "Kansai", "Chubu", "Kyushu", "Tohoku", "Hokkaido", "Other"]
REGION_P = np.array([0.38, 0.17, 0.12, 0.10, 0.07, 0.04, 0.12])
CHANNELS = ["organic", "paid_search", "social", "email", "referral"]
CHANNEL_P = np.array([0.35, 0.25, 0.20, 0.12, 0.08])
PAYMENT_METHODS = ["credit_card", "cash_on_delivery", "convenience_store", "e_wallet"]
PAYMENT_P = np.array([0.60, 0.10, 0.10, 0.20])
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_P = np.array([0.70, 0.25, 0.05])

# Relative traffic by hour of day (JST, 0-23) and by weekday (Mon-Sun).
HOUR_P = np.array(
    [1, 1, 1, 1, 1, 2, 3, 4, 4, 4, 4, 5, 6, 5, 5, 5, 5, 6, 7, 9, 10, 10, 8, 4], dtype=float
)
HOUR_P = HOUR_P / HOUR_P.sum()
DOW_WEIGHTS = np.array([1.0, 0.95, 0.95, 1.0, 1.1, 1.3, 1.35])

RETURN_REASONS = [
    "size_too_small",
    "size_too_large",
    "not_as_expected",
    "defective",
    "changed_mind",
]
RETURN_REASON_P_SIZED = np.array([0.30, 0.25, 0.25, 0.08, 0.12])
RETURN_REASON_P_ONE_SIZE = np.array([0.0, 0.0, 0.40, 0.20, 0.40])

# Funnel probabilities for clickstream.
P_ADD_TO_CART = 0.08
P_PURCHASE_AFTER_CART = 0.35


@dataclass(frozen=True)
class CategorySpec:
    """Per-category assumptions: sizes, price range (JPY), return rate and catalog share."""

    sizes: tuple[str, ...]
    price_range: tuple[int, int]
    return_rate: float
    share: float


CATEGORY_SPECS: dict[str, CategorySpec] = {
    "tops": CategorySpec(("XS", "S", "M", "L", "XL"), (3_000, 9_000), 0.18, 0.30),
    "bottoms": CategorySpec(("S", "M", "L", "XL"), (5_000, 14_000), 0.22, 0.20),
    "outerwear": CategorySpec(("S", "M", "L", "XL"), (12_000, 40_000), 0.15, 0.12),
    "dresses": CategorySpec(("XS", "S", "M", "L"), (8_000, 25_000), 0.28, 0.13),
    "shoes": CategorySpec(("23.0", "24.0", "25.0", "26.0", "27.0"), (8_000, 22_000), 0.25, 0.10),
    "accessories": CategorySpec(("F",), (2_000, 8_000), 0.05, 0.15),
}

# Columns written as DATE (the rest of the datetime columns stay TIMESTAMP).
DATE_COLUMNS = {
    "products": ["launch_date"],
    "price_changes": ["effective_date"],
    "customers": ["signup_date"],
    "inventory_snapshots": ["snapshot_date"],
}


@dataclass(frozen=True)
class GenerationConfig:
    """Size and seed of the generated dataset. Defaults give a medium-sized dataset."""

    seed: int = 42
    start_date: date = date(2026, 1, 1)
    days: int = 120
    n_styles: int = 200
    n_customers: int = 5_000
    n_orders: int = 20_000
    n_sessions: int = 60_000

    def __post_init__(self) -> None:
        for name in ("days", "n_styles", "n_customers", "n_orders", "n_sessions"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1, got {getattr(self, name)}")

    @property
    def end_date(self) -> date:
        return self.start_date + timedelta(days=self.days - 1)

    @classmethod
    def small(cls, **overrides) -> GenerationConfig:
        """A tiny configuration for fast tests."""
        params = {
            "days": 30,
            "n_styles": 20,
            "n_customers": 200,
            "n_orders": 500,
            "n_sessions": 1_000,
        }
        params.update(overrides)
        return cls(**params)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _days(cfg: GenerationConfig) -> pd.DatetimeIndex:
    return pd.date_range(cfg.start_date, periods=cfg.days, freq="D").astype("datetime64[ns]")


def _day_probs(days: pd.DatetimeIndex) -> np.ndarray:
    weights = DOW_WEIGHTS[days.day_of_week.to_numpy()]
    return weights / weights.sum()


def _seconds_of_day(rng: np.random.Generator, n: int) -> np.ndarray:
    hours = rng.choice(24, size=n, p=HOUR_P)
    return hours * 3600 + rng.integers(0, 3600, size=n)


def _round_100(value: float) -> int:
    return int(round(value / 100) * 100)


def _normalized(weights: np.ndarray) -> np.ndarray:
    return weights / weights.sum()


# ---------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------


def generate_products(
    rng: np.random.Generator, cfg: GenerationConfig
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """SKU-level product master and the price change history per style."""
    names = list(CATEGORY_SPECS)
    shares = _normalized(np.array([CATEGORY_SPECS[n].share for n in names]))
    category_idx = rng.choice(len(names), size=cfg.n_styles, p=shares)
    start = pd.Timestamp(cfg.start_date)
    end = pd.Timestamp(cfg.end_date)

    offsets = rng.integers(-30, max(cfg.days // 2, 1) + 1, size=cfg.n_styles)
    offsets[0] = min(offsets[0], 0)  # guarantee something is on sale from day one

    sku_rows: list[dict] = []
    price_rows: list[dict] = []
    for i in range(cfg.n_styles):
        style_id = f"S{i + 1:05d}"
        category = names[category_idx[i]]
        spec = CATEGORY_SPECS[category]
        launch = start + pd.Timedelta(days=int(offsets[i]))
        season_year = launch.year - (1 if launch.month == 1 else 0)
        season = f"{season_year}{'SS' if 2 <= launch.month <= 7 else 'AW'}"
        base_price = _round_100(rng.uniform(*spec.price_range))

        n_colors = int(rng.integers(1, 4))
        colors = rng.choice(COLORS, size=n_colors, replace=False)
        initial_stock = int(rng.integers(20, 120))
        for color in colors:
            for size in spec.sizes:
                sku_rows.append(
                    {
                        "sku": f"{style_id}-{color[:3].upper()}-{size}",
                        "style_id": style_id,
                        "category": category,
                        "color": str(color),
                        "size": size,
                        "season": season,
                        "launch_date": launch,
                        "base_price": base_price,
                        "initial_stock": initial_stock,
                    }
                )

        price_rows.append(
            {
                "style_id": style_id,
                "effective_date": launch,
                "price": base_price,
                "change_reason": "launch",
            }
        )
        if rng.random() < 0.6:
            first_markdown = launch + pd.Timedelta(days=int(rng.integers(30, 61)))
            if first_markdown <= end:
                price_rows.append(
                    {
                        "style_id": style_id,
                        "effective_date": first_markdown,
                        "price": _round_100(base_price * 0.8),
                        "change_reason": "markdown",
                    }
                )
            second_markdown = first_markdown + pd.Timedelta(days=30)
            if rng.random() < 0.3 and second_markdown <= end:
                price_rows.append(
                    {
                        "style_id": style_id,
                        "effective_date": second_markdown,
                        "price": _round_100(base_price * 0.6),
                        "change_reason": "markdown",
                    }
                )

    products = pd.DataFrame(sku_rows)
    products["launch_date"] = products["launch_date"].astype("datetime64[ns]")
    price_changes = pd.DataFrame(price_rows)
    price_changes["effective_date"] = price_changes["effective_date"].astype("datetime64[ns]")
    return products, price_changes


def generate_customers(rng: np.random.Generator, cfg: GenerationConfig) -> pd.DataFrame:
    """Customers who signed up up to a year before the window or during it."""
    n = cfg.n_customers
    offsets = rng.integers(0, 365 + cfg.days, size=n)
    offsets[0] = 0  # guarantee a customer exists from day one
    first_day = pd.Timestamp(cfg.start_date) - pd.Timedelta(days=365)
    return pd.DataFrame(
        {
            "customer_id": [f"C{i + 1:06d}" for i in range(n)],
            "signup_date": (first_day + pd.to_timedelta(offsets, unit="D")).astype(
                "datetime64[ns]"
            ),
            "region": rng.choice(REGIONS, size=n, p=REGION_P),
            "acquisition_channel": rng.choice(CHANNELS, size=n, p=CHANNEL_P),
        }
    )


def generate_orders(
    rng: np.random.Generator,
    cfg: GenerationConfig,
    products: pd.DataFrame,
    price_changes: pd.DataFrame,
    customers: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Orders and order lines. Prices come from the price in effect on the order date."""
    days = _days(cfg)
    per_day = rng.multinomial(cfg.n_orders, _day_probs(days))

    customer_ids = customers["customer_id"].to_numpy()
    signup = customers["signup_date"].to_numpy()
    customer_weight = rng.lognormal(0.0, 1.0, len(customers))  # a few heavy buyers
    skus = products["sku"].to_numpy()
    launch = products["launch_date"].to_numpy()
    sku_weight = rng.lognormal(0.0, 1.0, len(products))  # a few bestsellers

    order_parts: list[pd.DataFrame] = []
    item_parts: list[pd.DataFrame] = []
    next_key = 0
    for day, n in zip(days, per_day, strict=True):
        if n == 0:
            continue
        day64 = day.to_datetime64()
        customer_idx = np.flatnonzero(signup <= day64)
        sku_idx = np.flatnonzero(launch <= day64)
        buyers = rng.choice(customer_idx, size=n, p=_normalized(customer_weight[customer_idx]))
        keys = np.arange(next_key, next_key + n)
        next_key += n

        order_parts.append(
            pd.DataFrame(
                {
                    "_key": keys,
                    "customer_id": customer_ids[buyers],
                    "order_ts": day + pd.to_timedelta(_seconds_of_day(rng, n), unit="s"),
                    "status": np.where(rng.random(n) < CANCEL_RATE, "cancelled", "completed"),
                    "payment_method": rng.choice(PAYMENT_METHODS, size=n, p=PAYMENT_P),
                }
            )
        )

        lines_per_order = np.clip(1 + rng.poisson(0.6, n), 1, 5)
        n_lines = int(lines_per_order.sum())
        item_parts.append(
            pd.DataFrame(
                {
                    "_key": np.repeat(keys, lines_per_order),
                    "sku": skus[
                        rng.choice(sku_idx, size=n_lines, p=_normalized(sku_weight[sku_idx]))
                    ],
                    "quantity": 1 + (rng.random(n_lines) < 0.05).astype(int),
                    "order_date": day,
                }
            )
        )

    orders = pd.concat(order_parts, ignore_index=True)
    orders = orders.sort_values("order_ts", kind="stable").reset_index(drop=True)
    orders["order_id"] = [f"O{i:07d}" for i in range(1, len(orders) + 1)]

    items = pd.concat(item_parts, ignore_index=True)
    items["order_id"] = items["_key"].map(
        dict(zip(orders["_key"], orders["order_id"], strict=True))
    )
    items = items.drop_duplicates(["order_id", "sku"])
    items = items.merge(products[["sku", "style_id"]], on="sku", how="left")

    prices = price_changes[["style_id", "effective_date", "price"]].copy()
    prices["effective_date"] = prices["effective_date"].astype("datetime64[ns]")
    items["order_date"] = items["order_date"].astype("datetime64[ns]")
    items = pd.merge_asof(
        items.sort_values("order_date"),
        prices.sort_values("effective_date"),
        left_on="order_date",
        right_on="effective_date",
        by="style_id",
    )
    items = items.sort_values(["order_id", "sku"]).reset_index(drop=True)
    items["order_item_id"] = [f"OI{i:08d}" for i in range(1, len(items) + 1)]
    items = items.rename(columns={"price": "unit_price"})
    items = items[["order_item_id", "order_id", "sku", "quantity", "unit_price"]]

    subtotal = (items["unit_price"] * items["quantity"]).groupby(items["order_id"]).sum()
    orders["shipping_fee"] = np.where(
        orders["order_id"].map(subtotal) >= FREE_SHIPPING_THRESHOLD, 0, SHIPPING_FEE
    )
    orders["order_ts"] = orders["order_ts"] - JST_OFFSET  # store in UTC
    orders = orders[
        ["order_id", "customer_id", "order_ts", "status", "payment_method", "shipping_fee"]
    ]
    return orders, items


def generate_returns(
    rng: np.random.Generator,
    cfg: GenerationConfig,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
) -> pd.DataFrame:
    """Full-refund returns on completed orders, with category-specific return rates."""
    lines = order_items.merge(
        orders.loc[orders["status"] == "completed", ["order_id", "order_ts"]], on="order_id"
    ).merge(products[["sku", "category", "size"]], on="sku")

    rates = lines["category"].map({k: v.return_rate for k, v in CATEGORY_SPECS.items()})
    returned = lines[rng.random(len(lines)) < rates.to_numpy()].copy()

    delay = pd.to_timedelta(rng.integers(3, 31, len(returned)), unit="D") + pd.to_timedelta(
        rng.integers(0, 86_400, len(returned)), unit="s"
    )
    returned["return_ts"] = returned["order_ts"] + delay
    window_end_utc = pd.Timestamp(cfg.end_date) + pd.Timedelta(days=1) - JST_OFFSET
    returned = returned[returned["return_ts"] < window_end_utc].copy()

    one_size = (returned["size"] == "F").to_numpy()
    reasons = np.where(
        one_size,
        rng.choice(RETURN_REASONS, size=len(returned), p=RETURN_REASON_P_ONE_SIZE),
        rng.choice(RETURN_REASONS, size=len(returned), p=RETURN_REASON_P_SIZED),
    )
    returned["reason"] = reasons
    returned["refund_amount"] = returned["unit_price"] * returned["quantity"]

    returned = returned.sort_values("return_ts", kind="stable").reset_index(drop=True)
    returned["return_id"] = [f"R{i:07d}" for i in range(1, len(returned) + 1)]
    return returned[["return_id", "order_item_id", "return_ts", "reason", "refund_amount"]]


def generate_inventory_snapshots(
    cfg: GenerationConfig,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    returns: pd.DataFrame,
) -> pd.DataFrame:
    """Daily on-hand stock per SKU: initial stock minus sales plus returned units.

    ``initial_stock`` is the stock on the SKU's first day in the window (launch day or window
    start, whichever is later). Stock never goes below zero (no backorders are modelled).
    """
    days = _days(cfg)
    skus = products["sku"].to_numpy()
    position = pd.Series(np.arange(len(skus)), index=skus)

    def daily_units(frame: pd.DataFrame, ts_col: str) -> np.ndarray:
        matrix = np.zeros((len(skus), len(days)))
        local_day = (frame[ts_col] + JST_OFFSET).dt.normalize().astype("datetime64[ns]")
        day_idx = ((local_day - days[0]) / pd.Timedelta(days=1)).astype(int).to_numpy()
        np.add.at(
            matrix, (position[frame["sku"]].to_numpy(), day_idx), frame["quantity"].to_numpy()
        )
        return matrix

    sold_lines = order_items.merge(
        orders.loc[orders["status"] == "completed", ["order_id", "order_ts"]], on="order_id"
    )
    returned_lines = returns.merge(
        order_items[["order_item_id", "sku", "quantity"]], on="order_item_id"
    )
    net_sold = daily_units(sold_lines, "order_ts") - daily_units(returned_lines, "return_ts")
    on_hand = np.clip(
        products["initial_stock"].to_numpy()[:, None] - np.cumsum(net_sold, axis=1), 0, None
    ).astype(int)

    live = products["launch_date"].to_numpy()[:, None] <= days.to_numpy()[None, :]
    sku_idx, day_idx = np.nonzero(live)
    snapshots = pd.DataFrame(
        {
            "snapshot_date": days[day_idx],
            "sku": skus[sku_idx],
            "on_hand": on_hand[sku_idx, day_idx],
        }
    )
    return snapshots.sort_values(["snapshot_date", "sku"]).reset_index(drop=True)


def generate_clickstream(
    rng: np.random.Generator,
    cfg: GenerationConfig,
    products: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """Raw web events (page_view, add_to_cart, purchase) with no session id.

    Visitors are anonymous; about a third are linked to a customer (logged in). A visitor
    produces several sessions over time, so sessions have to be reconstructed from gaps.
    """
    days = _days(cfg)
    per_day = rng.multinomial(cfg.n_sessions, _day_probs(days))

    n_visitors = max(cfg.n_sessions // 3, 1)
    visitor_ids = np.array([f"v{i:08d}" for i in range(n_visitors)])
    linked = rng.random(n_visitors) < 0.35
    visitor_customer = np.where(
        linked, rng.choice(customers["customer_id"].to_numpy(), size=n_visitors), None
    )
    visitor_p = _normalized(rng.lognormal(0.0, 1.0, n_visitors))

    skus = products["sku"].to_numpy()
    launch = products["launch_date"].to_numpy()
    sku_weight = rng.lognormal(0.0, 1.0, len(products))

    parts: list[pd.DataFrame] = []
    for day, n_sessions in zip(days, per_day, strict=True):
        if n_sessions == 0:
            continue
        sku_idx = np.flatnonzero(launch <= day.to_datetime64())
        visitors = rng.choice(n_visitors, size=n_sessions, p=visitor_p)
        starts = day + pd.to_timedelta(_seconds_of_day(rng, n_sessions), unit="s")
        devices = rng.choice(DEVICES, size=n_sessions, p=DEVICE_P)

        views_per_session = np.minimum(rng.geometric(0.3, n_sessions), 30)
        n_views = int(views_per_session.sum())
        session_of_view = np.repeat(np.arange(n_sessions), views_per_session)
        first_view = np.cumsum(views_per_session) - views_per_session
        gaps = rng.exponential(45.0, n_views)
        gaps[first_view] = 0.0
        cumulative = np.cumsum(gaps)
        seconds_into_session = cumulative - cumulative[first_view][session_of_view]

        views = pd.DataFrame(
            {
                "event_ts": starts[session_of_view]
                + pd.to_timedelta(seconds_into_session, unit="s"),
                "visitor": visitors[session_of_view],
                "event_type": "page_view",
                "sku": skus[rng.choice(sku_idx, size=n_views, p=_normalized(sku_weight[sku_idx]))],
                "device": devices[session_of_view],
            }
        )
        carts = views[rng.random(n_views) < P_ADD_TO_CART].copy()
        carts["event_type"] = "add_to_cart"
        carts["event_ts"] = carts["event_ts"] + pd.to_timedelta(
            rng.uniform(10, 90, len(carts)), unit="s"
        )
        purchases = carts[rng.random(len(carts)) < P_PURCHASE_AFTER_CART].copy()
        purchases["event_type"] = "purchase"
        purchases["event_ts"] = purchases["event_ts"] + pd.to_timedelta(
            rng.uniform(30, 600, len(purchases)), unit="s"
        )
        parts.append(pd.concat([views, carts, purchases], ignore_index=True))

    events = pd.concat(parts, ignore_index=True)
    events = events.sort_values("event_ts", kind="stable").reset_index(drop=True)
    events["event_id"] = [f"E{i:09d}" for i in range(1, len(events) + 1)]
    events["anonymous_id"] = visitor_ids[events["visitor"].to_numpy()]
    events["customer_id"] = visitor_customer[events["visitor"].to_numpy()]
    events["event_ts"] = events["event_ts"] - JST_OFFSET  # store in UTC
    return events[
        ["event_id", "event_ts", "anonymous_id", "customer_id", "event_type", "sku", "device"]
    ]


# ---------------------------------------------------------------------------
# orchestration and output
# ---------------------------------------------------------------------------


def generate_all(cfg: GenerationConfig | None = None) -> dict[str, pd.DataFrame]:
    """Generate every table. Each table has its own random stream derived from the seed."""
    cfg = cfg or GenerationConfig()
    rngs = [np.random.default_rng(s) for s in np.random.SeedSequence(cfg.seed).spawn(5)]

    products, price_changes = generate_products(rngs[0], cfg)
    customers = generate_customers(rngs[1], cfg)
    orders, order_items = generate_orders(rngs[2], cfg, products, price_changes, customers)
    returns = generate_returns(rngs[3], cfg, products, orders, order_items)
    inventory = generate_inventory_snapshots(cfg, products, orders, order_items, returns)
    clickstream = generate_clickstream(rngs[4], cfg, products, customers)
    return {
        "products": products,
        "price_changes": price_changes,
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "returns": returns,
        "inventory_snapshots": inventory,
        "clickstream": clickstream,
    }


def write_tables(tables: dict[str, pd.DataFrame], out_dir: str | Path) -> None:
    """Write each table as Parquet under ``out_dir/<table>/``.

    The clickstream is partitioned by UTC event date (``event_date=YYYY-MM-DD``), the layout
    BigQuery and Spark expect for a large event table. Existing table folders are replaced.
    """
    out = Path(out_dir)
    for name, frame in tables.items():
        frame = frame.copy()
        for column in DATE_COLUMNS.get(name, []):
            frame[column] = frame[column].dt.date
        target = out / name
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        if name == "clickstream":
            event_date = frame["event_ts"].dt.date
            for day, part in frame.groupby(event_date):
                partition = target / f"event_date={day.isoformat()}"
                partition.mkdir()
                part.to_parquet(partition / "part-0000.parquet", index=False)
        else:
            frame.to_parquet(target / f"{name}.parquet", index=False)


def build_parser() -> argparse.ArgumentParser:
    defaults = GenerationConfig()
    parser = argparse.ArgumentParser(description="Generate synthetic fashion EC data (Parquet).")
    parser.add_argument("--out", default="data/raw", help="output directory (default: data/raw)")
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--start-date", type=date.fromisoformat, default=defaults.start_date)
    parser.add_argument("--days", type=int, default=defaults.days)
    parser.add_argument("--styles", type=int, default=defaults.n_styles)
    parser.add_argument("--customers", type=int, default=defaults.n_customers)
    parser.add_argument("--orders", type=int, default=defaults.n_orders)
    parser.add_argument("--sessions", type=int, default=defaults.n_sessions)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    cfg = GenerationConfig(
        seed=args.seed,
        start_date=args.start_date,
        days=args.days,
        n_styles=args.styles,
        n_customers=args.customers,
        n_orders=args.orders,
        n_sessions=args.sessions,
    )
    tables = generate_all(cfg)
    write_tables(tables, args.out)
    for name, frame in tables.items():
        print(f"{name:<22}{len(frame):>12,} rows")
    print(f"\nWritten to {Path(args.out).resolve()}")


if __name__ == "__main__":
    main()
