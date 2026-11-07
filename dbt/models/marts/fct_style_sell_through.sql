-- Sell-through of each style in its first sell_through_days days: units sold (gross, before
-- returns) divided by the units in stock at the start.
--
-- is_complete marks styles whose launch is inside the data and whose whole window has passed.
-- For earlier launches the data starts mid-life (initial stock is the stock on the first day
-- of the data), and for recent launches the window is not over yet.
-- The synthetic demand does not depend on stock, so nothing caps the rate at 1.
with bounds as (
    select
        min(ordered_on_jst) as first_order_on_jst,
        max(ordered_on_jst) as as_of_date
    from {{ ref('stg_orders') }}
),

styles as (
    select
        style_id,
        any_value(category) as category,
        min(launch_date) as launch_date,
        sum(initial_stock_units) as initial_units
    from {{ ref('stg_products') }}
    group by style_id
),

sales as (
    select
        style_id,
        sum(sold_units) as sold_units_in_window
    from {{ ref('int_order_items_enriched') }}
    where
        not is_cancelled
        and ordered_on_jst < date_add(launch_date, interval {{ var('sell_through_days') }} day)
    group by style_id
)

select
    s.style_id,
    s.category,
    s.launch_date,
    s.initial_units,
    coalesce(x.sold_units_in_window, 0) as sold_units_in_window,
    safe_divide(coalesce(x.sold_units_in_window, 0), s.initial_units) as sell_through_rate,
    s.launch_date >= b.first_order_on_jst
        and date_add(s.launch_date, interval {{ var('sell_through_days') }} day) <= b.as_of_date
        as is_complete
from styles as s
left join sales as x on x.style_id = s.style_id
cross join bounds as b
