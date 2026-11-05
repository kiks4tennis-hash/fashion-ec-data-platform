-- Return rate by order month and category (units).
--
-- Returns arrive up to return_window_days after the order, so the most recent months look
-- better than they are: their returns have not happened yet. is_mature marks cohorts whose
-- whole return window has passed; only those rates are comparable.
with as_of as (
    select max(ordered_on_jst) as as_of_date
    from {{ ref('stg_orders') }}
),

cohorts as (
    select
        date_trunc(ordered_on_jst, month) as order_month,
        category,
        sum(sold_units) as sold_units,
        sum(if(is_returned, sold_units, 0)) as returned_units
    from {{ ref('int_order_items_enriched') }}
    where not is_cancelled
    group by order_month, category
)

select
    c.order_month,
    c.category,
    c.sold_units,
    c.returned_units,
    safe_divide(c.returned_units, c.sold_units) as return_rate,
    date_add(last_day(c.order_month, month), interval {{ var('return_window_days') }} day)
        <= a.as_of_date as is_mature
from cohorts as c
cross join as_of as a
