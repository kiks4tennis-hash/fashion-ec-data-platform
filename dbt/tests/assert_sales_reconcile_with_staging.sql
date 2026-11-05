-- Total gross sales in the mart equal quantity x price of all non-cancelled lines in staging.
with expected as (
    select sum(i.quantity * i.unit_price_jpy) as gross_sales_jpy
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
    where not o.is_cancelled
),

actual as (
    select sum(gross_sales_jpy) as gross_sales_jpy
    from {{ ref('fct_sales_daily') }}
)

select e.gross_sales_jpy as expected_jpy, a.gross_sales_jpy as actual_jpy
from expected as e
cross join actual as a
where e.gross_sales_jpy is distinct from a.gross_sales_jpy
