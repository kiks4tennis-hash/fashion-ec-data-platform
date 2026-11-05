-- One row per customer with lifetime sales. Cancelled orders do not count.
with orders as (
    select
        customer_id,
        count(distinct order_id) as completed_orders,
        min(ordered_on_jst) as first_order_on_jst,
        max(ordered_on_jst) as last_order_on_jst,
        sum(line_amount_jpy) as gross_sales_jpy,
        sum(refund_amount_jpy) as refunded_jpy
    from {{ ref('int_order_items_enriched') }}
    where not is_cancelled
    group by customer_id
)

select
    c.customer_id,
    c.signup_date,
    c.region,
    c.acquisition_channel,
    coalesce(o.completed_orders, 0) as completed_orders,
    o.first_order_on_jst,
    o.last_order_on_jst,
    coalesce(o.gross_sales_jpy, 0) as gross_sales_jpy,
    coalesce(o.refunded_jpy, 0) as refunded_jpy,
    coalesce(o.gross_sales_jpy, 0) - coalesce(o.refunded_jpy, 0) as net_sales_jpy,
    coalesce(o.completed_orders, 0) >= 2 as is_repeat_customer
from {{ ref('stg_customers') }} as c
left join orders as o on o.customer_id = c.customer_id
