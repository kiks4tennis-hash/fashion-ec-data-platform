-- One row per order line with the order, product and return attached.
-- Cancelled orders are kept (is_cancelled): revenue models must filter them out.
select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    o.ordered_at,
    o.ordered_on_jst,
    o.is_cancelled,
    i.sku,
    p.style_id,
    p.category,
    p.color,
    p.size,
    p.season,
    p.launch_date,
    i.quantity as sold_units,
    i.unit_price_jpy,
    i.quantity * i.unit_price_jpy as line_amount_jpy,
    r.return_id is not null as is_returned,
    r.returned_at,
    r.returned_on_jst,
    r.return_reason,
    coalesce(r.refund_amount_jpy, 0) as refund_amount_jpy
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
inner join {{ ref('stg_products') }} as p on p.sku = i.sku
left join {{ ref('stg_returns') }} as r on r.order_item_id = i.order_item_id
