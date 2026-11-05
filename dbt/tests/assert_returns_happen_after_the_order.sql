-- A return must be registered after the order was placed.
select r.return_id, r.returned_at, o.ordered_at
from {{ ref('stg_returns') }} as r
join {{ ref('stg_order_items') }} as i on i.order_item_id = r.order_item_id
join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
where r.returned_at <= o.ordered_at
