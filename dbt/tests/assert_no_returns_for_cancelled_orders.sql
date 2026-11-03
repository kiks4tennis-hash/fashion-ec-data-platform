-- A cancelled order never shipped, so it cannot have returns.
select r.return_id, o.order_id
from {{ ref('stg_returns') }} as r
join {{ ref('stg_order_items') }} as i on i.order_item_id = r.order_item_id
join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
where o.is_cancelled
