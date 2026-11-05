-- A SKU can only be ordered on or after its launch date (compared in Japan time).
select i.order_item_id, o.ordered_on_jst, p.launch_date
from {{ ref('stg_order_items') }} as i
join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
join {{ ref('stg_products') }} as p on p.sku = i.sku
where o.ordered_on_jst < p.launch_date
