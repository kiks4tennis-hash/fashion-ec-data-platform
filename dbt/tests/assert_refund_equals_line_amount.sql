-- Returns are full refunds: the refund must equal quantity x unit price of the line.
select r.return_id, r.refund_amount_jpy, i.quantity * i.unit_price_jpy as line_amount_jpy
from {{ ref('stg_returns') }} as r
join {{ ref('stg_order_items') }} as i on i.order_item_id = r.order_item_id
where r.refund_amount_jpy != i.quantity * i.unit_price_jpy
