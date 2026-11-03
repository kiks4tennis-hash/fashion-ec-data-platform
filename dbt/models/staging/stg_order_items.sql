select
    order_item_id,
    order_id,
    sku,
    quantity,
    unit_price as unit_price_jpy
from {{ source('raw', 'order_items') }}
