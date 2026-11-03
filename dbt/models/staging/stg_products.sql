select
    sku,
    style_id,
    category,
    color,
    size,
    season,
    launch_date,
    base_price as base_price_jpy,
    initial_stock as initial_stock_units
from {{ source('raw', 'products') }}
