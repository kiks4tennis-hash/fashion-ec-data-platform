-- One row per SKU with its current list price (from the price history).
select
    p.sku,
    p.style_id,
    p.category,
    p.color,
    p.size,
    p.season,
    p.launch_date,
    p.base_price_jpy,
    p.initial_stock_units,
    p.size = 'F' as is_one_size,
    c.price_jpy as current_price_jpy,
    1 - safe_divide(c.price_jpy, p.base_price_jpy) as current_discount_rate,
    c.price_jpy < p.base_price_jpy as is_on_markdown
from {{ ref('stg_products') }} as p
left join {{ ref('int_price_periods') }} as c
    on c.style_id = p.style_id and c.is_current
