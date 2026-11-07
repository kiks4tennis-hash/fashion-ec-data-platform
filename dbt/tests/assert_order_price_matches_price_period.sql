-- Every order line was sold at the list price that was valid on the order date: exactly one
-- price period covers the date, and its price equals the price on the line.
-- valid_to is exclusive, and null for the current period.
with matches as (
    select
        i.order_item_id,
        i.unit_price_jpy,
        o.ordered_on_jst,
        count(pp.valid_from) as n_periods,
        max(pp.price_jpy) as period_price_jpy
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
    inner join {{ ref('stg_products') }} as p on p.sku = i.sku
    left join {{ ref('int_price_periods') }} as pp
        on
            pp.style_id = p.style_id
            and o.ordered_on_jst >= pp.valid_from
            and (pp.valid_to is null or o.ordered_on_jst < pp.valid_to)
    group by i.order_item_id, i.unit_price_jpy, o.ordered_on_jst
)

select *
from matches
where n_periods != 1 or period_price_jpy != unit_price_jpy
