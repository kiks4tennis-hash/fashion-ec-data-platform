-- Gross sales per Japan-time day and category, before returns. Cancelled orders excluded.
-- An order with items from two categories counts once in each category.
select
    ordered_on_jst,
    category,
    count(distinct order_id) as order_count,
    sum(sold_units) as sold_units,
    sum(line_amount_jpy) as gross_sales_jpy
from {{ ref('int_order_items_enriched') }}
where not is_cancelled
group by ordered_on_jst, category
