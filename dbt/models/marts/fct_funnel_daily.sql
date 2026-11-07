-- Conversion funnel per Japan-time day and device: sessions that viewed a product, added it
-- to the cart and bought it. Rates are null when the previous step has no sessions.
select
    started_on_jst,
    device_type,
    count(*) as sessions,
    countif(page_views > 0) as sessions_with_view,
    countif(add_to_carts > 0) as sessions_with_cart,
    countif(purchases > 0) as sessions_with_purchase,
    safe_divide(countif(add_to_carts > 0), countif(page_views > 0)) as view_to_cart_rate,
    safe_divide(countif(purchases > 0), countif(add_to_carts > 0)) as cart_to_purchase_rate,
    safe_divide(countif(purchases > 0), count(*)) as conversion_rate
from {{ ref('fct_sessions') }}
group by started_on_jst, device_type
