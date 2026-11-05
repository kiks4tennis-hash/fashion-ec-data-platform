select
    order_id,
    customer_id,
    order_ts as ordered_at,
    {{ jst_date('order_ts') }} as ordered_on_jst,
    status,
    status = 'cancelled' as is_cancelled,
    payment_method,
    shipping_fee as shipping_fee_jpy
from {{ source('raw', 'orders') }}
