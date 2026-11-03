select
    return_id,
    order_item_id,
    return_ts as returned_at,
    {{ jst_date('return_ts') }} as returned_on_jst,
    reason as return_reason,
    refund_amount as refund_amount_jpy
from {{ source('raw', 'returns') }}
