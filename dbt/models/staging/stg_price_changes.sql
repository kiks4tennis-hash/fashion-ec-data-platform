select
    style_id,
    effective_date as effective_from,
    price as price_jpy,
    change_reason
from {{ source('raw', 'price_changes') }}
