select
    event_id,
    event_ts as event_at,
    {{ jst_date('event_ts') }} as event_on_jst,
    anonymous_id,
    customer_id,
    event_type,
    sku,
    device as device_type
from {{ source('raw', 'clickstream') }}
