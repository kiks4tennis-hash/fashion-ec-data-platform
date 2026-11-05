select
    customer_id,
    signup_date,
    region,
    acquisition_channel
from {{ source('raw', 'customers') }}
