select
    snapshot_date,
    sku,
    on_hand as on_hand_units
from {{ source('raw', 'inventory_snapshots') }}
