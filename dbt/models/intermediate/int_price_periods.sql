-- Price history as validity periods (a type-2 dimension built from the change log).
-- valid_to is exclusive: the price applies on valid_from <= date < valid_to.
-- The current price has valid_to = null.
select
    style_id,
    effective_from as valid_from,
    lead(effective_from) over (partition by style_id order by effective_from) as valid_to,
    price_jpy,
    change_reason,
    lead(effective_from) over (partition by style_id order by effective_from) is null as is_current
from {{ ref('stg_price_changes') }}
