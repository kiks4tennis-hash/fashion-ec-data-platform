-- Each style's price history starts with exactly one 'launch' row.
select style_id, countif(change_reason = 'launch') as launch_rows
from {{ ref('stg_price_changes') }}
group by style_id
having countif(change_reason = 'launch') != 1
