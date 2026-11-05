-- Every style has exactly one current price period.
select style_id, countif(is_current) as current_periods
from {{ ref('int_price_periods') }}
group by style_id
having countif(is_current) != 1
