-- Refunds by the day the return was registered (cash view), per reason.
-- Compare with fct_returns_by_cohort, which attributes returns to the order month instead.
select
    returned_on_jst,
    return_reason,
    count(*) as return_count,
    sum(sold_units) as returned_units,
    sum(refund_amount_jpy) as refunded_jpy
from {{ ref('int_order_items_enriched') }}
where is_returned
group by returned_on_jst, return_reason
