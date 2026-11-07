-- Total refunds in the mart equal the sum of all returns in staging.
with expected as (
    select sum(refund_amount_jpy) as refunded_jpy
    from {{ ref('stg_returns') }}
),

actual as (
    select sum(refunded_jpy) as refunded_jpy
    from {{ ref('fct_refunds_daily') }}
)

select e.refunded_jpy as expected_jpy, a.refunded_jpy as actual_jpy
from expected as e
cross join actual as a
where e.refunded_jpy is distinct from a.refunded_jpy
