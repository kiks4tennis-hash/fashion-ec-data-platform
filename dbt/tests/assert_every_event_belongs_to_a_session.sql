-- No event is lost or counted twice when events are rolled up into sessions.
with expected as (
    select count(*) as n_events
    from {{ ref('stg_clickstream') }}
),

actual as (
    select sum(page_views + add_to_carts + purchases) as n_events
    from {{ ref('fct_sessions') }}
)

select e.n_events as expected_events, a.n_events as actual_events
from expected as e
cross join actual as a
where e.n_events is distinct from a.n_events
