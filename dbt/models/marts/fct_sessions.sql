-- One row per web session (see int_sessions for how sessions are derived).
-- The device is the one of the session's first event, so the result is the same on every run
-- even if a visitor id shows up on two devices inside one session.
with events as (
    select
        *,
        row_number() over (partition by session_id order by event_at, event_id) as event_number
    from {{ ref('int_sessions') }}
)

select
    session_id,
    any_value(anonymous_id) as anonymous_id,
    max(customer_id) as customer_id,
    max(if(event_number = 1, device_type, null)) as device_type,
    min(event_at) as started_at,
    max(event_at) as ended_at,
    {{ jst_date('min(event_at)') }} as started_on_jst,
    timestamp_diff(max(event_at), min(event_at), second) as duration_seconds,
    countif(event_type = 'page_view') as page_views,
    countif(event_type = 'add_to_cart') as add_to_carts,
    countif(event_type = 'purchase') as purchases,
    countif(event_type = 'purchase') > 0 as is_converted
from events
group by session_id
