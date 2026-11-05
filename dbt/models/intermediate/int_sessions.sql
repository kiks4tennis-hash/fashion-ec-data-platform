{{ config(materialized='table') }}

-- The raw clickstream has no session id. A new session starts when a visitor's previous
-- event is more than session_gap_minutes earlier (or there is none). The session id is the
-- visitor id plus a running session number.
with flagged as (
    select
        event_id,
        anonymous_id,
        customer_id,
        event_at,
        event_on_jst,
        event_type,
        sku,
        device_type,
        coalesce(
            event_at > timestamp_add(
                lag(event_at) over (partition by anonymous_id order by event_at, event_id),
                interval {{ var('session_gap_minutes') }} minute
            ),
            true
        ) as is_session_start
    from {{ ref('stg_clickstream') }}
)

select
    event_id,
    anonymous_id,
    customer_id,
    event_at,
    event_on_jst,
    event_type,
    sku,
    device_type,
    is_session_start,
    concat(
        anonymous_id,
        '-',
        cast(
            sum(cast(is_session_start as int64)) over (
                partition by anonymous_id order by event_at, event_id
            ) as string
        )
    ) as session_id
from flagged
