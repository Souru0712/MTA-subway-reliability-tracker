-- stg_alerts.sql
-- Cleans RAW.ALERTS and resolves the true active window of each alert.
--
-- THE KEY FIX: the alert's real time window is [active_period_start, active_period_end],
-- NOT event_time (which is just when our producer happened to poll the feed).
-- Correlation marts must join on this window, not on a DATE_TRUNC('hour') bucket.
--
-- active_period_end is nullable. A null end means the alert had no published
-- expiry at observation time. We do NOT invent an end here; downstream we treat
-- a null end with an explicit COALESCE policy so the assumption is visible.

with source as (

    select
        event_time,
        feed_id,
        alert_id,
        route_id,
        stop_id,
        cause,
        effect,
        header,
        active_period_start,
        active_period_end,
        dt
    from {{ source('raw', 'alerts') }}
    where route_id is not null   -- line-level correlation needs a line

),

typed as (

    select
        alert_id,
        route_id,
        stop_id,
        cause,
        effect,
        header,
        to_timestamp(active_period_start) as active_start_ts,
        -- nullable: real expiry may be absent
        to_timestamp(active_period_end)   as active_end_ts,
        dt
    from source

),

-- One row per (alert_id, route_id) with a resolved window.
-- An alert can be re-observed across many polls; collapse to the widest
-- window we ever saw for that alert so we don't double-count the same alert.
deduped as (

    select
        alert_id,
        route_id,
        any_value(stop_id)  as stop_id,
        any_value(cause)    as cause,
        any_value(effect)   as effect,
        any_value(header)   as header,
        min(active_start_ts) as active_start_ts,
        max(active_end_ts)   as active_end_ts
    from typed
    group by alert_id, route_id

)

select * from deduped
