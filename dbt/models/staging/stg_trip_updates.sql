-- stg_trip_updates.sql
-- Cleans RAW.TRIP_UPDATES into a typed, analysis-ready staging layer.
-- Source of truth for delay = MTA feed's delay_seconds (validated separately
-- against a schedule-derived figure; see validation notebook, not run in prod).
--
-- Design notes:
--  * delay_seconds bounds (-3600, 7200) mirror the Spark quarantine filter so
--    staging and ingestion agree on what "in range" means. Out-of-range rows
--    should already be quarantined upstream; this is defense-in-depth.
--  * event_time is the producer poll time (when we observed the feed).
--  * arrival_time / departure_time are unix timestamps -> cast for any downstream
--    dwell or schedule work; not used by the reliability marts.

with source as (

    select
        event_time,
        feed_id,
        trip_id,
        route_id,
        stop_id,
        arrival_time,
        departure_time,
        delay_seconds,
        dt
    from {{ source('raw', 'trip_updates') }}

),

typed as (

    select
        event_time,
        feed_id,
        trip_id,
        route_id,
        stop_id,
        to_timestamp(arrival_time)    as predicted_arrival_ts,
        to_timestamp(departure_time)  as predicted_departure_ts,
        delay_seconds,
        dt,
        -- time dimensions derived once here so every mart slices consistently
        hour(event_time)              as event_hour,
        dayofweek(event_time)         as event_dow,   -- 1=Sun ... 7=Sat (Snowflake default)
        case
            when dayofweek(event_time) in (1, 7) then 'weekend'
            else 'weekday'
        end                           as day_type,
        case
            when hour(event_time) between 7  and 9  then 'am_peak'
            when hour(event_time) between 17 and 19 then 'pm_peak'
            when hour(event_time) between 10 and 16 then 'midday'
            when hour(event_time) between 20 and 23 then 'evening'
            else 'overnight'
        end                           as time_period
    from source

),

bounded as (

    select *
    from typed
    -- null delay is allowed through (means "no delay reported" per our decision);
    -- it is simply excluded from delay aggregates downstream via the WHERE in marts.
    where delay_seconds is null
       or delay_seconds between -3600 and 7200

)

select * from bounded
