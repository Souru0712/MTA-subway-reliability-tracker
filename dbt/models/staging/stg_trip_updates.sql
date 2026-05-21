-- stg_trip_updates.sql
-- Typed, deduplicated, delay-computed base for all reliability marts.
--
-- THREE corrections established by validation, all applied here at the base so
-- every downstream mart inherits them:
--
--  1. DEDUP TO FINAL PREDICTION. RAW.TRIP_UPDATES holds ~134 predictions per
--     train-arrival (full prediction history). Raw aggregates were inflated ~134x
--     and biased toward trains lingering in the prediction window. We keep one row
--     per (trip, stop, NY service date): the latest event_time = the prediction
--     closest to actual arrival.
--
--  2. COMPUTED DELAY, NOT FEED DELAY. MTA only populates delay_seconds for the L
--     train (all 29 other lines return exactly 0 across tens of millions of rows).
--     So we derive delay = predicted_arrival - scheduled_arrival via the schedule
--     join. VALIDATED on the L against the feed: 15s median agreement, 90% within
--     30s, after dedup. The feed's delay_seconds is retained only as delay_feed
--     for reference / the L cross-check, never as the analytic delay.
--
--  3. TIMEZONE. Schedule HH:MM:SS is America/New_York; predicted arrival is UTC
--     epoch. All reconstruction is done in NY local time then converted back to
--     UTC, DST-aware (the L validation exposed a 4-hour EDT offset otherwise).

with ranked as (

    select
        event_time,
        feed_id,
        trip_id,
        route_id,
        stop_id,
        arrival_time   as predicted_arrival_unix,
        departure_time as predicted_departure_unix,
        delay_seconds  as delay_feed,             -- L-only; reference, not analytic
        to_date(convert_timezone('UTC','America/New_York',
            to_timestamp(arrival_time)))          as service_date_ny,
        row_number() over (
            partition by trip_id, stop_id,
                to_date(convert_timezone('UTC','America/New_York',
                    to_timestamp(arrival_time)))
            order by event_time desc
        ) as rn
    from {{ source('raw', 'trip_updates') }}

),

final_prediction as (

    select * exclude (rn)
    from ranked
    where rn = 1

),

with_schedule as (

    select
        fp.*,
        s.arrival_time as scheduled_hms
    from final_prediction fp
    left join {{ source('raw', 'stop_times_flat') }} s
      on s.trip_id like '%' || fp.trip_id || '%'
     and s.stop_id = fp.stop_id
     and s.trip_id ilike '%weekday%'              -- weekday schedule (see note below)

),

computed as (

    select
        event_time,
        trip_id,
        route_id,
        stop_id,
        service_date_ny,
        predicted_arrival_unix,
        delay_feed,
        scheduled_hms,
        -- schedule time reconstructed in NY local, converted back to UTC epoch
        case when scheduled_hms is not null then
            date_part('epoch_second',
                convert_timezone('America/New_York','UTC',
                    dateadd('second',
                        split_part(scheduled_hms,':',1)::int*3600 +
                        split_part(scheduled_hms,':',2)::int*60 +
                        split_part(scheduled_hms,':',3)::int,
                        date_trunc('day', convert_timezone('UTC','America/New_York',
                            to_timestamp(predicted_arrival_unix))::timestamp_ntz)
                    )
                )
            )
        end as scheduled_arrival_unix,
        -- time dimensions in NY local time
        hour(convert_timezone('UTC','America/New_York',
            to_timestamp(predicted_arrival_unix)))      as event_hour,
        dayofweek(convert_timezone('UTC','America/New_York',
            to_timestamp(predicted_arrival_unix)))      as event_dow
    from with_schedule

),

final as (

    select
        *,
        (predicted_arrival_unix - scheduled_arrival_unix) as delay_computed,
        case when dayofweek(service_date_ny) in (1,7) then 'weekend' else 'weekday' end as day_type,
        case
            when event_hour between 7  and 9  then 'am_peak'
            when event_hour between 17 and 19 then 'pm_peak'
            when event_hour between 10 and 16 then 'midday'
            when event_hour between 20 and 23 then 'evening'
            else 'overnight'
        end as time_period
    from computed

)

select *
from final
-- keep only rows we could compute a delay for, and drop the ~1% midnight-boundary
-- artifact (|delay| beyond ~22h is the cross-midnight anchoring edge, not real).
where scheduled_arrival_unix is not null
  and abs(delay_computed) < 80000
  and delay_computed between -3600 and 7200   -- same sane bound used in validation
