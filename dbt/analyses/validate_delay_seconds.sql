-- validate_delay_seconds.sql
-- ONE-TIME VALIDATION. Not a dbt model, not a production path. Run once on a
-- sample to confirm the feed's delay_seconds agrees with a schedule-derived
-- delay, record the agreement, then never run again. Production marts read
-- delay_seconds directly.
--
-- TIMEZONE FIX (why it's here, not a comment):
--   MTA GTFS static schedule times are America/New_York local (EDT = UTC-4 in
--   summer, EST = UTC-5 in winter). The feed's predicted_arrival_unix is UTC.
--   A naive to_timestamp() renders unix time in UTC, producing a systematic
--   ~14,400s offset vs the local HH:MM:SS. The fix anchors the scheduled time
--   in NYC local time using convert_timezone (DST-aware), then converts back to
--   UTC seconds before differencing. Hardcoding -14400 would break on EST dates;
--   the named zone handles it correctly year-round.
--
-- DESIGN:
--   * Weekday-only. Static calendar is ternary (Weekday/Saturday/Sunday); our
--     day_type is binary, so weekend rows are ambiguous. Weekday-only removes it.
--   * Require static id to contain 'Weekday' to collapse day-type fan-out to ~1.
--   * LIMIT 5000: one-time check, keep it cheap.
--   * Report BOTH delays and their difference. Headline = median abs diff and
--     share within tolerance.

with rt as (
    select
        trip_id       as rt_trip_id,
        route_id,
        stop_id,
        dt,
        arrival_time  as predicted_arrival_unix,
        delay_seconds as feed_delay
    from {{ source('raw', 'trip_updates') }}
    where delay_seconds is not null
      and delay_seconds between -3600 and 7200
      and dayofweek(event_time) not in (1, 7)     -- weekday observations only
    limit 5000
),

joined as (
    select
        rt.rt_trip_id,
        rt.route_id,
        rt.stop_id,
        rt.dt,
        rt.predicted_arrival_unix,
        rt.feed_delay,
        s.trip_id      as static_trip_id,
        s.arrival_time as scheduled_hms            -- 'HH:MM:SS' string, America/New_York local
    from rt
    join {{ source('raw', 'stop_times_flat') }} s
      on s.trip_id like '%' || rt.rt_trip_id || '%'
     and s.stop_id = rt.stop_id
     and s.trip_id ilike '%weekday%'               -- collapse day-type fan-out
),

computed as (
    select
        *,
        -- Step 1: parse scheduled HH:MM:SS as NYC-local timestamp on the partition date.
        --   GTFS past-midnight times (e.g. '25:14:00') roll into the next day naturally.
        dateadd(
            'second',
            split_part(scheduled_hms, ':', 1)::int * 3600 +
            split_part(scheduled_hms, ':', 2)::int * 60 +
            split_part(scheduled_hms, ':', 3)::int,
            to_date(dt)::timestamp_ntz             -- midnight NYC local on partition date
        ) as scheduled_local_ts,

        -- Step 2: render predicted unix timestamp in NYC local time for inspection.
        --   DST-aware: applies UTC-4 (EDT) or UTC-5 (EST) per each row's actual date.
        convert_timezone('UTC', 'America/New_York',
            to_timestamp(predicted_arrival_unix)
        ) as predicted_local_ts,

        -- Step 3: convert scheduled local back to UTC seconds to match the feed's epoch.
        datediff(
            'second',
            '1970-01-01'::timestamp_ntz,
            convert_timezone('America/New_York', 'UTC', scheduled_local_ts)
        ) as scheduled_arrival_utc_unix
    from joined
),

diffs as (
    select
        rt_trip_id,
        route_id,
        feed_delay,
        predicted_local_ts,
        scheduled_local_ts,
        (predicted_arrival_unix - scheduled_arrival_utc_unix) as computed_delay,
        abs(feed_delay - (predicted_arrival_unix - scheduled_arrival_utc_unix)) as abs_diff
    from computed
)

-- The validation verdict. One row. This is the sentence you quote in interviews.
select
    count(*)                                                                      as n_validated,
    round(median(abs_diff), 1)                                                    as median_abs_diff_sec,
    round(percentile_cont(0.95) within group (order by abs_diff), 1)              as p95_abs_diff_sec,
    round(sum(case when abs_diff <= 30  then 1 else 0 end) * 100.0 / count(*), 1) as pct_within_30s,
    round(sum(case when abs_diff <= 120 then 1 else 0 end) * 100.0 / count(*), 1) as pct_within_2min
from diffs
