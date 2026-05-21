-- validate_delay_seconds.sql
-- ONE-TIME VALIDATION. Not a dbt model, not a production path. Run once on a
-- sample to confirm the feed's delay_seconds agrees with a schedule-derived
-- delay, record the agreement, then never run again. Production marts read
-- delay_seconds directly.
--
-- WHY THIS IS NOT PRODUCTION:
--   * The join is LIKE '%' || rt_trip_id || '%' (substring), which cannot use a
--     hash join and degrades toward a cross-product. Fine on a LIMITed sample,
--     disastrous nightly.
--   * The diagnostics proved the realtime fragment matches the static id ONCE PER
--     SERVICE DAY-TYPE (Weekday/Saturday/Sunday). A bare substring join fans out
--     2-3x and inflates any average. We disambiguate by day-type below.
--
-- DESIGN:
--   * Restrict to WEEKDAY observations only. The static calendar is ternary
--     (Weekday/Saturday/Sunday) but our day_type is binary, so weekend rows are
--     ambiguous (Sat vs Sun). Weekday-only removes the ambiguity entirely, which
--     is acceptable for a sample validation.
--   * Require the static id to contain 'Weekday' so we match the weekday schedule
--     row, collapsing the day-type fan-out to ~1.
--   * Parse the static HH:MM:SS scheduled time, handling GTFS past-midnight times
--     (e.g. '25:14:00') which DATEADD-from-midnight handles naturally.
--   * Report BOTH delays and their difference. The headline number is the median
--     absolute difference and the share within a small tolerance.

with rt as (
    select
        trip_id      as rt_trip_id,
        route_id,
        stop_id,
        dt,
        arrival_time as predicted_arrival_unix,   -- unix seconds
        delay_seconds as feed_delay
    from {{ source('raw', 'trip_updates') }}
    where delay_seconds is not null
      and delay_seconds between -3600 and 7200
      and dayofweek(event_time) not in (1, 7)     -- weekday observations only
    -- SAMPLE: keep this validation cheap. Pull a bounded slice.
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
        s.arrival_time as scheduled_hms          -- 'HH:MM:SS' string, may exceed 24h
    from rt
    join {{ source('raw', 'stop_times_flat') }} s
      on s.trip_id like '%' || rt.rt_trip_id || '%'   -- substring (tract_geoid pattern)
     and s.stop_id = rt.stop_id                        -- same physical stop on the trip
     and s.trip_id ilike '%weekday%'                   -- collapse day-type fan-out
),

computed as (
    select
        *,
        -- scheduled arrival as a unix timestamp: midnight of the partition date
        -- plus the HH:MM:SS offset (handles 25:xx:xx past-midnight times correctly).
        datediff(
            'second',
            '1970-01-01'::timestamp_ntz,
            dateadd(
                'second',
                split_part(scheduled_hms, ':', 1)::int * 3600 +
                split_part(scheduled_hms, ':', 2)::int * 60 +
                split_part(scheduled_hms, ':', 3)::int,
                to_date(dt)
            )
        ) as scheduled_arrival_unix
    from joined
),

diffs as (
    select
        rt_trip_id,
        route_id,
        feed_delay,
        (predicted_arrival_unix - scheduled_arrival_unix) as computed_delay,
        abs(feed_delay - (predicted_arrival_unix - scheduled_arrival_unix)) as abs_diff
    from computed
)

-- The validation verdict. One row. This is the sentence you quote in interviews.
select
    count(*)                                                          as n_validated,
    round(median(abs_diff), 1)                                        as median_abs_diff_sec,
    round(percentile_cont(0.95) within group (order by abs_diff), 1)  as p95_abs_diff_sec,
    round(sum(case when abs_diff <= 30  then 1 else 0 end) * 100.0 / count(*), 1) as pct_within_30s,
    round(sum(case when abs_diff <= 120 then 1 else 0 end) * 100.0 / count(*), 1) as pct_within_2min
from diffs
