-- mart_line_reliability.sql
-- THE SPINE. Line-level reliability sliced by week x time_period x day_type.
-- This is the most defensible artifact in the project: every value traces to a
-- directly measured delay_seconds, and percentiles carry no hidden threshold.
--
-- Defensibility rules baked in:
--  * p50 / p90 / p95 are the PRIMARY metrics (no chosen parameter).
--  * on_time_pct is DERIVED and the threshold is named in the column itself
--    (on_time = delay <= 60s). One threshold, used nowhere else inconsistently.
--  * sample_count is always present so a reader can judge whether a bucket is
--    thin. Buckets with tiny n are kept (not silently dropped) but visible.
--  * This mart provides the BASELINE that makes the alert mart's "departure from
--    typical" claim sayable. The two ship together by design.

with base as (

    select *
    from {{ ref('stg_trip_updates') }}
    where delay_seconds is not null   -- delay aggregates need a delay

)

select
    date_trunc('week', event_time)  as week,
    route_id,
    day_type,
    time_period,

    count(*)                        as sample_count,

    -- PRIMARY: parameter-free distribution shape
    round(percentile_cont(0.50) within group (order by delay_seconds), 1) as p50_delay_sec,
    round(percentile_cont(0.90) within group (order by delay_seconds), 1) as p90_delay_sec,
    round(percentile_cont(0.95) within group (order by delay_seconds), 1) as p95_delay_sec,
    round(avg(delay_seconds), 1)                                          as avg_delay_sec,

    -- DERIVED: threshold named explicitly in the column name
    round(sum(case when delay_seconds <= 60  then 1 else 0 end) * 100.0 / count(*), 1)
        as on_time_pct_le_60s,
    round(sum(case when delay_seconds > 300 then 1 else 0 end) * 100.0 / count(*), 1)
        as major_delay_pct_gt_300s

from base
group by 1, 2, 3, 4
order by week, route_id, day_type, time_period
