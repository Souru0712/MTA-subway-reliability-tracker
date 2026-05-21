-- mart_line_reliability.sql
-- Cross-line reliability by week x route x day_type x time_period.
-- Uses delay_computed (schedule-derived, validated on the L to 15s median).
-- Now genuinely cross-line: every route has computed delays, unlike the feed's
-- delay_seconds which only populated for the L.
--
-- Defensibility:
--  * percentiles are primary (no hidden threshold);
--  * on_time_pct is derived with the threshold NAMED in the column;
--  * sample_count is per-ARRIVAL now (base is deduped), not per-prediction, so
--    counts are real train-arrivals -- a count you can actually cite.

with base as (
    select * from {{ ref('stg_trip_updates') }}
)

select
    date_trunc('week', to_timestamp(predicted_arrival_unix)) as week,
    route_id,
    day_type,
    time_period,

    count(*) as arrivals,    -- real arrivals (deduped), not predictions

    round(percentile_cont(0.50) within group (order by delay_computed), 1) as p50_delay_sec,
    round(percentile_cont(0.90) within group (order by delay_computed), 1) as p90_delay_sec,
    round(percentile_cont(0.95) within group (order by delay_computed), 1) as p95_delay_sec,
    round(avg(delay_computed), 1)                                          as avg_delay_sec,

    round(sum(case when delay_computed <= 60  then 1 else 0 end) * 100.0 / count(*), 1)
        as on_time_pct_le_60s,
    round(sum(case when delay_computed > 300 then 1 else 0 end) * 100.0 / count(*), 1)
        as major_delay_pct_gt_300s

from base
group by 1, 2, 3, 4
having count(*) >= 30   -- suppress single-train noise (e.g. a 1-arrival bucket
                        -- whose lone weird trip becomes "the median"). 30 is a
                        -- pragmatic floor; thin buckets are excluded, not hidden.
order by week, route_id, day_type, time_period
