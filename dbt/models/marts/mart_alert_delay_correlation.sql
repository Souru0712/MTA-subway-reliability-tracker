-- mart_alert_delay_correlation.sql
-- THE HEADLINE. Are delays elevated during a line's service alerts, and does the
-- rise PRECEDE the alert's activation?
--
-- This is the project's differentiator: almost no hobby MTA project correlates the
-- separate alerts feed against the delay feed. But it is only defensible if:
--   (1) the join uses the alert's ACTUAL active window, not a poll-time hour bucket;
--   (2) "elevated" is measured against a BASELINE, not stated in the absolute;
--   (3) causal language is absent -- we describe timing, we do not prosecute it.
--
-- Two pieces:
--   A) during_vs_baseline : delay inside the alert window vs the line/period baseline
--   B) lead_time          : did delays cross the baseline BEFORE active_start_ts?
--
-- WINDOW POLICY (stated, not hidden):
--   * alert window = [active_start_ts, COALESCE(active_end_ts, active_start_ts + 1hr)]
--     A null end is assumed to last 1 hour. This is a CHOICE; change the interval
--     and the "during" population changes. It is named here so it is defensible.
--   * pre-alert window = [active_start_ts - 60min, active_start_ts)
--     We look 60 minutes back for the rise. A degradation that began earlier than
--     60 min will be under-detected; this is the stated limit of the lead-time claim.

with trips as (
    select route_id, event_time, delay_seconds, time_period, day_type
    from {{ ref('stg_trip_updates') }}
    where delay_seconds is not null
),

alerts as (
    select
        alert_id,
        route_id,
        effect,
        active_start_ts,
        coalesce(active_end_ts, dateadd('minute', 60, active_start_ts)) as active_end_ts
    from {{ ref('stg_alerts') }}
),

-- (A) Delays observed DURING an alert's real active window, per alert.
during_alert as (
    select
        a.alert_id,
        a.route_id,
        a.effect,
        count(t.delay_seconds)                                                as n_during,
        round(percentile_cont(0.50) within group (order by t.delay_seconds), 1) as p50_during,
        round(avg(t.delay_seconds), 1)                                        as avg_during
    from alerts a
    join trips t
      on t.route_id = a.route_id
     and t.event_time >= a.active_start_ts
     and t.event_time <  a.active_end_ts
    group by a.alert_id, a.route_id, a.effect
),

-- Baseline = the line's typical delay OUTSIDE any alert window, same line.
-- A row is "baseline" if it does not fall inside ANY alert window for its route.
trips_flagged as (
    select
        t.route_id,
        t.delay_seconds,
        case when exists (
            select 1 from alerts a
            where a.route_id = t.route_id
              and t.event_time >= a.active_start_ts
              and t.event_time <  a.active_end_ts
        ) then 1 else 0 end as in_alert
    from trips t
),

baseline as (
    select
        route_id,
        count(*)                                                              as n_baseline,
        round(percentile_cont(0.50) within group (order by delay_seconds), 1) as p50_baseline,
        round(avg(delay_seconds), 1)                                          as avg_baseline
    from trips_flagged
    where in_alert = 0
    group by route_id
),

-- (B) Lead time: per alert, the earliest moment in the 60-min pre-window where the
-- line's delay crossed its own baseline p50. NULL = never crossed before activation.
pre_alert as (
    select
        a.alert_id,
        a.route_id,
        a.active_start_ts,
        b.p50_baseline,
        min(t.event_time) as first_cross_ts
    from alerts a
    join baseline b on b.route_id = a.route_id
    join trips t
      on t.route_id = a.route_id
     and t.event_time >= dateadd('minute', -60, a.active_start_ts)
     and t.event_time <  a.active_start_ts
     and t.delay_seconds > b.p50_baseline
    group by a.alert_id, a.route_id, a.active_start_ts, b.p50_baseline
)

select
    d.route_id,
    d.alert_id,
    d.effect,
    d.n_during,
    d.p50_during,
    b.p50_baseline,
    -- the defensible delta: delay during the alert vs the line's typical level
    round(d.p50_during - b.p50_baseline, 1)                  as p50_delta_vs_baseline,
    -- lead time in minutes: positive = delays rose this many minutes BEFORE the alert
    case
        when p.first_cross_ts is null then null
        else round(datediff('second', p.first_cross_ts, d_alert.active_start_ts) / 60.0, 1)
    end                                                       as lead_time_min
from during_alert d
join baseline b      on b.route_id = d.route_id
join alerts d_alert  on d_alert.alert_id = d.alert_id and d_alert.route_id = d.route_id
left join pre_alert p on p.alert_id = d.alert_id and p.route_id = d.route_id
order by p50_delta_vs_baseline desc
