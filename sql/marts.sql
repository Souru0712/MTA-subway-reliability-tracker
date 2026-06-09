-- date range
SELECT 
    -- Grouping by the local New York service date
    TO_DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(arrival_time))) AS service_date_ny,
    
    -- Metrics per day
    COUNT(*) AS daily_record_count,
    COUNT(DISTINCT trip_id || '|' || stop_id) AS daily_distinct_trip_stops
FROM RAW.TRIP_UPDATES
GROUP BY 1
ORDER BY service_date_ny DESC;



-- Task 1: Missing delay_seconds Field Across the System
    -- Show the audience that almost all subway lines are missing official delay metrics, 
    -- validating your architectural pivot to a custom compute engine.
SELECT
    route_id,
    COUNT(*)                                            AS total,
    SUM(CASE WHEN delay_seconds = 0 THEN 1 ELSE 0 END)  AS zeros,
    SUM(CASE WHEN delay_seconds <> 0 THEN 1 ELSE 0 END) AS nonzeros,
    SUM(CASE WHEN delay_seconds < 0 THEN 1 ELSE 0 END)  AS negatives,
    MIN(delay_seconds)                                  AS min_delay,
    MAX(delay_seconds)                                  AS max_delay
FROM RAW.TRIP_UPDATES
WHERE delay_seconds IS NOT NULL
GROUP BY route_id
ORDER BY nonzeros DESC;



-- Task 2: Demonstrate the 176x History Inflation Bug (The L Train Profile)
    -- Show how a single trip/stop combination has massive duplicate prediction loops
    -- due to continuous polling, requiring a strict deduplication strategy.
SELECT
    COUNT(*)                                      AS total_predictions,
    COUNT(DISTINCT trip_id || '|' || stop_id)     AS distinct_trip_stops,
    ROUND(COUNT(*) * 1.0 / 
          COUNT(DISTINCT trip_id || '|' || stop_id), 1) AS avg_predictions_per_stop
FROM MTA.RAW.TRIP_UPDATES
WHERE route_id = 'L';



-- Task 3: Execute the Windowed Deduplication (Fixing the 176x Inflation)
    -- Show how you used ROW_NUMBER() OVER (PARTITION BY ... ORDER BY event_time DESC) \
    -- to capture only the absolute latest, most accurate prediction state before a train arrives.
WITH ranked AS (
    SELECT
        *,
        TO_DATE(
            CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(arrival_time))
        ) AS service_date_ny,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id, stop_id,
                TO_DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(arrival_time)))
            ORDER BY event_time DESC
        ) AS rn
    FROM RAW.TRIP_UPDATES
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn = 1
LIMIT 100;



-- Task 4: Verify the 15-Second Agreement & Mid-Night Past Boundary Defect Fix
    -- Run this audit script to demonstrate that your timezone shifts and midnight adjustments 
    -- bring computed delays within a tight 15-second median alignment with 
    -- the official feed, while highlighting the tracked midnight_outliers.
WITH dedup AS (
    SELECT
        trip_id, route_id, stop_id, event_time,
        arrival_time AS predicted_arrival_unix,
        delay_seconds AS delay_feed,
        TO_DATE(CONVERT_TIMEZONE('UTC','America/New_York',TO_TIMESTAMP(arrival_time))) AS service_date_ny,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id, stop_id,
                TO_DATE(CONVERT_TIMEZONE('UTC','America/New_York',TO_TIMESTAMP(arrival_time)))
            ORDER BY event_time DESC
        ) AS rn
    FROM RAW.TRIP_UPDATES
    WHERE route_id = 'L'
      AND delay_seconds IS NOT NULL
      AND delay_seconds BETWEEN -3600 AND 7200
      AND DAYOFWEEK(event_time) NOT IN (1,7)
),
final_pred AS (
    SELECT * FROM dedup WHERE rn = 1
),
joined AS (
    SELECT
        f.event_time, f.trip_id, f.stop_id, f.predicted_arrival_unix, f.delay_feed,
        s.arrival_time AS scheduled_hms
    FROM final_pred f
    JOIN RAW.STOP_TIMES_FLAT s
      ON s.trip_id LIKE '%' || f.trip_id || '%'
     AND s.stop_id = f.stop_id
     AND s.trip_id ILIKE '%weekday%'
),
computed AS (
    SELECT *,
        DATE_PART('epoch_second',
            CONVERT_TIMEZONE('America/New_York','UTC',
                DATEADD('second',
                    SPLIT_PART(scheduled_hms,':',1)::INT*3600 +
                    SPLIT_PART(scheduled_hms,':',2)::INT*60 +
                    SPLIT_PART(scheduled_hms,':',3)::INT,
                    DATE_TRUNC('day', CONVERT_TIMEZONE('UTC','America/New_York',
                        TO_TIMESTAMP(predicted_arrival_unix))::TIMESTAMP_NTZ)
                )
            )
        ) AS scheduled_arrival_unix
    FROM joined
), 
-- For auditing individual rows: To keep a late-night train tied to its original service day, the schedule keeps counting past 24 hours. A train scheduled for 1:30 AM is written as 25:30:00.
-- diffs AS (
--     SELECT 
--         event_time,
--         trip_id,
--         stop_id,
--         delay_feed,
--         predicted_arrival_unix,
--         scheduled_arrival_unix,
--         (predicted_arrival_unix - scheduled_arrival_unix) AS computed_delay,
--         ABS(delay_feed - (predicted_arrival_unix - scheduled_arrival_unix)) AS abs_diff
--     FROM computed
-- )
-- SELECT * 
-- FROM diffs
-- WHERE abs_diff > 80000;
diffs AS (
    SELECT delay_feed,
        predicted_arrival_unix - scheduled_arrival_unix AS computed_delay,
        ABS(delay_feed - (predicted_arrival_unix - scheduled_arrival_unix)) AS abs_diff
    FROM computed
)
SELECT
    COUNT(*)                                                         AS n_validated,
    ROUND(MEDIAN(abs_diff),1)                                        AS median_abs_diff_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY abs_diff),1)  AS p95_abs_diff_sec,
    ROUND(SUM(CASE WHEN abs_diff <= 30 THEN 1 ELSE 0 END)*100.0/COUNT(*),1)  AS pct_within_30s,
    ROUND(SUM(CASE WHEN abs_diff <= 120 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_within_2min,
    SUM(CASE WHEN abs_diff > 80000 THEN 1 ELSE 0 END)                AS midnight_outliers
FROM diffs;



-- Task 5: Display the Final Line Reliability Aggregations (The dbt Mart Output)
    -- Show the final core output of your data warehouse modeling. 
    -- This showcases weekly on-time percentiles (p50, p90, p95) and \
    -- macro trends segmented by traffic periods (am_peak, pm_peak, etc.).
WITH ranked AS (
    SELECT
        trip_id, route_id, stop_id, arrival_time AS predicted_arrival_unix, event_time,
        delay_seconds AS delay_feed,
        TO_DATE(CONVERT_TIMEZONE('UTC','America/New_York', TO_TIMESTAMP(arrival_time))) AS service_date_ny,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id, stop_id,
                TO_DATE(CONVERT_TIMEZONE('UTC','America/New_York', TO_TIMESTAMP(arrival_time)))
            ORDER BY event_time DESC
        ) AS rn
    FROM MTA.RAW.TRIP_UPDATES
),
final_prediction AS (
    SELECT * 
    FROM ranked 
    WHERE rn = 1
),
with_schedule AS (
    SELECT fp.*, s.arrival_time AS scheduled_hms
    FROM final_prediction fp
    LEFT JOIN MTA.RAW.STOP_TIMES_FLAT s
        ON s.trip_id LIKE '%' || fp.trip_id || '%'
       AND s.stop_id = fp.stop_id
       AND s.trip_id ILIKE '%weekday%'
),
computed AS (
    SELECT *,
        CASE WHEN scheduled_hms IS NOT NULL THEN
            DATE_PART('epoch_second',
                CONVERT_TIMEZONE('America/New_York','UTC',
                    DATEADD('second',
                        SPLIT_PART(scheduled_hms,':',1)::INT*3600 +
                        SPLIT_PART(scheduled_hms,':',2)::INT*60 +
                        SPLIT_PART(scheduled_hms,':',3)::INT,
                        service_date_ny::TIMESTAMP_NTZ
                    )
                )
            )
        END AS scheduled_arrival_unix,
        HOUR(CONVERT_TIMEZONE('UTC','America/New_York', TO_TIMESTAMP(predicted_arrival_unix))) AS event_hour
    FROM with_schedule
),
final AS (
    SELECT *,
        (predicted_arrival_unix - scheduled_arrival_unix) AS delay_computed,
        CASE WHEN DAYOFWEEK(service_date_ny) IN (1,7) THEN 'weekend' ELSE 'weekday' END AS day_type,
        CASE
            WHEN event_hour BETWEEN 7  AND 9  THEN 'am_peak'
            WHEN event_hour BETWEEN 17 AND 19 THEN 'pm_peak'
            WHEN event_hour BETWEEN 10 AND 16 THEN 'midday'
            WHEN event_hour BETWEEN 20 AND 23 THEN 'evening'
            ELSE 'overnight'
        END AS time_period
    FROM computed
),
base AS (
    SELECT * FROM final
    WHERE scheduled_arrival_unix IS NOT NULL
      AND ABS(delay_computed) < 80000
      AND delay_computed BETWEEN -3600 AND 7200
)
SELECT
    DATE_TRUNC('week', TO_TIMESTAMP(predicted_arrival_unix))                               AS week,
    route_id, day_type, time_period,
    COUNT(*)                                                                               AS arrivals,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY delay_computed), 1)                 AS p50_delay_sec,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY delay_computed), 1)                 AS p90_delay_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_computed), 1)                 AS p95_delay_sec,
    ROUND(AVG(delay_computed), 1)                                                          AS avg_delay_sec,
    ROUND(SUM(CASE WHEN delay_computed <= 60  THEN 1 ELSE 0 END)*100.0/COUNT(*),1)         AS on_time_pct_le_60s,
    -- on_time_pct_le_60s - LAG(on_time_pct_le_60s, 1) OVER (
    --     PARTITION BY route_id, day_type, time_period 
    --     ORDER BY DATE_TRUNC('week', TO_TIMESTAMP(predicted_arrival_unix))
    -- ) AS on_time_pct_change_pp,
    ROUND(SUM(CASE WHEN delay_computed > 300  THEN 1 ELSE 0 END)*100.0/COUNT(*),1)         AS major_delay_pct_gt_300s
FROM base
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 100   -- <-- This enforces your noise suppression floor live
ORDER BY route_id, time_period, week, day_type;