-- ============================================================
-- Urban Traffic EDA — Analysis Queries
-- ============================================================


-- ── 1. PEAK HOUR ANALYSIS ───────────────────────────────────

-- Average vehicle count by hour of day (all intersections)
SELECT
    EXTRACT(HOUR FROM timestamp)        AS hour_of_day,
    ROUND(AVG(vehicle_count), 2)        AS avg_vehicles,
    MAX(vehicle_count)                  AS max_vehicles,
    COUNT(*)                            AS observations
FROM traffic_observations
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- Top 5 peak hours by average vehicle count
SELECT
    EXTRACT(HOUR FROM timestamp)        AS hour_of_day,
    ROUND(AVG(vehicle_count), 2)        AS avg_vehicles
FROM traffic_observations
GROUP BY hour_of_day
ORDER BY avg_vehicles DESC
LIMIT 5;


-- Peak hour breakdown by day of week
SELECT
    TO_CHAR(timestamp, 'Day')           AS day_of_week,
    EXTRACT(HOUR FROM timestamp)        AS hour_of_day,
    ROUND(AVG(vehicle_count), 2)        AS avg_vehicles
FROM traffic_observations
GROUP BY day_of_week, hour_of_day
ORDER BY avg_vehicles DESC
LIMIT 20;


-- Congestion level distribution during peak hours (7-10am, 5-8pm)
SELECT
    congestion_level,
    COUNT(*)                            AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM traffic_observations
WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 7 AND 10
   OR EXTRACT(HOUR FROM timestamp) BETWEEN 17 AND 20
GROUP BY congestion_level
ORDER BY count DESC;


-- ── 2. SIGNAL TIMING VS WAIT TIME ───────────────────────────

-- Correlation view: green duration vs average wait time
SELECT
    green_duration_sec,
    ROUND(AVG(wait_time_sec), 2)        AS avg_wait_time,
    COUNT(*)                            AS observations
FROM traffic_observations
WHERE green_duration_sec IS NOT NULL
  AND wait_time_sec IS NOT NULL
GROUP BY green_duration_sec
ORDER BY green_duration_sec;


-- Average wait time by signal phase
SELECT
    signal_phase,
    ROUND(AVG(wait_time_sec), 2)        AS avg_wait_time,
    ROUND(AVG(vehicle_count), 2)        AS avg_vehicles,
    COUNT(*)                            AS observations
FROM traffic_observations
GROUP BY signal_phase
ORDER BY avg_wait_time DESC;


-- Intersections with highest average wait time
SELECT
    o.intersection_id,
    s.location_name,
    ROUND(AVG(o.wait_time_sec), 2)      AS avg_wait_time,
    ROUND(AVG(o.green_duration_sec), 2) AS avg_green_duration,
    s.green_ratio
FROM traffic_observations o
JOIN traffic_signals s ON o.intersection_id = s.intersection_id
GROUP BY o.intersection_id, s.location_name, s.green_ratio
ORDER BY avg_wait_time DESC
LIMIT 10;


-- Wait time efficiency: does longer green = less wait?
SELECT
    CASE
        WHEN green_duration_sec < 30  THEN 'Short (<30s)'
        WHEN green_duration_sec < 60  THEN 'Medium (30-60s)'
        ELSE                               'Long (>60s)'
    END                                 AS green_bucket,
    ROUND(AVG(wait_time_sec), 2)        AS avg_wait_time,
    ROUND(AVG(vehicle_count), 2)        AS avg_vehicles
FROM traffic_observations
WHERE green_duration_sec IS NOT NULL
GROUP BY green_bucket
ORDER BY avg_wait_time DESC;
