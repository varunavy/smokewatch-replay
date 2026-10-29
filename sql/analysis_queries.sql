
-- SmokeWatch Replay
-- PostgreSQL analytical queries

-- Q1: Number of source events by agency
SELECT
    s.source_name,
    COUNT(e.source_event_key) AS total_records
FROM dim_source s
LEFT JOIN fact_source_event e
    ON s.source_id = e.source_id
GROUP BY s.source_name
ORDER BY s.source_name;


-- Q2: Results by county
SELECT
    c.county_name,
    COUNT(f.fact_id) AS county_hour_rows,
    SUM(COALESCE(f.fire_detection_count, 0))
        AS total_fire_detections,
    ROUND(AVG(f.pm25_ug_m3)::numeric, 2)
        AS avg_pm25,
    ROUND(AVG(f.temperature_c)::numeric, 2)
        AS avg_temperature_c
FROM fact_county_hour f
JOIN dim_county c
    ON f.county_id = c.county_id
GROUP BY c.county_name
ORDER BY c.county_name;


-- Q3: Results by replay scenario
SELECT
    s.scenario_code,
    s.speed_seconds_per_event_hour,
    s.delay_level,
    COUNT(f.fact_id) AS county_hour_rows,
    COUNT(f.first_join_ready_at) AS join_ready_rows,
    ROUND(
        100.0 * COUNT(f.first_join_ready_at)
        / NULLIF(COUNT(f.fact_id), 0), 2
    ) AS join_ready_percentage
FROM fact_county_hour f
JOIN dim_replay_scenario s
    ON f.scenario_id = s.scenario_id
GROUP BY
    s.scenario_code,
    s.speed_seconds_per_event_hour,
    s.delay_level
ORDER BY
    s.speed_seconds_per_event_hour DESC,
    s.delay_level;


-- Q4: Timing and performance by replay scenario
SELECT
    s.scenario_code,
    COUNT(f.fact_id) AS total_county_hours,
    ROUND(
        AVG(f.processing_latency_ms)::numeric, 2
    ) AS avg_processing_latency_ms,
    ROUND(
        PERCENTILE_CONT(0.5)
        WITHIN GROUP (
            ORDER BY f.processing_latency_ms
        )::numeric, 2
    ) AS median_processing_latency_ms,
    ROUND(
        PERCENTILE_CONT(0.95)
        WITHIN GROUP (
            ORDER BY f.processing_latency_ms
        )::numeric, 2
    ) AS p95_processing_latency_ms,
    ROUND(
        AVG(f.freshness_lag_event_hours)::numeric, 3
    ) AS avg_freshness_lag_hours
FROM fact_county_hour f
JOIN dim_replay_scenario s
    ON f.scenario_id = s.scenario_id
GROUP BY s.scenario_code
ORDER BY s.scenario_code;


-- Q5: Missing measurements by county
SELECT
    c.county_name,
    COUNT(*) FILTER (
        WHERE NOT f.firms_available
    ) AS missing_firms_hours,
    COUNT(*) FILTER (
        WHERE NOT f.epa_available
    ) AS missing_epa_hours,
    COUNT(*) FILTER (
        WHERE NOT f.noaa_available
    ) AS missing_noaa_hours
FROM fact_county_hour f
JOIN dim_county c
    ON f.county_id = c.county_id
GROUP BY c.county_name
ORDER BY c.county_name;


-- Q6: Check for duplicate county-hour fact keys
SELECT
    run_id,
    scenario_id,
    county_id,
    time_id,
    COUNT(*) AS duplicate_count
FROM fact_county_hour
GROUP BY run_id, scenario_id, county_id, time_id
HAVING COUNT(*) > 1;
