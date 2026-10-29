
-- SmokeWatch Replay | PostgreSQL analytical star schema
-- One county-hour fact per run, scenario, county, and UTC hour.

CREATE TABLE IF NOT EXISTS dim_time (
    time_id BIGSERIAL PRIMARY KEY,
    event_hour TIMESTAMPTZ NOT NULL UNIQUE,
    event_date DATE NOT NULL,
    hour_of_day SMALLINT NOT NULL
        CHECK (hour_of_day BETWEEN 0 AND 23)
);

CREATE TABLE IF NOT EXISTS dim_county (
    county_id BIGSERIAL PRIMARY KEY,
    county_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_source (
    source_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
        CHECK (source_name IN ('FIRMS', 'EPA', 'NOAA'))
);

CREATE TABLE IF NOT EXISTS dim_replay_scenario (
    scenario_id BIGSERIAL PRIMARY KEY,
    scenario_code TEXT NOT NULL UNIQUE,
    speed_seconds_per_event_hour NUMERIC(10,5) NOT NULL
        CHECK (speed_seconds_per_event_hour > 0),
    delay_level TEXT NOT NULL
        CHECK (delay_level IN ('none', 'moderate', 'severe'))
);

CREATE TABLE IF NOT EXISTS dim_run (
    run_id BIGSERIAL PRIMARY KEY,
    run_code TEXT NOT NULL UNIQUE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stores source-specific event records.
CREATE TABLE IF NOT EXISTS fact_source_event (
    source_event_key BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES dim_run(run_id),
    scenario_id BIGINT NOT NULL
        REFERENCES dim_replay_scenario(scenario_id),
    source_id BIGINT NOT NULL REFERENCES dim_source(source_id),
    county_id BIGINT NOT NULL REFERENCES dim_county(county_id),
    time_id BIGINT NOT NULL REFERENCES dim_time(time_id),
    event_id TEXT NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    UNIQUE (run_id, scenario_id, source_id, event_id)
);

-- Stores joined analytical results for each county-hour.
CREATE TABLE IF NOT EXISTS fact_county_hour (
    fact_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES dim_run(run_id),
    scenario_id BIGINT NOT NULL
        REFERENCES dim_replay_scenario(scenario_id),
    county_id BIGINT NOT NULL REFERENCES dim_county(county_id),
    time_id BIGINT NOT NULL REFERENCES dim_time(time_id),

    distinct_source_count SMALLINT NOT NULL DEFAULT 0
        CHECK (distinct_source_count BETWEEN 0 AND 3),

    fire_detection_count INTEGER,
    pm25_ug_m3 DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    wind_speed_m_s DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,

    firms_available BOOLEAN NOT NULL DEFAULT FALSE,
    epa_available BOOLEAN NOT NULL DEFAULT FALSE,
    noaa_available BOOLEAN NOT NULL DEFAULT FALSE,

    first_join_ready_at TIMESTAMPTZ,
    processing_latency_ms DOUBLE PRECISION,
    freshness_lag_event_hours DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (run_id, scenario_id, county_id, time_id),

    CHECK (
        first_join_ready_at IS NULL
        OR distinct_source_count = 3
    )
);

-- Indexes for analytical queries.
CREATE INDEX IF NOT EXISTS idx_source_event_source
    ON fact_source_event(source_id, ingestion_timestamp);

CREATE INDEX IF NOT EXISTS idx_source_event_county_time
    ON fact_source_event(county_id, time_id);

CREATE INDEX IF NOT EXISTS idx_fact_county_time
    ON fact_county_hour(county_id, time_id);

CREATE INDEX IF NOT EXISTS idx_fact_scenario
    ON fact_county_hour(scenario_id, run_id);

-- Register the three environmental data sources.
INSERT INTO dim_source (source_name)
VALUES ('FIRMS'), ('EPA'), ('NOAA')
ON CONFLICT DO NOTHING;
