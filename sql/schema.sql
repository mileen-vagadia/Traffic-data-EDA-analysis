-- ============================================================
-- Urban Traffic EDA — PostgreSQL Schema
-- Dataset: Smart Traffic Management Dataset (Kaggle)
-- ============================================================

-- Drop tables if re-running
DROP TABLE IF EXISTS traffic_signals CASCADE;
DROP TABLE IF EXISTS traffic_observations CASCADE;

-- Raw traffic observations loaded from CSV
CREATE TABLE traffic_observations (
    id                  SERIAL PRIMARY KEY,
    intersection_id     VARCHAR(20)     NOT NULL,
    timestamp           TIMESTAMP       NOT NULL,
    vehicle_count       INTEGER         NOT NULL,
    vehicle_type        VARCHAR(20),        -- car, bus, truck, bike
    direction           VARCHAR(10),        -- N, S, E, W
    signal_phase        VARCHAR(10),        -- green, red, yellow
    green_duration_sec  INTEGER,            -- green light duration in seconds
    wait_time_sec       INTEGER,            -- average vehicle wait time in seconds
    congestion_level    VARCHAR(10)         -- low, medium, high
);

-- Signal timing reference table
CREATE TABLE traffic_signals (
    intersection_id     VARCHAR(20)     PRIMARY KEY,
    location_name       VARCHAR(100),
    city                VARCHAR(50),
    total_cycle_sec     INTEGER,            -- full signal cycle duration
    green_ratio         FLOAT               -- fraction of cycle that is green
);

-- ============================================================
-- Indexes for faster query performance
-- ============================================================
CREATE INDEX idx_obs_timestamp       ON traffic_observations(timestamp);
CREATE INDEX idx_obs_intersection    ON traffic_observations(intersection_id);
CREATE INDEX idx_obs_congestion      ON traffic_observations(congestion_level);
