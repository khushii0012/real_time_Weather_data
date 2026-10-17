-- ============================================================================
-- Script: create_tables.sql
-- Description: DDL for weather_readings table, constraints, and performance indexes
-- Target Database: weather_db
-- ============================================================================

-- Connect to weather_db:
-- psql -U postgres -h localhost -d weather_db -f sql/create_tables.sql

CREATE TABLE IF NOT EXISTS weather_readings (
    event_id VARCHAR(64) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(8, 4) NOT NULL,
    longitude NUMERIC(8, 4) NOT NULL,
    temperature NUMERIC(5, 2) NOT NULL,
    relative_humidity NUMERIC(5, 2) NOT NULL,
    apparent_temperature NUMERIC(5, 2),
    precipitation NUMERIC(6, 2) DEFAULT 0.0,
    surface_pressure NUMERIC(7, 2),
    wind_speed NUMERIC(6, 2) DEFAULT 0.0,
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,

    -- Business logic check constraints
    CONSTRAINT chk_temperature_range CHECK (temperature BETWEEN -90.0 AND 60.0),
    CONSTRAINT chk_humidity_range CHECK (relative_humidity BETWEEN 0.0 AND 100.0)
);

-- ============================================================================
-- Performance Indexes (Optimized for Power BI and Time-Series Analytics)
-- ============================================================================

-- Index for filtering by city and ordering by most recent observations
CREATE INDEX IF NOT EXISTS idx_weather_city_time 
    ON weather_readings (city, observation_time DESC);

-- Index for time-range slicing across all cities
CREATE INDEX IF NOT EXISTS idx_weather_observation_time 
    ON weather_readings (observation_time DESC);

-- ============================================================================
-- Verification & Analytical Queries
-- ============================================================================

-- 1. Check total ingested records
-- SELECT COUNT(*) AS total_readings FROM weather_readings;

-- 2. Latest weather reading per city (Used in Power BI KPI Cards)
-- SELECT DISTINCT ON (city)
--     city,
--     temperature,
--     relative_humidity,
--     wind_speed,
--     observation_time,
--     ingestion_timestamp
-- FROM weather_readings
-- ORDER BY city, observation_time DESC;

-- 3. Calculate 1-hour moving average metrics
-- SELECT 
--     city,
--     date_trunc('hour', observation_time) AS hourly_bucket,
--     ROUND(AVG(temperature), 2) AS avg_temperature,
--     ROUND(AVG(relative_humidity), 2) AS avg_humidity,
--     ROUND(MAX(temperature), 2) AS max_temperature,
--     ROUND(MIN(temperature), 2) AS min_temperature,
--     COUNT(*) AS sample_count
-- FROM weather_readings
-- GROUP BY city, date_trunc('hour', observation_time)
-- ORDER BY hourly_bucket DESC, city;
