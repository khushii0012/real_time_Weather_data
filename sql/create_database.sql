-- ============================================================================
-- Script: create_database.sql
-- Description: Creates the dedicated database for the Real-Time Weather Pipeline
-- ============================================================================

-- Connect to the default 'postgres' database before running:
-- psql -U postgres -h localhost -f sql/create_database.sql

-- Terminate any existing connections to weather_db if recreating
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'weather_db'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS weather_db;

CREATE DATABASE weather_db
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE weather_db IS 'Analytical datastore for Real-Time Weather Data Engineering Pipeline';
