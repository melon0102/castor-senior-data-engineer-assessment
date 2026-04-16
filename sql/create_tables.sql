
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS staging.telemetry_raw (
    device_id TEXT,
    event_timestamp TEXT,
    event_date TEXT,
    event_type TEXT,
    metric_value TEXT,
    logical_date DATE NOT NULL,
    ingestion_ts TIMESTAMP NOT NULL,
    source_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.telemetry_rejects (
    device_id TEXT,
    event_timestamp TEXT,
    event_date TEXT,
    event_type TEXT,
    metric_value TEXT,
    logical_date DATE NOT NULL,
    ingestion_ts TIMESTAMP,
    source_file TEXT,
    reject_reason TEXT NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.device_master (
    device_id TEXT PRIMARY KEY,
    device_name TEXT,
    status TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.telemetry (
    device_id TEXT NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_date DATE NOT NULL,
    event_type TEXT NOT NULL,
    metric_value NUMERIC,
    logical_date DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id, event_timestamp, event_type)
);

CREATE INDEX IF NOT EXISTS idx_staging_telemetry_raw_logical_date
    ON staging.telemetry_raw (logical_date);

CREATE INDEX IF NOT EXISTS idx_staging_telemetry_rejects_logical_date
    ON staging.telemetry_rejects (logical_date);

CREATE INDEX IF NOT EXISTS idx_core_telemetry_logical_date
    ON core.telemetry (logical_date);