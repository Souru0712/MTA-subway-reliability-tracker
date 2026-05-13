-- Create Airflow metadata database
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Create MTA application database
CREATE DATABASE mta;
CREATE USER mta WITH PASSWORD 'mta';
GRANT ALL PRIVILEGES ON DATABASE mta TO mta;

-- Switch to mta database for schema creation
\connect mta

CREATE TABLE IF NOT EXISTS reliability_metrics (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    route_id            TEXT            NOT NULL,
    stop_id             TEXT,
    -- Deterministic key column so ON CONFLICT works with nullable stop_id
    stop_id_key         TEXT            GENERATED ALWAYS AS (COALESCE(stop_id, '')) STORED,
    on_time_pct         DOUBLE PRECISION,
    delay_p50_seconds   DOUBLE PRECISION,
    delay_p95_seconds   DOUBLE PRECISION,
    sample_count        INTEGER,
    updated_at          TIMESTAMPTZ     DEFAULT now(),
    PRIMARY KEY (window_start, route_id, stop_id_key)
);

CREATE INDEX IF NOT EXISTS idx_rm_route_window
    ON reliability_metrics (route_id, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_rm_stop_window
    ON reliability_metrics (stop_id, window_start DESC);

GRANT ALL ON TABLE reliability_metrics TO mta;
GRANT USAGE ON SCHEMA public TO mta;
