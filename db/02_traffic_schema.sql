

-- Raw sensor readings
CREATE TABLE IF NOT EXISTS traffic_raw (
    id              SERIAL PRIMARY KEY,
    sensor_id       VARCHAR(20)     NOT NULL,
    junction_name   VARCHAR(50)     NOT NULL,
    event_time      TIMESTAMP       NOT NULL,
    ingest_time     TIMESTAMP       DEFAULT NOW(),
    vehicle_count   INTEGER         NOT NULL,
    avg_speed       NUMERIC(5,2)    NOT NULL
);

-- 5-minute tumbling window aggregates with Congestion Index
CREATE TABLE IF NOT EXISTS congestion_windows (
    id                  SERIAL PRIMARY KEY,
    sensor_id           VARCHAR(20)     NOT NULL,
    junction_name       VARCHAR(50)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    avg_vehicle_count   NUMERIC(8,2)    NOT NULL,
    avg_speed           NUMERIC(5,2)    NOT NULL,
    congestion_index    NUMERIC(5,2)    NOT NULL,
    congestion_level    VARCHAR(20)     NOT NULL,
    processed_at        TIMESTAMP       DEFAULT NOW()
);

-- Critical traffic alerts (speed < 10 km/h)
CREATE TABLE IF NOT EXISTS critical_traffic (
    id               SERIAL PRIMARY KEY,
    sensor_id        VARCHAR(20)     NOT NULL,
    junction_name    VARCHAR(50)     NOT NULL,
    alert_time       TIMESTAMP       NOT NULL,
    vehicle_count    INTEGER         NOT NULL,
    avg_speed        NUMERIC(5,2)    NOT NULL,
    congestion_index NUMERIC(5,2)    NOT NULL,
    alert_message    TEXT            NOT NULL,
    created_at       TIMESTAMP       DEFAULT NOW()
);

-- Nightly Airflow report output
CREATE TABLE IF NOT EXISTS nightly_report (
    id                  SERIAL PRIMARY KEY,
    report_date         DATE            NOT NULL,
    sensor_id           VARCHAR(20)     NOT NULL,
    junction_name       VARCHAR(50)     NOT NULL,
    peak_hour           INTEGER         NOT NULL,
    peak_vehicle_count  NUMERIC(8,2)    NOT NULL,
    peak_avg_speed      NUMERIC(5,2)    NOT NULL,
    total_vehicles_day  BIGINT          NOT NULL,
    alert_count_day     INTEGER         NOT NULL,
    intervention_needed BOOLEAN         NOT NULL,
    generated_at        TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traffic_raw_sensor  ON traffic_raw (sensor_id, event_time);
CREATE INDEX IF NOT EXISTS idx_congestion_window   ON congestion_windows (sensor_id, window_start);
CREATE INDEX IF NOT EXISTS idx_critical_alert_time ON critical_traffic (alert_time);
CREATE INDEX IF NOT EXISTS idx_nightly_report_date ON nightly_report (report_date, sensor_id);