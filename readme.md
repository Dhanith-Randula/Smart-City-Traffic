# Smart City Traffic & Congestion System
> Applied Big Data Engineering – Mini Project | Scenario 1

## Quick Start

```bash
# 1. Clone / unzip the project
cd smart-city-traffic

# 2. Make the DB init script executable
chmod +x db/init_multi.sh

# 3. Start the entire stack
docker compose up --build -d

# 4. Wait ~60 seconds for all services to become healthy, then check
docker compose ps
```

## Service URLs

| Service          | URL                        | Credentials        |
|------------------|----------------------------|--------------------|
| Spark Master UI  | http://localhost:8080      | –                  |
| Airflow UI       | http://localhost:8081      | admin / admin      |
| PostgreSQL       | localhost:5432             | trafficuser / trafficpass |

## Watching the pipeline live

```bash
# Producer logs (sensor readings + critical events)
docker logs -f traffic-producer

# Spark streaming logs
docker logs -f spark-streaming

# Airflow scheduler logs
docker logs -f airflow-scheduler
```

## Triggering the Airflow DAG manually (for testing)

```bash
docker exec airflow-scheduler \
  airflow dags trigger smart_city_nightly_report
```
Reports are written to `./reports/traffic_report_YYYY-MM-DD.csv`.

## Querying PostgreSQL directly

```bash
docker exec -it postgres psql -U trafficuser -d trafficdb
```

Useful queries:
```sql
-- Latest raw readings
SELECT * FROM traffic_raw ORDER BY event_time DESC LIMIT 20;

-- Current congestion windows
SELECT * FROM congestion_windows ORDER BY window_start DESC LIMIT 10;

-- All critical alerts today
SELECT * FROM critical_traffic WHERE alert_time::date = CURRENT_DATE;

-- Latest nightly report
SELECT * FROM nightly_report ORDER BY report_date DESC;
```

## Architecture

```
[4 Sensor Simulators]
        │  JSON  {sensor_id, junction_name, timestamp, vehicle_count, avg_speed}
        ▼
[Kafka - topic: traffic-raw  (4 partitions)]
        │
        ▼
[Spark Structured Streaming]
  ├─ event_time watermark: 10 min
  ├─ Sink 1: every raw event → traffic_raw
  ├─ Sink 2: 5-min tumbling window → congestion_windows (Congestion Index)
  └─ Sink 3: avg_speed < 10 km/h → critical_traffic (immediate alert)
        │
        ▼
[PostgreSQL]  ←──── [Airflow DAG: nightly_report @ 23:55]
                          ├─ check_data_availability
                          ├─ compute_peak_hours
                          ├─ generate_intervention_report
                          ├─ export_csv_report  → ./reports/
                          └─ cleanup_old_reports

```

## Stopping the stack

```bash
docker compose down           # stop containers
docker compose down -v        # stop + delete volumes (full reset)
```