from __future__ import annotations

import csv
import logging
import os
from datetime import date, datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "trafficdb"),
    "user":     os.getenv("POSTGRES_USER",     "trafficuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "trafficpass"),
}

REPORTS_DIR            = "/reports"
CRITICAL_ALERT_THRESHOLD = 5     # junctions with >= 5 critical alerts need police

log = logging.getLogger(__name__)



def get_conn():
    return psycopg2.connect(**DB_CONFIG)



def check_data_availability(**context):
    """Fail early if yesterday's data is missing."""
    yesterday = context["ds"]   
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM traffic_raw WHERE event_time::date = %s",
            (yesterday,),
        )
        count = cur.fetchone()[0]
    log.info("Records found for %s: %d", yesterday, count)
    if count == 0:
        raise ValueError(f"No traffic data found for {yesterday}. Aborting pipeline.")
    log.info("Data check passed.")



def compute_peak_hours(**context):
    """
    For each junction, find the hour with the highest average vehicle count.
    Pushes results list to XCom so downstream tasks can use it.
    """
    yesterday = context["ds"]

    sql = """
        SELECT
            sensor_id,
            junction_name,
            EXTRACT(HOUR FROM event_time)::INTEGER   AS hour_of_day,
            AVG(vehicle_count)                        AS avg_vehicles,
            AVG(avg_speed)                            AS avg_speed,
            COUNT(*)                                  AS reading_count
        FROM traffic_raw
        WHERE event_time::date = %s
        GROUP BY sensor_id, junction_name, EXTRACT(HOUR FROM event_time)
        ORDER BY sensor_id, avg_vehicles DESC
    """

    peaks = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (yesterday,))
        rows = cur.fetchall()

    for sensor_id, junction_name, hour, avg_vehicles, avg_speed, readings in rows:
        
        if sensor_id not in peaks:
            peaks[sensor_id] = {
                "sensor_id":     sensor_id,
                "junction_name": junction_name,
                "peak_hour":     int(hour),
                "avg_vehicles":  float(round(avg_vehicles, 2)),
                "avg_speed":     float(round(avg_speed, 2)),
                "readings":      int(readings),
            }

    log.info("Peak hours computed for %d junctions.", len(peaks))
    for v in peaks.values():
        log.info(
            "  %-30s  peak_hour=%02d:00  avg_vehicles=%.1f  avg_speed=%.1f km/h",
            v["junction_name"], v["peak_hour"], v["avg_vehicles"], v["avg_speed"],
        )

    context["ti"].xcom_push(key="peak_hours", value=list(peaks.values()))



def generate_intervention_report(**context):
    """
    Mark junctions that need physical police intervention based on:
      - Critical alert count during the day >= CRITICAL_ALERT_THRESHOLD
    Enriches the peak-hour records and pushes updated list to XCom.
    """
    yesterday   = context["ds"]
    peak_hours  = context["ti"].xcom_pull(key="peak_hours", task_ids="compute_peak_hours")

    
    sql = """
        SELECT sensor_id, COUNT(*) AS alert_count
        FROM critical_traffic
        WHERE alert_time::date = %s
        GROUP BY sensor_id
    """
    alert_counts = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (yesterday,))
        for sensor_id, cnt in cur.fetchall():
            alert_counts[sensor_id] = int(cnt)

    
    sql2 = """
        SELECT sensor_id, SUM(vehicle_count) AS total
        FROM traffic_raw
        WHERE event_time::date = %s
        GROUP BY sensor_id
    """
    totals = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql2, (yesterday,))
        for sensor_id, total in cur.fetchall():
            totals[sensor_id] = int(total)

    report_rows = []
    for row in peak_hours:
        sid         = row["sensor_id"]
        alert_count = alert_counts.get(sid, 0)
        intervention = alert_count >= CRITICAL_ALERT_THRESHOLD

        row["alert_count_day"]     = alert_count
        row["total_vehicles_day"]  = totals.get(sid, 0)
        row["intervention_needed"] = intervention
        report_rows.append(row)

        status = "⚠  INTERVENTION NEEDED" if intervention else "✓  No intervention"
        log.info(
            "  %-30s  alerts=%d  total_vehicles=%d  → %s",
            row["junction_name"], alert_count, row["total_vehicles_day"], status,
        )

    context["ti"].xcom_push(key="report_rows", value=report_rows)

    
    with get_conn() as conn, conn.cursor() as cur:
        for row in report_rows:
            cur.execute("""
                INSERT INTO nightly_report
                    (report_date, sensor_id, junction_name, peak_hour,
                     peak_vehicle_count, peak_avg_speed, total_vehicles_day,
                     alert_count_day, intervention_needed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                yesterday,
                row["sensor_id"],
                row["junction_name"],
                row["peak_hour"],
                row["avg_vehicles"],
                row["avg_speed"],
                row["total_vehicles_day"],
                row["alert_count_day"],
                row["intervention_needed"],
            ))
        conn.commit()
    log.info("Nightly report rows written to DB.")



def export_csv_report(**context):
    """Write the final report as a human-readable CSV file."""
    yesterday   = context["ds"]
    report_rows = context["ti"].xcom_pull(key="report_rows",
                                          task_ids="generate_intervention_report")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = os.path.join(REPORTS_DIR, f"traffic_report_{yesterday}.csv")

    fieldnames = [
        "report_date", "sensor_id", "junction_name",
        "peak_hour_label", "avg_vehicles_peak_hour", "avg_speed_peak_hour_kmh",
        "total_vehicles_day", "critical_alerts_day", "intervention_needed",
        "recommendation",
    ]

    with open(filename, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in report_rows:
            recommendation = (
                "Deploy traffic police at peak hour"
                if row["intervention_needed"]
                else "Standard monitoring sufficient"
            )
            writer.writerow({
                "report_date":              yesterday,
                "sensor_id":                row["sensor_id"],
                "junction_name":            row["junction_name"],
                "peak_hour_label":          f"{row['peak_hour']:02d}:00 - {row['peak_hour']+1:02d}:00",
                "avg_vehicles_peak_hour":   row["avg_vehicles"],
                "avg_speed_peak_hour_kmh":  row["avg_speed"],
                "total_vehicles_day":       row["total_vehicles_day"],
                "critical_alerts_day":      row["alert_count_day"],
                "intervention_needed":      "YES" if row["intervention_needed"] else "NO",
                "recommendation":           recommendation,
            })

    log.info("Report exported → %s", filename)
    context["ti"].xcom_push(key="report_path", value=filename)



def cleanup_old_reports(**context):
    """Remove CSV reports older than 30 days."""
    cutoff = date.today() - timedelta(days=30)
    removed = 0
    for fname in os.listdir(REPORTS_DIR):
        if not fname.startswith("traffic_report_") or not fname.endswith(".csv"):
            continue
        try:
            file_date = date.fromisoformat(fname[len("traffic_report_"):-len(".csv")])
            if file_date < cutoff:
                os.remove(os.path.join(REPORTS_DIR, fname))
                removed += 1
                log.info("Removed old report: %s", fname)
        except ValueError:
            pass
    log.info("Cleanup complete. %d old report(s) removed.", removed)



default_args = {
    "owner":            "smart-city-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_nightly_report",
    description="Nightly traffic aggregation, peak-hour detection & police intervention report",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="55 23 * * *",   # 23:55 daily (Colombo in env)
    catchup=False,
    tags=["smart-city", "traffic", "batch"],
) as dag:

    t1 = PythonOperator(
        task_id="check_data_availability",
        python_callable=check_data_availability,
    )

    t2 = PythonOperator(
        task_id="compute_peak_hours",
        python_callable=compute_peak_hours,
    )

    t3 = PythonOperator(
        task_id="generate_intervention_report",
        python_callable=generate_intervention_report,
    )

    t4 = PythonOperator(
        task_id="export_csv_report",
        python_callable=export_csv_report,
    )

    t5 = PythonOperator(
        task_id="cleanup_old_reports",
        python_callable=cleanup_old_reports,
    )

    
    t1 >> t2 >> t3 >> t4 >> t5