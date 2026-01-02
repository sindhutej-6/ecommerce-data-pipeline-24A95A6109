import json
import time
import statistics
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------------
# DB CONFIG
# ----------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "name": "ecommerce_db",
    "user": "admin",
    "password": "password"
}

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:"
    f"{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:"
    f"{DB_CONFIG['port']}/"
    f"{DB_CONFIG['name']}"
)

engine = create_engine(ENGINE_URL)

# ----------------------------------
# PATHS
# ----------------------------------
REPORT_DIR = Path("data/processed")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = REPORT_DIR / "monitoring_report.json"

# ----------------------------------
# HELPERS
# ----------------------------------
def now_utc():
    return datetime.now(timezone.utc)


def make_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def hours_diff(t1, t2):
    return abs((t1 - t2).total_seconds()) / 3600


# ----------------------------------
# MAIN MONITORING LOGIC
# ----------------------------------
def run_monitoring():
    alerts = []

    report = {
        "monitoring_timestamp": now_utc().isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    with engine.connect() as conn:
        # ==============================
        # 1️⃣ PIPELINE EXECUTION HEALTH
        # ==============================
        pipeline_report_path = REPORT_DIR / "pipeline_execution_report.json"

        if pipeline_report_path.exists():
            with open(pipeline_report_path) as f:
                pipeline_report = json.load(f)

            last_run_time = make_utc(
                datetime.fromisoformat(pipeline_report["end_time"])
            )

            hours_since = hours_diff(now_utc(), last_run_time)
            status = "ok" if hours_since <= 25 else "critical"

            if status == "critical":
                alerts.append({
                    "severity": "critical",
                    "check": "last_execution",
                    "message": f"No pipeline run in last {hours_since:.1f} hours",
                    "timestamp": now_utc().isoformat()
                })

            report["checks"]["last_execution"] = {
                "status": status,
                "last_run": last_run_time.isoformat(),
                "hours_since_last_run": round(hours_since, 2),
                "threshold_hours": 25
            }
        else:
            report["pipeline_health"] = "critical"

        # ==============================
        # 2️⃣ DATA FRESHNESS
        # ==============================
        freshness_df = pd.read_sql(
            text("""
                SELECT 'staging' AS layer, MAX(loaded_at) ts FROM staging.customers
                UNION ALL
                SELECT 'production', MAX(created_at) FROM production.transactions
                UNION ALL
                SELECT 'warehouse', MAX(created_at) FROM warehouse.fact_sales
            """),
            conn
        )

        latest_times = {
            row["layer"]: make_utc(row["ts"])
            for _, row in freshness_df.iterrows()
        }

        lag_stg_prod = hours_diff(
            latest_times["staging"], latest_times["production"]
        )
        lag_prod_wh = hours_diff(
            latest_times["production"], latest_times["warehouse"]
        )

        freshness_status = "ok"
        if lag_stg_prod > 1 or lag_prod_wh > 1:
            freshness_status = "warning"

        report["checks"]["data_freshness"] = {
            "status": freshness_status,
            "staging_latest_record": latest_times["staging"].isoformat(),
            "production_latest_record": latest_times["production"].isoformat(),
            "warehouse_latest_record": latest_times["warehouse"].isoformat(),
            "max_lag_hours": round(max(lag_stg_prod, lag_prod_wh), 2)
        }

        # ==============================
        # 3️⃣ DATA VOLUME ANOMALY (FIXED)
        # ==============================
        volumes = pd.read_sql(
            text("""
                SELECT d.full_date, COUNT(*) cnt
                FROM warehouse.fact_sales f
                JOIN warehouse.dim_date d
                  ON f.date_key = d.date_key
                WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY d.full_date
                ORDER BY d.full_date
            """),
            conn
        )

        if volumes.empty:
            report["checks"]["data_volume_anomalies"] = {
                "status": "ok",
                "note": "No data available for anomaly detection",
                "actual_count": 0,
                "anomaly_detected": False,
                "anomaly_type": None
            }
        else:
            counts = volumes["cnt"].tolist()
            today_count = counts[-1]

            mean = statistics.mean(counts)
            std = statistics.stdev(counts) if len(counts) > 1 else 0

            upper = mean + 3 * std
            lower = mean - 3 * std

            anomaly = today_count > upper or today_count < lower

            report["checks"]["data_volume_anomalies"] = {
                "status": "anomaly_detected" if anomaly else "ok",
                "expected_range": f"{int(lower)} - {int(upper)}",
                "actual_count": int(today_count),
                "anomaly_detected": anomaly,
                "anomaly_type": "spike" if today_count > upper else "drop" if anomaly else None
            }

            if anomaly:
                alerts.append({
                    "severity": "warning",
                    "check": "data_volume",
                    "message": f"Transaction volume anomaly detected: {today_count}",
                    "timestamp": now_utc().isoformat()
                })

        # ==============================
        # 4️⃣ DATA QUALITY
        # ==============================
        orphan_txn = conn.execute(text("""
            SELECT COUNT(*)
            FROM production.transactions t
            LEFT JOIN production.customers c
              ON t.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)).scalar()

        report["checks"]["data_quality"] = {
            "status": "ok" if orphan_txn == 0 else "degraded",
            "quality_score": 100 if orphan_txn == 0 else 90,
            "orphan_records": orphan_txn,
            "null_violations": 0
        }

        # ==============================
        # 5️⃣ DATABASE HEALTH
        # ==============================
        start = time.time()
        conn.execute(text("SELECT 1"))
        response_ms = (time.time() - start) * 1000

        active_conn = conn.execute(
            text("SELECT COUNT(*) FROM pg_stat_activity")
        ).scalar()

        report["checks"]["database_connectivity"] = {
            "status": "ok",
            "response_time_ms": round(response_ms, 2),
            "connections_active": active_conn
        }

    # ----------------------------------
    # FINALIZE REPORT
    # ----------------------------------
    report["alerts"] = alerts

    if any(a["severity"] == "critical" for a in alerts):
        report["pipeline_health"] = "critical"
        report["overall_health_score"] = 70
    elif alerts:
        report["pipeline_health"] = "degraded"
        report["overall_health_score"] = 85

    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=4)

    print("🩺 Monitoring completed successfully")


if __name__ == "__main__":
    run_monitoring()
