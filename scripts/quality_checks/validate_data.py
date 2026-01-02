import os
import json
import logging
import psycopg2
import yaml
from datetime import datetime

# -------------------------------------------------
# Paths
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQL_PATH = os.path.join(BASE_DIR, "sql", "queries", "data_quality_checks.sql")
LOG_DIR = os.path.join(BASE_DIR, "logs")
REPORT_DIR = os.path.join(BASE_DIR, "data", "quality_reports")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# -------------------------------------------------
# Logging
# -------------------------------------------------
log_file = os.path.join(
    LOG_DIR,
    f"data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------
# Load config
# -------------------------------------------------
with open(os.path.join(BASE_DIR, "config", "config.yaml")) as f:
    config = yaml.safe_load(f)

db = config["database"]

# -------------------------------------------------
# DB connection
# -------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )

# -------------------------------------------------
# Quality Checks
# -------------------------------------------------
def run_quality_checks(cursor):
    report = {
        "check_timestamp": datetime.now().isoformat(),
        "checks_performed": {},
    }

    with open(SQL_PATH) as f:
        sql = f.read()

    queries = [q.strip() for q in sql.split(";") if q.strip()]
    results = {}

    for q in queries:
        cursor.execute(q)
        rows = cursor.fetchall()

        for row in rows:
            # Case 1: (label, count)
            if len(row) == 2:
                results[row[0]] = row[1]

            # Case 2: (count,)
            elif len(row) == 1:
                key = (
                    q.split("AS")[-1]
                    .strip()
                    .lower()
                    .replace(" ", "_")
                )
                results[key] = row[0]

    # -------------------------
    # Completeness
    # -------------------------
    null_violations = sum(
        v for k, v in results.items()
        if "customers." in k or "products." in k or "transactions." in k
    )

    report["checks_performed"]["null_checks"] = {
        "status": "passed" if null_violations == 0 else "failed",
        "null_violations": null_violations,
        "details": {
            k: v for k, v in results.items()
            if "customers." in k or "products." in k or "transactions." in k
        }
    }

    # -------------------------
    # Uniqueness
    # -------------------------
    duplicates = (
        results.get("duplicate_emails", 0)
        + results.get("duplicate_transactions", 0)
    )

    report["checks_performed"]["duplicate_checks"] = {
        "status": "passed" if duplicates == 0 else "failed",
        "duplicates_found": duplicates,
        "details": {
            "duplicate_emails": results.get("duplicate_emails", 0),
            "duplicate_transactions": results.get("duplicate_transactions", 0),
        }
    }

    # -------------------------
    # Referential Integrity
    # -------------------------
    orphan_total = (
        results.get("orphan_transactions", 0)
        + results.get("orphan_items_transaction", 0)
        + results.get("orphan_items_product", 0)
    )

    report["checks_performed"]["referential_integrity"] = {
        "status": "passed" if orphan_total == 0 else "failed",
        "orphan_records": orphan_total,
        "details": {
            "orphan_transactions": results.get("orphan_transactions", 0),
            "orphan_items_transaction": results.get("orphan_items_transaction", 0),
            "orphan_items_product": results.get("orphan_items_product", 0),
        }
    }

    # -------------------------
    # Range / Validity
    # -------------------------
    range_violations = results.get("range_violations", 0)

    report["checks_performed"]["range_checks"] = {
        "status": "passed" if range_violations == 0 else "failed",
        "violations": range_violations,
        "details": {"range_violations": range_violations}
    }

    # -------------------------
    # Consistency
    # -------------------------
    mismatches = results.get("line_total_mismatch", 0)

    report["checks_performed"]["data_consistency"] = {
        "status": "passed" if mismatches == 0 else "failed",
        "mismatches": mismatches,
        "details": {"line_total_mismatch": mismatches}
    }

    # -------------------------
    # Scoring (Weighted)
    # -------------------------
    scores = {
        "referential_integrity": 0 if orphan_total else 100,
        "consistency": 0 if mismatches else 100,
        "completeness": 0 if null_violations else 100,
        "validity": 0 if range_violations else 100,
        "uniqueness": 0 if duplicates else 100,
    }

    overall_score = (
        scores["referential_integrity"] * 0.30 +
        scores["consistency"] * 0.20 +
        scores["completeness"] * 0.20 +
        scores["validity"] * 0.15 +
        scores["uniqueness"] * 0.15
    )

    report["overall_quality_score"] = round(overall_score, 2)

    if overall_score >= 90:
        report["quality_grade"] = "A"
    elif overall_score >= 80:
        report["quality_grade"] = "B"
    elif overall_score >= 70:
        report["quality_grade"] = "C"
    elif overall_score >= 60:
        report["quality_grade"] = "D"
    else:
        report["quality_grade"] = "F"

    return report

# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    conn = get_connection()
    cursor = conn.cursor()

    logging.info("Starting data quality checks")

    report = run_quality_checks(cursor)

    report_file = os.path.join(
        REPORT_DIR,
        f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Data quality checks completed")
    print(" Data Quality Checks Completed")
    print(json.dumps(report, indent=2))

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
