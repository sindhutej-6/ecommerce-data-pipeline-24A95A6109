import subprocess
import time
import json
import logging
import traceback
import sys
from datetime import datetime, timezone
from pathlib import Path

# =========================
# PATH CONFIG
# =========================
LOG_DIR = Path("logs")
REPORT_DIR = Path("data/processed")
LOG_DIR.mkdir(exist_ok=True, parents=True)
REPORT_DIR.mkdir(exist_ok=True, parents=True)

PIPELINE_ID = f"PIPE_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

PYTHON_EXEC = sys.executable  

# =========================
# LOGGING CONFIG
# =========================
log_file = LOG_DIR / f"pipeline_orchestrator_{PIPELINE_ID}.log"
error_log_file = LOG_DIR / "pipeline_errors.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(error_log_file)
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

# =========================
# PIPELINE STEPS
# =========================
PIPELINE_STEPS = [
    ("data_generation", f"{PYTHON_EXEC} scripts/data_generation/generate_data.py"),
    ("data_ingestion", f"{PYTHON_EXEC} scripts/ingestion/ingest_to_staging.py"),
    ("data_quality", f"{PYTHON_EXEC} scripts/quality_checks/validate_data.py"),
    ("staging_to_production", f"{PYTHON_EXEC} scripts/transformation/staging_to_production.py"),
    ("warehouse_load", f"{PYTHON_EXEC} scripts/transformation/load_warehouse.py"),
    ("analytics_generation", f"{PYTHON_EXEC} scripts/transformation/generate_analytics.py")
]

MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# =========================
# EXECUTION HELPERS
# =========================
def run_step(step_name, command):
    start_time = time.time()
    retries = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"➡ Starting step: {step_name} (Attempt {attempt})")
            subprocess.run(command, shell=True, check=True)

            duration = time.time() - start_time
            logging.info(f"Completed step: {step_name} in {duration:.2f}s")

            return {
                "status": "success",
                "duration_seconds": round(duration, 2),
                "records_processed": None,
                "retry_attempts": retries
            }

        except subprocess.CalledProcessError as e:
            retries += 1
            logging.error(f"Step failed: {step_name} | Attempt {attempt}")
            error_logger.error(traceback.format_exc())

            if attempt < MAX_RETRIES:
                sleep_time = BACKOFF_SECONDS[attempt - 1]
                logging.warning(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                duration = time.time() - start_time
                return {
                    "status": "failed",
                    "duration_seconds": round(duration, 2),
                    "records_processed": 0,
                    "error_message": str(e),
                    "retry_attempts": retries
                }

# =========================
# MAIN PIPELINE
# =========================
def main():
    pipeline_start = time.time()
    report = {
        "pipeline_execution_id": PIPELINE_ID,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "end_time": None,
        "total_duration_seconds": None,
        "status": "success",
        "steps_executed": {},
        "errors": [],
        "warnings": []
    }

    for step_name, command in PIPELINE_STEPS:
        result = run_step(step_name, command)
        report["steps_executed"][step_name] = result

        if result["status"] == "failed":
            report["status"] = "failed"
            report["errors"].append(f"{step_name} failed")
            logging.error(f" Pipeline stopped at step: {step_name}")
            break

    pipeline_end = time.time()
    report["end_time"] = datetime.now(timezone.utc).isoformat()
    report["total_duration_seconds"] = round(pipeline_end - pipeline_start, 2)

    report_path = REPORT_DIR / "pipeline_execution_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)

    logging.info(f" Pipeline report written to {report_path}")

    if report["status"] == "success":
        logging.info(" PIPELINE COMPLETED SUCCESSFULLY")
    else:
        logging.error(" PIPELINE FAILED")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()