import schedule
import subprocess
import time
import logging
import os
from datetime import datetime
import yaml

LOCK_FILE = "scheduler.lock"

# Load config
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

SCHEDULE_TIME = config.get("pipeline_schedule_time", "02:00")
RETENTION_DAYS = config.get("retention_days", 7)

# Logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def is_pipeline_running():
    return os.path.exists(LOCK_FILE)

def run_pipeline():
    if is_pipeline_running():
        logging.warning("Pipeline already running. Skipping this execution.")
        return

    try:
        open(LOCK_FILE, "w").close()
        logging.info("Pipeline execution started")

        result = subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("Pipeline execution SUCCESS")

            # Run cleanup only on success
            subprocess.run(
                ["python", "scripts/cleanup_old_data.py"],
                capture_output=True,
                text=True
            )
        else:
            logging.error("Pipeline execution FAILED")
            logging.error(result.stderr)

    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}")

    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        logging.info("Pipeline execution finished")

def main():
    logging.info("Scheduler started")
    schedule.every().day.at(SCHEDULE_TIME).do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
