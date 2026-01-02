import os
import time
import logging
from datetime import datetime, timedelta
import yaml

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config.get("retention_days", 7)

TARGET_DIRS = [
    "data/raw",
    "data/staging",
    "logs"
]

PRESERVE_KEYWORDS = [
    "metadata",
    "report",
    "summary"
]

logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def should_preserve(filename):
    return any(keyword in filename.lower() for keyword in PRESERVE_KEYWORDS)

def cleanup():
    cutoff_time = time.time() - (RETENTION_DAYS * 86400)
    today = datetime.now().date()

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            continue

        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)

            if not os.path.isfile(file_path):
                continue

            file_mtime = os.path.getmtime(file_path)
            file_date = datetime.fromtimestamp(file_mtime).date()

            if file_date == today:
                continue

            if should_preserve(file):
                continue

            if file_mtime < cutoff_time:
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")

def main():
    logging.info("Cleanup job started")
    cleanup()
    logging.info("Cleanup job completed")

if __name__ == "__main__":
    main()
