import os
import json
import time
import logging
import psycopg2
import yaml
from datetime import datetime

# -------------------------------------------------
# Base paths
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
LOG_DIR = os.path.join(BASE_DIR, "logs")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------------------------------
# Logging setup
# -------------------------------------------------
log_file = os.path.join(
    LOG_DIR,
    f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------
# Load config
# -------------------------------------------------
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

db = config["database"]

# -------------------------------------------------
# Database connection
# -------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", db["host"]),
        port=int(os.getenv("DB_PORT", db["port"])),
        dbname=os.getenv("DB_NAME", db["name"]),
        user=os.getenv("DB_USER", db["user"]),
        password=os.getenv("DB_PASSWORD", db["password"]),
    )

# -------------------------------------------------
# Bulk COPY loader (FIXED)
# -------------------------------------------------
def copy_csv(cursor, table, csv_file):
    column_map = {
        "staging.customers": """
            (customer_id, first_name, last_name, email, phone,
             registration_date, city, state, country, age_group)
        """,
        "staging.products": """
            (product_id, product_name, category, sub_category,
             price, cost, brand, stock_quantity, supplier_id)
        """,
        "staging.transactions": """
            (transaction_id, customer_id, transaction_date,
             transaction_time, payment_method, shipping_address,
             total_amount)
        """,
        "staging.transaction_items": """
            (item_id, transaction_id, product_id, quantity,
             unit_price, discount_percentage, line_total)
        """
    }

    with open(csv_file, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"""
            COPY {table} {column_map[table]}
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
            """,
            f
        )

# -------------------------------------------------
# Validation
# -------------------------------------------------
def validate_staging_load(cursor, table, csv_file):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    db_count = cursor.fetchone()[0]

    with open(csv_file, "r", encoding="utf-8") as f:
        csv_count = sum(1 for _ in f) - 1

    return db_count == csv_count, db_count, csv_count

# -------------------------------------------------
# Main ingestion
# -------------------------------------------------
def main():
    start_time = time.time()

    summary = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    tables = {
        "staging.customers": os.path.join(RAW_DIR, "customers.csv"),
        "staging.products": os.path.join(RAW_DIR, "products.csv"),
        "staging.transactions": os.path.join(RAW_DIR, "transactions.csv"),
        "staging.transaction_items": os.path.join(RAW_DIR, "transaction_items.csv")
    }

    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        logging.info("Starting staging ingestion")

        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")

        for table, csv_file in tables.items():
            logging.info(f"Loading {table}")

            cursor.execute(f"TRUNCATE TABLE {table}")
            copy_csv(cursor, table, csv_file)

            valid, db_rows, csv_rows = validate_staging_load(
                cursor, table, csv_file
            )

            if not valid:
                raise Exception(
                    f"Row count mismatch for {table} (db={db_rows}, csv={csv_rows})"
                )

            summary["tables_loaded"][table] = {
                "rows_loaded": db_rows,
                "status": "success"
            }

            logging.info(f"{table} loaded successfully ({db_rows} rows)")

        conn.commit()
        logging.info("Ingestion committed successfully")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(str(e))
        print("ERROR:", e)
        return

    finally:
        if conn:
            conn.close()

    summary["total_execution_time_seconds"] = round(
        time.time() - start_time, 2
    )

    with open(os.path.join(STAGING_DIR, "ingestion_summary.json"), "w") as f:
        json.dump(summary, f, indent=4)

    print("Staging ingestion completed successfully")

# -------------------------------------------------
if __name__ == "__main__":
    main()
