import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
from pathlib import Path
import os 
import yaml

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
OUTPUT_DIR = Path("data/processed/analytics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

with open(os.path.join(BASE_DIR, "config", "config.yaml")) as f:
    config = yaml.safe_load(f)

db = config["database"]

def get_connection():
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"]
    )
SQL_FILE = "sql/queries/analytical_queries.sql"



# --------------------------------------------------
# LOAD SQL QUERIES SAFELY
# --------------------------------------------------
def load_queries():
    queries = {}
    current_query = []
    query_name = None

    with open(SQL_FILE, "r") as f:
        for line in f:
            if line.strip().startswith("-- QUERY"):
                if query_name:
                    queries[query_name] = "".join(current_query).strip()
                    current_query = []

                query_number = line.split(":")[0].replace("-- QUERY", "").strip()
                query_name = f"query{query_number}"

            elif query_name:
                current_query.append(line)

        # add last query
        if query_name:
            queries[query_name] = "".join(current_query).strip()

    return queries


# --------------------------------------------------
# EXECUTE QUERY
# --------------------------------------------------
def execute_query(conn, sql):
    start = time.time()
    df = pd.read_sql_query(sql, conn)
    exec_time = round((time.time() - start) * 1000, 2)
    return df, exec_time


# --------------------------------------------------
# EXPORT CSV
# --------------------------------------------------
def export_to_csv(df, filename):
    df.to_csv(OUTPUT_DIR / filename, index=False)


# --------------------------------------------------
# MAIN DRIVER
# --------------------------------------------------
def main():
    conn = get_connection()
    queries = load_queries()

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    total_start = time.time()

    for name, sql in queries.items():
        print(f"➡ Executing {name}")
        df, exec_time = execute_query(conn, sql)

        export_to_csv(df, f"{name}.csv")

        summary["query_results"][name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": exec_time
        }

        summary["queries_executed"] += 1

    summary["total_execution_time_seconds"] = round(time.time() - total_start, 2)

    with open(OUTPUT_DIR / "analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    conn.close()
    print("Analytics generation completed successfully")


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    main()
