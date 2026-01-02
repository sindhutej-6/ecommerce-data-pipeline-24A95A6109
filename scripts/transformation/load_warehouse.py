import os
import psycopg2
import yaml
from datetime import date, timedelta

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
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

# --------------------------------------------------
# DIM DATE
# --------------------------------------------------
def load_dim_date(cur):
    print("➡ Loading dim_date")

    cur.execute("DELETE FROM warehouse.dim_date")

    start = date(2024, 1, 1)
    end = date(2024, 12, 31)

    d = start
    while d <= end:
        cur.execute("""
            INSERT INTO warehouse.dim_date (
                date_key,
                date,
                year,
                month,
                week
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (
            int(d.strftime("%Y%m%d")),
            d,
            d.year,
            d.month,
            int(d.strftime("%W"))
        ))
        d += timedelta(days=1)

# --------------------------------------------------
# DIM PAYMENT METHOD
# --------------------------------------------------
def load_dim_payment_method(cur):
    print("➡ Loading dim_payment_method")

    cur.execute("DELETE FROM warehouse.dim_payment_method")

    cur.execute("""
        INSERT INTO warehouse.dim_payment_method (
            payment_method_name,
            payment_type
        )
        SELECT DISTINCT
            payment_method,
            CASE
                WHEN payment_method IN ('Credit Card','Debit Card','UPI','Net Banking')
                THEN 'Online'
                ELSE 'Offline'
            END
        FROM production.transactions
    """)

# --------------------------------------------------
# DIM CUSTOMERS (SCD TYPE 2)
# --------------------------------------------------
def load_dim_customers(cur):
    print("➡ Loading dim_customers (SCD2)")

    cur.execute("""
        INSERT INTO warehouse.dim_customers (
            customer_id,
            full_name,
            email,
            city,
            state,
            country,
            age_group,
            customer_segment,
            registration_date,
            effective_date,
            end_date,
            is_current
        )
        SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name,
            c.email,
            c.city,
            c.state,
            'USA',
            'Adult',
            'New',
            c.registration_date,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.customers c
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.dim_customers d
            WHERE d.customer_id = c.customer_id
              AND d.is_current = TRUE
        )
    """)

# --------------------------------------------------
# DIM PRODUCTS (SCD TYPE 2)
# --------------------------------------------------
def load_dim_products(cur):
    print("➡ Loading dim_products (SCD2)")

    cur.execute("""
        INSERT INTO warehouse.dim_products (
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price_range,
            effective_date,
            end_date,
            is_current
        )
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.sub_category,
            p.brand,
            CASE
                WHEN p.price < 50 THEN 'Budget'
                WHEN p.price < 200 THEN 'Mid-range'
                ELSE 'Premium'
            END,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.products p
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.dim_products d
            WHERE d.product_id = p.product_id
              AND d.is_current = TRUE
        )
    """)

# --------------------------------------------------
# FACT SALES
# --------------------------------------------------
def load_fact_sales(cur):
    print("➡ Loading fact_sales")

    cur.execute("""
        INSERT INTO warehouse.fact_sales (
            transaction_id,
            customer_id,
            product_id,
            date,
            quantity,
            revenue,
            cost
        )
        SELECT
            t.transaction_id,
            t.customer_id,
            ti.product_id,
            t.transaction_date,
            ti.quantity,
            ti.line_total,
            (p.cost * ti.quantity)
        FROM production.transaction_items ti
        JOIN production.transactions t
            ON ti.transaction_id = t.transaction_id
        JOIN production.products p
            ON ti.product_id = p.product_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.fact_sales f
            WHERE f.transaction_id = t.transaction_id
              AND f.product_id = ti.product_id
        )
    """)

# --------------------------------------------------
# AGGREGATES (ALL FIXED)
# --------------------------------------------------
def load_aggregates(cur):
    print("➡ Loading aggregates")

    # DAILY SALES
    cur.execute("DELETE FROM warehouse.agg_daily_sales")
    cur.execute("""
        INSERT INTO warehouse.agg_daily_sales (
            date_key,
            total_transactions,
            total_revenue,
            total_profit,
            unique_customers
        )
        SELECT
            TO_CHAR(date, 'YYYYMMDD')::INTEGER,
            COUNT(DISTINCT transaction_id),
            SUM(revenue),
            SUM(revenue - cost),
            COUNT(DISTINCT customer_id)
        FROM warehouse.fact_sales
        GROUP BY date
    """)

    

        # PRODUCT PERFORMANCE (SUBQUERY FIX)
    # PRODUCT PERFORMANCE (ROW_NUMBER FIX)
    cur.execute("DELETE FROM warehouse.agg_product_performance")
    cur.execute("""
        INSERT INTO warehouse.agg_product_performance (
            product_key,
            total_quantity_sold,
            total_revenue,
            total_profit,
            avg_discount_percentage
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
            SUM(quantity),
            SUM(revenue),
            SUM(revenue - cost),
            0
        FROM warehouse.fact_sales
        GROUP BY product_id
    """)


    # CUSTOMER METRICS (ROW_NUMBER FIX)
    cur.execute("DELETE FROM warehouse.agg_customer_metrics")
    cur.execute("""
        INSERT INTO warehouse.agg_customer_metrics (
            customer_key,
            total_transactions,
            total_spent,
            avg_order_value,
            last_purchase_date
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
            COUNT(DISTINCT transaction_id),
            SUM(revenue),
            AVG(revenue),
            MAX(date)
        FROM warehouse.fact_sales
        GROUP BY customer_id
    """)

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    conn = get_connection()
    cur = conn.cursor()
    conn.autocommit = False

    try:
        load_dim_date(cur)
        load_dim_payment_method(cur)
        load_dim_customers(cur)
        load_dim_products(cur)
        load_fact_sales(cur)
        load_aggregates(cur)

        conn.commit()
        print("✅ Warehouse load completed successfully")

    except Exception as e:
        conn.rollback()
        print("❌ Warehouse load failed:", e)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
