import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://admin:password@localhost:5432/ecommerce_db"
)


def test_dimension_tables_exist():
    tables = pd.read_sql("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'warehouse'
    """, engine)["table_name"].tolist()

    for t in ["dim_customers", "dim_products", "fact_sales"]:
        assert t in tables


def test_surrogate_keys_used():
    df = pd.read_sql(
        "SELECT customer_key FROM warehouse.dim_customers LIMIT 5",
        engine
    )

    assert df["customer_key"].dtype.kind in "iu"


def test_fact_grain():
    fact = pd.read_sql(
        "SELECT COUNT(*) c FROM warehouse.fact_sales",
        engine
    )

    items = pd.read_sql(
        "SELECT COUNT(*) c FROM production.transaction_items",
        engine
    )

    # fact table can be <= items (filters, late data)
    assert fact["c"][0] <= items["c"][0]
