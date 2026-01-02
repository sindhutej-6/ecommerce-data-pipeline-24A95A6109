import pandas as pd
from sqlalchemy import create_engine
import os

engine = create_engine(
    "postgresql+psycopg2://admin:password@localhost:5432/ecommerce_db"
)


def test_production_tables_populated():
    df = pd.read_sql(
        "SELECT COUNT(*) c FROM production.transactions",
        engine
    )
    assert df["c"][0] > 0


def test_no_orphan_records():
    df = pd.read_sql("""
        SELECT COUNT(*) c
        FROM production.transactions t
        LEFT JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """, engine)

    assert df["c"][0] == 0


def test_business_rules_applied():
    df = pd.read_sql("""
        SELECT revenue, cost
        FROM production.transactions
    """, engine)

    # revenue should never be less than cost
    assert (df["revenue"] >= df["cost"]).all()
