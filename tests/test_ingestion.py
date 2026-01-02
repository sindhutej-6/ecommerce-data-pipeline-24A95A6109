import pandas as pd
from sqlalchemy import create_engine, inspect, text

engine = create_engine(
    "postgresql+psycopg2://admin:password@localhost:5432/ecommerce_db"
)


def test_database_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1


def test_staging_tables_exist():
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="staging")

    expected = {
        "customers",
        "products",
        "transactions",
        "transaction_items"
    }

    assert expected.issubset(set(tables))


def test_staging_tables_have_data():
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM staging.customers")
        ).scalar()

    assert count > 0


def test_loaded_at_populated():
    with engine.connect() as conn:
        nulls = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM staging.customers
                WHERE loaded_at IS NULL
            """)
        ).scalar()

    assert nulls == 0
