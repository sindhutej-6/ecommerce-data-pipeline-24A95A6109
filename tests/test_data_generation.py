import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("data/raw")

CUSTOMERS = DATA_DIR / "customers.csv"
PRODUCTS = DATA_DIR / "products.csv"
TRANSACTIONS = DATA_DIR / "transactions.csv"
ITEMS = DATA_DIR / "transaction_items.csv"


def test_files_exist():
    assert CUSTOMERS.exists()
    assert PRODUCTS.exists()
    assert TRANSACTIONS.exists()
    assert ITEMS.exists()


def test_required_columns():
    customers = pd.read_csv(CUSTOMERS)
    assert {"customer_id", "email", "registration_date"}.issubset(customers.columns)


def test_no_null_customer_id():
    customers = pd.read_csv(CUSTOMERS)
    assert customers["customer_id"].isnull().sum() == 0


def test_referential_integrity():
    customers = pd.read_csv(CUSTOMERS)
    txns = pd.read_csv(TRANSACTIONS)

    assert set(txns["customer_id"]).issubset(
        set(customers["customer_id"])
    )


def test_line_total_calculation():
    items = pd.read_csv(ITEMS)

    calculated = items["quantity"] * items["unit_price"]

    # allow rounding / discount tolerance
    assert np.allclose(
        items["line_total"],
        calculated,
        atol=0.01
    )
