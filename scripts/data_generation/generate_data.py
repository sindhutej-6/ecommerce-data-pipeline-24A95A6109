import os
import json
import yaml
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

fake = Faker()

# ================= CONFIG =================

CONFIG_PATH = "config/config.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

CUSTOMER_COUNT = config["data_generation"]["customers"]
PRODUCT_COUNT = config["data_generation"]["products"]
TRANSACTION_COUNT = config["data_generation"]["transactions"]

DATE_START = datetime.strptime(
    config["data_generation"]["transaction_date_range"]["start_date"], "%Y-%m-%d"
)
DATE_END = datetime.strptime(
    config["data_generation"]["transaction_date_range"]["end_date"], "%Y-%m-%d"
)

RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ================= CUSTOMERS =================

def generate_customers(n):
    customers = []
    used_emails = set()
    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]

    for i in range(1, n + 1):
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(age_groups)
        })

    return pd.DataFrame(customers)


customers_df = generate_customers(CUSTOMER_COUNT)
customers_df.to_csv(os.path.join(RAW_DATA_DIR, "customers.csv"), index=False)
print(f"customers.csv generated with {len(customers_df)} records")

# ================= PRODUCTS =================

def generate_products(n):
    categories = {
        "Electronics": ["Mobile", "Laptop", "Headphones", "Camera"],
        "Clothing": ["Shirt", "Jeans", "Jacket", "T-Shirt"],
        "Home & Kitchen": ["Mixer", "Cookware", "Vacuum", "Toaster"],
        "Books": ["Fiction", "Non-Fiction", "Comics", "Education"],
        "Sports": ["Cricket Bat", "Football", "Tennis Racket", "Yoga Mat"],
        "Beauty": ["Skincare", "Makeup", "Perfume", "Haircare"]
    }

    products = []

    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])

        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": f"{fake.word().title()} {sub_category}",
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1,50):03d}"
        })

    return pd.DataFrame(products)


products_df = generate_products(PRODUCT_COUNT)
products_df.to_csv(os.path.join(RAW_DATA_DIR, "products.csv"), index=False)
print(f"products.csv generated with {len(products_df)} records")

# ================= TRANSACTIONS =================

PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def generate_transactions(customers_df, n):
    transactions = []

    for i in range(1, n + 1):
        txn_date = random_date(DATE_START, DATE_END)

        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customers_df["customer_id"].tolist()),
            "transaction_date": txn_date.date(),
            "transaction_time": fake.time(),
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0
        })

    return pd.DataFrame(transactions)

# ================= TRANSACTION ITEMS =================

def generate_transaction_items(transactions_df, products_df):
    items = []
    item_id = 1

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        chosen_products = products_df.sample(num_items)

        txn_total = 0.0

        for _, prod in chosen_products.iterrows():
            quantity = random.randint(1, 3)
            unit_price = prod["price"]

            line_total = round(quantity * unit_price, 2)

            items.append({
                "item_id": f"ITEM{item_id:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total
            })

            txn_total += line_total
            item_id += 1

        transactions_df.loc[
            transactions_df["transaction_id"] == txn["transaction_id"],
            "total_amount"
        ] = round(txn_total, 2)

    return transactions_df, pd.DataFrame(items)

# ================= SAVE FILES =================

transactions_df = generate_transactions(customers_df, TRANSACTION_COUNT)
transactions_df, transaction_items_df = generate_transaction_items(
    transactions_df, products_df
)

transactions_df.to_csv(os.path.join(RAW_DATA_DIR, "transactions.csv"), index=False)
transaction_items_df.to_csv(os.path.join(RAW_DATA_DIR, "transaction_items.csv"), index=False)

print(f"transactions.csv generated with {len(transactions_df)} records")
print(f"transaction_items.csv generated with {len(transaction_items_df)} records")

# ================= METADATA =================

def validate_referential_integrity(customers_df, products_df, transactions_df, items_df):
    issues = 0

    issues += (~transactions_df["customer_id"].isin(customers_df["customer_id"])).sum()
    issues += (~items_df["product_id"].isin(products_df["product_id"])).sum()
    issues += (~items_df["transaction_id"].isin(transactions_df["transaction_id"])).sum()

    score = 100 if issues == 0 else max(0, 100 - issues)

    return {
        "orphan_records": int(issues),
        "constraint_violations": 0,
        "data_quality_score": score
    }

metadata = {
    "generation_timestamp": datetime.now(timezone.utc).isoformat(),
    "record_counts": {
        "customers": len(customers_df),
        "products": len(products_df),
        "transactions": len(transactions_df),
        "transaction_items": len(transaction_items_df)
    },
    "transaction_date_range": {
        "start": str(transactions_df["transaction_date"].min()),
        "end": str(transactions_df["transaction_date"].max())
    },
    "data_quality": validate_referential_integrity(
        customers_df, products_df, transactions_df, transaction_items_df
    )
}

with open(os.path.join(RAW_DATA_DIR, "generation_metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)

print("generation_metadata.json created")
