
# E-Commerce Data Pipeline & Analytics Platform

**Student Name:** Kalisetti Tejaswi
**Roll Number:** 24A95A6109
**Submission Date:** 02-01-2026

---

## 1. Project Overview

This project implements an **end-to-end batch-based e-commerce data analytics pipeline** using modern data engineering best practices.
The pipeline processes raw transactional data into analytics-ready datasets and supports BI dashboarding.

---

## 2. Project Architecture

### Data Flow Diagram

```
Raw CSV Data
   |
   v
Staging Schema (PostgreSQL)
   |
   v
Production Schema (Cleaned & Normalized)
   |
   v
Warehouse Schema (Star Schema)
   |
   v
Analytics & Aggregates
   |
   v
BI Dashboards (Tableau / Power BI)
```

The architecture ensures:

* Data quality
* Clear schema separation
* High analytical performance

---

## 3. Technology Stack

| Layer              | Technology                            |
| ------------------ | ------------------------------------- |
| Data Generation    | Python (Faker)                        |
| Database           | PostgreSQL 15                         |
| ETL / ELT          | Python (Pandas, SQLAlchemy, psycopg2) |
| Orchestration      | Python-based pipeline orchestrator    |
| Analytics          | SQL (PostgreSQL)                      |
| BI & Visualization | Tableau Public / Power BI Desktop     |
| Containerization   | Docker & Docker Compose               |
| Testing            | Pytest                                |

---

## 4. Project Structure

```
ecommerce-data-pipeline/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── analytics/
│
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── pipeline_orchestrator.py
│   └── utils/
│
├── dashboards/
│   ├── powerbi/
│   ├── tableau/
│   └── screenshots/
│
├── docs/
│   ├── architecture.md
│   └── dashboard_guide.md
│
├── tests/
├── docker-compose.yml
├── requirements.txt
├── run_tests.sh
└── README.md
```

---

## 5. Prerequisites

* Python 3.10+
* PostgreSQL 15+
* Docker & Docker Compose
* Git
* Tableau Public or Power BI Desktop (optional)

---

## 6. Installation Steps

### Clone Repository

```bash
git clone https://github.com/sindhutej-6/ecommerce-data-pipeline-24A95A6109
cd ecommerce-data-pipeline-24A95A6109
```

### Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## 7. Database Setup

### Start PostgreSQL using Docker

```bash
docker-compose up -d
```

### Create Schemas

```sql
CREATE SCHEMA staging;
CREATE SCHEMA production;
CREATE SCHEMA warehouse;
```

---

## 8. Running the Pipeline

### Full Pipeline Execution

```bash
python scripts/pipeline_orchestrator.py
```

### Run Individual Steps

```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

---

## 9. Running Tests

```bash
python run_tests.py
```

OR

```bash
pytest tests/ -v
```

---

## 10. Database Schemas

### Staging Schema

* `staging.customers`
* `staging.products`
* `staging.transactions`
* `staging.transaction_items`

### Production Schema

* `production.customers`
* `production.products`
* `production.transactions`
* `production.transaction_items`

### Warehouse Schema

* `warehouse.dim_customers`
* `warehouse.dim_products`
* `warehouse.dim_date`
* `warehouse.dim_payment_method`
* `warehouse.fact_sales`
* `warehouse.agg_daily_sales`
* `warehouse.agg_product_performance`
* `warehouse.agg_customer_metrics`

---

## 11. Dashboard Access

* **Power BI File:** `dashboards/powerbi/ecommerce_analytics.pbix`
* **Dashboard Screenshots:** `dashboards/screenshots/`

---

## 12. Key Insights from Analytics

* Top-performing category contributes the highest revenue
* Month-over-month revenue growth is consistent
* VIP customers generate a significant share of total revenue
* Urban regions outperform rural regions
* Digital payment methods dominate transaction volume

---

## 13. Challenges & Solutions

1. **Schema mismatches** → Implemented strict schema contracts
2. **Data quality issues** → Automated quality checks with Pytest
3. **Slow analytics queries** → Introduced aggregate tables
4. **SCD complexity** → Applied SCD Type 2 for dimensions

---

## 14. Future Enhancements

* Real-time streaming with Apache Kafka
* Cloud deployment (AWS / GCP / Azure)
* Machine learning for demand forecasting
* Monitoring and alerting

---

## 15. Contact

**Name:** Kalisetti Tejaswi
**Roll Number:** 24A95A6109
**Email:** 24A95A6109@aec.edu.in

---

