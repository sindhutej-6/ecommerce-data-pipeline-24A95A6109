# E-Commerce Data Pipeline Architecture

## Overview

This document describes the architecture of the **E-Commerce Data Analytics Platform**, designed to ingest, process, store, and visualize large-scale transactional data using a layered approach.

---

## System Components

### 1. Data Generation Layer

- Generates synthetic e-commerce datasets using **Python Faker**
- Output: CSV files
  - customers
  - products
  - transactions
  - transaction_items

Purpose: Simulate real-world e-commerce data for analytics and testing.

---

### 2. Data Ingestion Layer

- Loads raw CSV files into PostgreSQL **staging schema**
- Technology: Python + psycopg2
- Pattern: Batch ingestion

Purpose: Persist raw data with minimal transformation.

---

### 3. Data Storage Layer

#### Staging Schema
- Exact replica of CSV structure
- Minimal validation
- Temporary storage

#### Production Schema
- Cleaned and normalized (3NF)
- Foreign key constraints enforced
- Business rules applied

#### Warehouse Schema
- Star schema optimized for analytics
- Fact and dimension tables
- Pre-aggregated summary tables

---

### 4. Data Processing Layer

- Data quality checks (nulls, ranges, duplicates)
- Transformations and enrichment
- Slowly Changing Dimensions (SCD Type 2)
- Aggregate table generation

---

### 5. Data Serving Layer

- Analytical SQL queries
- CSV exports for BI tools
- Optimized aggregations

---

### 6. Visualization Layer

- Tableau Public / Power BI
- Interactive dashboards
- 16+ visualizations across 4 pages

---

### 7. Orchestration Layer

- Pipeline orchestrator
- Daily scheduler
- Monitoring and logging

---

## Data Models

### Staging Model

- Mirrors raw CSV structure
- No joins or constraints
- Designed for fast ingestion

### Production Model

- Normalized (3NF)
- Referential integrity
- Clean business entities

### Warehouse Model (Star Schema)

- Dimensions:
  - Customers
  - Products
  - Date
  - Payment Method
- Fact:
  - Sales
- Aggregates:
  - Daily sales
  - Product performance
  - Customer metrics

Optimized for analytical workloads.

---

## Technologies Used

- Python 3.12
- PostgreSQL 15
- Pandas
- SQLAlchemy
- psycopg2
- Docker
- Pytest
- Tableau / Power BI

---

## Deployment Architecture

- Dockerized PostgreSQL database
- Local Python execution environment
- Optional cloud-ready design

The modular architecture enables easy scaling, monitoring, and future enhancements.

