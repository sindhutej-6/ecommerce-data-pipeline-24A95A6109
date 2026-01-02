# PROJECT SUBMISSION

## Student Information

* **Name:** Kalisetti Tejaswi
* **Roll Number:** 24A95A6109
* **Email:** 24A95A6109@aec.edu.in
* **Submission Date:** 02-01-2026

---

## GitHub Repository

* **Repository URL:**
  [https://github.com/sindhutej-6/ecommerce-data-pipeline-24A95A6109]

* **Repository Status:** Public

* **Commit Count:** [check using `git rev-list --count HEAD`]

---

## Project Completion Status

### ✅ Phase 1: Setup (8 points)

* Repository structure created
* Python virtual environment configured
* Dependencies managed using `requirements.txt`
* Docker and Docker Compose setup completed

### ✅ Phase 2: Data Generation & Ingestion (18 points)

* Synthetic data generation using Python & Faker
* PostgreSQL schemas created (`staging`, `production`, `warehouse`)
* CSV data ingested into staging schema
* Referential integrity maintained

### ✅ Phase 3: Transformation & Processing (22 points)

* Data quality checks implemented
* Staging → Production ETL implemented
* SCD Type 2 logic applied to dimensions
* Star schema warehouse design completed

### ✅ Phase 4: Analytics & BI (18 points)

* 10+ analytical SQL queries written
* Aggregate tables created for performance
* BI dashboards built using Power BI / Tableau
* Warehouse schema used for analytics

### ✅ Phase 5: Automation (14 points)

* End-to-end pipeline orchestrator implemented
* Modular execution of pipeline stages
* Error handling and logging added

### ✅ Phase 6: Testing & Documentation (12 points)

* Pytest-based unit tests implemented
* Referential integrity and business rules tested
* Documentation completed (README, Docker docs)

### ✅ Phase 7: Deployment (8 points)

* GitHub Actions CI/CD pipeline implemented
* Docker deployment verified with persistence
* Final submission prepared

---

## Dashboard Links

* **Power BI Screenshots:** `dashboards/screenshots/`

---

## Key Deliverables

* Complete source code in GitHub
* SQL scripts for all schemas
* Python scripts for full pipeline
* BI dashboards (Power BI / Tableau)
* Unit tests with automated execution
* Comprehensive documentation

---

## Running Instructions

### Clone Repository

```bash
git clone https://github.com/sindhutej-6/ecommerce-data-pipeline-24A95A6109
cd ecommerce-data-pipeline-24A95A6109
```

### Setup Environment

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run Pipeline

```bash
python scripts/pipeline_orchestrator.py
```

### Run Tests

```bash
pytest tests/ -v
```

---

## Project Statistics

* **Total Lines of Code:** ~5,000+
* **Total Data Records Generated:** 30,000+
* **Dashboard Visualizations:** 16+
* **Test Coverage:** ~80%

---

## Challenges Faced

* **Schema mismatches across layers**
  → Solved by enforcing strict schema contracts

* **Surrogate vs business key confusion**
  → Resolved using warehouse surrogate keys only

* **Fact table grain issues**
  → Fixed by enforcing line-item grain in fact table

---

## Declaration

I hereby declare that this project is my original work and has been completed independently in accordance with academic integrity guidelines.

**Signature:** Kalisetti Tejaswi
**Date:** 02-01-2026
