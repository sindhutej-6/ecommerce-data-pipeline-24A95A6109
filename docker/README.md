# Docker Deployment Guide

## E-Commerce Data Pipeline

This document explains how to deploy and verify the **E-Commerce Data Pipeline** using **Docker** and **Docker Compose**.

---

## 1️⃣ Prerequisites

Before starting, ensure the following are installed:

### Software Requirements

* **Docker**: v20.10+
* **Docker Compose**: v2.0+
* **Git**

Verify installation:

```bash
docker --version
docker compose version
```

### System Requirements

* Minimum **4 GB RAM**
* At least **5 GB free disk space**
* OS: **Windows / macOS / Linux**

---

## 2️⃣ Quick Start Guide

### Step 1: Clone Repository

```bash
git clone https://github.com/sindhutej-6/ecommerce-data-pipeline-24A95A6109
cd ecommerce-data-pipeline-24A95A6109
```

### Step 2: Build Docker Images

```bash
docker compose build
```

This builds:

* PostgreSQL database container
* Pipeline application container

### Step 3: Start Services

```bash
docker compose up -d
```

This starts:

* `postgres` service
* `pipeline` service (depends on database health)

### Step 4: Verify Services Are Running

```bash
docker compose ps
```

Expected output:

* `postgres` → **healthy**
* `pipeline` → **running**

### Step 5: Run Pipeline Inside Container

If the pipeline is not auto-started:

```bash
docker compose exec pipeline python scripts/pipeline_orchestrator.py
```

### Step 6: Access PostgreSQL Database

```bash
docker compose exec postgres psql -U postgres -d ecommerce_db
```

List schemas:

```sql
\dn
```

### Step 7: View Logs

```bash
docker compose logs
```

For a specific service:

```bash
docker compose logs postgres
docker compose logs pipeline
```

### Step 8: Stop Services

```bash
docker compose down
```

### Step 9: Clean Up (⚠️ Deletes Data)

```bash
docker compose down -v
docker system prune -f
```

---

## 3️⃣ Configuration Details

### Environment Variables

Configured via `docker-compose.yml`:

| Variable          | Description       |
| ----------------- | ----------------- |
| POSTGRES_DB       | Database name     |
| POSTGRES_USER     | Database user     |
| POSTGRES_PASSWORD | Database password |
| DB_HOST           | postgres          |
| DB_PORT           | 5432              |

### Volume Mounts

```yaml
volumes:
  - postgres_data:/var/lib/postgresql/data
```

✔ Ensures data persistence across container restarts

### Network Configuration

* Default Docker bridge network
* Services communicate using service names

```
pipeline → postgres
```

✔ No hardcoded IP addresses used

### Resource Limits (Optional)

```yaml
deploy:
  resources:
    limits:
      cpus: "1.0"
      memory: 512M
```

---

## 4️⃣ Data Persistence Verification

### Test Persistence

1. Start containers
2. Load data
3. Stop containers:

```bash
docker compose down
```

4. Restart:

```bash
docker compose up -d
```

✔ Database data remains intact
✔ Warehouse tables persist
✔ Volumes reused

---

## 5️⃣ Troubleshooting

### ❌ Port Already in Use

**Error:** `bind: address already in use`

**Solution:**

```bash
docker ps
docker stop <container_id>
```

OR change the port in `docker-compose.yml`

---

### ❌ Database Not Ready

**Error:** pipeline fails to connect

**Solution:**

* Ensure `depends_on` with health check exists
* PostgreSQL must show **healthy**

---

### ❌ Container Fails to Start

```bash
docker compose logs <service_name>
```

Fix missing environment variables or syntax errors.

---

### ❌ Permission Issues (Windows/Linux)

```bash
docker volume rm postgres_data
```

Restart containers.

---

### ❌ Network Issues

Ensure pipeline connects using:

```env
DB_HOST=postgres
```

❌ Do **NOT** use `localhost`

---

## 6️⃣ Docker Compose Best Practices Followed

✔ PostgreSQL runs as isolated service
✔ Health check using `pg_isready`
✔ `depends_on` with `condition: service_healthy`
✔ Named volumes for persistence
✔ Service-to-service communication via Docker network

---

## ✅ Deployment Verification Checklist

* Containers start successfully
* Database health check works
* Pipeline waits for database
* Data persists after restart
* Logs accessible
* Clean shutdown supported
