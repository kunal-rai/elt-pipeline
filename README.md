# ELT Pipeline — Airflow Orchestrated (demo-ready)

This repository contains a **fully containerised ELT pipeline** orchestrated with **Apache Airflow**.
It is designed to run locally using **Docker only** (no cloud required) and demonstrates:

- CSV ingestion → staging (Postgres)
- PySpark-based transformation (defaults + PII anonymisation)
- Load into reporting schema for analytics
- Orchestration via Airflow (hourly configurable)
- Observability via Metabase dashboards
- All services defined with Docker Compose

---

## Prerequisites (mandatory)
- Docker Desktop (running)
- Docker Compose (bundled with Docker Desktop)
- Git  
Recommended: allocate **~8 GB** RAM to Docker for smooth Airflow + Postgres + Spark runs.

---

## Quick overview
- Input CSV path: `./data/input.csv`
- Staging table: `staging.customers`
- Reporting table: `report.customers`
- Pipeline entrypoint: `app/run_once.py`
- Orchestration Airflow DAG: `elt_pipeline_dag` (http://localhost:8080/)
- Reporting UI: Metabase (http://localhost:3000)

---

## 1) Clone repository

git clone <YOUR_GITHUB_REPO_URL>
cd <REPO_NAME>

---

## Step 2 — Prepare Configuration

Create the environment file (defaults are sufficient):

cp .env.example .env

Place the input dataset at:

./data/input.csv


---

## Step 3 — Start the Airflow-Based Pipeline

Always start from a clean state:

docker-compose -f docker-compose.airflow.yml down -v

Start the full stack:

docker-compose -f docker-compose.airflow.yml up --build -d

If you edit pipeline code or Dockerfile, rebuild the pipeline image explicitly:

docker-compose -f docker-compose.airflow.yml build --no-cache pipeline
docker-compose -f docker-compose.airflow.yml up -d

This starts:
- Postgres
- Pipeline container
- Airflow (scheduler + UI)
- Metabase

---

## Step 4 — Access Airflow

Open Airflow UI:

http://localhost:8080

Login credentials:

Username: admin  
Password: admin@123 

To confirm Airflow started correctly:

docker-compose -f docker-compose.airflow.yml logs airflow

---

## Step 5 — Run the ELT Pipeline via Airflow

1. In Airflow UI, locate the DAG named: **elt_pipeline_dag**
2. Toggle the DAG **ON**
3. Click **▶ Trigger DAG**

Each DAG run executes the ELT pipeline once.

Internally, Airflow triggers execution inside the pipeline container using:

docker exec elt-pipeline-pipeline-1 python app/run_once.py

---

## Step 6 — Verify Successful Execution

### Check Airflow Task Status

- DAG run should be **green (SUCCESS)**
- Task: `run_elt_pipeline`

---

### Check Pipeline Logs

docker-compose -f docker-compose.airflow.yml logs pipeline

Expected output includes:

Pipeline success: ingested=XXXX transformed=XXXX

---

### Verify Data in Postgres

docker-compose -f docker-compose.airflow.yml exec postgres \
psql -U etl_user -d etl_db


SELECT COUNT(*) FROM staging.customers; # source table
SELECT COUNT(*) FROM report.customers;  # target table
SELECT * FROM report.pipeline_runs ORDER BY run_ts DESC LIMIT 5;

Exit with:

\q

---

## Step 7 — View Data in Metabase

Open Metabase:

http://localhost:3000

For one time set-up, add Postgres as a data source:

- Host: postgres
- Database: etl_db
- Username: etl_user
- Password: etl_password

---

## Stop All Services

docker-compose -f docker-compose.airflow.yml down -v

---

## Common Commands

# Start services
docker-compose up -d

# Stop services and remove all data
docker-compose down -v

# View pipeline logs
docker-compose logs pipeline

----

## Summary

This repository demonstrates a **clean, reproducible, Airflow-orchestrated ELT pipeline** using open-source tools.
