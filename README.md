# ELT Pipeline — Airflow Orchestrated (Runnable on Laptop)

This repository contains a **fully containerised ELT pipeline** orchestrated using **Apache Airflow**.
The solution is designed in such a way, we just need to **clone the repo and run everything locally using Docker only**.

No cloud setup is required.

---

## What This Pipeline Does

1. Reads a CSV file
2. Loads raw data into a **staging schema** in Postgres
3. Transforms data using **PySpark**:
   - Fills missing values using defaults
   - Anonymises PII fields
4. Loads transformed data into a **reporting schema**
5. Tracks pipeline execution metrics
6. Orchestrates execution using **Apache Airflow**
7. Visualises results using **Metabase**

---

## Prerequisites (Mandatory)

- Docker Desktop (running)
- Git

That’s it.

---

## Step 1 — Clone the Repository

git clone <YOUR_GITHUB_REPO_URL>
cd <REPO_NAME>
```

---

## Step 2 — Prepare Configuration

Create the environment file (defaults are sufficient):

cp .env.example .env

Place the input dataset at:

./data/input.csv


---

## Step 3 — Start the Airflow-Based Pipeline

Always start from a clean state:

docker-compose down -v

Start the full stack:

docker-compose -f docker-compose.airflow.yml up --build -d

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
Password: 5pQ9nnZhVEVXr4A7

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


SELECT COUNT(*) FROM staging.customers;
SELECT COUNT(*) FROM report.customers;
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
