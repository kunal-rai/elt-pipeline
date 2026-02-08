# pipeline/app/pipeline.py
"""
PySpark ELT pipeline for HG Insights assignment.

Flow:
1. Read CSV into Spark
2. Write raw data to Postgres staging schema
3. Transform data:
   - Apply defaults for missing values
   - Anonymise PII (CustomerID)
4. Write transformed data to reporting schema
5. Record pipeline run metrics

Author: <Your Name>
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, lit
from pyspark.sql.types import DoubleType, IntegerType

from sqlalchemy.exc import SQLAlchemyError

from db_utils import ensure_schemas
from config import (
    CSV_PATH,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    PII_SALT,
)

JDBC_DRIVER_PATH = "/opt/jdbc/postgresql.jar"


# ----------------------------
# Configuration (dataset-aware)
# ----------------------------

# Defaults applied ONLY to known columns
DEFAULTS = {
    "Age": 0,
    "Tenure": 0,
    "MonthlyCharges": 0.0,
    "TotalCharges": 0.0,
    "Gender": "Unknown",
    "InternetService": "Unknown",
    "TechSupport": "Unknown",
}

# Only CustomerID is treated as PII for this dataset
PII_COLUMN = "CustomerID"


# ----------------------------
# Helpers
# ----------------------------

def _jdbc_url():
    host = POSTGRES_HOST or "postgres"
    port = POSTGRES_PORT or "5432"
    db = POSTGRES_DB or "etl_db"
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _spark_session():
    return (
        SparkSession.builder
        .appName("elt_pipeline")
        .master("local[*]")
        .config("spark.jars", JDBC_DRIVER_PATH)
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _jdbc_write(df, table, mode="overwrite"):
    url = _jdbc_url()
    user = POSTGRES_USER or "etl_user"
    password = POSTGRES_PASSWORD or "etl_password"

    for attempt in range(1, 6):
        try:
            df.write.format("jdbc") \
                .option("url", url) \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            return
        except Exception as e:
            wait = 2 ** attempt
            print(f"JDBC write failed (attempt {attempt}): {e}. Retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)

    raise RuntimeError(f"Failed to write table {table}")


# ----------------------------
# Pipeline steps
# ----------------------------

def ingest(spark):
    """
    Read CSV and write raw data to staging.customers
    """
    print(f"Reading CSV from {CSV_PATH}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)
    row_count = df.count()

    try:
        ensure_schemas()
    except SQLAlchemyError as e:
        print("Schema creation warning:", e, file=sys.stderr)

    _jdbc_write(df, "staging.customers", mode="overwrite")
    return row_count


def transform(spark):
    """
    Apply defaults and PII anonymisation, write to report.customers
    """
    url = _jdbc_url()
    user = POSTGRES_USER or "etl_user"
    password = POSTGRES_PASSWORD or "etl_password"

    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "staging.customers")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Apply defaults
    for col_name, default in DEFAULTS.items():
        if col_name in df.columns:
            df = df.na.fill({col_name: default})

    # Enforce numeric types
    if "Age" in df.columns:
        df = df.withColumn("Age", col("Age").cast(IntegerType()))
    if "Tenure" in df.columns:
        df = df.withColumn("Tenure", col("Tenure").cast(IntegerType()))
    if "MonthlyCharges" in df.columns:
        df = df.withColumn("MonthlyCharges", col("MonthlyCharges").cast(DoubleType()))
    if "TotalCharges" in df.columns:
        df = df.withColumn("TotalCharges", col("TotalCharges").cast(DoubleType()))

    # Anonymise PII
    if PII_COLUMN in df.columns:
        salt = PII_SALT or "hg_insights_salt"
        df = df.withColumn(
            PII_COLUMN,
            sha2(concat_ws("", col(PII_COLUMN).cast("string"), lit(salt)), 256)
        )

    _jdbc_write(df, "report.customers", mode="overwrite")
    return df.count()


def write_metrics(rows_in, rows_out, duration, success=True):
    spark = SparkSession.builder.getOrCreate()
    data = [(rows_in, rows_out, duration, success)]
    cols = ["rows_ingested", "rows_transformed", "duration_seconds", "success"]
    df = spark.createDataFrame(data, cols)
    _jdbc_write(df, "report.pipeline_runs", mode="append")


def run_pipeline():
    print("Starting ELT pipeline")
    start = time.time()
    spark = _spark_session()

    try:
        rows_in = ingest(spark)
        rows_out = transform(spark)
        duration = time.time() - start
        write_metrics(rows_in, rows_out, duration, True)
        print(f"Pipeline succeeded | ingested={rows_in} transformed={rows_out}")
    except Exception as e:
        duration = time.time() - start
        write_metrics(0, 0, duration, False)
        print("Pipeline failed:", e, file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
