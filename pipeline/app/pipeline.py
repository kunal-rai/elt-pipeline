# pipeline/app/pipeline.py
"""
PySpark-based pipeline:
- Reads CSV from disk into Spark DataFrame
- Writes raw to staging via JDBC
- Reads staging via JDBC, applies defaults and PII anonymisation, writes to report via JDBC
- Writes a single-row metrics entry into report.pipeline_runs
"""

import time
import os
import hmac
import hashlib
import json
import sys
from typing import List

from sqlalchemy.exc import SQLAlchemyError

from db_utils import ensure_schemas
from config import CSV_PATH, PII_SALT, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from db_utils import engine

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, lit, when
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Defaults (same meaning as pandas version)
DEFAULTS = {
    'tenure': 0,
    'MonthlyCharges': 0.0,
    'TotalCharges': 0.0,
    'gender': 'Unknown',
}

# PII columns to anonymise (dataset-agnostic). Keep 'customerID' / 'customerid' mapping in mind.
PII_COLUMNS = ['customerID', 'customerid']

# JDBC driver path (downloaded in Dockerfile)
JDBC_DRIVER_PATH = "/opt/jdbc/postgresql.jar"

def _build_jdbc_url():
    # Spark JDBC expects jdbc:postgresql://host:port/db
    host = POSTGRES_HOST or os.getenv('POSTGRES_HOST', 'postgres')
    port = POSTGRES_PORT or os.getenv('POSTGRES_PORT', '5432')
    db = POSTGRES_DB or os.getenv('POSTGRES_DB', 'etl_db')
    return f"jdbc:postgresql://{host}:{port}/{db}"

def _create_spark(app_name="elt_pipeline"):
    """
    Create a local SparkSession configured to use the Postgres JDBC driver jar.
    """
    jdbc_jar = JDBC_DRIVER_PATH
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.jars", jdbc_jar) \
        .config("spark.driver.extraClassPath", jdbc_jar) \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]")  # use all cores available
    spark = builder.getOrCreate()
    return spark

def _hmac_hash_py(val: str) -> str:
    if val is None:
        return None
    # handle NaNs passed as float
    try:
        if isinstance(val, float):
            # check NaN
            import math
            if math.isnan(val):
                return None
    except Exception:
        pass
    s = str(val).encode('utf-8')
    salt = (PII_SALT or os.getenv('PII_SALT', 'my_super_secret_salt')).encode('utf-8')
    return hmac.new(salt, s, hashlib.sha256).hexdigest()

# register udf
_hmac_hash_udf = udf(_hmac_hash_py, StringType())

def _safe_write_jdbc(df: DataFrame, table: str, mode: str = "overwrite"):
    """
    Write a Spark DataFrame to Postgres via JDBC. Retries a few times if DB not ready.
    """
    jdbc_url = _build_jdbc_url()
    user = POSTGRES_USER or os.getenv('POSTGRES_USER', 'etl_user')
    password = POSTGRES_PASSWORD or os.getenv('POSTGRES_PASSWORD', 'etl_password')
    props = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # Try a few times to avoid race with DB startup
    attempts = 0
    while attempts < 5:
        try:
            df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table) \
                .option("user", props["user"]) \
                .option("password", props["password"]) \
                .option("driver", props["driver"]) \
                .mode(mode) \
                .save()
            return
        except Exception as e:
            attempts += 1
            wait = 2 ** attempts
            print(f"JDBC write attempt {attempts} failed for table {table}: {e} — retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    # If we reach here, raise final exception
    raise RuntimeError(f"Failed to write to JDBC table {table} after {attempts} attempts")

def ingest(spark: SparkSession) -> int:
    """
    Read CSV from CSV_PATH into Spark, write raw to staging.customers
    Returns number of rows read.
    """
    print(f"Ingesting from {CSV_PATH}")
    # read with header and infer schema (you may tune inferSchema=False for performance)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)
    rows = df.count()
    print(f"Read {rows} rows")

    # ensure schemas exist (uses db_utils via SQLAlchemy)
    try:
        ensure_schemas()
    except SQLAlchemyError as e:
        print("Error ensuring schemas (SQLAlchemy):", e)
        # continue and let JDBC writes handle retries
    # Write to staging.customers
    _safe_write_jdbc(df, "staging.customers", mode="overwrite")
    return rows

def transform(spark: SparkSession) -> int:
    """
    Read staging.customers via JDBC into Spark, apply defaults and anonymisation,
    then write to report.customers
    """
    jdbc_url = _build_jdbc_url()
    user = POSTGRES_USER or os.getenv('POSTGRES_USER', 'etl_user')
    password = POSTGRES_PASSWORD or os.getenv('POSTGRES_PASSWORD', 'etl_password')

    # Read staging.customers via JDBC
    read_attempts = 0
    while read_attempts < 5:
        try:
            staging_df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "staging.customers") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            break
        except Exception as e:
            read_attempts += 1
            wait = 2 ** read_attempts
            print(f"JDBC read attempt {read_attempts} failed: {e} — retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    else:
        raise RuntimeError("Failed to read staging.customers via JDBC")

    df = staging_df

    # Apply defaults using na.fill for strings/numerics where possible
    # NOTE: Spark's na.fill accepts dict mapping column -> value
    fill_map = {}
    for k, v in DEFAULTS.items():
        if k in df.columns:
            fill_map[k] = v
    if fill_map:
        df = df.na.fill(fill_map)

    # Ensure numeric columns are coerced to numeric types and filled
    for numeric_col in ['MonthlyCharges', 'TotalCharges', 'tenure']:
        if numeric_col in df.columns:
            try:
                df = df.withColumn(numeric_col, col(numeric_col).cast(DoubleType()))
                df = df.na.fill({numeric_col: float(DEFAULTS.get(numeric_col, 0))})
            except Exception:
                # fallback: leave as-is
                pass

    # Apply anonymisation for PII columns
    for c in PII_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, _hmac_hash_udf(col(c)))

    # Write to report.customers
    _safe_write_jdbc(df, "report.customers", mode="overwrite")
    rows = df.count()
    return rows

def write_metrics(rows_in: int, rows_out: int, duration: float, success: bool = True):
    """
    Write a single-row metrics entry to report.pipeline_runs using Spark's JDBC write.
    """
    # Build a SparkSession only if not provided
    spark = SparkSession.builder.getOrCreate()

    data = [(rows_in, rows_out, float(duration), success)]
    schema = ["rows_ingested", "rows_transformed", "duration_seconds", "success"]
    metrics_df = spark.createDataFrame(data, schema)
    # Add run_ts default in DB (report.pipeline_runs has default now())
    # We'll insert with mode='append' to add new row
    jdbc_url = _build_jdbc_url()
    user = POSTGRES_USER or os.getenv('POSTGRES_USER', 'etl_user')
    password = POSTGRES_PASSWORD or os.getenv('POSTGRES_PASSWORD', 'etl_password')
    attempts = 0
    while attempts < 5:
        try:
            metrics_df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "report.pipeline_runs") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            return
        except Exception as e:
            attempts += 1
            wait = 2 ** attempts
            print(f"Metrics write attempt {attempts} failed: {e} — retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError("Failed to write pipeline metrics after retries")

def run_pipeline():
    """
    Main entrypoint: create Spark, run ingestion + transform, write metrics
    """
    print('--- Pipeline run starting (PySpark) ---')
    t0 = time.time()
    spark = _create_spark()
    try:
        rows_in = ingest(spark)
        rows_out = transform(spark)
        duration = time.time() - t0
        write_metrics(rows_in, rows_out, duration, success=True)
        print(f'Pipeline success: ingested={rows_in} transformed={rows_out} duration={duration:.2f}s')
    except Exception as e:
        duration = time.time() - t0
        try:
            write_metrics(0, 0, duration, success=False)
        except Exception as inner:
            print("Failed to write failed-run metrics:", inner, file=sys.stderr)
        print('Pipeline failed:', e, file=sys.stderr)
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass
