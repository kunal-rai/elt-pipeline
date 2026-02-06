from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config import DATABASE_URL
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def ensure_schemas():
    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS staging'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS report'))
            conn.execute(text('CREATE TABLE IF NOT EXISTS report.pipeline_runs ('
                          'run_id serial primary key, '
                          'run_ts timestamptz default now(), '
                          'rows_ingested int, rows_transformed int, duration_seconds numeric, success boolean)'))
    except SQLAlchemyError as e:
        print("Error ensuring schemas:", e)
        raise
