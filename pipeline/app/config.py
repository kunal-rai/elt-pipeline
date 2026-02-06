import os
from dotenv import load_dotenv
load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER', 'etl_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'etl_password')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'etl_db')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

CSV_PATH = os.getenv('CSV_PATH', '/data/input.csv')
INTERVAL_MINUTES = int(os.getenv('INTERVAL_MINUTES', '60'))

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    DATABASE_URL = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

PII_SALT = os.getenv('PII_SALT', 'my_super_secret_salt')
