import os
import sys
import logging
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.settings import raw_data, date, log_dir

def load_env():
    load_dotenv()
    required = {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")
    return required

def setup_logging():
    logging.basicConfig(
        filename=os.path.join(log_dir, 'upload_to_postgres.log'),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

def upload_to_postgres(csv_path: str, table_name: str, day: str):
    config = load_env()
    setup_logging()

    os.environ["AWS_ACCESS_KEY_ID"] = config["MINIO_ROOT_USER"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["MINIO_ROOT_PASSWORD"]

    db = duckdb.connect()
    db.execute("INSTALL httpfs;")
    db.execute("LOAD httpfs;")
    db.execute("INSTALL postgres;")
    db.execute("LOAD postgres;")

    db.execute("SET s3_region='us-east-1';")
    db.execute(f"SET s3_endpoint='{config['MINIO_ENDPOINT']}';")
    db.execute("SET s3_url_style='path';")
    db.execute("SET s3_use_ssl=false;")

    logging.info(f"Reading from: {csv_path}")
    df = db.execute(f"SELECT * FROM '{csv_path}'").fetchdf()
    print(df.columns)
    print(df.head())

    pg_conn_str = (
        f"postgresql://{config['POSTGRES_USER']}:{config['POSTGRES_PASSWORD']}"
        f"@{config['POSTGRES_HOST']}:{config['POSTGRES_PORT']}/{config['POSTGRES_DB']}"
    )
    db.execute(f"ATTACH '{pg_conn_str}' AS postgres (TYPE postgres);")


    db.execute(f"""
        CREATE TABLE IF NOT EXISTS postgres.public.{table_name} (
            date DATE,
            base TEXT,
            currency TEXT,
            rate FLOAT
        );
    """)

    db.register("df_to_insert", df)
    db.execute(f"DELETE FROM postgres.public.{table_name} WHERE date = DATE '{day}';")
    db.execute(f"INSERT INTO postgres.public.{table_name} SELECT * FROM df_to_insert;")

    logging.info("✅ Upload to PostgreSQL completed.")

# Example usage:
upload_to_postgres("s3://exchange.rate/validated/exchange_rate_2025-06-12.csv/part-00000-957aa075-171f-4332-a7bc-30dc68ba5616-c000.csv", "exchange_rate", "2025-06-12")
# 'exchange.rate/validated/exchange_rate_2025-06-12.csv/part-00000-957aa075-171f-4332-a7bc-30dc68ba5616-c000.csv'